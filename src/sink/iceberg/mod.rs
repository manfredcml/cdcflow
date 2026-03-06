pub mod record_batch;
pub mod row_delta;
pub mod schema_builder;
pub mod schema_evolution;
pub mod type_mapping;

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use crate::config::{IcebergCatalogConfig, IcebergSinkConfig, SinkMode, SourceConnectionConfig};
use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp};
use crate::sink::Sink;

pub struct IcebergSink {
    config: IcebergSinkConfig,
    mode: SinkMode,
    /// Source DB connection config for schema inference (derived from top-level source config).
    source_connection: SourceConnectionConfig,
    catalog: Arc<dyn Catalog>,
    /// Cached table handles: (schema, table_name) → Table
    tables: HashMap<(String, String), iceberg::table::Table>,
    /// Cached Arrow schemas for RecordBatch conversion
    arrow_schemas: HashMap<(String, String), arrow_schema::Schema>,
    /// Replication mode: cached PK column names per table
    pk_columns: HashMap<(String, String), Vec<String>>,
    /// Replication mode: Arrow schema with only PK columns (for delete batches)
    pk_arrow_schemas: HashMap<(String, String), arrow_schema::Schema>,
    /// Replication mode: Iceberg schema ref per table (for EqualityDeleteWriterConfig)
    iceberg_schemas: HashMap<(String, String), Arc<iceberg::spec::Schema>>,
    /// Replication mode: PK field IDs per table
    pk_field_ids: HashMap<(String, String), Vec<i32>>,
}

impl IcebergSink {
    pub async fn new(
        config: IcebergSinkConfig,
        mode: SinkMode,
        source_connection: SourceConnectionConfig,
    ) -> Result<Self> {
        let catalog = Self::build_catalog(&config).await?;
        Ok(Self {
            config,
            mode,
            source_connection,
            catalog,
            tables: HashMap::new(),
            arrow_schemas: HashMap::new(),
            pk_columns: HashMap::new(),
            pk_arrow_schemas: HashMap::new(),
            iceberg_schemas: HashMap::new(),
            pk_field_ids: HashMap::new(),
        })
    }

    async fn build_catalog(config: &IcebergSinkConfig) -> Result<Arc<dyn Catalog>> {
        match &config.catalog {
            IcebergCatalogConfig::Rest(rest) => {
                let mut props = HashMap::new();
                props.insert("uri".to_string(), rest.uri.clone());
                if let Some(ref wh) = rest.warehouse {
                    props.insert("warehouse".to_string(), wh.clone());
                }
                props.extend(rest.properties.clone());

                let timeout = std::time::Duration::from_secs(rest.timeout_secs);
                let uri = rest.uri.clone();
                let catalog = tokio::time::timeout(timeout, async {
                    RestCatalogBuilder::default()
                        .load("rest", props)
                        .await
                        .map_err(|e| CdcError::Iceberg(format!("catalog: {e}")))
                })
                .await
                .map_err(|_| {
                    CdcError::Iceberg(format!(
                        "catalog connection timed out after {}s — check that the catalog URI is reachable: {}",
                        rest.timeout_secs, uri
                    ))
                })??;

                Ok(Arc::new(catalog))
            }
        }
    }

    /// Create an IcebergSink with a pre-built catalog (for testing).
    #[cfg(test)]
    pub(crate) fn with_catalog(
        config: IcebergSinkConfig,
        mode: SinkMode,
        source_connection: SourceConnectionConfig,
        catalog: Arc<dyn Catalog>,
    ) -> Self {
        Self {
            config,
            mode,
            source_connection,
            catalog,
            tables: HashMap::new(),
            arrow_schemas: HashMap::new(),
            pk_columns: HashMap::new(),
            pk_arrow_schemas: HashMap::new(),
            iceberg_schemas: HashMap::new(),
            pk_field_ids: HashMap::new(),
        }
    }

    /// Pre-populate table and schema caches for testing (bypasses ensure_table).
    #[cfg(test)]
    pub(crate) fn cache_table(
        &mut self,
        source_schema: &str,
        source_table: &str,
        table: iceberg::table::Table,
    ) -> crate::error::Result<()> {
        let key = (source_schema.to_string(), source_table.to_string());
        let arrow_schema =
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| CdcError::Iceberg(format!("arrow schema: {e}")))?;
        self.arrow_schemas.insert(key.clone(), arrow_schema);
        self.tables.insert(key, table);
        Ok(())
    }

    /// Get or create the Iceberg table for a given source table.
    async fn ensure_table(&mut self, source_schema: &str, source_table: &str) -> Result<()> {
        let key = (source_schema.to_string(), source_table.to_string());
        if self.tables.contains_key(&key) {
            return Ok(());
        }

        let iceberg_table_name = format!("{}{}", self.config.table_prefix, source_table);
        let ns = NamespaceIdent::from_strs(&self.config.namespace)
            .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
        let ident = TableIdent::new(ns.clone(), iceberg_table_name);

        // Try loading; if not found, auto-create
        let table = match self.catalog.load_table(&ident).await {
            Ok(t) => t,
            Err(_) => {
                tracing::info!(
                    schema = source_schema,
                    table = source_table,
                    "auto-creating Iceberg table"
                );

                let iceberg_schema = match &self.mode {
                    SinkMode::Cdc => {
                        // Flattened typed schema: 7 _cdc_* metadata + N source + N _old_ columns
                        let columns = crate::schema::discovery::fetch_columns(
                            &self.source_connection,
                            source_schema,
                            source_table,
                        )
                        .await?;
                        schema_builder::build_flattened_cdc_schema(&columns)?
                    }
                    SinkMode::Replication => {
                        let columns = crate::schema::discovery::fetch_columns(
                            &self.source_connection,
                            source_schema,
                            source_table,
                        )
                        .await?;
                        let pk_cols = crate::schema::discovery::fetch_primary_keys(
                            &self.source_connection,
                            source_schema,
                            source_table,
                        )
                        .await?;
                        if pk_cols.is_empty() {
                            return Err(CdcError::Iceberg(
                                "replication mode requires a primary key".into(),
                            ));
                        }
                        schema_builder::build_replication_schema(&columns, &pk_cols)?
                    }
                };

                let creation = TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema(iceberg_schema)
                    .build();

                self.catalog
                    .create_table(&ns, creation)
                    .await
                    .map_err(|e| CdcError::Iceberg(format!("create table: {e}")))?
            }
        };

        // Cache Arrow schema derived from Iceberg schema
        let current_schema = table.metadata().current_schema();
        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(current_schema)
            .map_err(|e| CdcError::Iceberg(format!("arrow schema: {e}")))?;
        self.arrow_schemas.insert(key.clone(), arrow_schema);

        // For replication mode, cache PK info
        if self.mode == SinkMode::Replication {
            let pk_cols = crate::schema::discovery::fetch_primary_keys(
                &self.source_connection,
                source_schema,
                source_table,
            )
            .await?;

            if pk_cols.is_empty() {
                return Err(CdcError::Iceberg(
                    "replication mode requires a primary key".into(),
                ));
            }

            // Determine PK field IDs from the Iceberg schema
            let pk_ids: Vec<i32> = current_schema
                .as_struct()
                .fields()
                .iter()
                .filter(|f| pk_cols.contains(&f.name))
                .map(|f| f.id)
                .collect();

            // Build PK-only arrow schema (for delete batches)
            let pk_arrow_schema = {
                let full_arrow = iceberg::arrow::schema_to_arrow_schema(current_schema)
                    .map_err(|e| CdcError::Iceberg(format!("pk arrow schema: {e}")))?;
                let pk_fields: Vec<_> = full_arrow
                    .fields()
                    .iter()
                    .filter(|f| pk_cols.contains(f.name()))
                    .cloned()
                    .collect();
                arrow_schema::Schema::new(pk_fields)
            };

            self.iceberg_schemas
                .insert(key.clone(), current_schema.clone());
            self.pk_columns.insert(key.clone(), pk_cols);
            self.pk_arrow_schemas.insert(key.clone(), pk_arrow_schema);
            self.pk_field_ids.insert(key.clone(), pk_ids);
        }

        self.tables.insert(key, table);

        Ok(())
    }

    /// Detect new or dropped columns in incoming events and evolve the Iceberg table
    /// schema via the REST catalog if needed. This is a no-op when no schema changes
    /// are detected.
    async fn evolve_schema_if_needed(
        &mut self,
        source_schema: &str,
        source_table: &str,
        events: &[CdcEvent],
    ) -> Result<()> {
        let key = (source_schema.to_string(), source_table.to_string());

        // If arrow schema is not cached yet, ensure_table hasn't run — skip (first batch
        // is always handled by ensure_table which fetches the full schema from source).
        let arrow_schema = match self.arrow_schemas.get(&key) {
            Some(s) => s,
            None => return Ok(()),
        };

        // ── Phase 1: Detect and add new columns ──────────────────────
        let new_col_names = schema_evolution::detect_new_columns(arrow_schema, events);
        if !new_col_names.is_empty() {
            tracing::info!(
                schema = source_schema,
                table = source_table,
                new_columns = ?new_col_names,
                "detected {} new column(s), evolving Iceberg schema",
                new_col_names.len(),
            );

            // Fetch full column info from source DB, then filter to just the new ones
            let all_columns = crate::schema::discovery::fetch_columns(
                &self.source_connection,
                source_schema,
                source_table,
            )
            .await?;

            let new_col_set: std::collections::HashSet<&str> =
                new_col_names.iter().map(|s| s.as_str()).collect();
            let new_columns: Vec<_> = all_columns
                .into_iter()
                .filter(|c| new_col_set.contains(c.name.as_str()))
                .collect();

            if new_columns.is_empty() {
                tracing::warn!(
                    schema = source_schema,
                    table = source_table,
                    "new columns detected in events but not found in source DB; skipping evolution",
                );
            } else {
                // Build evolved schema (CDC adds paired _old_ columns, replication adds source only)
                let table = self
                    .tables
                    .get(&key)
                    .ok_or_else(|| CdcError::Iceberg("table not cached".into()))?;
                let current_schema = table.metadata().current_schema();

                let evolved_schema = match self.mode {
                    SinkMode::Cdc => {
                        schema_evolution::build_evolved_cdc_schema(current_schema, &new_columns)?
                    }
                    SinkMode::Replication => schema_evolution::build_evolved_iceberg_schema(
                        current_schema,
                        &new_columns,
                    )?,
                };

                let IcebergCatalogConfig::Rest(rest_config) = &self.config.catalog;
                let namespace = NamespaceIdent::from_strs(&self.config.namespace)
                    .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
                let iceberg_table_name = format!("{}{}", self.config.table_prefix, source_table);

                schema_evolution::commit_schema_evolution(
                    table,
                    evolved_schema,
                    rest_config,
                    &namespace,
                    &iceberg_table_name,
                )
                .await?;

                self.reload_table_and_refresh_caches(&key, source_table)
                    .await?;
            }
        }

        // ── Phase 2: Detect and drop removed columns (replication only) ──
        // In CDC mode the table is append-only; historical rows still reference
        // dropped columns, so we must keep them in the schema.
        if self.mode != SinkMode::Replication {
            return Ok(());
        }

        // Re-fetch arrow_schema since it may have been updated in Phase 1
        let arrow_schema = match self.arrow_schemas.get(&key) {
            Some(s) => s,
            None => return Ok(()),
        };

        let (has_full_row_events, missing_from_events) =
            schema_evolution::detect_dropped_columns_from_events(arrow_schema, events, false);

        if has_full_row_events && !missing_from_events.is_empty() {
            tracing::debug!(
                schema = source_schema,
                table = source_table,
                ?missing_from_events,
                "columns missing from events, confirming against source DB",
            );

            // Confirm the drop by querying the source database
            let source_cols = crate::schema::discovery::fetch_columns(
                &self.source_connection,
                source_schema,
                source_table,
            )
            .await?;
            let source_col_names: Vec<String> =
                source_cols.iter().map(|c| c.name.clone()).collect();

            tracing::debug!(
                schema = source_schema,
                table = source_table,
                ?source_col_names,
                "source DB columns fetched for drop confirmation",
            );

            let dropped = crate::schema::evolution::detect_dropped_columns(
                &missing_from_events,
                &source_col_names,
            );

            if !dropped.is_empty() {
                tracing::info!(
                    schema = source_schema,
                    table = source_table,
                    ?dropped,
                    "confirmed {} dropped column(s), evolving Iceberg schema",
                    dropped.len(),
                );

                let table = self
                    .tables
                    .get(&key)
                    .ok_or_else(|| CdcError::Iceberg("table not cached".into()))?;
                let current_schema = table.metadata().current_schema();

                let evolved_schema = schema_evolution::build_schema_without_columns(
                    current_schema,
                    &dropped,
                    false, // replication mode only — never CDC
                )?;

                let IcebergCatalogConfig::Rest(rest_config) = &self.config.catalog;
                let namespace = NamespaceIdent::from_strs(&self.config.namespace)
                    .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
                let iceberg_table_name = format!("{}{}", self.config.table_prefix, source_table);

                schema_evolution::commit_schema_evolution(
                    table,
                    evolved_schema,
                    rest_config,
                    &namespace,
                    &iceberg_table_name,
                )
                .await?;

                self.reload_table_and_refresh_caches(&key, source_table)
                    .await?;
            }
        }

        Ok(())
    }

    /// Reload a table from the catalog and refresh all derived caches
    /// (arrow schemas, iceberg schemas, PK schemas, PK field IDs).
    async fn reload_table_and_refresh_caches(
        &mut self,
        key: &(String, String),
        source_table: &str,
    ) -> Result<()> {
        let iceberg_table_name = format!("{}{}", self.config.table_prefix, source_table);
        let ns = NamespaceIdent::from_strs(&self.config.namespace)
            .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
        let ident = TableIdent::new(ns, iceberg_table_name);
        let updated_table =
            self.catalog.load_table(&ident).await.map_err(|e| {
                CdcError::Iceberg(format!("reload table after schema evolution: {e}"))
            })?;

        // Refresh all caches for this table
        let updated_schema = updated_table.metadata().current_schema();
        let new_arrow_schema = iceberg::arrow::schema_to_arrow_schema(updated_schema)
            .map_err(|e| CdcError::Iceberg(format!("arrow schema after evolution: {e}")))?;
        self.arrow_schemas.insert(key.clone(), new_arrow_schema);

        if self.mode == SinkMode::Replication {
            self.iceberg_schemas
                .insert(key.clone(), updated_schema.clone());

            // PK columns stay the same, but PK arrow schema needs refresh since it's
            // derived from the full arrow schema
            if let Some(pk_cols) = self.pk_columns.get(key) {
                let pk_cols = pk_cols.clone();
                let full_arrow =
                    iceberg::arrow::schema_to_arrow_schema(updated_schema).map_err(|e| {
                        CdcError::Iceberg(format!("pk arrow schema after evolution: {e}"))
                    })?;
                let pk_fields: Vec<_> = full_arrow
                    .fields()
                    .iter()
                    .filter(|f| pk_cols.contains(f.name()))
                    .cloned()
                    .collect();
                self.pk_arrow_schemas
                    .insert(key.clone(), arrow_schema::Schema::new(pk_fields));

                let pk_ids: Vec<i32> = updated_schema
                    .as_struct()
                    .fields()
                    .iter()
                    .filter(|f| pk_cols.contains(&f.name))
                    .map(|f| f.id)
                    .collect();
                self.pk_field_ids.insert(key.clone(), pk_ids);
            }
        }

        self.tables.insert(key.clone(), updated_table);

        Ok(())
    }

    async fn write_table_batch(
        &mut self,
        source_schema: &str,
        source_table: &str,
        events: &[CdcEvent],
    ) -> Result<()> {
        match &self.mode {
            SinkMode::Cdc => {
                self.write_table_batch_cdc(source_schema, source_table, events)
                    .await
            }
            SinkMode::Replication => {
                self.write_table_batch_replication(source_schema, source_table, events)
                    .await
            }
        }
    }

    async fn write_table_batch_cdc(
        &mut self,
        source_schema: &str,
        source_table: &str,
        events: &[CdcEvent],
    ) -> Result<()> {
        let key = (source_schema.to_string(), source_table.to_string());
        let arrow_schema = self
            .arrow_schemas
            .get(&key)
            .ok_or_else(|| CdcError::Iceberg("arrow schema not cached".into()))?;

        let batch = record_batch::events_to_flattened_cdc_batch(events, arrow_schema)?;

        let table = self
            .tables
            .get(&key)
            .ok_or_else(|| CdcError::Iceberg("table not cached".into()))?;

        let data_files = self.write_data_files(table, batch).await?;

        // Commit via fast_append
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(|e| CdcError::Iceberg(format!("apply: {e}")))?;
        let updated = tx
            .commit(&*self.catalog)
            .await
            .map_err(|e| CdcError::Iceberg(format!("commit: {e}")))?;

        self.tables.insert(key, updated);
        Ok(())
    }

    async fn write_table_batch_replication(
        &mut self,
        source_schema: &str,
        source_table: &str,
        events: &[CdcEvent],
    ) -> Result<()> {
        let key = (source_schema.to_string(), source_table.to_string());

        // Handle TRUNCATE: find the last truncate position, commit truncate,
        // then only process events after the last truncate.
        let events_to_process = if let Some(last_truncate_pos) =
            events.iter().rposition(|e| e.op == ChangeOp::Truncate)
        {
            let table = self
                .tables
                .get(&key)
                .ok_or_else(|| CdcError::Iceberg("table not cached".into()))?;

            let IcebergCatalogConfig::Rest(rest_config) = &self.config.catalog;
            let namespace = NamespaceIdent::from_strs(&self.config.namespace)
                .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
            let iceberg_table_name = format!("{}{}", self.config.table_prefix, source_table);

            row_delta::commit_truncate(table, rest_config, &namespace, &iceberg_table_name).await?;

            tracing::info!(
                schema = source_schema,
                table = source_table,
                "truncate committed, processing {} post-truncate events",
                events.len() - last_truncate_pos - 1,
            );

            // Reload table from catalog to get updated metadata after truncate
            let ns = NamespaceIdent::from_strs(&self.config.namespace)
                .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
            let ident = TableIdent::new(ns, iceberg_table_name);
            let updated = self
                .catalog
                .load_table(&ident)
                .await
                .map_err(|e| CdcError::Iceberg(format!("reload table after truncate: {e}")))?;
            self.tables.insert(key.clone(), updated);

            &events[last_truncate_pos + 1..]
        } else {
            events
        };

        if events_to_process.is_empty() {
            return Ok(());
        }

        let arrow_schema = self
            .arrow_schemas
            .get(&key)
            .ok_or_else(|| CdcError::Iceberg("arrow schema not cached".into()))?;

        let table = self
            .tables
            .get(&key)
            .ok_or_else(|| CdcError::Iceberg("table not cached".into()))?;

        // 1. Write data files (Insert/Update/Snapshot → event.new)
        let data_batch =
            record_batch::events_to_replication_data_batch(events_to_process, arrow_schema)?;
        let data_files = if let Some(batch) = data_batch {
            self.write_data_files(table, batch).await?
        } else {
            vec![]
        };

        // 2. Write equality delete files (Delete/Update → event.old PK columns)
        let pk_arrow_schema = self
            .pk_arrow_schemas
            .get(&key)
            .ok_or_else(|| CdcError::Iceberg("pk arrow schema not cached".into()))?;
        let delete_batch =
            record_batch::events_to_delete_batch(events_to_process, pk_arrow_schema)?;

        let delete_files = if let Some(batch) = delete_batch {
            let iceberg_schema = self
                .iceberg_schemas
                .get(&key)
                .ok_or_else(|| CdcError::Iceberg("iceberg schema not cached".into()))?;
            let pk_ids = self
                .pk_field_ids
                .get(&key)
                .ok_or_else(|| CdcError::Iceberg("pk field ids not cached".into()))?;

            self.write_equality_delete_files(table, iceberg_schema.clone(), pk_ids, batch)
                .await?
        } else {
            vec![]
        };

        // 3. Commit
        if data_files.is_empty() && delete_files.is_empty() {
            return Ok(());
        }

        if delete_files.is_empty() {
            // Insert-only: use fast_append (simpler, well-tested path)
            let tx = Transaction::new(table);
            let action = tx.fast_append().add_data_files(data_files);
            let tx = action
                .apply(tx)
                .map_err(|e| CdcError::Iceberg(format!("apply: {e}")))?;
            let updated = tx
                .commit(&*self.catalog)
                .await
                .map_err(|e| CdcError::Iceberg(format!("commit: {e}")))?;
            self.tables.insert(key, updated);
        } else {
            // Has deletes: use row_delta commit via REST API
            let IcebergCatalogConfig::Rest(rest_config) = &self.config.catalog;
            let namespace = NamespaceIdent::from_strs(&self.config.namespace)
                .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
            let iceberg_table_name = format!("{}{}", self.config.table_prefix, source_table);

            row_delta::commit_row_delta(
                table,
                rest_config,
                &namespace,
                &iceberg_table_name,
                data_files,
                delete_files,
            )
            .await?;

            // Reload table from catalog to get updated metadata
            let ns = NamespaceIdent::from_strs(&self.config.namespace)
                .map_err(|e| CdcError::Iceberg(format!("namespace: {e}")))?;
            let ident = TableIdent::new(ns, iceberg_table_name);
            let updated = self
                .catalog
                .load_table(&ident)
                .await
                .map_err(|e| CdcError::Iceberg(format!("reload table: {e}")))?;
            self.tables.insert(key, updated);
        }

        Ok(())
    }

    /// Write a RecordBatch as data files using the DataFileWriter pipeline.
    async fn write_data_files(
        &self,
        table: &iceberg::table::Table,
        batch: arrow_array::RecordBatch,
    ) -> Result<Vec<iceberg::spec::DataFile>> {
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| CdcError::Iceberg(format!("location gen: {e}")))?;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string();
        let file_name_gen = DefaultFileNameGenerator::new(
            "cdc-data".to_string(),
            Some(suffix),
            DataFileFormat::Parquet,
        );

        let pw_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new((**table.metadata().current_schema()).clone()),
        );

        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let dfw_builder = DataFileWriterBuilder::new(rolling_builder);
        let mut writer = dfw_builder
            .build(None)
            .await
            .map_err(|e| CdcError::Iceberg(format!("writer build: {e}")))?;

        writer
            .write(batch)
            .await
            .map_err(|e| CdcError::Iceberg(format!("write: {e}")))?;
        writer
            .close()
            .await
            .map_err(|e| CdcError::Iceberg(format!("close writer: {e}")))
    }

    /// Write a RecordBatch as equality delete files.
    async fn write_equality_delete_files(
        &self,
        table: &iceberg::table::Table,
        iceberg_schema: Arc<iceberg::spec::Schema>,
        pk_field_ids: &[i32],
        batch: arrow_array::RecordBatch,
    ) -> Result<Vec<iceberg::spec::DataFile>> {
        use iceberg::writer::base_writer::equality_delete_writer::{
            EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
        };

        let eq_config =
            EqualityDeleteWriterConfig::new(pk_field_ids.to_vec(), iceberg_schema.clone())
                .map_err(|e| CdcError::Iceberg(format!("equality delete config: {e}")))?;

        let delete_schema =
            iceberg::arrow::arrow_schema_to_schema(eq_config.projected_arrow_schema_ref())
                .map_err(|e| CdcError::Iceberg(format!("delete schema: {e}")))?;

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| CdcError::Iceberg(format!("delete location gen: {e}")))?;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string();
        let file_name_gen = DefaultFileNameGenerator::new(
            "cdc-delete".to_string(),
            Some(suffix),
            DataFileFormat::Parquet,
        );

        let pw_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(delete_schema));

        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling_builder, eq_config)
            .build(None)
            .await
            .map_err(|e| CdcError::Iceberg(format!("delete writer build: {e}")))?;

        writer
            .write(batch)
            .await
            .map_err(|e| CdcError::Iceberg(format!("delete write: {e}")))?;
        writer
            .close()
            .await
            .map_err(|e| CdcError::Iceberg(format!("close delete writer: {e}")))
    }
}

impl Sink for IcebergSink {
    async fn write_batch(&mut self, events: &[CdcEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Group events by (schema, table)
        let mut grouped: HashMap<(String, String), Vec<CdcEvent>> = HashMap::new();
        for event in events {
            let key = (event.table.schema.clone(), event.table.name.clone());
            grouped.entry(key).or_default().push(event.clone());
        }

        for ((schema, name), table_events) in grouped {
            self.ensure_table(&schema, &name).await?;
            self.evolve_schema_if_needed(&schema, &name, &table_events)
                .await?;
            self.write_table_batch(&schema, &name, &table_events)
                .await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        // Writes are committed per batch in write_batch, nothing to flush
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        IcebergCatalogConfig, IcebergSinkConfig, RestCatalogConfig, SourceConnectionConfig,
    };
    use crate::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, TableId};
    use crate::schema::SourceDialect;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::collections::BTreeMap;

    fn sample_config() -> IcebergSinkConfig {
        IcebergSinkConfig {
            catalog: IcebergCatalogConfig::Rest(RestCatalogConfig {
                uri: "http://localhost:8181".into(),
                warehouse: Some("s3://bucket/wh".into()),
                properties: HashMap::new(),
                timeout_secs: 30,
            }),
            namespace: vec!["my_database".into()],
            table_prefix: "cdc_".into(),
        }
    }

    fn sample_source_connection() -> SourceConnectionConfig {
        SourceConnectionConfig::Postgres {
            url: "postgres://localhost/db".into(),
        }
    }

    #[test]
    fn test_dialect_detection_postgres() {
        let config = sample_config();
        let sink = IcebergSink {
            config,
            source_connection: sample_source_connection(),
            catalog: Arc::new(MockNoCatalog),
            mode: SinkMode::Cdc,
            tables: HashMap::new(),
            arrow_schemas: HashMap::new(),
            pk_columns: HashMap::new(),
            pk_arrow_schemas: HashMap::new(),
            iceberg_schemas: HashMap::new(),
            pk_field_ids: HashMap::new(),
        };
        assert_eq!(
            SourceDialect::from(&sink.source_connection),
            SourceDialect::Postgres
        );
    }

    #[test]
    fn test_dialect_detection_mysql() {
        let config = sample_config();
        let sink = IcebergSink {
            config,
            source_connection: SourceConnectionConfig::Mysql {
                url: "mysql://localhost/db".into(),
            },
            catalog: Arc::new(MockNoCatalog),
            mode: SinkMode::Cdc,
            tables: HashMap::new(),
            arrow_schemas: HashMap::new(),
            pk_columns: HashMap::new(),
            pk_arrow_schemas: HashMap::new(),
            iceberg_schemas: HashMap::new(),
            pk_field_ids: HashMap::new(),
        };
        assert_eq!(
            SourceDialect::from(&sink.source_connection),
            SourceDialect::Mysql
        );
    }

    #[tokio::test]
    async fn test_write_batch_empty_events() {
        let config = sample_config();
        let mut sink = IcebergSink {
            config,
            source_connection: sample_source_connection(),
            catalog: Arc::new(MockNoCatalog),
            mode: SinkMode::Cdc,
            tables: HashMap::new(),
            arrow_schemas: HashMap::new(),
            pk_columns: HashMap::new(),
            pk_arrow_schemas: HashMap::new(),
            iceberg_schemas: HashMap::new(),
            pk_field_ids: HashMap::new(),
        };
        // Empty events should succeed without touching catalog
        sink.write_batch(&[]).await.unwrap();
    }

    /// A minimal catalog that always errors — used for unit tests that
    /// don't actually touch the catalog (e.g., empty batch, dialect detection).
    #[derive(Debug)]
    struct MockNoCatalog;

    macro_rules! mock_err {
        () => {
            Err(iceberg::Error::new(iceberg::ErrorKind::Unexpected, "mock"))
        };
    }

    #[async_trait::async_trait]
    impl Catalog for MockNoCatalog {
        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            mock_err!()
        }
        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            mock_err!()
        }
        async fn get_namespace(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            mock_err!()
        }
        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            mock_err!()
        }
        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            mock_err!()
        }
        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            mock_err!()
        }
        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            mock_err!()
        }
        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            mock_err!()
        }
        async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
            mock_err!()
        }
        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            mock_err!()
        }
        async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
            mock_err!()
        }
        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            mock_err!()
        }
        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            mock_err!()
        }
        async fn update_table(
            &self,
            _commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            mock_err!()
        }
    }

    // ── Write path test helpers ─────────────────────────────────────

    async fn setup_test_catalog(warehouse: &std::path::Path) -> Arc<dyn Catalog> {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.to_str().unwrap().to_string(),
                )]),
            )
            .await
            .unwrap();
        Arc::new(catalog)
    }

    fn test_iceberg_schema() -> iceberg::spec::Schema {
        // Flattened CDC schema: 7 metadata + 2 source (id, name) + 2 old (_old_id, _old_name)
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "_cdc_op", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "_cdc_lsn", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "_cdc_timestamp_us", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(4, "_cdc_snapshot", Type::Primitive(PrimitiveType::Boolean))
                    .into(),
                NestedField::required(5, "_cdc_schema", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(6, "_cdc_table", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(
                    7,
                    "_cdc_primary_key_columns",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
                NestedField::optional(8, "id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(9, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(10, "_old_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(11, "_old_name", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .build()
            .unwrap()
    }

    fn test_sink_config() -> IcebergSinkConfig {
        IcebergSinkConfig {
            catalog: IcebergCatalogConfig::Rest(RestCatalogConfig {
                uri: "http://unused".into(),
                warehouse: None,
                properties: HashMap::new(),
                timeout_secs: 30,
            }),
            namespace: vec!["default".into()],
            table_prefix: "cdc_test_".into(),
        }
    }

    fn test_source_connection() -> SourceConnectionConfig {
        SourceConnectionConfig::Postgres {
            url: "postgres://unused".into(),
        }
    }

    fn make_test_table_id(name: &str) -> TableId {
        TableId {
            schema: "public".into(),
            name: name.into(),
            oid: 1,
        }
    }

    /// Create a namespace + table in the catalog, cache it in the sink, return the table ident.
    async fn create_and_cache_table(
        catalog: &Arc<dyn Catalog>,
        sink: &mut IcebergSink,
        iceberg_table_name: &str,
        source_schema: &str,
        source_table: &str,
    ) -> TableIdent {
        let ns = NamespaceIdent::new("default".into());

        // Ensure namespace exists (ignore error if already exists)
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;

        let ident = TableIdent::new(ns.clone(), iceberg_table_name.into());
        let creation = TableCreation::builder()
            .name(iceberg_table_name.into())
            .schema(test_iceberg_schema())
            .build();
        let table = catalog.create_table(&ns, creation).await.unwrap();

        sink.cache_table(source_schema, source_table, table)
            .unwrap();

        ident
    }

    // ── Write path tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_write_batch_single_insert() {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = setup_test_catalog(tmp.path()).await;
        let mut sink = IcebergSink::with_catalog(
            test_sink_config(),
            SinkMode::Cdc,
            test_source_connection(),
            catalog.clone(),
        );

        let ident =
            create_and_cache_table(&catalog, &mut sink, "cdc_test_users", "public", "users").await;

        let events = vec![
            CdcEvent {
                lsn: Lsn(100),
                timestamp_us: 1_000_000,
                xid: 1,
                table: make_test_table_id("users"),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: make_test_table_id("users"),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("2".into())),
                    ("name".into(), ColumnValue::Text("Bob".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
        ];

        sink.write_batch(&events).await.unwrap();

        // Reload table from catalog and verify a snapshot was committed
        let reloaded = catalog.load_table(&ident).await.unwrap();
        let snapshot = reloaded.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation.as_str(), "append",);
    }

    #[tokio::test]
    async fn test_write_batch_mixed_change_kinds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = setup_test_catalog(tmp.path()).await;
        let mut sink = IcebergSink::with_catalog(
            test_sink_config(),
            SinkMode::Cdc,
            test_source_connection(),
            catalog.clone(),
        );

        let ident =
            create_and_cache_table(&catalog, &mut sink, "cdc_test_users", "public", "users").await;

        let events = vec![
            CdcEvent {
                lsn: Lsn(100),
                timestamp_us: 1_000_000,
                xid: 1,
                table: make_test_table_id("users"),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: make_test_table_id("users"),
                op: ChangeOp::Update,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice Updated".into())),
                ])),
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(300),
                timestamp_us: 3_000_000,
                xid: 3,
                table: make_test_table_id("users"),
                op: ChangeOp::Delete,
                new: None,
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice Updated".into())),
                ])),
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(400),
                timestamp_us: 4_000_000,
                xid: 4,
                table: make_test_table_id("users"),
                op: ChangeOp::Truncate,
                new: None,
                old: None,
                primary_key_columns: vec![],
            },
        ];

        sink.write_batch(&events).await.unwrap();

        let reloaded = catalog.load_table(&ident).await.unwrap();
        assert!(reloaded.metadata().current_snapshot().is_some());
    }

    #[tokio::test]
    async fn test_write_batch_multiple_tables() {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = setup_test_catalog(tmp.path()).await;
        let mut sink = IcebergSink::with_catalog(
            test_sink_config(),
            SinkMode::Cdc,
            test_source_connection(),
            catalog.clone(),
        );

        let ident_a =
            create_and_cache_table(&catalog, &mut sink, "cdc_test_table_a", "public", "table_a")
                .await;

        let ident_b =
            create_and_cache_table(&catalog, &mut sink, "cdc_test_table_b", "public", "table_b")
                .await;

        let events = vec![
            CdcEvent {
                lsn: Lsn(100),
                timestamp_us: 1_000_000,
                xid: 1,
                table: make_test_table_id("table_a"),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("row_a".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: make_test_table_id("table_b"),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("row_b".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
        ];

        sink.write_batch(&events).await.unwrap();

        // Both tables should have snapshots
        let reloaded_a = catalog.load_table(&ident_a).await.unwrap();
        assert!(reloaded_a.metadata().current_snapshot().is_some());

        let reloaded_b = catalog.load_table(&ident_b).await.unwrap();
        assert!(reloaded_b.metadata().current_snapshot().is_some());
    }

    #[tokio::test]
    async fn test_write_batch_updates_cached_table() {
        let tmp = tempfile::TempDir::new().unwrap();
        let catalog = setup_test_catalog(tmp.path()).await;
        let mut sink = IcebergSink::with_catalog(
            test_sink_config(),
            SinkMode::Cdc,
            test_source_connection(),
            catalog.clone(),
        );

        let ident =
            create_and_cache_table(&catalog, &mut sink, "cdc_test_users", "public", "users").await;

        // First batch
        let events_1 = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: make_test_table_id("users"),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        }];
        sink.write_batch(&events_1).await.unwrap();

        // Second batch — should succeed because cached table was updated
        let events_2 = vec![CdcEvent {
            lsn: Lsn(200),
            timestamp_us: 2_000_000,
            xid: 2,
            table: make_test_table_id("users"),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("2".into())),
                ("name".into(), ColumnValue::Text("Bob".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        }];
        sink.write_batch(&events_2).await.unwrap();

        // Reload and verify 2 snapshots exist
        let reloaded = catalog.load_table(&ident).await.unwrap();
        let snapshots: Vec<_> = reloaded.metadata().snapshots().collect();
        assert_eq!(snapshots.len(), 2);
    }

    // ── Replication mode tests ──────────────────────────────────────

    fn test_replication_iceberg_schema() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .build()
            .unwrap()
    }

    /// Helper: create a replication-mode sink with a table pre-cached.
    async fn create_replication_sink_with_table(
        warehouse: &std::path::Path,
    ) -> (IcebergSink, Arc<dyn Catalog>, TableIdent) {
        let catalog = setup_test_catalog(warehouse).await;
        let mut sink = IcebergSink::with_catalog(
            test_sink_config(),
            SinkMode::Replication,
            test_source_connection(),
            catalog.clone(),
        );

        let ns = NamespaceIdent::new("default".into());
        let _ = catalog.create_namespace(&ns, HashMap::new()).await;

        let ident = TableIdent::new(ns.clone(), "cdc_test_users".into());
        let creation = TableCreation::builder()
            .name("cdc_test_users".into())
            .schema(test_replication_iceberg_schema())
            .build();
        let table = catalog.create_table(&ns, creation).await.unwrap();

        // Manually cache table + replication metadata
        let current_schema = table.metadata().current_schema();
        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(current_schema).unwrap();
        let pk_arrow_schema = arrow_schema::Schema::new(vec![arrow_schema.fields()[0].clone()]);
        let key = ("public".to_string(), "users".to_string());
        sink.arrow_schemas.insert(key.clone(), arrow_schema);
        sink.pk_columns.insert(key.clone(), vec!["id".to_string()]);
        sink.pk_arrow_schemas.insert(key.clone(), pk_arrow_schema);
        sink.iceberg_schemas
            .insert(key.clone(), current_schema.clone());
        sink.pk_field_ids.insert(key.clone(), vec![1]);
        sink.tables.insert(key, table);

        (sink, catalog, ident)
    }

    #[tokio::test]
    async fn test_replication_mode_write_inserts() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (mut sink, catalog, ident) = create_replication_sink_with_table(tmp.path()).await;

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: make_test_table_id("users"),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        }];

        sink.write_batch(&events).await.unwrap();

        // Verify a snapshot was committed
        let reloaded = catalog.load_table(&ident).await.unwrap();
        assert!(reloaded.metadata().current_snapshot().is_some());
    }

    #[tokio::test]
    async fn test_replication_mode_deletes_only_rest_commit_attempted() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (mut sink, _catalog, _ident) = create_replication_sink_with_table(tmp.path()).await;

        // Only deletes → goes through row_delta commit path which requires REST catalog.
        // Since unit tests use MemoryCatalog, the REST POST will fail with a connection error.
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: make_test_table_id("users"),
            op: ChangeOp::Delete,
            new: None,
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text("1".into())),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            primary_key_columns: vec![],
        }];

        // Should error since there's no REST catalog at http://unused
        let result = sink.write_batch(&events).await;
        assert!(
            result.is_err(),
            "expected REST commit error in unit test environment"
        );
    }

    #[tokio::test]
    async fn test_replication_mode_mixed_events_rest_commit_attempted() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (mut sink, _catalog, _ident) = create_replication_sink_with_table(tmp.path()).await;

        let events = vec![
            CdcEvent {
                lsn: Lsn(100),
                timestamp_us: 1_000_000,
                xid: 1,
                table: make_test_table_id("users"),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: make_test_table_id("users"),
                op: ChangeOp::Update,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice Updated".into())),
                ])),
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(300),
                timestamp_us: 3_000_000,
                xid: 3,
                table: make_test_table_id("users"),
                op: ChangeOp::Delete,
                new: None,
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Text("1".into())),
                    ("name".into(), ColumnValue::Text("Alice Updated".into())),
                ])),
                primary_key_columns: vec![],
            },
        ];

        // Mixed events with deletes go through row_delta commit path,
        // which requires REST catalog. Should error in unit test environment.
        let result = sink.write_batch(&events).await;
        assert!(
            result.is_err(),
            "expected REST commit error in unit test environment"
        );
    }

    // ── TRUNCATE batch slicing tests ────────────────────────────────

    fn make_truncate_event(table_name: &str) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(0),
            timestamp_us: 0,
            xid: 0,
            table: make_test_table_id(table_name),
            op: ChangeOp::Truncate,
            new: None,
            old: None,
            primary_key_columns: vec![],
        }
    }

    fn make_insert_event(table_name: &str, id: &str) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(0),
            timestamp_us: 0,
            xid: 0,
            table: make_test_table_id(table_name),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Text(id.into())),
                ("name".into(), ColumnValue::Text("test".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        }
    }

    #[test]
    fn test_truncate_batch_slicing_last_position() {
        let events = [
            make_insert_event("users", "1"),
            make_truncate_event("users"),
            make_insert_event("users", "2"),
            make_insert_event("users", "3"),
        ];

        let last_truncate_pos = events.iter().rposition(|e| e.op == ChangeOp::Truncate);
        assert_eq!(last_truncate_pos, Some(1));

        let events_after = &events[last_truncate_pos.unwrap() + 1..];
        assert_eq!(events_after.len(), 2);
        assert_eq!(events_after[0].op, ChangeOp::Insert);
        assert_eq!(events_after[1].op, ChangeOp::Insert);
    }

    #[test]
    fn test_truncate_only_batch_returns_empty_slice() {
        let events = [make_truncate_event("users")];

        let last_truncate_pos = events.iter().rposition(|e| e.op == ChangeOp::Truncate);
        assert_eq!(last_truncate_pos, Some(0));

        let events_after = &events[last_truncate_pos.unwrap() + 1..];
        assert!(events_after.is_empty());
    }

    #[test]
    fn test_truncate_followed_by_inserts() {
        let events = [
            make_truncate_event("users"),
            make_insert_event("users", "1"),
            make_insert_event("users", "2"),
        ];

        let last_truncate_pos = events.iter().rposition(|e| e.op == ChangeOp::Truncate);
        assert_eq!(last_truncate_pos, Some(0));

        let events_after = &events[last_truncate_pos.unwrap() + 1..];
        assert_eq!(events_after.len(), 2);
    }

    #[test]
    fn test_multiple_truncates_uses_last() {
        let events = [
            make_insert_event("users", "1"),
            make_truncate_event("users"),
            make_insert_event("users", "2"),
            make_truncate_event("users"),
            make_insert_event("users", "3"),
        ];

        let last_truncate_pos = events.iter().rposition(|e| e.op == ChangeOp::Truncate);
        assert_eq!(last_truncate_pos, Some(3));

        let events_after = &events[last_truncate_pos.unwrap() + 1..];
        assert_eq!(events_after.len(), 1);
    }

    #[test]
    fn test_no_truncate_returns_all() {
        let events = [
            make_insert_event("users", "1"),
            make_insert_event("users", "2"),
        ];

        let last_truncate_pos = events.iter().rposition(|e| e.op == ChangeOp::Truncate);
        assert!(last_truncate_pos.is_none());
    }
}
