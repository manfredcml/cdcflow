use std::collections::HashMap;

use iceberg::spec::{
    DataFile, DataFileFormat, ManifestFile, ManifestListWriter, ManifestWriterBuilder, Operation,
    Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Summary, MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use uuid::Uuid;

use crate::config::RestCatalogConfig;
use crate::error::{CdcError, Result};

const META_ROOT_PATH: &str = "metadata";

/// Build the REST catalog commit URL for a table.
///
/// Format: `{uri}/v1/{prefix}/namespaces/{ns}/tables/{table}`
/// where prefix is optional.
pub(crate) fn build_commit_url(
    uri: &str,
    properties: &HashMap<String, String>,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> String {
    let uri = uri.trim_end_matches('/');
    let ns_encoded = namespace.to_url_string();

    let mut parts: Vec<&str> = vec![uri, "v1"];
    let prefix_val;
    if let Some(prefix) = properties.get("prefix") {
        prefix_val = prefix.clone();
        parts.push(&prefix_val);
    }
    parts.push("namespaces");
    let ns_str = ns_encoded;
    parts.push(&ns_str);
    parts.push("tables");
    parts.push(table_name);
    parts.join("/")
}

/// Generate a unique snapshot ID that doesn't collide with existing snapshots.
fn generate_snapshot_id(table: &Table) -> i64 {
    let generate = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let id = (lhs ^ rhs) as i64;
        if id < 0 { -id } else { id }
    };
    let mut id = generate();
    while table.metadata().snapshots().any(|s| s.snapshot_id() == id) {
        id = generate();
    }
    id
}

/// Build a snapshot summary for a row-delta commit.
///
/// Uses `SnapshotSummaryCollector` for data file metrics, then adds delete file
/// metrics manually. Sets `Operation::Overwrite` per the Iceberg spec for
/// row-level mutations.
fn build_summary(data_files: &[DataFile], delete_files: &[DataFile], table: &Table) -> Summary {
    let schema = table.metadata().current_schema().clone();
    let partition_spec = table.metadata().default_partition_spec().clone();

    let mut collector = SnapshotSummaryCollector::default();
    for f in data_files {
        collector.add_file(f, schema.clone(), partition_spec.clone());
    }
    let mut props = collector.build();

    // Add delete file metrics manually (SnapshotSummaryCollector only tracks data files)
    props.insert(
        "added-delete-files".to_string(),
        delete_files.len().to_string(),
    );
    let eq_delete_rows: u64 = delete_files.iter().map(|f| f.record_count()).sum();
    props.insert(
        "added-equality-deletes".to_string(),
        eq_delete_rows.to_string(),
    );

    Summary {
        operation: Operation::Overwrite,
        additional_properties: props,
    }
}

/// Write a manifest file containing data files.
async fn write_data_manifest(
    table: &Table,
    snapshot_id: i64,
    commit_uuid: &Uuid,
    manifest_counter: u64,
    data_files: Vec<DataFile>,
    sequence_number: i64,
) -> Result<ManifestFile> {
    let manifest_path = format!(
        "{}/{}/{}-m{}.{}",
        table.metadata().location(),
        META_ROOT_PATH,
        commit_uuid,
        manifest_counter,
        DataFileFormat::Avro,
    );
    let output = table
        .file_io()
        .new_output(manifest_path)
        .map_err(|e| CdcError::Iceberg(format!("data manifest output: {e}")))?;

    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None, // key_metadata
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    );
    let mut writer = builder.build_v2_data();

    for file in data_files {
        writer
            .add_file(file, sequence_number)
            .map_err(|e| CdcError::Iceberg(format!("add data file to manifest: {e}")))?;
    }

    writer
        .write_manifest_file()
        .await
        .map_err(|e| CdcError::Iceberg(format!("write data manifest: {e}")))
}

/// Write a manifest file containing equality delete files.
async fn write_delete_manifest(
    table: &Table,
    snapshot_id: i64,
    commit_uuid: &Uuid,
    manifest_counter: u64,
    delete_files: Vec<DataFile>,
    sequence_number: i64,
) -> Result<ManifestFile> {
    let manifest_path = format!(
        "{}/{}/{}-m{}.{}",
        table.metadata().location(),
        META_ROOT_PATH,
        commit_uuid,
        manifest_counter,
        DataFileFormat::Avro,
    );
    let output = table
        .file_io()
        .new_output(manifest_path)
        .map_err(|e| CdcError::Iceberg(format!("delete manifest output: {e}")))?;

    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    );
    let mut writer = builder.build_v2_deletes();

    for file in delete_files {
        writer
            .add_file(file, sequence_number)
            .map_err(|e| CdcError::Iceberg(format!("add delete file to manifest: {e}")))?;
    }

    writer
        .write_manifest_file()
        .await
        .map_err(|e| CdcError::Iceberg(format!("write delete manifest: {e}")))
}

/// Load existing manifests from the current snapshot's manifest list.
async fn load_existing_manifests(table: &Table) -> Result<Vec<ManifestFile>> {
    match table.metadata().current_snapshot() {
        Some(snapshot) => {
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await
                .map_err(|e| CdcError::Iceberg(format!("load manifest list: {e}")))?;
            Ok(manifest_list.entries().to_vec())
        }
        None => Ok(vec![]),
    }
}

/// Assemble and write the manifest list containing existing + new manifests.
async fn write_manifest_list(
    table: &Table,
    snapshot_id: i64,
    commit_uuid: &Uuid,
    sequence_number: i64,
    manifests: Vec<ManifestFile>,
) -> Result<String> {
    let manifest_list_path = format!(
        "{}/{}/snap-{}-0-{}.{}",
        table.metadata().location(),
        META_ROOT_PATH,
        snapshot_id,
        commit_uuid,
        DataFileFormat::Avro,
    );

    let output = table
        .file_io()
        .new_output(manifest_list_path.clone())
        .map_err(|e| CdcError::Iceberg(format!("manifest list output: {e}")))?;

    let mut writer = ManifestListWriter::v2(
        output,
        snapshot_id,
        table.metadata().current_snapshot_id(),
        sequence_number,
    );

    writer
        .add_manifests(manifests.into_iter())
        .map_err(|e| CdcError::Iceberg(format!("add manifests: {e}")))?;

    writer
        .close()
        .await
        .map_err(|e| CdcError::Iceberg(format!("close manifest list: {e}")))?;

    Ok(manifest_list_path)
}

/// Commit a truncate operation by creating a new snapshot with an empty manifest list.
///
/// This follows the Iceberg spec: the new snapshot uses `Operation::Delete` and
/// carries forward no manifests, so all prior data becomes invisible at the current
/// snapshot. The physical data files remain on disk (available via time travel).
pub async fn commit_truncate(
    table: &Table,
    catalog_config: &RestCatalogConfig,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> Result<()> {
    let commit_uuid = Uuid::new_v4();
    let snapshot_id = generate_snapshot_id(table);
    let sequence_number = table.metadata().next_sequence_number();

    let summary = build_truncate_summary();

    let manifest_list_path = write_manifest_list(
        table,
        snapshot_id,
        &commit_uuid,
        sequence_number,
        vec![],
    )
    .await?;

    let commit_ts = chrono::Utc::now().timestamp_millis();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(table.metadata().current_snapshot_id())
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(commit_ts)
        .with_manifest_list(manifest_list_path)
        .with_summary(summary)
        .with_schema_id(table.metadata().current_schema_id())
        .build();

    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    let updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];

    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: table.metadata().current_snapshot_id(),
        },
    ];

    let url = build_commit_url(
        &catalog_config.uri,
        &catalog_config.properties,
        namespace,
        table_name,
    );

    let request = CommitTableRequest {
        identifier: Some(table_ident),
        requirements,
        updates,
    };

    let timeout = std::time::Duration::from_secs(catalog_config.timeout_secs);
    let client = reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("HTTP client: {e}")))?;

    let response = client
        .post(&url)
        .json(&request)
        .send()
        .await
        .map_err(|e| CdcError::Iceberg(format!("REST truncate commit request: {e}")))?;

    let status = response.status();
    if status.is_success() {
        tracing::info!(
            table = table_name,
            snapshot_id,
            "truncate commit succeeded"
        );
        Ok(())
    } else {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable>".into());
        Err(CdcError::Iceberg(format!(
            "REST truncate commit failed (HTTP {status}): {body}"
        )))
    }
}

/// Build a snapshot summary for a truncate operation.
///
/// Uses `Operation::Delete` per the Iceberg spec. All counts are zero since
/// no new files are being added.
fn build_truncate_summary() -> Summary {
    let props = HashMap::from([
        ("added-data-files".to_string(), "0".to_string()),
        ("added-records".to_string(), "0".to_string()),
        ("added-delete-files".to_string(), "0".to_string()),
        ("added-equality-deletes".to_string(), "0".to_string()),
    ]);

    Summary {
        operation: Operation::Delete,
        additional_properties: props,
    }
}

/// Commit data files and equality delete files atomically as a single Iceberg
/// snapshot via the REST catalog HTTP API.
///
/// This bypasses iceberg-rust's `Transaction` system (which only supports
/// `fast_append` with data-only files) and directly constructs the snapshot
/// metadata + POSTs the commit to the REST catalog.
pub async fn commit_row_delta(
    table: &Table,
    catalog_config: &RestCatalogConfig,
    namespace: &NamespaceIdent,
    table_name: &str,
    data_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
) -> Result<()> {
    if data_files.is_empty() && delete_files.is_empty() {
        return Ok(());
    }

    let commit_uuid = Uuid::new_v4();
    let snapshot_id = generate_snapshot_id(table);
    let sequence_number = table.metadata().next_sequence_number();

    let summary = build_summary(&data_files, &delete_files, table);

    let mut manifest_counter: u64 = 0;
    let mut new_manifests: Vec<ManifestFile> = Vec::new();

    if !data_files.is_empty() {
        let data_manifest = write_data_manifest(
            table,
            snapshot_id,
            &commit_uuid,
            manifest_counter,
            data_files,
            sequence_number,
        )
        .await?;
        new_manifests.push(data_manifest);
        manifest_counter += 1;
    }

    if !delete_files.is_empty() {
        let delete_manifest = write_delete_manifest(
            table,
            snapshot_id,
            &commit_uuid,
            manifest_counter,
            delete_files,
            sequence_number,
        )
        .await?;
        new_manifests.push(delete_manifest);
    }

    let existing_manifests = load_existing_manifests(table).await?;
    let all_manifests: Vec<_> = new_manifests.into_iter().chain(existing_manifests).collect();

    let manifest_list_path = write_manifest_list(
        table,
        snapshot_id,
        &commit_uuid,
        sequence_number,
        all_manifests,
    )
    .await?;

    let commit_ts = chrono::Utc::now().timestamp_millis();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(table.metadata().current_snapshot_id())
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(commit_ts)
        .with_manifest_list(manifest_list_path)
        .with_summary(summary)
        .with_schema_id(table.metadata().current_schema_id())
        .build();

    let table_ident =
        TableIdent::new(namespace.clone(), table_name.to_string());

    let updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];

    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: table.metadata().current_snapshot_id(),
        },
    ];

    let url = build_commit_url(
        &catalog_config.uri,
        &catalog_config.properties,
        namespace,
        table_name,
    );

    let request = CommitTableRequest {
        identifier: Some(table_ident),
        requirements,
        updates,
    };

    let timeout = std::time::Duration::from_secs(catalog_config.timeout_secs);
    let client = reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("HTTP client: {e}")))?;

    let response = client
        .post(&url)
        .json(&request)
        .send()
        .await
        .map_err(|e| CdcError::Iceberg(format!("REST commit request: {e}")))?;

    let status = response.status();
    if status.is_success() {
        tracing::info!(
            table = table_name,
            snapshot_id,
            "row delta commit succeeded"
        );
        Ok(())
    } else {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable>".into());
        Err(CdcError::Iceberg(format!(
            "REST commit failed (HTTP {status}): {body}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_commit_url_no_prefix() {
        let url = build_commit_url(
            "http://localhost:8181",
            &HashMap::new(),
            &NamespaceIdent::from_strs(["default"]).unwrap(),
            "my_table",
        );
        assert_eq!(
            url,
            "http://localhost:8181/v1/namespaces/default/tables/my_table"
        );
    }

    #[test]
    fn test_build_commit_url_with_prefix() {
        let mut props = HashMap::new();
        props.insert("prefix".to_string(), "my_warehouse".to_string());
        let url = build_commit_url(
            "http://localhost:8181",
            &props,
            &NamespaceIdent::from_strs(["default"]).unwrap(),
            "my_table",
        );
        assert_eq!(
            url,
            "http://localhost:8181/v1/my_warehouse/namespaces/default/tables/my_table"
        );
    }

    #[test]
    fn test_build_commit_url_trailing_slash() {
        let url = build_commit_url(
            "http://localhost:8181/",
            &HashMap::new(),
            &NamespaceIdent::from_strs(["db"]).unwrap(),
            "t",
        );
        assert_eq!(url, "http://localhost:8181/v1/namespaces/db/tables/t");
    }

    #[test]
    fn test_build_commit_url_multi_level_namespace() {
        let url = build_commit_url(
            "http://localhost:8181",
            &HashMap::new(),
            &NamespaceIdent::from_strs(["level1", "level2"]).unwrap(),
            "t",
        );
        assert_eq!(
            url,
            "http://localhost:8181/v1/namespaces/level1\u{001f}level2/tables/t"
        );
    }

    #[test]
    fn test_build_truncate_summary() {
        let summary = build_truncate_summary();
        assert_eq!(summary.operation, Operation::Delete);
        assert_eq!(
            summary.additional_properties.get("added-data-files").unwrap(),
            "0"
        );
        assert_eq!(
            summary.additional_properties.get("added-records").unwrap(),
            "0"
        );
        assert_eq!(
            summary
                .additional_properties
                .get("added-delete-files")
                .unwrap(),
            "0"
        );
        assert_eq!(
            summary
                .additional_properties
                .get("added-equality-deletes")
                .unwrap(),
            "0"
        );
    }

}
