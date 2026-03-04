use tokio_postgres::Client;

use crate::error::{CdcError, Result};
use crate::event::Lsn;

/// Information about a replication slot.
#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub slot_name: String,
    pub plugin: String,
    pub confirmed_flush_lsn: Option<Lsn>,
    pub active: bool,
}

/// Create a logical replication slot using the pgoutput plugin.
/// Returns the consistent point LSN (the snapshot LSN).
pub async fn create_slot(client: &Client, slot_name: &str) -> Result<Lsn> {
    let row = client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot_name],
        )
        .await
        .map_err(|e| CdcError::Protocol(format!("create slot '{slot_name}': {e}")))?;

    let lsn_str: &str = row.get(0);
    let lsn: Lsn = lsn_str
        .parse()
        .map_err(|e: CdcError| CdcError::Parse(format!("slot LSN: {e}")))?;

    tracing::info!(%lsn, slot_name, "created replication slot");
    Ok(lsn)
}

/// Drop a replication slot.
pub async fn drop_slot(client: &Client, slot_name: &str) -> Result<()> {
    client
        .execute(
            "SELECT pg_drop_replication_slot($1)",
            &[&slot_name],
        )
        .await
        .map_err(|e| CdcError::Protocol(format!("drop slot '{slot_name}': {e}")))?;

    tracing::info!(slot_name, "dropped replication slot");
    Ok(())
}

/// Query information about a replication slot.
/// Returns None if the slot doesn't exist.
pub async fn get_slot_info(client: &Client, slot_name: &str) -> Result<Option<SlotInfo>> {
    let row = client
        .query_opt(
            "SELECT slot_name, plugin, confirmed_flush_lsn::text, active \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| CdcError::Protocol(format!("query slot '{slot_name}': {e}")))?;

    match row {
        Some(row) => {
            let name: String = row.get(0);
            let plugin: String = row.get(1);
            let flush_lsn_str: Option<String> = row.get(2);
            let active: bool = row.get(3);

            let confirmed_flush_lsn = flush_lsn_str
                .map(|s| {
                    s.parse::<Lsn>()
                        .map_err(|e: CdcError| CdcError::Parse(format!("slot flush LSN: {e}")))
                })
                .transpose()?;

            Ok(Some(SlotInfo {
                slot_name: name,
                plugin,
                confirmed_flush_lsn,
                active,
            }))
        }
        None => Ok(None),
    }
}

/// Check if a replication slot exists.
pub async fn slot_exists(client: &Client, slot_name: &str) -> Result<bool> {
    Ok(get_slot_info(client, slot_name).await?.is_some())
}

