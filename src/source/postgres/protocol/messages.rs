//! Raw pgoutput protocol messages parsed from the WAL stream.
//! These are low-level protocol types — they get converted to CdcEvents by EventConverter.

/// Begin message — marks the start of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginBody {
    /// The final LSN of the transaction.
    pub final_lsn: u64,
    /// Commit timestamp of the transaction (microseconds since 2000-01-01).
    pub timestamp: i64,
    /// Transaction ID.
    pub xid: u32,
}

/// Commit message — marks the end of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBody {
    /// Flags (currently unused in pgoutput v1).
    pub flags: u8,
    /// The LSN of the commit.
    pub commit_lsn: u64,
    /// The end LSN of the transaction.
    pub end_lsn: u64,
    /// Commit timestamp (microseconds since 2000-01-01).
    pub timestamp: i64,
}

/// Relation message — describes the schema of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationBody {
    /// Relation OID.
    pub rel_id: u32,
    /// Namespace (schema name).
    pub namespace: String,
    /// Relation (table) name.
    pub name: String,
    /// Replica identity setting: 'd' (default), 'n' (nothing), 'f' (full), 'i' (index).
    pub replica_identity: u8,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
}

/// Column definition within a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Flags: 1 = part of the key.
    pub flags: u8,
    /// Column name.
    pub name: String,
    /// Data type OID.
    pub type_oid: u32,
    /// Type modifier (e.g., varchar length).
    pub type_modifier: i32,
}

/// Insert message — a new row was inserted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertBody {
    /// Relation OID (identifies which table).
    pub rel_id: u32,
    /// The new tuple data.
    pub new_tuple: TupleData,
}

/// Update message — an existing row was updated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateBody {
    /// Relation OID.
    pub rel_id: u32,
    /// The old key/tuple data (present if REPLICA IDENTITY is set to FULL or uses index).
    /// Prefixed with 'K' (key) or 'O' (old tuple) in the wire format.
    pub old_tuple: Option<TupleData>,
    /// The new tuple data.
    pub new_tuple: TupleData,
}

/// Delete message — a row was deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteBody {
    /// Relation OID.
    pub rel_id: u32,
    /// The old key/tuple data.
    /// Prefixed with 'K' (key) or 'O' (old tuple) in the wire format.
    pub old_tuple: TupleData,
}

/// Truncate message — one or more tables were truncated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateBody {
    /// Number of relations being truncated.
    pub num_relations: u32,
    /// Option flags: 1 = CASCADE, 2 = RESTART IDENTITY.
    pub options: u8,
    /// Relation OIDs of truncated tables.
    pub rel_ids: Vec<u32>,
}

/// Tuple data — the column values for a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub columns: Vec<TupleColumn>,
}

/// A single column value within a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleColumn {
    /// Null value.
    Null,
    /// Unchanged TOAST value (not sent, column data too large).
    UnchangedToast,
    /// Text representation of the value.
    Text(String),
}

/// Top-level pgoutput message enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputMessage {
    Begin(BeginBody),
    Commit(CommitBody),
    Relation(RelationBody),
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
    Truncate(TruncateBody),
}
