use bytes::Buf;

use crate::error::{CdcError, Result};

use super::messages::*;
use super::tuple::parse_tuple_data;

/// Read a null-terminated C string from the buffer.
fn read_cstring(buf: &mut &[u8]) -> Result<String> {
    let nul_pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| CdcError::Protocol("missing null terminator in string".into()))?;
    let s = std::str::from_utf8(&buf[..nul_pos])
        .map_err(|e| CdcError::Protocol(format!("invalid UTF-8 in string: {e}")))?
        .to_string();
    buf.advance(nul_pos + 1); // skip string + null terminator
    Ok(s)
}

/// Parse a raw pgoutput message payload into a typed message.
///
/// The first byte is the message type tag:
/// - `B` = Begin
/// - `C` = Commit
/// - `R` = Relation
/// - `I` = Insert
/// - `U` = Update
/// - `D` = Delete
/// - `T` = Truncate
pub fn parse(data: &[u8]) -> Result<PgOutputMessage> {
    if data.is_empty() {
        return Err(CdcError::Protocol("empty pgoutput message".into()));
    }

    let tag = data[0];
    let mut buf = &data[1..];

    match tag {
        b'B' => parse_begin(&mut buf),
        b'C' => parse_commit(&mut buf),
        b'R' => parse_relation(&mut buf),
        b'I' => parse_insert(&mut buf),
        b'U' => parse_update(&mut buf),
        b'D' => parse_delete(&mut buf),
        b'T' => parse_truncate(&mut buf),
        _ => Err(CdcError::Protocol(format!(
            "unknown pgoutput message type: 0x{tag:02x} ('{}')",
            tag as char
        ))),
    }
}

fn parse_begin(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 8 + 8 + 4 {
        return Err(CdcError::Protocol("Begin message truncated".into()));
    }
    let final_lsn = buf.get_u64();
    let timestamp = buf.get_i64();
    let xid = buf.get_u32();
    Ok(PgOutputMessage::Begin(BeginBody {
        final_lsn,
        timestamp,
        xid,
    }))
}

fn parse_commit(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 1 + 8 + 8 + 8 {
        return Err(CdcError::Protocol("Commit message truncated".into()));
    }
    let flags = buf.get_u8();
    let commit_lsn = buf.get_u64();
    let end_lsn = buf.get_u64();
    let timestamp = buf.get_i64();
    Ok(PgOutputMessage::Commit(CommitBody {
        flags,
        commit_lsn,
        end_lsn,
        timestamp,
    }))
}

fn parse_relation(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 4 {
        return Err(CdcError::Protocol("Relation message truncated".into()));
    }
    let rel_id = buf.get_u32();
    let namespace = read_cstring(buf)?;
    let name = read_cstring(buf)?;

    if buf.remaining() < 1 + 2 {
        return Err(CdcError::Protocol(
            "Relation message truncated at replica identity".into(),
        ));
    }
    let replica_identity = buf.get_u8();
    let num_cols = buf.get_i16() as usize;

    let mut columns = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        if buf.remaining() < 1 {
            return Err(CdcError::Protocol(
                "Relation column truncated at flags".into(),
            ));
        }
        let flags = buf.get_u8();
        let col_name = read_cstring(buf)?;
        if buf.remaining() < 4 + 4 {
            return Err(CdcError::Protocol(
                "Relation column truncated at type info".into(),
            ));
        }
        let type_oid = buf.get_u32();
        let type_modifier = buf.get_i32();
        columns.push(ColumnDef {
            flags,
            name: col_name,
            type_oid,
            type_modifier,
        });
    }

    Ok(PgOutputMessage::Relation(RelationBody {
        rel_id,
        namespace,
        name,
        replica_identity,
        columns,
    }))
}

fn parse_insert(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 4 + 1 {
        return Err(CdcError::Protocol("Insert message truncated".into()));
    }
    let rel_id = buf.get_u32();
    let tag = buf.get_u8();
    if tag != b'N' {
        return Err(CdcError::Protocol(format!(
            "Insert: expected 'N' tag, got 0x{tag:02x}"
        )));
    }
    let new_tuple = parse_tuple_data(buf)?;
    Ok(PgOutputMessage::Insert(InsertBody { rel_id, new_tuple }))
}

fn parse_update(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 4 + 1 {
        return Err(CdcError::Protocol("Update message truncated".into()));
    }
    let rel_id = buf.get_u32();
    let tag = buf.get_u8();

    let (old_tuple, new_tuple) = match tag {
        // 'K' = old key tuple, 'O' = old full tuple; both followed by 'N' new tuple
        b'K' | b'O' => {
            let old = parse_tuple_data(buf)?;
            if buf.remaining() < 1 {
                return Err(CdcError::Protocol(
                    "Update: missing 'N' tag after old tuple".into(),
                ));
            }
            let n_tag = buf.get_u8();
            if n_tag != b'N' {
                return Err(CdcError::Protocol(format!(
                    "Update: expected 'N' after old tuple, got 0x{n_tag:02x}"
                )));
            }
            let new = parse_tuple_data(buf)?;
            (Some(old), new)
        }
        // 'N' = new tuple only (no old tuple sent)
        b'N' => {
            let new = parse_tuple_data(buf)?;
            (None, new)
        }
        _ => {
            return Err(CdcError::Protocol(format!(
                "Update: unexpected tag 0x{tag:02x}"
            )));
        }
    };

    Ok(PgOutputMessage::Update(UpdateBody {
        rel_id,
        old_tuple,
        new_tuple,
    }))
}

fn parse_delete(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 4 + 1 {
        return Err(CdcError::Protocol("Delete message truncated".into()));
    }
    let rel_id = buf.get_u32();
    let tag = buf.get_u8();

    // 'K' = key tuple, 'O' = old full tuple
    if tag != b'K' && tag != b'O' {
        return Err(CdcError::Protocol(format!(
            "Delete: expected 'K' or 'O' tag, got 0x{tag:02x}"
        )));
    }
    let old_tuple = parse_tuple_data(buf)?;
    Ok(PgOutputMessage::Delete(DeleteBody { rel_id, old_tuple }))
}

fn parse_truncate(buf: &mut &[u8]) -> Result<PgOutputMessage> {
    if buf.remaining() < 4 + 1 {
        return Err(CdcError::Protocol("Truncate message truncated".into()));
    }
    let num_relations = buf.get_u32();
    let options = buf.get_u8();

    if buf.remaining() < (num_relations as usize) * 4 {
        return Err(CdcError::Protocol("Truncate: not enough relation IDs".into()));
    }
    let mut rel_ids = Vec::with_capacity(num_relations as usize);
    for _ in 0..num_relations {
        rel_ids.push(buf.get_u32());
    }

    Ok(PgOutputMessage::Truncate(TruncateBody {
        num_relations,
        options,
        rel_ids,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_begin(final_lsn: u64, timestamp: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&final_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    fn make_commit(flags: u8, commit_lsn: u64, end_lsn: u64, timestamp: i64) -> Vec<u8> {
        let mut buf = vec![b'C', flags];
        buf.extend_from_slice(&commit_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf
    }

    fn make_relation(
        rel_id: u32,
        namespace: &str,
        name: &str,
        replica_identity: u8,
        columns: &[(u8, &str, u32, i32)],
    ) -> Vec<u8> {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&rel_id.to_be_bytes());
        buf.extend_from_slice(namespace.as_bytes());
        buf.push(0); // null terminator
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.push(replica_identity);
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for &(flags, col_name, type_oid, type_modifier) in columns {
            buf.push(flags);
            buf.extend_from_slice(col_name.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&type_oid.to_be_bytes());
            buf.extend_from_slice(&type_modifier.to_be_bytes());
        }
        buf
    }

    fn make_tuple(columns: &[TupleColumn]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for col in columns {
            match col {
                TupleColumn::Null => buf.push(b'n'),
                TupleColumn::UnchangedToast => buf.push(b'u'),
                TupleColumn::Text(s) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
            }
        }
        buf
    }

    fn make_insert(rel_id: u32, columns: &[TupleColumn]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&rel_id.to_be_bytes());
        buf.push(b'N');
        buf.extend(&make_tuple(columns));
        buf
    }

    #[test]
    fn test_parse_begin() {
        let data = make_begin(0x16B3748, 728_553_600_000_000, 42);
        match parse(&data).unwrap() {
            PgOutputMessage::Begin(b) => {
                assert_eq!(b.final_lsn, 0x16B3748);
                assert_eq!(b.timestamp, 728_553_600_000_000);
                assert_eq!(b.xid, 42);
            }
            _ => panic!("expected Begin"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let data = make_commit(0, 0x16B3748, 0x16B3800, 728_553_600_000_000);
        match parse(&data).unwrap() {
            PgOutputMessage::Commit(c) => {
                assert_eq!(c.flags, 0);
                assert_eq!(c.commit_lsn, 0x16B3748);
                assert_eq!(c.end_lsn, 0x16B3800);
                assert_eq!(c.timestamp, 728_553_600_000_000);
            }
            _ => panic!("expected Commit"),
        }
    }

    #[test]
    fn test_parse_relation() {
        let data = make_relation(
            16384,
            "public",
            "users",
            b'd',
            &[
                (1, "id", 23, -1),
                (0, "name", 25, -1),
                (0, "email", 25, -1),
            ],
        );
        match parse(&data).unwrap() {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.rel_id, 16384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.replica_identity, b'd');
                assert_eq!(r.columns.len(), 3);
                assert_eq!(r.columns[0].flags, 1);
                assert_eq!(r.columns[0].name, "id");
                assert_eq!(r.columns[0].type_oid, 23);
                assert_eq!(r.columns[1].name, "name");
                assert_eq!(r.columns[2].name, "email");
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn test_parse_relation_zero_columns() {
        let data = make_relation(100, "public", "empty_table", b'd', &[]);
        match parse(&data).unwrap() {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.columns.len(), 0);
                assert_eq!(r.name, "empty_table");
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let data = make_insert(
            16384,
            &[
                TupleColumn::Text("1".into()),
                TupleColumn::Text("Alice".into()),
                TupleColumn::Null,
            ],
        );
        match parse(&data).unwrap() {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.rel_id, 16384);
                assert_eq!(i.new_tuple.columns.len(), 3);
                assert_eq!(i.new_tuple.columns[0], TupleColumn::Text("1".into()));
                assert_eq!(i.new_tuple.columns[1], TupleColumn::Text("Alice".into()));
                assert_eq!(i.new_tuple.columns[2], TupleColumn::Null);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_update_new_only() {
        let mut data = vec![b'U'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'N');
        data.extend(&make_tuple(&[TupleColumn::Text("updated".into())]));

        match parse(&data).unwrap() {
            PgOutputMessage::Update(u) => {
                assert_eq!(u.rel_id, 16384);
                assert!(u.old_tuple.is_none());
                assert_eq!(
                    u.new_tuple.columns[0],
                    TupleColumn::Text("updated".into())
                );
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_update_with_old_key() {
        let mut data = vec![b'U'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'K');
        data.extend(&make_tuple(&[TupleColumn::Text("old_id".into())]));
        data.push(b'N');
        data.extend(&make_tuple(&[TupleColumn::Text("new_id".into())]));

        match parse(&data).unwrap() {
            PgOutputMessage::Update(u) => {
                assert!(u.old_tuple.is_some());
                assert_eq!(
                    u.old_tuple.unwrap().columns[0],
                    TupleColumn::Text("old_id".into())
                );
                assert_eq!(
                    u.new_tuple.columns[0],
                    TupleColumn::Text("new_id".into())
                );
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_update_with_old_full() {
        let mut data = vec![b'U'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'O');
        data.extend(&make_tuple(&[
            TupleColumn::Text("1".into()),
            TupleColumn::Text("old_name".into()),
        ]));
        data.push(b'N');
        data.extend(&make_tuple(&[
            TupleColumn::Text("1".into()),
            TupleColumn::Text("new_name".into()),
        ]));

        match parse(&data).unwrap() {
            PgOutputMessage::Update(u) => {
                let old = u.old_tuple.unwrap();
                assert_eq!(old.columns.len(), 2);
                assert_eq!(old.columns[1], TupleColumn::Text("old_name".into()));
                assert_eq!(u.new_tuple.columns[1], TupleColumn::Text("new_name".into()));
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_delete_with_key() {
        let mut data = vec![b'D'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'K');
        data.extend(&make_tuple(&[TupleColumn::Text("1".into())]));

        match parse(&data).unwrap() {
            PgOutputMessage::Delete(d) => {
                assert_eq!(d.rel_id, 16384);
                assert_eq!(d.old_tuple.columns[0], TupleColumn::Text("1".into()));
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_with_old_full() {
        let mut data = vec![b'D'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'O');
        data.extend(&make_tuple(&[
            TupleColumn::Text("1".into()),
            TupleColumn::Text("Alice".into()),
        ]));

        match parse(&data).unwrap() {
            PgOutputMessage::Delete(d) => {
                assert_eq!(d.old_tuple.columns.len(), 2);
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_truncate() {
        let mut data = vec![b'T'];
        data.extend_from_slice(&2u32.to_be_bytes()); // 2 relations
        data.push(0); // no options
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.extend_from_slice(&16385u32.to_be_bytes());

        match parse(&data).unwrap() {
            PgOutputMessage::Truncate(t) => {
                assert_eq!(t.num_relations, 2);
                assert_eq!(t.options, 0);
                assert_eq!(t.rel_ids, vec![16384, 16385]);
            }
            _ => panic!("expected Truncate"),
        }
    }

    #[test]
    fn test_parse_truncate_with_options() {
        let mut data = vec![b'T'];
        data.extend_from_slice(&1u32.to_be_bytes());
        data.push(3); // CASCADE | RESTART IDENTITY
        data.extend_from_slice(&16384u32.to_be_bytes());

        match parse(&data).unwrap() {
            PgOutputMessage::Truncate(t) => {
                assert_eq!(t.options, 3);
                assert_eq!(t.rel_ids, vec![16384]);
            }
            _ => panic!("expected Truncate"),
        }
    }

    #[test]
    fn test_parse_empty_message() {
        assert!(parse(&[]).is_err());
    }

    #[test]
    fn test_parse_unknown_type() {
        assert!(parse(b"Z").is_err());
    }

    #[test]
    fn test_parse_begin_truncated() {
        let data = vec![b'B', 0, 0]; // way too short
        assert!(parse(&data).is_err());
    }

    #[test]
    fn test_parse_commit_truncated() {
        let data = vec![b'C', 0]; // too short
        assert!(parse(&data).is_err());
    }

    #[test]
    fn test_parse_relation_truncated() {
        let data = vec![b'R', 0]; // too short
        assert!(parse(&data).is_err());
    }

    #[test]
    fn test_parse_insert_bad_tag() {
        let mut data = vec![b'I'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'X'); // wrong tag (should be 'N')
        assert!(parse(&data).is_err());
    }

    #[test]
    fn test_parse_delete_bad_tag() {
        let mut data = vec![b'D'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'X'); // wrong tag
        assert!(parse(&data).is_err());
    }

    #[test]
    fn test_parse_relation_empty_namespace() {
        let data = make_relation(1, "", "t", b'd', &[(0, "c", 25, -1)]);
        match parse(&data).unwrap() {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.namespace, "");
                assert_eq!(r.name, "t");
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn test_parse_insert_with_unchanged_toast() {
        let data = make_insert(
            1,
            &[
                TupleColumn::Text("1".into()),
                TupleColumn::UnchangedToast,
            ],
        );
        match parse(&data).unwrap() {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.new_tuple.columns[1], TupleColumn::UnchangedToast);
            }
            _ => panic!("expected Insert"),
        }
    }
}
