use bytes::Buf;

use crate::error::{CdcError, Result};

use super::messages::{TupleColumn, TupleData};

/// Parse tuple data from a pgoutput message buffer.
///
/// Wire format:
///   - i16: number of columns
///   - For each column:
///     - u8: column type tag
///       - 'n' = NULL
///       - 'u' = unchanged TOAST
///       - 't' = text value, followed by:
///         - i32: length of the value in bytes
///         - [u8; length]: the value bytes (UTF-8)
pub fn parse_tuple_data(buf: &mut &[u8]) -> Result<TupleData> {
    if buf.remaining() < 2 {
        return Err(CdcError::Protocol(
            "tuple data truncated: missing column count".into(),
        ));
    }
    let num_cols = buf.get_i16() as usize;
    let mut columns = Vec::with_capacity(num_cols);

    for i in 0..num_cols {
        if buf.remaining() < 1 {
            return Err(CdcError::Protocol(format!(
                "tuple data truncated at column {i}"
            )));
        }
        let tag = buf.get_u8();
        match tag {
            b'n' => columns.push(TupleColumn::Null),
            b'u' => columns.push(TupleColumn::UnchangedToast),
            b't' => {
                if buf.remaining() < 4 {
                    return Err(CdcError::Protocol(format!(
                        "tuple data truncated: missing text length at column {i}"
                    )));
                }
                let len = buf.get_i32() as usize;
                if buf.remaining() < len {
                    return Err(CdcError::Protocol(format!(
                        "tuple data truncated: expected {len} bytes at column {i}, got {}",
                        buf.remaining()
                    )));
                }
                let text_bytes = &buf[..len];
                let text = std::str::from_utf8(text_bytes)
                    .map_err(|e| CdcError::Protocol(format!("invalid UTF-8 at column {i}: {e}")))?
                    .to_string();
                buf.advance(len);
                columns.push(TupleColumn::Text(text));
            }
            _ => {
                return Err(CdcError::Protocol(format!(
                    "unknown tuple column type tag: 0x{tag:02x}"
                )));
            }
        }
    }

    Ok(TupleData { columns })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_tuple() {
        let data: Vec<u8> = vec![0, 0]; // 0 columns
        let tuple = parse_tuple_data(&mut data.as_slice()).unwrap();
        assert!(tuple.columns.is_empty());
    }

    #[test]
    fn test_parse_null_column() {
        let data: Vec<u8> = vec![
            0, 1,    // 1 column
            b'n', // NULL
        ];
        let tuple = parse_tuple_data(&mut data.as_slice()).unwrap();
        assert_eq!(tuple.columns, vec![TupleColumn::Null]);
    }

    #[test]
    fn test_parse_unchanged_toast() {
        let data: Vec<u8> = vec![
            0, 1,    // 1 column
            b'u', // unchanged TOAST
        ];
        let tuple = parse_tuple_data(&mut data.as_slice()).unwrap();
        assert_eq!(tuple.columns, vec![TupleColumn::UnchangedToast]);
    }

    #[test]
    fn test_parse_text_column() {
        let text = "hello";
        let mut data: Vec<u8> = vec![
            0, 1,    // 1 column
            b't', // text
        ];
        data.extend_from_slice(&(text.len() as i32).to_be_bytes());
        data.extend_from_slice(text.as_bytes());

        let tuple = parse_tuple_data(&mut data.as_slice()).unwrap();
        assert_eq!(tuple.columns, vec![TupleColumn::Text("hello".into())]);
    }

    #[test]
    fn test_parse_empty_text_column() {
        let mut data: Vec<u8> = vec![
            0, 1,    // 1 column
            b't', // text
            0, 0, 0, 0, // length 0
        ];
        let _ = &mut data; // suppress unused warning

        let tuple = parse_tuple_data(&mut data.as_slice()).unwrap();
        assert_eq!(tuple.columns, vec![TupleColumn::Text(String::new())]);
    }

    #[test]
    fn test_parse_multiple_columns() {
        let text1 = "42";
        let text2 = "Alice";
        let mut data: Vec<u8> = vec![
            0, 4,    // 4 columns
            b't', // text
        ];
        data.extend_from_slice(&(text1.len() as i32).to_be_bytes());
        data.extend_from_slice(text1.as_bytes());
        data.push(b'n'); // NULL
        data.push(b't'); // text
        data.extend_from_slice(&(text2.len() as i32).to_be_bytes());
        data.extend_from_slice(text2.as_bytes());
        data.push(b'u'); // unchanged TOAST

        let tuple = parse_tuple_data(&mut data.as_slice()).unwrap();
        assert_eq!(
            tuple.columns,
            vec![
                TupleColumn::Text("42".into()),
                TupleColumn::Null,
                TupleColumn::Text("Alice".into()),
                TupleColumn::UnchangedToast,
            ]
        );
    }

    #[test]
    fn test_parse_truncated_column_count() {
        let data: Vec<u8> = vec![0]; // only 1 byte, need 2
        assert!(parse_tuple_data(&mut data.as_slice()).is_err());
    }

    #[test]
    fn test_parse_truncated_at_tag() {
        let data: Vec<u8> = vec![0, 1]; // 1 column but no tag byte
        assert!(parse_tuple_data(&mut data.as_slice()).is_err());
    }

    #[test]
    fn test_parse_truncated_text_length() {
        let data: Vec<u8> = vec![
            0, 1,    // 1 column
            b't', // text
            0, 0, // only 2 bytes of the 4-byte length
        ];
        assert!(parse_tuple_data(&mut data.as_slice()).is_err());
    }

    #[test]
    fn test_parse_truncated_text_data() {
        let data: Vec<u8> = vec![
            0, 1,    // 1 column
            b't', // text
            0, 0, 0, 5, // length 5
            b'h', b'i', // only 2 of 5 bytes
        ];
        assert!(parse_tuple_data(&mut data.as_slice()).is_err());
    }

    #[test]
    fn test_parse_unknown_tag() {
        let data: Vec<u8> = vec![
            0, 1,    // 1 column
            b'x', // unknown tag
        ];
        assert!(parse_tuple_data(&mut data.as_slice()).is_err());
    }
}
