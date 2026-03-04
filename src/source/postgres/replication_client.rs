use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{CdcError, Result};

/// Events received during logical replication streaming (CopyBoth mode).
#[derive(Debug)]
pub enum ReplicationEvent {
    /// XLogData: contains WAL data from the server.
    XLogData {
        /// The starting point of the WAL data in this message.
        wal_start: u64,
        /// The current end of WAL on the server.
        wal_end: u64,
        /// The server's system clock at the time of transmission (microseconds since 2000-01-01).
        timestamp: i64,
        /// The raw WAL data (pgoutput payload).
        data: Vec<u8>,
    },
    /// Primary keepalive: server heartbeat.
    Keepalive {
        /// Current end of WAL on the server.
        wal_end: u64,
        /// The server's system clock (microseconds since 2000-01-01).
        timestamp: i64,
        /// True if the server is requesting a reply.
        reply_requested: bool,
    },
}

/// A minimal PostgreSQL replication client that handles:
/// - TCP connection with startup message
/// - SCRAM-SHA-256 authentication
/// - Simple query execution (for replication commands)
/// - CopyBoth mode for streaming WAL data
pub struct ReplicationClient {
    stream: TcpStream,
    buf: BytesMut,
}

impl ReplicationClient {
    /// Connect to PostgreSQL and perform startup + authentication.
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: Option<&str>,
        database: &str,
    ) -> Result<Self> {
        let addr = format!("{host}:{port}");
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| CdcError::Protocol(format!("failed to connect to {addr}: {e}")))?;

        let mut client = Self {
            stream,
            buf: BytesMut::with_capacity(8192),
        };

        client.send_startup(user, database).await?;
        client.handle_auth(password).await?;
        client.wait_for_ready().await?;

        Ok(client)
    }

    /// Send the startup message with replication=database.
    async fn send_startup(&mut self, user: &str, database: &str) -> Result<()> {
        let mut msg = BytesMut::new();

        // Length placeholder (will be filled at the end)
        msg.put_i32(0);
        // Protocol version 3.0
        msg.put_i32(196608); // 3 << 16
        // Parameters
        put_cstring(&mut msg, "user");
        put_cstring(&mut msg, user);
        put_cstring(&mut msg, "database");
        put_cstring(&mut msg, database);
        put_cstring(&mut msg, "replication");
        put_cstring(&mut msg, "database");
        // Terminating null byte
        msg.put_u8(0);

        // Fill in the length
        let len = msg.len() as i32;
        msg[0..4].copy_from_slice(&len.to_be_bytes());

        self.stream.write_all(&msg).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Handle the authentication exchange.
    async fn handle_auth(&mut self, password: Option<&str>) -> Result<()> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'R' => {
                    if payload.len() < 4 {
                        return Err(CdcError::Protocol("auth message too short".into()));
                    }
                    let auth_type = i32::from_be_bytes(payload[0..4].try_into().unwrap());
                    match auth_type {
                        0 => return Ok(()), // AuthenticationOk
                        10 => {
                            // AuthenticationSASL — initiate SCRAM-SHA-256
                            self.handle_sasl_auth(password).await?;
                            return Ok(());
                        }
                        3 => {
                            // CleartextPassword
                            let pw = password.ok_or_else(|| {
                                CdcError::Config("password required but not provided".into())
                            })?;
                            self.send_password(pw).await?;
                        }
                        5 => {
                            // MD5Password
                            return Err(CdcError::Protocol(
                                "MD5 auth not supported, use SCRAM-SHA-256".into(),
                            ));
                        }
                        _ => {
                            return Err(CdcError::Protocol(format!(
                                "unsupported auth type: {auth_type}"
                            )));
                        }
                    }
                }
                b'E' => {
                    let err_msg = parse_error_response(&payload);
                    return Err(CdcError::Protocol(format!("auth error: {err_msg}")));
                }
                _ => {
                    // Ignore other messages during auth (e.g., NegotiateProtocolVersion)
                }
            }
        }
    }

    /// Handle SCRAM-SHA-256 authentication.
    async fn handle_sasl_auth(&mut self, password: Option<&str>) -> Result<()> {
        let password = password
            .ok_or_else(|| CdcError::Config("password required for SCRAM-SHA-256 auth".into()))?;

        // Use postgres-protocol's SASL implementation
        // We use ChannelBinding::unsupported() since we're on a plain TCP connection
        let channel_binding =
            postgres_protocol::authentication::sasl::ChannelBinding::unsupported();
        let mut sasl = postgres_protocol::authentication::sasl::ScramSha256::new(
            password.as_bytes(),
            channel_binding,
        );

        // Send SASLInitialResponse
        let client_first = sasl.message();
        self.send_sasl_initial_response("SCRAM-SHA-256", client_first)
            .await?;

        // Read AuthenticationSASLContinue (type 11)
        let (tag, payload) = self.read_message().await?;
        if tag != b'R' {
            return Err(CdcError::Protocol(format!(
                "expected AuthenticationSASLContinue, got tag {tag}"
            )));
        }
        if payload.len() < 4 {
            return Err(CdcError::Protocol("SASL continue message too short".into()));
        }
        let auth_type = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        if auth_type != 11 {
            return Err(CdcError::Protocol(format!(
                "expected AuthenticationSASLContinue (11), got {auth_type}"
            )));
        }
        let server_first = &payload[4..];

        // Process server-first message and generate client-final
        sasl.update(server_first)
            .map_err(|e| CdcError::Protocol(format!("SCRAM update error: {e}")))?;
        let client_final = sasl.message();
        self.send_sasl_response(client_final).await?;

        // Read AuthenticationSASLFinal (type 12)
        let (tag, payload) = self.read_message().await?;
        if tag != b'R' {
            return Err(CdcError::Protocol(format!(
                "expected AuthenticationSASLFinal, got tag {tag}"
            )));
        }
        if payload.len() < 4 {
            return Err(CdcError::Protocol("SASL final message too short".into()));
        }
        let auth_type = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        if auth_type != 12 {
            return Err(CdcError::Protocol(format!(
                "expected AuthenticationSASLFinal (12), got {auth_type}"
            )));
        }
        let server_final = &payload[4..];
        sasl.finish(server_final)
            .map_err(|e| CdcError::Protocol(format!("SCRAM finish error: {e}")))?;

        // Read AuthenticationOk (type 0)
        let (tag, payload) = self.read_message().await?;
        if tag != b'R' || payload.len() < 4 {
            return Err(CdcError::Protocol("expected AuthenticationOk".into()));
        }
        let auth_type = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        if auth_type != 0 {
            return Err(CdcError::Protocol(format!(
                "expected AuthenticationOk (0), got {auth_type}"
            )));
        }

        Ok(())
    }

    async fn send_password(&mut self, password: &str) -> Result<()> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'p');
        let len = 4 + password.len() + 1;
        msg.put_i32(len as i32);
        msg.extend_from_slice(password.as_bytes());
        msg.put_u8(0);
        self.stream.write_all(&msg).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn send_sasl_initial_response(
        &mut self,
        mechanism: &str,
        data: &[u8],
    ) -> Result<()> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'p');
        let len = 4 + mechanism.len() + 1 + 4 + data.len();
        msg.put_i32(len as i32);
        msg.extend_from_slice(mechanism.as_bytes());
        msg.put_u8(0); // null terminator for mechanism name
        msg.put_i32(data.len() as i32);
        msg.extend_from_slice(data);
        self.stream.write_all(&msg).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn send_sasl_response(&mut self, data: &[u8]) -> Result<()> {
        let mut msg = BytesMut::new();
        msg.put_u8(b'p');
        let len = 4 + data.len();
        msg.put_i32(len as i32);
        msg.extend_from_slice(data);
        self.stream.write_all(&msg).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Wait for ReadyForQuery after successful authentication.
    async fn wait_for_ready(&mut self) -> Result<()> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'Z' => return Ok(()), // ReadyForQuery
                b'E' => {
                    let err_msg = parse_error_response(&payload);
                    return Err(CdcError::Protocol(format!("server error: {err_msg}")));
                }
                b'K' | b'S' => {
                    // BackendKeyData or ParameterStatus — skip
                }
                _ => {
                    // Skip unknown messages during startup
                }
            }
        }
    }

    /// Execute a simple query. Returns the raw row data as strings.
    /// Used for replication commands like IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, etc.
    pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<String>>> {
        // Send Query message
        let mut msg = BytesMut::new();
        msg.put_u8(b'Q');
        let len = 4 + sql.len() + 1;
        msg.put_i32(len as i32);
        msg.extend_from_slice(sql.as_bytes());
        msg.put_u8(0);
        self.stream.write_all(&msg).await?;
        self.stream.flush().await?;

        let mut rows = Vec::new();
        let mut num_cols = 0usize;

        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'T' => {
                    // RowDescription
                    if payload.len() >= 2 {
                        num_cols =
                            i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
                    }
                }
                b'D' => {
                    // DataRow
                    let row = parse_data_row(&payload, num_cols)?;
                    rows.push(row);
                }
                b'C' => {
                    // CommandComplete
                }
                b'Z' => {
                    // ReadyForQuery
                    return Ok(rows);
                }
                b'E' => {
                    let err_msg = parse_error_response(&payload);
                    return Err(CdcError::Protocol(format!("query error: {err_msg}")));
                }
                b'W' => {
                    // CopyBothResponse — we've entered replication mode
                    return Ok(rows);
                }
                _ => {
                    // Skip unknown messages
                }
            }
        }
    }

    /// Read the next replication event from the CopyBoth stream.
    /// Returns None if the server closed the stream (CopyDone).
    pub async fn next_event(&mut self) -> Result<Option<ReplicationEvent>> {
        loop {
            let (tag, payload) = self.read_message().await?;
            tracing::debug!(tag = ?( tag as char), payload_len = payload.len(), "replication message received");
            match tag {
                b'd' => {
                    // CopyData
                    if payload.is_empty() {
                        continue;
                    }
                    match payload[0] {
                        b'w' => {
                            // XLogData
                            if payload.len() < 1 + 8 + 8 + 8 {
                                return Err(CdcError::Protocol(
                                    "XLogData message too short".into(),
                                ));
                            }
                            let mut buf = &payload[1..];
                            let wal_start = buf.get_u64();
                            let wal_end = buf.get_u64();
                            let timestamp = buf.get_i64();
                            let data = buf.to_vec();
                            return Ok(Some(ReplicationEvent::XLogData {
                                wal_start,
                                wal_end,
                                timestamp,
                                data,
                            }));
                        }
                        b'k' => {
                            // Primary keepalive
                            if payload.len() < 1 + 8 + 8 + 1 {
                                return Err(CdcError::Protocol(
                                    "Keepalive message too short".into(),
                                ));
                            }
                            let mut buf = &payload[1..];
                            let wal_end = buf.get_u64();
                            let timestamp = buf.get_i64();
                            let reply_requested = buf.get_u8() != 0;
                            return Ok(Some(ReplicationEvent::Keepalive {
                                wal_end,
                                timestamp,
                                reply_requested,
                            }));
                        }
                        other => {
                            return Err(CdcError::Protocol(format!(
                                "unknown CopyData sub-message type: 0x{other:02x}"
                            )));
                        }
                    }
                }
                b'c' => {
                    // CopyDone
                    tracing::info!("received CopyDone from server");
                    return Ok(None);
                }
                b'E' => {
                    let err_msg = parse_error_response(&payload);
                    return Err(CdcError::Protocol(format!(
                        "replication error: {err_msg}"
                    )));
                }
                _ => {
                    tracing::warn!(tag = ?( tag as char), "skipping unexpected replication message");
                }
            }
        }
    }

    /// Send a Standby Status Update to acknowledge WAL processing progress.
    pub async fn send_standby_status_update(
        &mut self,
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        timestamp: i64,
        reply_requested: bool,
    ) -> Result<()> {
        let mut inner = BytesMut::new();
        inner.put_u8(b'r');
        inner.put_u64(write_lsn);
        inner.put_u64(flush_lsn);
        inner.put_u64(apply_lsn);
        inner.put_i64(timestamp);
        inner.put_u8(if reply_requested { 1 } else { 0 });

        // Wrap in CopyData frame
        let mut msg = BytesMut::new();
        msg.put_u8(b'd');
        msg.put_i32((4 + inner.len()) as i32);
        msg.extend_from_slice(&inner);

        self.stream.write_all(&msg).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Read a single PostgreSQL backend message (tag + length-prefixed payload).
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>)> {
        // Ensure we have at least 5 bytes (1 tag + 4 length)
        while self.buf.len() < 5 {
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Err(CdcError::Protocol("connection closed".into()));
            }
        }

        let tag = self.buf[0];
        let len =
            i32::from_be_bytes(self.buf[1..5].try_into().unwrap()) as usize;

        // Total message size: 1 (tag) + len (which includes its own 4 bytes)
        let total = 1 + len;
        while self.buf.len() < total {
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Err(CdcError::Protocol("connection closed mid-message".into()));
            }
        }

        let _ = self.buf.split_to(1); // consume tag
        let _ = self.buf.split_to(4); // consume length
        let payload = self.buf.split_to(len - 4).to_vec();

        Ok((tag, payload))
    }
}

/// Write a null-terminated C string into a BytesMut.
fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Parse an ErrorResponse message into a human-readable string.
fn parse_error_response(payload: &[u8]) -> String {
    let mut msg = String::new();
    let mut buf = payload;
    while !buf.is_empty() {
        let field_type = buf[0];
        buf = &buf[1..];
        if field_type == 0 {
            break;
        }
        if let Some(nul_pos) = buf.iter().position(|&b| b == 0) {
            let value = String::from_utf8_lossy(&buf[..nul_pos]);
            match field_type {
                b'S' => {
                    if !msg.is_empty() {
                        msg.push_str(": ");
                    }
                    msg.push_str(&value);
                }
                b'M' => {
                    if !msg.is_empty() {
                        msg.push_str(": ");
                    }
                    msg.push_str(&value);
                }
                _ => {}
            }
            buf = &buf[nul_pos + 1..];
        } else {
            break;
        }
    }
    if msg.is_empty() {
        msg = "unknown error".into();
    }
    msg
}

/// Parse a DataRow message into a Vec of string values.
fn parse_data_row(payload: &[u8], expected_cols: usize) -> Result<Vec<String>> {
    if payload.len() < 2 {
        return Err(CdcError::Protocol("DataRow too short".into()));
    }
    let num_cols = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    if num_cols != expected_cols {
        return Err(CdcError::Protocol(format!(
            "DataRow column count mismatch: expected {expected_cols}, got {num_cols}"
        )));
    }

    let mut buf = &payload[2..];
    let mut row = Vec::with_capacity(num_cols);

    for _ in 0..num_cols {
        if buf.len() < 4 {
            return Err(CdcError::Protocol("DataRow truncated at column length".into()));
        }
        let col_len = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        buf = &buf[4..];

        if col_len == -1 {
            row.push(String::new()); // NULL → empty string for simple_query usage
        } else {
            let len = col_len as usize;
            if buf.len() < len {
                return Err(CdcError::Protocol("DataRow truncated at column data".into()));
            }
            let value = String::from_utf8_lossy(&buf[..len]).into_owned();
            buf = &buf[len..];
            row.push(value);
        }
    }

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a StandbyStatusUpdate message body (without CopyData framing).
    fn build_standby_status_update(
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        timestamp: i64,
        reply_requested: bool,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(34);
        buf.push(b'r');
        buf.extend_from_slice(&write_lsn.to_be_bytes());
        buf.extend_from_slice(&flush_lsn.to_be_bytes());
        buf.extend_from_slice(&apply_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.push(if reply_requested { 1 } else { 0 });
        buf
    }

    /// Build a startup message for testing.
    fn build_startup_message(user: &str, database: &str) -> Vec<u8> {
        let mut msg = BytesMut::new();
        msg.put_i32(0); // placeholder
        msg.put_i32(196608); // version 3.0
        put_cstring(&mut msg, "user");
        put_cstring(&mut msg, user);
        put_cstring(&mut msg, "database");
        put_cstring(&mut msg, database);
        put_cstring(&mut msg, "replication");
        put_cstring(&mut msg, "database");
        msg.put_u8(0); // terminating null
        let len = msg.len() as i32;
        msg[0..4].copy_from_slice(&len.to_be_bytes());
        msg.to_vec()
    }

    #[test]
    fn test_build_startup_message() {
        let msg = build_startup_message("replicator", "mydb");

        // Should start with length (4 bytes, big-endian)
        let len = i32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, msg.len());

        // Protocol version 3.0
        let version = i32::from_be_bytes(msg[4..8].try_into().unwrap());
        assert_eq!(version, 196608);

        // Should contain parameter strings
        let params = String::from_utf8_lossy(&msg[8..]);
        assert!(params.contains("user"));
        assert!(params.contains("replicator"));
        assert!(params.contains("database"));
        assert!(params.contains("mydb"));
        assert!(params.contains("replication"));
    }

    #[test]
    fn test_build_standby_status_update() {
        let msg = build_standby_status_update(0x100, 0x100, 0x100, 12345, false);

        assert_eq!(msg[0], b'r');
        assert_eq!(msg.len(), 34);

        // Parse back
        let mut buf = &msg[1..];
        let write_lsn = buf.get_u64();
        let flush_lsn = buf.get_u64();
        let apply_lsn = buf.get_u64();
        let timestamp = buf.get_i64();
        let reply = buf.get_u8();

        assert_eq!(write_lsn, 0x100);
        assert_eq!(flush_lsn, 0x100);
        assert_eq!(apply_lsn, 0x100);
        assert_eq!(timestamp, 12345);
        assert_eq!(reply, 0);
    }

    #[test]
    fn test_build_standby_status_update_reply_requested() {
        let msg = build_standby_status_update(0, 0, 0, 0, true);
        assert_eq!(msg[33], 1); // last byte = reply_requested
    }

    #[test]
    fn test_parse_error_response() {
        // Build a fake ErrorResponse payload
        let mut payload = Vec::new();
        payload.push(b'S'); // Severity
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M'); // Message
        payload.extend_from_slice(b"relation does not exist\0");
        payload.push(0); // terminator

        let result = parse_error_response(&payload);
        assert!(result.contains("ERROR"));
        assert!(result.contains("relation does not exist"));
    }

    #[test]
    fn test_parse_error_response_empty() {
        let payload = vec![0]; // just terminator
        let result = parse_error_response(&payload);
        assert_eq!(result, "unknown error");
    }

    #[test]
    fn test_parse_data_row() {
        // Build a DataRow with 2 columns: "42" and "Alice"
        let mut payload = Vec::new();
        payload.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
        payload.extend_from_slice(&2i32.to_be_bytes()); // col 1 len = 2
        payload.extend_from_slice(b"42");
        payload.extend_from_slice(&5i32.to_be_bytes()); // col 2 len = 5
        payload.extend_from_slice(b"Alice");

        let row = parse_data_row(&payload, 2).unwrap();
        assert_eq!(row, vec!["42", "Alice"]);
    }

    #[test]
    fn test_parse_data_row_with_null() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&2i16.to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        payload.extend_from_slice(&3i32.to_be_bytes());
        payload.extend_from_slice(b"Bob");

        let row = parse_data_row(&payload, 2).unwrap();
        assert_eq!(row, vec!["", "Bob"]);
    }

    #[test]
    fn test_parse_data_row_column_count_mismatch() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes());
        payload.extend_from_slice(&2i32.to_be_bytes());
        payload.extend_from_slice(b"42");

        let result = parse_data_row(&payload, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_data_row_truncated() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes());
        // Missing column data
        let result = parse_data_row(&payload, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_put_cstring() {
        let mut buf = BytesMut::new();
        put_cstring(&mut buf, "hello");
        assert_eq!(&buf[..], b"hello\0");
    }

    #[test]
    fn test_put_cstring_empty() {
        let mut buf = BytesMut::new();
        put_cstring(&mut buf, "");
        assert_eq!(&buf[..], b"\0");
    }
}
