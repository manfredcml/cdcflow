use mongodb::bson::Timestamp;
use mongodb::change_stream::event::ResumeToken;
use serde::{Deserialize, Serialize};

use crate::error::{CdcError, Result};

/// A cluster time (BSON timestamp) in a form that round-trips through plain JSON.
///
/// `bson::Timestamp`'s own serde representation depends on the serializer, so the
/// two components are stored explicitly instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterTime {
    pub time: u32,
    pub increment: u32,
}

impl From<Timestamp> for ClusterTime {
    fn from(ts: Timestamp) -> Self {
        Self {
            time: ts.time,
            increment: ts.increment,
        }
    }
}

impl From<ClusterTime> for Timestamp {
    fn from(ct: ClusterTime) -> Self {
        Timestamp {
            time: ct.time,
            increment: ct.increment,
        }
    }
}

/// The persisted offset for a MongoDB source.
///
/// Externally tagged so every stored offset carries an unambiguous kind. A bare
/// resume token must never be stored on its own: `ResumeToken` deserializes from
/// *any* JSON object, so an untagged offset from the snapshot phase would silently
/// be accepted as a (bogus) resume token and rejected by the server on resume.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MongoOffset {
    /// Snapshot is partially done. Snapshots are not resumable mid-way, so a
    /// restart from this offset re-runs the whole snapshot.
    SnapshotInProgress { collection: String, count: u64 },
    /// Snapshot finished for all collections. The change stream starts at the
    /// cluster time recorded before the snapshot began.
    SnapshotComplete { cluster_time: ClusterTime },
    /// Streaming position: an opaque change stream resume token.
    Token(ResumeToken),
}

impl MongoOffset {
    /// Serialize to the JSON string persisted by the offset store.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| CdcError::Mongodb(format!("failed to serialize offset: {e}")))
    }

    /// Parse an offset previously produced by [`MongoOffset::to_json`].
    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|e| {
            CdcError::Mongodb(format!(
                "failed to parse stored offset ({e}); offset value: {s}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> ResumeToken {
        // Resume tokens are opaque documents of the form {"_data": "<hex>"}.
        serde_json::from_str(r#"{"_data":"8265F1A2B3000000012B"}"#).unwrap()
    }

    #[test]
    fn test_token_offset_round_trip() {
        let offset = MongoOffset::Token(sample_token());
        let json = offset.to_json().unwrap();

        match MongoOffset::from_json(&json).unwrap() {
            MongoOffset::Token(token) => {
                assert_eq!(
                    serde_json::to_string(&token).unwrap(),
                    serde_json::to_string(&sample_token()).unwrap()
                );
            }
            other => panic!("expected Token, got {other:?}"),
        }
    }

    #[test]
    fn test_snapshot_complete_round_trip() {
        let offset = MongoOffset::SnapshotComplete {
            cluster_time: ClusterTime {
                time: 1700000000,
                increment: 7,
            },
        };
        let json = offset.to_json().unwrap();

        match MongoOffset::from_json(&json).unwrap() {
            MongoOffset::SnapshotComplete { cluster_time } => {
                assert_eq!(cluster_time.time, 1700000000);
                assert_eq!(cluster_time.increment, 7);
            }
            other => panic!("expected SnapshotComplete, got {other:?}"),
        }
    }

    #[test]
    fn test_snapshot_in_progress_round_trip() {
        let offset = MongoOffset::SnapshotInProgress {
            collection: "users".into(),
            count: 1000,
        };
        let json = offset.to_json().unwrap();

        match MongoOffset::from_json(&json).unwrap() {
            MongoOffset::SnapshotInProgress { collection, count } => {
                assert_eq!(collection, "users");
                assert_eq!(count, 1000);
            }
            other => panic!("expected SnapshotInProgress, got {other:?}"),
        }
    }

    #[test]
    fn test_snapshot_offset_is_not_parsed_as_resume_token() {
        // The old (buggy) snapshot offset format must be rejected rather than
        // silently accepted as a resume token.
        let legacy = r#"{"snapshot_complete":"users","count":42}"#;
        assert!(MongoOffset::from_json(legacy).is_err());
    }

    #[test]
    fn test_arbitrary_json_is_rejected() {
        for input in ["", "null", "\"token\"", "{}", r#"{"unknown":{}}"#] {
            assert!(
                MongoOffset::from_json(input).is_err(),
                "expected rejection for {input}"
            );
        }
    }

    #[test]
    fn test_cluster_time_timestamp_conversion() {
        let ts = Timestamp {
            time: 42,
            increment: 3,
        };
        let round_tripped: Timestamp = ClusterTime::from(ts).into();
        assert_eq!(round_tripped.time, 42);
        assert_eq!(round_tripped.increment, 3);
    }
}
