use serde::{Deserialize, Serialize};

pub const GROUP_PREFIX: &str = "group:";
pub const MAX_NAME_LEN: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retry {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

impl Default for Retry {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Request,
    Fire,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: u64,
    pub target: String,
    pub action: String,
    #[serde(default)]
    pub caps: Vec<String>,
    #[serde(default)]
    pub ttl: u64,
    #[serde(default)]
    pub retry: Retry,
    #[serde(default)]
    pub mode: Mode,
    #[serde(with = "serde_bytes", default)]
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionSpec {
    pub name: String,
    #[serde(default)]
    pub caps: Vec<String>,
}

impl ActionSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            caps: Vec::new(),
        }
    }

    pub fn with_caps<I, S>(name: impl Into<String>, caps: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            caps: caps.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetInfo {
    pub name: String,
    pub actions: Vec<ActionSpec>,
    pub groups: Vec<String>,
    pub online: bool,
    pub queued: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Protocol,
    Unauthorized,
    Forbidden,
    Invalid,
    NameTaken,
    UnknownAction,
    QueueFull,
    RateLimited,
    Rejected,
    Expired,
    Unavailable,
    TargetFailed,
    Overloaded,
    QuotaExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Hello {
        client: String,
        token: Option<String>,
    },
    Welcome {
        session: u64,
    },
    Register {
        target: String,
        actions: Vec<ActionSpec>,
        #[serde(default)]
        groups: Vec<String>,
    },
    Registered {
        target: String,
    },
    Send(Envelope),
    Deliver(Envelope),
    Accepted {
        id: u64,
    },
    Reply {
        id: u64,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    StreamItem {
        id: u64,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    StreamEnd {
        id: u64,
    },
    Fail {
        id: u64,
        code: ErrorCode,
        message: String,
        #[serde(default)]
        retryable: bool,
    },
    Subscribe {
        id: u64,
        target: String,
        event: String,
    },
    Unsubscribe {
        target: String,
        event: String,
    },
    Emit {
        event: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    Event {
        target: String,
        event: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    ListTargets {
        id: u64,
    },
    Targets {
        id: u64,
        targets: Vec<TargetInfo>,
    },
    Error {
        code: ErrorCode,
        message: String,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
}

impl Frame {
    pub fn kind(&self) -> &'static str {
        match self {
            Frame::Hello { .. } => "hello",
            Frame::Welcome { .. } => "welcome",
            Frame::Register { .. } => "register",
            Frame::Registered { .. } => "registered",
            Frame::Send(_) => "send",
            Frame::Deliver(_) => "deliver",
            Frame::Accepted { .. } => "accepted",
            Frame::Reply { .. } => "reply",
            Frame::StreamItem { .. } => "stream_item",
            Frame::StreamEnd { .. } => "stream_end",
            Frame::Fail { .. } => "fail",
            Frame::Subscribe { .. } => "subscribe",
            Frame::Unsubscribe { .. } => "unsubscribe",
            Frame::Emit { .. } => "emit",
            Frame::Event { .. } => "event",
            Frame::ListTargets { .. } => "list_targets",
            Frame::Targets { .. } => "targets",
            Frame::Error { .. } => "error",
            Frame::Ping { .. } => "ping",
            Frame::Pong { .. } => "pong",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("encode failed: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("decode failed: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}

pub fn encode(frame: &Frame) -> Result<Vec<u8>, WireError> {
    Ok(rmp_serde::to_vec_named(frame)?)
}

pub fn decode(bytes: &[u8]) -> Result<Frame, WireError> {
    Ok(rmp_serde::from_slice(bytes)?)
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_envelope() -> Envelope {
        Envelope {
            id: 42,
            target: "group:games".into(),
            action: "restart".into(),
            caps: vec!["game.admin".into()],
            ttl: 5_000,
            retry: Retry {
                max_attempts: 3,
                backoff_ms: 100,
            },
            mode: Mode::Stream,
            payload: vec![0, 1, 2, 255],
        }
    }

    #[test]
    fn frames_round_trip() {
        let frames = vec![
            Frame::Hello {
                client: "cli".into(),
                token: Some("secret".into()),
            },
            Frame::Send(sample_envelope()),
            Frame::Deliver(sample_envelope()),
            Frame::Register {
                target: "game-1".into(),
                actions: vec![ActionSpec::with_caps("restart", ["game.admin"])],
                groups: vec!["games".into()],
            },
            Frame::Fail {
                id: 7,
                code: ErrorCode::QueueFull,
                message: "full".into(),
                retryable: true,
            },
            Frame::ListTargets { id: 1 },
            Frame::Targets {
                id: 1,
                targets: vec![TargetInfo {
                    name: "game-1".into(),
                    actions: vec![ActionSpec::new("ping")],
                    groups: vec![],
                    online: true,
                    queued: 0,
                }],
            },
            Frame::Event {
                target: "game-1".into(),
                event: "player_joined".into(),
                payload: b"bob".to_vec(),
            },
        ];
        for frame in frames {
            let bytes = encode(&frame).unwrap();
            assert_eq!(decode(&bytes).unwrap(), frame);
        }
    }

    #[derive(Serialize, Deserialize)]
    struct RawSend {
        #[serde(rename = "type")]
        kind: String,
        id: u64,
        target: String,
        action: String,
    }

    #[derive(Deserialize)]
    struct RawError {
        #[serde(rename = "type")]
        kind: String,
        code: String,
        message: String,
    }

    #[test]
    fn frames_encode_as_maps_with_string_tags() {
        let bytes = encode(&Frame::Error {
            code: ErrorCode::QueueFull,
            message: "full".into(),
        })
        .unwrap();
        let raw: RawError = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(raw.kind, "error");
        assert_eq!(raw.code, "queue_full");
        assert_eq!(raw.message, "full");
    }

    #[test]
    fn optional_envelope_fields_default() {
        let bytes = rmp_serde::to_vec_named(&RawSend {
            kind: "send".into(),
            id: 1,
            target: "t".into(),
            action: "a".into(),
        })
        .unwrap();
        let Frame::Send(env) = decode(&bytes).unwrap() else {
            panic!("expected send frame");
        };
        assert_eq!(env.id, 1);
        assert_eq!(env.mode, Mode::Request);
        assert_eq!(env.retry, Retry::default());
        assert_eq!(env.ttl, 0);
        assert!(env.caps.is_empty());
        assert!(env.payload.is_empty());
    }

    #[test]
    fn frame_kind_matches_wire_tag_without_exposing_contents() {
        let frames = [
            Frame::Hello {
                client: "cli".into(),
                token: Some("secret".into()),
            },
            Frame::Deliver(sample_envelope()),
            Frame::StreamItem {
                id: 1,
                payload: b"private".to_vec(),
            },
            Frame::ListTargets { id: 1 },
        ];
        for frame in frames {
            let tag: RawTag = rmp_serde::from_slice(&encode(&frame).unwrap()).unwrap();
            assert_eq!(frame.kind(), tag.kind);
            assert!(!frame.kind().contains("secret") && !frame.kind().contains("private"));
        }
    }

    #[derive(Deserialize)]
    struct RawTag {
        #[serde(rename = "type")]
        kind: String,
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(decode(&[0xc1, 0x00, 0x13]).is_err());
    }

    #[test]
    fn names_are_validated() {
        assert!(valid_name("game-1.eu/west"));
        assert!(!valid_name(""));
        assert!(!valid_name("has space"));
        assert!(!valid_name(&"x".repeat(MAX_NAME_LEN + 1)));
    }
}
