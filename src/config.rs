use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::transport::DEFAULT_MAX_FRAME_BYTES;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub log_messages: bool,
    #[serde(default)]
    pub rules: Vec<Rule>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            limits: Limits::default(),
            auth: AuthConfig {
                anonymous: Some(Grant::everything()),
                tokens: Vec::new(),
            },
            rate_limit: None,
            log_messages: false,
            rules: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid config {path}: {source}")]
    Parse {
        path: PathBuf,
        source: Box<toml::de::Error>,
    },
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if self.server.tcp.is_none() && self.server.ws.is_none() && self.server.unix.is_none() {
            return invalid(
                "at least one of server.tcp, server.ws or server.unix must be set".into(),
            );
        }
        if self.limits.max_frame_bytes < 1024 {
            return invalid("limits.max_frame_bytes must be at least 1024".into());
        }
        if self.limits.default_ttl_ms == 0 || self.limits.max_ttl_ms < self.limits.default_ttl_ms {
            return invalid("limits.default_ttl_ms must be > 0 and <= limits.max_ttl_ms".into());
        }
        if self.limits.max_attempts == 0 {
            return invalid("limits.max_attempts must be at least 1".into());
        }
        let limits = &self.limits;
        let positive = [
            ("max_queue_per_target", limits.max_queue_per_target),
            ("max_pending", limits.max_pending),
            ("max_pending_bytes", limits.max_pending_bytes),
            (
                "max_pending_per_principal",
                limits.max_pending_per_principal,
            ),
            (
                "max_pending_bytes_per_principal",
                limits.max_pending_bytes_per_principal,
            ),
            (
                "max_subscriptions_per_session",
                limits.max_subscriptions_per_session,
            ),
            (
                "max_subscriptions_per_principal",
                limits.max_subscriptions_per_principal,
            ),
            ("max_targets", limits.max_targets),
            (
                "max_targets_per_principal",
                limits.max_targets_per_principal,
            ),
        ];
        if let Some((name, _)) = positive.iter().find(|(_, value)| *value == 0) {
            return invalid(format!("limits.{name} must be at least 1"));
        }
        if limits.max_pending_per_principal > limits.max_pending
            || limits.max_pending_bytes_per_principal > limits.max_pending_bytes
            || limits.max_subscriptions_per_session > limits.max_subscriptions_per_principal
            || limits.max_targets_per_principal > limits.max_targets
        {
            return invalid(
                "per-principal and per-session limits must not exceed their totals".into(),
            );
        }
        for token in &self.auth.tokens {
            let ok =
                token.sha256.len() == 64 && token.sha256.bytes().all(|b| b.is_ascii_hexdigit());
            if !ok {
                return invalid(format!(
                    "auth.tokens '{}': sha256 must be 64 hex characters",
                    token.name
                ));
            }
        }
        if let Some(rate) = &self.rate_limit
            && (rate.per_second <= 0.0 || rate.burst < 1.0)
        {
            return invalid("rate_limit.per_second must be > 0 and rate_limit.burst >= 1".into());
        }
        for rule in &self.rules {
            if rule.route_to.is_empty() {
                return invalid(format!(
                    "rule for '{}' has an empty route_to",
                    rule.when_down
                ));
            }
            if rule.route_to.contains(&rule.when_down) {
                return invalid(format!("rule for '{}' routes to itself", rule.when_down));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub tcp: Option<String>,
    pub ws: Option<String>,
    pub unix: Option<PathBuf>,
    #[serde(default = "default_unix_mode")]
    pub unix_mode: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            tcp: Some("127.0.0.1:7878".into()),
            ws: None,
            unix: None,
            unix_mode: default_unix_mode(),
        }
    }
}

fn default_unix_mode() -> u32 {
    0o600
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_queue_per_target: usize,
    pub default_ttl_ms: u64,
    pub max_ttl_ms: u64,
    pub max_attempts: u32,
    pub max_backoff_ms: u64,
    pub handshake_timeout_ms: u64,
    pub session_buffer: usize,
    pub max_pending: usize,
    pub max_pending_bytes: usize,
    pub max_pending_per_principal: usize,
    pub max_pending_bytes_per_principal: usize,
    pub max_subscriptions_per_session: usize,
    pub max_subscriptions_per_principal: usize,
    pub max_targets: usize,
    pub max_targets_per_principal: usize,
    pub offline_target_retention_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_queue_per_target: 1000,
            default_ttl_ms: 30_000,
            max_ttl_ms: 3_600_000,
            max_attempts: 10,
            max_backoff_ms: 60_000,
            handshake_timeout_ms: 5_000,
            session_buffer: 1024,
            max_pending: 100_000,
            max_pending_bytes: 256 * 1024 * 1024,
            max_pending_per_principal: 10_000,
            max_pending_bytes_per_principal: 64 * 1024 * 1024,
            max_subscriptions_per_session: 256,
            max_subscriptions_per_principal: 4_096,
            max_targets: 10_000,
            max_targets_per_principal: 1_000,
            offline_target_retention_ms: 3_600_000,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub anonymous: Option<Grant>,
    #[serde(default)]
    pub tokens: Vec<TokenConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    #[serde(default)]
    pub caps: Vec<String>,
    #[serde(default)]
    pub register: Vec<String>,
    #[serde(default)]
    pub send: Vec<String>,
    #[serde(default)]
    pub groups: Vec<String>,
}

impl Grant {
    pub fn everything() -> Self {
        Self {
            caps: vec!["*".into()],
            register: vec!["*".into()],
            send: vec!["*".into()],
            groups: vec!["*".into()],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    pub name: String,
    pub sha256: String,
    #[serde(flatten)]
    pub grant: Grant,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    pub per_second: f64,
    pub burst: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub when_down: String,
    pub route_to: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_config_parses() {
        let config: Config = toml::from_str(
            r#"
            log_messages = true

            [server]
            tcp = "0.0.0.0:7878"
            ws = "127.0.0.1:7879"
            unix = "/run/sarca/sarca.sock"

            [limits]
            max_queue_per_target = 10

            [auth.anonymous]
            send = ["public-*"]

            [[auth.tokens]]
            name = "ops"
            sha256 = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
            caps = ["game.admin"]
            register = ["game-*"]
            send = ["*"]
            groups = ["games"]

            [rate_limit]
            per_second = 50
            burst = 100

            [[rules]]
            when_down = "game-1"
            route_to = ["game-2", "group:games"]
            "#,
        )
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.limits.max_queue_per_target, 10);
        assert_eq!(config.limits.default_ttl_ms, 30_000);
        assert_eq!(config.auth.tokens[0].grant.caps, vec!["game.admin"]);
        assert_eq!(config.auth.tokens[0].grant.groups, vec!["games"]);
        assert_eq!(config.rules[0].route_to.len(), 2);
    }

    #[test]
    fn semantically_invalid_configs_fail_validation() {
        let cases = [
            "[server]\n",
            "[[auth.tokens]]\nname = \"x\"\nsha256 = \"abc\"\n",
            "[[rules]]\nwhen_down = \"a\"\nroute_to = [\"a\"]\n",
            "[[rules]]\nwhen_down = \"a\"\nroute_to = []\n",
            "[rate_limit]\nper_second = 0\nburst = 1\n",
            "[limits]\nmax_attempts = 0\n",
            "[limits]\nmax_pending = 0\n",
            "[limits]\nmax_targets = 10\nmax_targets_per_principal = 20\n",
            "[limits]\nmax_subscriptions_per_session = 0\n",
            "[limits]\ndefault_ttl_ms = 10\nmax_ttl_ms = 5\n",
        ];
        for case in cases {
            let config: Config = toml::from_str(case).unwrap_or_else(|err| panic!("{case}: {err}"));
            assert!(config.validate().is_err(), "accepted: {case}");
        }
    }

    #[test]
    fn unknown_keys_fail_parsing() {
        assert!(toml::from_str::<Config>("bogus = 1").is_err());
        assert!(toml::from_str::<Config>("[server]\ntpc = \"x\"\n").is_err());
    }

    #[test]
    fn file_config_without_auth_denies_everyone() {
        let config: Config = toml::from_str("").unwrap();
        config.validate().unwrap();
        assert!(config.auth.anonymous.is_none());
        assert!(config.auth.tokens.is_empty());
    }
}
