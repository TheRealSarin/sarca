use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crate::config::RateLimit;
use crate::wire::{Envelope, ErrorCode};

pub struct HookContext<'a> {
    pub session: u64,
    pub principal: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub code: ErrorCode,
    pub message: String,
}

impl Rejection {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub trait Hook: Send + Sync {
    fn on_send(&self, ctx: &HookContext<'_>, envelope: &mut Envelope) -> Result<(), Rejection>;

    fn on_disconnect(&self, _session: u64) {}
}

pub struct LoggingHook;

impl Hook for LoggingHook {
    fn on_send(&self, ctx: &HookContext<'_>, envelope: &mut Envelope) -> Result<(), Rejection> {
        tracing::info!(
            session = ctx.session,
            principal = ctx.principal,
            id = envelope.id,
            target = %envelope.target,
            action = %envelope.action,
            mode = ?envelope.mode,
            payload_bytes = envelope.payload.len(),
            "send"
        );
        Ok(())
    }
}

pub struct RateLimitHook {
    limit: RateLimit,
    buckets: Mutex<HashMap<u64, Bucket>>,
}

struct Bucket {
    tokens: f64,
    updated: Instant,
}

impl RateLimitHook {
    pub fn new(limit: RateLimit) -> Self {
        Self {
            limit,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    fn take(&self, session: u64, now: Instant) -> bool {
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let bucket = buckets.entry(session).or_insert(Bucket {
            tokens: self.limit.burst,
            updated: now,
        });
        let elapsed = now.saturating_duration_since(bucket.updated).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.limit.per_second).min(self.limit.burst);
        bucket.updated = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

impl Hook for RateLimitHook {
    fn on_send(&self, ctx: &HookContext<'_>, _envelope: &mut Envelope) -> Result<(), Rejection> {
        if self.take(ctx.session, Instant::now()) {
            Ok(())
        } else {
            Err(Rejection::new(
                ErrorCode::RateLimited,
                "rate limit exceeded",
            ))
        }
    }

    fn on_disconnect(&self, session: u64) {
        self.buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&session);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bucket_allows_burst_then_refills() {
        let hook = RateLimitHook::new(RateLimit {
            per_second: 10.0,
            burst: 2.0,
        });
        let start = Instant::now();
        assert!(hook.take(1, start));
        assert!(hook.take(1, start));
        assert!(!hook.take(1, start));
        assert!(hook.take(2, start));
        assert!(hook.take(1, start + Duration::from_millis(100)));
        assert!(!hook.take(1, start + Duration::from_millis(100)));
    }

    #[test]
    fn disconnect_forgets_bucket() {
        let hook = RateLimitHook::new(RateLimit {
            per_second: 1.0,
            burst: 1.0,
        });
        let now = Instant::now();
        assert!(hook.take(1, now));
        hook.on_disconnect(1);
        assert!(hook.take(1, now));
    }
}
