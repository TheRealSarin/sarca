use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::client::{ClientError, RemoteError, open_link, spawn_writer};
use crate::wire::{ActionSpec, Envelope, ErrorCode, Frame, Mode};

#[derive(Debug, Clone)]
pub enum Trigger {
    Every(Duration),
    Connected,
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct LocalRule {
    pub trigger: Trigger,
    pub action: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TargetConfig {
    pub url: String,
    pub token: Option<String>,
    pub name: String,
    pub actions: Vec<ActionSpec>,
    pub groups: Vec<String>,
    pub rules: Vec<LocalRule>,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub max_concurrent: usize,
}

pub const DEFAULT_MAX_CONCURRENT: usize = 64;

impl TargetConfig {
    pub fn new(url: impl Into<String>, name: impl Into<String>, actions: Vec<ActionSpec>) -> Self {
        Self {
            url: url.into(),
            token: None,
            name: name.into(),
            actions,
            groups: Vec::new(),
            rules: Vec::new(),
            reconnect_min: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
            max_concurrent: DEFAULT_MAX_CONCURRENT,
        }
    }
}

pub trait Handler: Send + Sync + 'static {
    fn call(&self, call: Call) -> impl Future<Output = ()> + Send;
}

impl<F, Fut> Handler for F
where
    F: Fn(Call) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    fn call(&self, call: Call) -> impl Future<Output = ()> + Send {
        self(call)
    }
}

pub struct Call {
    envelope: Envelope,
    out: Option<mpsc::Sender<Frame>>,
    answered: bool,
    deadline: Option<Instant>,
}

impl Call {
    fn local(target: &str, rule: &LocalRule) -> Self {
        Self {
            envelope: Envelope {
                id: 0,
                target: target.to_string(),
                action: rule.action.clone(),
                caps: Vec::new(),
                ttl: 0,
                retry: Default::default(),
                mode: Mode::Fire,
                payload: rule.payload.clone(),
            },
            out: None,
            answered: false,
            deadline: None,
        }
    }

    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    pub fn action(&self) -> &str {
        &self.envelope.action
    }

    pub fn payload(&self) -> &[u8] {
        &self.envelope.payload
    }

    pub fn is_local(&self) -> bool {
        self.out.is_none()
    }

    pub async fn reply(mut self, payload: impl Into<Vec<u8>>) -> Result<(), ClientError> {
        self.answered = true;
        let id = self.envelope.id;
        self.respond(Frame::Reply {
            id,
            payload: payload.into(),
        })
        .await
    }

    pub async fn fail(
        mut self,
        code: ErrorCode,
        message: impl Into<String>,
        retryable: bool,
    ) -> Result<(), ClientError> {
        self.answered = true;
        let id = self.envelope.id;
        self.respond(Frame::Fail {
            id,
            code,
            message: message.into(),
            retryable,
        })
        .await
    }

    pub async fn stream_item(&mut self, payload: impl Into<Vec<u8>>) -> Result<(), ClientError> {
        let id = self.envelope.id;
        self.respond(Frame::StreamItem {
            id,
            payload: payload.into(),
        })
        .await
    }

    pub async fn end(mut self) -> Result<(), ClientError> {
        self.answered = true;
        let id = self.envelope.id;
        self.respond(Frame::StreamEnd { id }).await
    }

    async fn respond(&self, frame: Frame) -> Result<(), ClientError> {
        if self.envelope.mode == Mode::Fire {
            return Ok(());
        }
        match &self.out {
            Some(out) => out.send(frame).await.map_err(|_| ClientError::Disconnected),
            None => Ok(()),
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        if self.answered || self.envelope.mode == Mode::Fire {
            return;
        }
        let expired = self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        let (code, message) = if expired {
            (ErrorCode::Expired, "handler did not finish within the ttl")
        } else {
            (ErrorCode::TargetFailed, "handler finished without replying")
        };
        if let Some(out) = &self.out {
            let _ = out.try_send(Frame::Fail {
                id: self.envelope.id,
                code,
                message: message.into(),
                retryable: false,
            });
        }
    }
}

#[derive(Clone, Default)]
pub struct TargetHandle {
    out: Arc<Mutex<Option<mpsc::Sender<Frame>>>>,
}

impl TargetHandle {
    fn set(&self, out: Option<mpsc::Sender<Frame>>) {
        *self
            .out
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = out;
    }

    pub fn is_connected(&self) -> bool {
        self.out
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|out| !out.is_closed())
    }

    pub async fn emit(&self, event: &str, payload: impl Into<Vec<u8>>) -> Result<(), ClientError> {
        let out = self
            .out
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or(ClientError::Disconnected)?;
        out.send(Frame::Emit {
            event: event.to_string(),
            payload: payload.into(),
        })
        .await
        .map_err(|_| ClientError::Disconnected)
    }
}

pub struct Target {
    config: TargetConfig,
    handle: TargetHandle,
}

fn is_fatal(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Unauthorized | ErrorCode::Forbidden | ErrorCode::Invalid
    )
}

impl Target {
    pub fn new(config: TargetConfig) -> Self {
        Self {
            config,
            handle: TargetHandle::default(),
        }
    }

    pub fn handle(&self) -> TargetHandle {
        self.handle.clone()
    }

    pub async fn run<H: Handler>(self, handler: H) -> Result<(), ClientError> {
        let handler = Arc::new(handler);
        let mut background = JoinSet::new();
        for rule in &self.config.rules {
            if let Trigger::Every(period) = rule.trigger {
                let handler = handler.clone();
                let rule = rule.clone();
                let name = self.config.name.clone();
                background.spawn(async move {
                    let mut ticker = tokio::time::interval(period.max(Duration::from_millis(1)));
                    ticker.tick().await;
                    loop {
                        ticker.tick().await;
                        handler.call(Call::local(&name, &rule)).await;
                    }
                });
            }
        }

        let mut delay = self.config.reconnect_min;
        loop {
            match self.session(&handler).await {
                Err(ClientError::Refused(error)) if is_fatal(error.code) => {
                    self.handle.set(None);
                    return Err(ClientError::Refused(error));
                }
                Ok(()) => {
                    delay = self.config.reconnect_min;
                    self.run_rules(&handler, |t| matches!(t, Trigger::Disconnected))
                        .await;
                }
                Err(err) => tracing::debug!(%err, target = %self.config.name, "connect failed"),
            }
            self.handle.set(None);
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(self.config.reconnect_max);
        }
    }

    async fn run_rules<H: Handler>(&self, handler: &Arc<H>, matches: impl Fn(&Trigger) -> bool) {
        for rule in self.config.rules.iter().filter(|r| matches(&r.trigger)) {
            handler.call(Call::local(&self.config.name, rule)).await;
        }
    }

    async fn session<H: Handler>(&self, handler: &Arc<H>) -> Result<(), ClientError> {
        let config = &self.config;
        let link = open_link(&config.url, config.token.as_deref(), &config.name).await?;
        let mut stream = link.stream;
        let closed = CancellationToken::new();
        let out = spawn_writer(link.sink, closed.clone());
        out.send(Frame::Register {
            target: config.name.clone(),
            actions: config.actions.clone(),
            groups: config.groups.clone(),
        })
        .await
        .map_err(|_| ClientError::Disconnected)?;

        let permits = Arc::new(Semaphore::new(config.max_concurrent.max(1)));
        let mut handlers = JoinSet::new();
        let mut registered = false;
        loop {
            let frame = tokio::select! {
                item = stream.next() => match item {
                    Some(Ok(frame)) => frame,
                    _ => break,
                },
                Some(_) = handlers.join_next(), if !handlers.is_empty() => continue,
                () = closed.cancelled() => break,
            };
            match frame {
                Frame::Registered { .. } => {
                    registered = true;
                    tracing::info!(target = %config.name, "registered with bus");
                    self.handle.set(Some(out.clone()));
                    self.run_rules(handler, |t| matches!(t, Trigger::Connected))
                        .await;
                }
                Frame::Deliver(envelope) => {
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        tracing::warn!(
                            target = %config.name,
                            id = envelope.id,
                            action = %envelope.action,
                            "concurrent handler limit reached, rejecting delivery"
                        );
                        if envelope.mode != Mode::Fire {
                            let _ = out
                                .send(Frame::Fail {
                                    id: envelope.id,
                                    code: ErrorCode::Overloaded,
                                    message: "target is at its concurrent handler limit".into(),
                                    retryable: true,
                                })
                                .await;
                        }
                        continue;
                    };
                    let deadline = Instant::now() + Duration::from_millis(envelope.ttl.max(1));
                    let call = Call {
                        envelope,
                        out: Some(out.clone()),
                        answered: false,
                        deadline: Some(deadline),
                    };
                    let handler = handler.clone();
                    handlers.spawn(async move {
                        let _permit = permit;
                        let _ = tokio::time::timeout_at(deadline, handler.call(call)).await;
                    });
                }
                Frame::Ping { nonce } => {
                    let _ = out.send(Frame::Pong { nonce }).await;
                }
                Frame::Error { code, message } => {
                    let error = RemoteError { code, message };
                    tracing::warn!(target = %config.name, %error, "bus error");
                    if !registered {
                        return Err(ClientError::Refused(error));
                    }
                }
                _ => {}
            }
        }
        handlers.shutdown().await;
        self.handle.set(None);
        if registered {
            Ok(())
        } else {
            Err(ClientError::Disconnected)
        }
    }
}
