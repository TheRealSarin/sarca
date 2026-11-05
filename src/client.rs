use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::transport::{DEFAULT_MAX_FRAME_BYTES, Endpoint, FrameSink, FrameStream, TransportError};
use crate::wire::{Envelope, ErrorCode, Frame, Mode, Retry, TargetInfo};

const OUTBOUND_BUFFER: usize = 256;
const STREAM_BUFFER: usize = 256;
const EVENT_BUFFER: usize = 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct RemoteError {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("bus refused the connection: {0}")]
    Refused(RemoteError),
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("disconnected from bus")]
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub target: String,
    pub event: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct SendOptions {
    pub caps: Vec<String>,
    pub ttl: Option<Duration>,
    pub retry: Retry,
}

pub(crate) struct Link {
    pub session: u64,
    pub sink: FrameSink,
    pub stream: FrameStream,
}

pub(crate) async fn open_link(
    url: &str,
    token: Option<&str>,
    client: &str,
) -> Result<Link, ClientError> {
    let endpoint = Endpoint::parse(url)?;
    let (mut sink, mut stream) = endpoint.connect(DEFAULT_MAX_FRAME_BYTES).await?;
    sink.send(Frame::Hello {
        client: client.to_string(),
        token: token.map(str::to_string),
    })
    .await?;
    let reply = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.next())
        .await
        .map_err(|_| ClientError::Handshake("timed out".into()))?;
    match reply {
        Some(Ok(Frame::Welcome { session })) => Ok(Link {
            session,
            sink,
            stream,
        }),
        Some(Ok(Frame::Error { code, message })) => {
            Err(ClientError::Refused(RemoteError { code, message }))
        }
        Some(Ok(other)) => Err(ClientError::Handshake(format!(
            "unexpected frame {}",
            other.kind()
        ))),
        Some(Err(err)) => Err(err.into()),
        None => Err(ClientError::Disconnected),
    }
}

pub(crate) fn spawn_writer(mut sink: FrameSink, closed: CancellationToken) -> mpsc::Sender<Frame> {
    let (tx, mut rx) = mpsc::channel::<Frame>(OUTBOUND_BUFFER);
    tokio::spawn(async move {
        let _closed = closed.clone().drop_guard();
        let write = async {
            while let Some(frame) = rx.recv().await {
                sink.send(frame).await?;
            }
            sink.close().await
        };
        tokio::select! {
            result = write => {
                if let Err(err) = result {
                    tracing::debug!(%err, "client write failed");
                }
            }
            () = closed.cancelled() => {}
        }
    });
    tx
}

type ReplySender = oneshot::Sender<Result<Vec<u8>, RemoteError>>;
type StreamSender = mpsc::Sender<Result<Vec<u8>, RemoteError>>;

enum Waiter {
    Reply(ReplySender),
    Accepted(oneshot::Sender<Result<(), RemoteError>>),
    Stream(StreamSender),
    Targets(oneshot::Sender<Result<Vec<TargetInfo>, RemoteError>>),
}

struct State {
    open: bool,
    waiters: HashMap<u64, Waiter>,
    events: Option<mpsc::Receiver<Event>>,
}

struct Inner {
    session: u64,
    out: mpsc::Sender<Frame>,
    closed: CancellationToken,
    state: Mutex<State>,
    next_id: AtomicU64,
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    pub async fn connect(url: &str, token: Option<&str>) -> Result<Self, ClientError> {
        Self::connect_as(url, token, "sarca-client").await
    }

    pub async fn connect_as(
        url: &str,
        token: Option<&str>,
        client: &str,
    ) -> Result<Self, ClientError> {
        let link = open_link(url, token, client).await?;
        let (events_tx, events_rx) = mpsc::channel(EVENT_BUFFER);
        let closed = CancellationToken::new();
        let inner = Arc::new(Inner {
            session: link.session,
            out: spawn_writer(link.sink, closed.clone()),
            closed: closed.clone(),
            state: Mutex::new(State {
                open: true,
                waiters: HashMap::new(),
                events: Some(events_rx),
            }),
            next_id: AtomicU64::new(1),
        });
        tokio::spawn(read_loop(
            Arc::downgrade(&inner),
            link.stream,
            events_tx,
            closed,
        ));
        Ok(Self { inner })
    }

    pub fn session(&self) -> u64 {
        self.inner.session
    }

    pub fn is_connected(&self) -> bool {
        !self.inner.closed.is_cancelled()
    }

    pub fn events(&self) -> Option<mpsc::Receiver<Event>> {
        self.inner.state().events.take()
    }

    pub async fn request(
        &self,
        target: &str,
        action: &str,
        payload: impl Into<Vec<u8>>,
        opts: &SendOptions,
    ) -> Result<Vec<u8>, ClientError> {
        let (tx, rx) = oneshot::channel();
        let envelope = build_envelope(target, action, payload.into(), opts, Mode::Request);
        self.submit(Waiter::Reply(tx), |id| {
            Frame::Send(Envelope { id, ..envelope })
        })
        .await?;
        Ok(rx.await.map_err(|_| ClientError::Disconnected)??)
    }

    pub async fn fire(
        &self,
        target: &str,
        action: &str,
        payload: impl Into<Vec<u8>>,
        opts: &SendOptions,
    ) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let envelope = build_envelope(target, action, payload.into(), opts, Mode::Fire);
        self.submit(Waiter::Accepted(tx), |id| {
            Frame::Send(Envelope { id, ..envelope })
        })
        .await?;
        Ok(rx.await.map_err(|_| ClientError::Disconnected)??)
    }

    pub async fn stream(
        &self,
        target: &str,
        action: &str,
        payload: impl Into<Vec<u8>>,
        opts: &SendOptions,
    ) -> Result<mpsc::Receiver<Result<Vec<u8>, RemoteError>>, ClientError> {
        let (tx, rx) = mpsc::channel(STREAM_BUFFER + 1);
        let envelope = build_envelope(target, action, payload.into(), opts, Mode::Stream);
        self.submit(Waiter::Stream(tx), |id| {
            Frame::Send(Envelope { id, ..envelope })
        })
        .await?;
        Ok(rx)
    }

    pub async fn subscribe(&self, target: &str, event: &str) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.submit(Waiter::Accepted(tx), |id| Frame::Subscribe {
            id,
            target: target.to_string(),
            event: event.to_string(),
        })
        .await?;
        Ok(rx.await.map_err(|_| ClientError::Disconnected)??)
    }

    pub async fn unsubscribe(&self, target: &str, event: &str) -> Result<(), ClientError> {
        self.inner
            .out
            .send(Frame::Unsubscribe {
                target: target.to_string(),
                event: event.to_string(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)
    }

    pub async fn targets(&self) -> Result<Vec<TargetInfo>, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.submit(Waiter::Targets(tx), |id| Frame::ListTargets { id })
            .await?;
        Ok(rx.await.map_err(|_| ClientError::Disconnected)??)
    }

    async fn submit(
        &self,
        waiter: Waiter,
        frame: impl FnOnce(u64) -> Frame,
    ) -> Result<(), ClientError> {
        // Reserve first: cancellation while waiting must not leave an orphan waiter.
        let permit = self
            .inner
            .out
            .reserve()
            .await
            .map_err(|_| ClientError::Disconnected)?;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut state = self.inner.state();
            if !state.open {
                return Err(ClientError::Disconnected);
            }
            state.waiters.insert(id, waiter);
        }
        permit.send(frame(id));
        Ok(())
    }
}

fn build_envelope(
    target: &str,
    action: &str,
    payload: Vec<u8>,
    opts: &SendOptions,
    mode: Mode,
) -> Envelope {
    Envelope {
        id: 0,
        target: target.to_string(),
        action: action.to_string(),
        caps: opts.caps.clone(),
        ttl: opts.ttl.map_or(0, |ttl| {
            u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1)
        }),
        retry: opts.retry,
        mode,
        payload,
    }
}

async fn read_loop(
    inner: std::sync::Weak<Inner>,
    stream: FrameStream,
    events: mpsc::Sender<Event>,
    closed: CancellationToken,
) {
    let mut stream = std::pin::pin!(stream.take_until(closed.clone().cancelled_owned()));
    while let Some(item) = stream.next().await {
        let frame = match item {
            Ok(frame) => frame,
            Err(err) => {
                tracing::debug!(%err, "client read failed");
                break;
            }
        };
        let Some(inner) = inner.upgrade() else {
            return;
        };
        match frame {
            Frame::Reply { id, payload } => match take(&inner, id) {
                Some(Waiter::Reply(tx)) => {
                    let _ = tx.send(Ok(payload));
                }
                Some(Waiter::Stream(tx)) => {
                    let _ = tx.try_send(Ok(payload));
                }
                _ => {}
            },
            Frame::StreamItem { id, payload } => {
                let mut state = inner.state();
                let keep = match state.waiters.get(&id) {
                    Some(Waiter::Stream(tx)) => push_stream_item(tx, payload),
                    _ => true,
                };
                if !keep {
                    state.waiters.remove(&id);
                }
            }
            Frame::StreamEnd { id } => {
                take(&inner, id);
            }
            Frame::Accepted { id } => {
                if let Some(Waiter::Accepted(tx)) = take(&inner, id) {
                    let _ = tx.send(Ok(()));
                }
            }
            Frame::Fail {
                id, code, message, ..
            } => {
                let error = RemoteError { code, message };
                match take(&inner, id) {
                    Some(Waiter::Reply(tx)) => {
                        let _ = tx.send(Err(error));
                    }
                    Some(Waiter::Accepted(tx)) => {
                        let _ = tx.send(Err(error));
                    }
                    Some(Waiter::Stream(tx)) => {
                        let _ = tx.try_send(Err(error));
                    }
                    Some(Waiter::Targets(tx)) => {
                        let _ = tx.send(Err(error));
                    }
                    None => {}
                }
            }
            Frame::Targets { id, targets } => {
                if let Some(Waiter::Targets(tx)) = take(&inner, id) {
                    let _ = tx.send(Ok(targets));
                }
            }
            Frame::Event {
                target,
                event,
                payload,
            } => {
                if events
                    .try_send(Event {
                        target,
                        event,
                        payload,
                    })
                    .is_err()
                {
                    tracing::warn!("event buffer full or unread, dropping event");
                }
            }
            Frame::Error { code, message } => {
                tracing::warn!(?code, %message, "bus reported an error")
            }
            _ => {}
        }
    }
    closed.cancel();
    if let Some(inner) = inner.upgrade() {
        let mut state = inner.state();
        state.open = false;
        for (_, waiter) in state.waiters.drain() {
            if let Waiter::Stream(tx) = waiter {
                // Stream items always leave one slot for a terminal error.
                let _ = tx.try_send(Err(RemoteError {
                    code: ErrorCode::Unavailable,
                    message: "connection closed before stream completed".into(),
                }));
            }
        }
    }
}

fn push_stream_item(tx: &StreamSender, payload: Vec<u8>) -> bool {
    if tx.capacity() > 1 {
        return tx.try_send(Ok(payload)).is_ok();
    }
    let _ = tx.try_send(Err(RemoteError {
        code: ErrorCode::Overloaded,
        message: "stream receiver fell behind, stream terminated".into(),
    }));
    false
}

fn take(inner: &Inner, id: u64) -> Option<Waiter> {
    inner.state().waiters.remove(&id)
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    fn client(out: mpsc::Sender<Frame>) -> Client {
        Client {
            inner: Arc::new(Inner {
                session: 1,
                out,
                closed: CancellationToken::new(),
                next_id: AtomicU64::new(1),
                state: Mutex::new(State {
                    open: true,
                    waiters: HashMap::new(),
                    events: None,
                }),
            }),
        }
    }
    #[tokio::test]
    async fn cancelled_submission_does_not_register_a_waiter() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(Frame::Ping { nonce: 0 }).await.unwrap();
        let client = client(tx);
        let result = tokio::time::timeout(Duration::from_millis(10), client.targets()).await;
        assert!(result.is_err());
        assert!(client.inner.state().waiters.is_empty());
    }
    #[tokio::test]
    async fn disconnect_reports_error_even_with_full_stream_buffer() {
        let (out, _rx) = mpsc::channel(1);
        let client = client(out);
        let (tx, mut rx) = mpsc::channel(STREAM_BUFFER + 1);
        for _ in 0..STREAM_BUFFER {
            tx.try_send(Ok(vec![1])).unwrap();
        }
        client.inner.state().waiters.insert(1, Waiter::Stream(tx));
        let (events, _events_rx) = mpsc::channel(1);
        read_loop(
            Arc::downgrade(&client.inner),
            Box::pin(futures_util::stream::empty()),
            events,
            client.inner.closed.clone(),
        )
        .await;
        for _ in 0..STREAM_BUFFER {
            assert!(rx.recv().await.unwrap().is_ok());
        }
        assert_eq!(
            rx.recv().await.unwrap().unwrap_err().code,
            ErrorCode::Unavailable
        );
        assert!(rx.recv().await.is_none());
    }
}
