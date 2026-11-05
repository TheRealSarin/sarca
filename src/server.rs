use std::future::Future;
use std::net::SocketAddr;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::auth::Authenticator;
use crate::bus::{self, BusHandle};
use crate::config::{Config, Limits};
use crate::hooks::Hook;
use crate::transport::{FrameSink, FrameStream, framed_io_server, framed_ws_server};
use crate::wire::{ErrorCode, Frame};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("cannot bind {addr}: {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },
    #[error("unix socket {0} is in use by another process")]
    SocketInUse(PathBuf),
    #[error("{0} exists and is not a socket")]
    NotASocket(PathBuf),
}

struct Shared {
    bus: BusHandle,
    auth: Authenticator,
    limits: Limits,
    connections: Arc<Semaphore>,
}

pub struct Server {
    tcp: Option<TcpListener>,
    ws: Option<TcpListener>,
    unix: Option<(UnixListener, PathBuf)>,
    shared: Arc<Shared>,
}

async fn bind_tcp(addr: &str) -> Result<TcpListener, ServerError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| ServerError::Bind {
            addr: addr.to_string(),
            source,
        })
}

async fn bind_unix(path: &Path, mode: u32) -> Result<UnixListener, ServerError> {
    let bind_error = |source| ServerError::Bind {
        addr: path.display().to_string(),
        source,
    };
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() {
            return Err(ServerError::NotASocket(path.to_path_buf()));
        }
        if UnixStream::connect(path).await.is_ok() {
            return Err(ServerError::SocketInUse(path.to_path_buf()));
        }
        std::fs::remove_file(path).map_err(bind_error)?;
    }
    let staging = staging_dir(path);
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&staging)
        .map_err(bind_error)?;
    let staged = staging.join("s");
    let result = bind_staged(&staged, path, mode);
    let _ = std::fs::remove_file(&staged);
    let _ = std::fs::remove_dir(&staging);
    result.map_err(bind_error)
}

fn bind_staged(staged: &Path, path: &Path, mode: u32) -> std::io::Result<UnixListener> {
    let listener = UnixListener::bind(staged)?;
    std::fs::set_permissions(staged, std::fs::Permissions::from_mode(mode))?;
    std::fs::rename(staged, path)?;
    Ok(listener)
}

fn staging_dir(path: &Path) -> PathBuf {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    parent.join(format!(".sarca{}.{nanos}", std::process::id()))
}

impl Server {
    pub async fn bind(config: &Config, hooks: Vec<Arc<dyn Hook>>) -> Result<Self, ServerError> {
        let tcp = match &config.server.tcp {
            Some(addr) => Some(bind_tcp(addr).await?),
            None => None,
        };
        let ws = match &config.server.ws {
            Some(addr) => Some(bind_tcp(addr).await?),
            None => None,
        };
        let unix = match &config.server.unix {
            Some(path) => Some((
                bind_unix(path, config.server.unix_mode).await?,
                path.clone(),
            )),
            None => None,
        };
        let shared = Arc::new(Shared {
            bus: bus::spawn(config, hooks),
            auth: Authenticator::new(&config.auth),
            limits: config.limits.clone(),
            connections: Arc::new(Semaphore::new(config.limits.max_connections)),
        });
        Ok(Self {
            tcp,
            ws,
            unix,
            shared,
        })
    }

    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub fn ws_addr(&self) -> Option<SocketAddr> {
        self.ws.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        let mut tasks = JoinSet::new();
        if let Some(listener) = self.tcp {
            log_listening("tcp", listener.local_addr());
            tasks.spawn(accept_tcp(listener, self.shared.clone(), false));
        }
        if let Some(listener) = self.ws {
            log_listening("ws", listener.local_addr());
            tasks.spawn(accept_tcp(listener, self.shared.clone(), true));
        }
        let socket_path = self.unix.as_ref().map(|(_, path)| path.clone());
        if let Some((listener, path)) = self.unix {
            tracing::info!(path = %path.display(), "listening on unix socket");
            tasks.spawn(accept_unix(listener, self.shared.clone()));
        }
        shutdown.await;
        tasks.shutdown().await;
        if let Some(path) = socket_path {
            let _ = std::fs::remove_file(path);
        }
        tracing::info!("server stopped");
    }
}

fn log_listening(kind: &str, addr: std::io::Result<SocketAddr>) {
    match addr {
        Ok(addr) => tracing::info!(%addr, "listening on {kind}"),
        Err(err) => tracing::warn!(%err, "listening on {kind}, address unknown"),
    }
}

async fn backoff_after_accept_error(err: std::io::Error) {
    tracing::warn!(%err, "accept failed");
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn accept_tcp(listener: TcpListener, shared: Arc<Shared>, websocket: bool) {
    let mut sessions = JoinSet::new();
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                backoff_after_accept_error(err).await;
                continue;
            }
        };
        let Ok(permit) = shared.connections.clone().acquire_owned().await else {
            return;
        };
        let _ = stream.set_nodelay(true);
        let shared = shared.clone();
        while sessions.try_join_next().is_some() {}
        sessions.spawn(async move {
            let _permit = permit;
            let peer = peer.to_string();
            if !websocket {
                let (sink, stream) = framed_io_server(stream, shared.limits.max_frame_bytes);
                return run_session(shared, sink, stream, peer).await;
            }
            let ws_config = WebSocketConfig::default()
                .max_message_size(Some(shared.limits.max_frame_bytes))
                .max_frame_size(Some(shared.limits.max_frame_bytes));
            let handshake = tokio_tungstenite::accept_async_with_config(stream, Some(ws_config));
            match tokio::time::timeout(handshake_timeout(&shared), handshake).await {
                Ok(Ok(ws)) => {
                    let (sink, stream) = framed_ws_server(ws);
                    run_session(shared, sink, stream, peer).await;
                }
                Ok(Err(err)) => tracing::debug!(%peer, %err, "websocket handshake failed"),
                Err(_) => tracing::debug!(%peer, "websocket handshake timed out"),
            }
        });
    }
}

async fn accept_unix(listener: UnixListener, shared: Arc<Shared>) {
    let mut sessions = JoinSet::new();
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => {
                backoff_after_accept_error(err).await;
                continue;
            }
        };
        let Ok(permit) = shared.connections.clone().acquire_owned().await else {
            return;
        };
        let (sink, stream) = framed_io_server(stream, shared.limits.max_frame_bytes);
        while sessions.try_join_next().is_some() {}
        let shared = shared.clone();
        sessions.spawn(async move {
            let _permit = permit;
            run_session(shared, sink, stream, "unix".to_string()).await;
        });
    }
}

fn handshake_timeout(shared: &Shared) -> Duration {
    Duration::from_millis(shared.limits.handshake_timeout_ms)
}

async fn reject(mut sink: FrameSink, code: ErrorCode, message: &str) {
    let _ = sink
        .send(Frame::Error {
            code,
            message: message.to_string(),
        })
        .await;
    let _ = sink.close().await;
}

async fn run_session(
    shared: Arc<Shared>,
    mut sink: FrameSink,
    mut stream: FrameStream,
    peer: String,
) {
    let hello = tokio::time::timeout(handshake_timeout(&shared), stream.next()).await;
    let (client, token) = match hello {
        Ok(Some(Ok(Frame::Hello { client, token }))) => (client, token),
        _ => return reject(sink, ErrorCode::Protocol, "expected hello").await,
    };
    let Some(principal) = shared.auth.authenticate(token.as_deref()) else {
        tracing::warn!(%peer, ?client, "authentication failed");
        return reject(sink, ErrorCode::Unauthorized, "authentication failed").await;
    };
    let principal_name = principal.name.clone();
    let Ok((session, mut outbound)) = shared.bus.connect(principal).await else {
        return reject(sink, ErrorCode::Unavailable, "bus unavailable").await;
    };
    tracing::info!(session, %peer, ?client, principal = %principal_name, "session opened");

    let closed_by_bus = outbound.closed();
    let mut active_charge = None;
    let kicked = {
        let writer = async {
            while let Some((frame, charge)) = outbound.recv_charged().await {
                active_charge = Some(charge);
                if sink.send(frame).await.is_err() {
                    return;
                }
                active_charge.take();
            }
            let _ = sink.close().await;
        };
        let reader = async {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(frame) => {
                        if shared.bus.frame(session, frame).await.is_err() {
                            return;
                        }
                    }
                    Err(err) => {
                        tracing::debug!(session, %err, "read failed");
                        return;
                    }
                }
            }
        };
        tokio::select! {
            () = writer => false,
            () = reader => false,
            () = closed_by_bus => true,
        }
    };
    if kicked {
        let flush = async {
            // A cancelled send may have left encoded bytes in the sink.
            if sink.flush().await.is_err() {
                return;
            }
            active_charge.take();
            while let Some((frame, _charge)) = outbound.try_recv_charged() {
                if sink.send(frame).await.is_err() {
                    return;
                }
            }
            let _ = sink.close().await;
        };
        let _ = tokio::time::timeout(handshake_timeout(&shared), flush).await;
    }
    drop(sink);
    drop(active_charge);
    drop(outbound);
    shared.bus.disconnect(session).await;
    tracing::info!(session, "session closed");
}
