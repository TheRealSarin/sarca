use std::path::PathBuf;
use std::pin::Pin;

use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, UnixStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::wire::{self, Frame, WireError};

pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("websocket: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error("unexpected text message on websocket")]
    TextMessage,
    #[error("invalid endpoint '{0}': expected tcp://host:port, unix:/path or ws://host:port/path")]
    InvalidEndpoint(String),
}

pub type FrameSink = Pin<Box<dyn Sink<Frame, Error = TransportError> + Send>>;
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, TransportError>> + Send>>;

pub fn framed_io<S>(io: S, max_frame_bytes: usize) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    framed_io_mode(io, max_frame_bytes, false)
}

pub(crate) fn framed_io_server<S>(io: S, max_frame_bytes: usize) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    framed_io_mode(io, max_frame_bytes, true)
}

fn framed_io_mode<S>(io: S, max_frame_bytes: usize, mut handshake: bool) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(max_frame_bytes)
        .new_codec();
    let (sink, stream) = Framed::new(io, codec).split();
    let sink = sink
        .sink_map_err(TransportError::from)
        .with(|frame: Frame| {
            future::ready(
                wire::encode(&frame)
                    .map(Bytes::from)
                    .map_err(TransportError::from),
            )
        });
    let stream = stream.map(move |item| {
        let bytes = item?;
        let frame = decode_incoming(&bytes, handshake)?;
        handshake = false;
        Ok(frame)
    });
    (Box::pin(sink), Box::pin(stream))
}

pub fn framed_ws<S>(ws: WebSocketStream<S>) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    framed_ws_mode(ws, false)
}

pub(crate) fn framed_ws_server<S>(ws: WebSocketStream<S>) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    framed_ws_mode(ws, true)
}

fn framed_ws_mode<S>(ws: WebSocketStream<S>, mut handshake: bool) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sink, stream) = ws.split();
    let sink = sink
        .sink_map_err(TransportError::from)
        .with(|frame: Frame| {
            future::ready(
                wire::encode(&frame)
                    .map(|bytes| Message::Binary(bytes.into()))
                    .map_err(TransportError::from),
            )
        });
    let stream = stream
        .take_while(|msg| future::ready(!matches!(msg, Ok(Message::Close(_)))))
        .filter_map(move |msg| {
            future::ready(match msg {
                Ok(Message::Binary(data)) => {
                    let result = decode_incoming(&data, handshake);
                    if result.is_ok() {
                        handshake = false;
                    }
                    Some(result)
                }
                Ok(Message::Text(_)) => Some(Err(TransportError::TextMessage)),
                Ok(_) => None,
                Err(err) => Some(Err(err.into())),
            })
        });
    (Box::pin(sink), Box::pin(stream))
}

fn decode_incoming(bytes: &[u8], handshake: bool) -> Result<Frame, TransportError> {
    if handshake && bytes.len() > 4096 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "hello exceeds 4096 bytes",
        )
        .into());
    }
    let frame = wire::decode(bytes)?;
    if handshake && !matches!(frame, Frame::Hello { .. }) {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "expected hello").into());
    }
    Ok(frame)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Tcp(String),
    Unix(PathBuf),
    WebSocket(String),
}

impl Endpoint {
    pub fn parse(url: &str) -> Result<Self, TransportError> {
        let invalid = || TransportError::InvalidEndpoint(url.to_string());
        if let Some(addr) = url.strip_prefix("tcp://") {
            if addr.is_empty() {
                return Err(invalid());
            }
            return Ok(Self::Tcp(addr.to_string()));
        }
        if let Some(path) = url
            .strip_prefix("unix://")
            .or_else(|| url.strip_prefix("unix:"))
        {
            if path.is_empty() {
                return Err(invalid());
            }
            return Ok(Self::Unix(PathBuf::from(path)));
        }
        if url.starts_with("ws://") && url.len() > "ws://".len() {
            return Ok(Self::WebSocket(url.to_string()));
        }
        Err(invalid())
    }

    pub async fn connect(
        &self,
        max_frame_bytes: usize,
    ) -> Result<(FrameSink, FrameStream), TransportError> {
        match self {
            Self::Tcp(addr) => {
                let stream = TcpStream::connect(addr).await?;
                stream.set_nodelay(true)?;
                Ok(framed_io(stream, max_frame_bytes))
            }
            Self::Unix(path) => Ok(framed_io(UnixStream::connect(path).await?, max_frame_bytes)),
            Self::WebSocket(url) => {
                let (ws, _) = tokio_tungstenite::connect_async(url.as_str()).await?;
                Ok(framed_ws(ws))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_parse() {
        assert_eq!(
            Endpoint::parse("tcp://127.0.0.1:7878").unwrap(),
            Endpoint::Tcp("127.0.0.1:7878".into())
        );
        assert_eq!(
            Endpoint::parse("unix:///run/sarca.sock").unwrap(),
            Endpoint::Unix("/run/sarca.sock".into())
        );
        assert_eq!(
            Endpoint::parse("unix:/run/sarca.sock").unwrap(),
            Endpoint::Unix("/run/sarca.sock".into())
        );
        assert_eq!(
            Endpoint::parse("ws://localhost:7879/bus").unwrap(),
            Endpoint::WebSocket("ws://localhost:7879/bus".into())
        );
        for bad in ["", "tcp://", "unix:", "wss://x", "http://x"] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn length_delimited_round_trip() {
        let (a, b) = tokio::io::duplex(1024);
        let (mut a_tx, _a_rx) = framed_io(a, 1024);
        let (_b_tx, mut b_rx) = framed_io(b, 1024);
        a_tx.send(Frame::Ping { nonce: 3 }).await.unwrap();
        assert_eq!(
            b_rx.next().await.unwrap().unwrap(),
            Frame::Ping { nonce: 3 }
        );
    }

    #[tokio::test]
    async fn oversized_frames_are_rejected() {
        let (a, b) = tokio::io::duplex(4096);
        let (mut a_tx, _a_rx) = framed_io(a, 4096);
        let (_b_tx, mut b_rx) = framed_io(b, 16);
        a_tx.send(Frame::Emit {
            event: "big".into(),
            payload: vec![0; 64],
        })
        .await
        .unwrap();
        assert!(b_rx.next().await.unwrap().is_err());
    }
}

#[cfg(test)]
mod handshake_tests {
    use super::*;
    #[test]
    fn handshake_rejects_non_hello_and_large_hello() {
        assert!(decode_incoming(&wire::encode(&Frame::Ping { nonce: 1 }).unwrap(), true).is_err());
        let hello = Frame::Hello {
            client: "x".repeat(4096),
            token: None,
        };
        assert!(decode_incoming(&wire::encode(&hello).unwrap(), true).is_err());
        let hello = Frame::Hello {
            client: "client".into(),
            token: None,
        };
        assert_eq!(
            decode_incoming(&wire::encode(&hello).unwrap(), true).unwrap(),
            hello
        );
    }
}
