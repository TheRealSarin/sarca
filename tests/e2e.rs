use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use sarca::auth::hash_token;
use sarca::client::{Client, ClientError, SendOptions};
use sarca::config::{AuthConfig, Config, Grant, TokenConfig};
use sarca::server::Server;
use sarca::target::{Call, LocalRule, Target, TargetConfig, Trigger};
use sarca::transport::{DEFAULT_MAX_FRAME_BYTES, Endpoint};
use sarca::wire::Frame;
use sarca::wire::{ActionSpec, ErrorCode};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(5);

struct Running {
    tcp: String,
    ws: String,
    unix: Option<PathBuf>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Running {
    async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.unwrap();
        }
    }
}

fn unique_socket_path() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("sarca-test-{}-{n}.sock", std::process::id()))
}

async fn start(mut config: Config, with_unix: bool) -> Running {
    config.server.tcp = Some("127.0.0.1:0".into());
    config.server.ws = Some("127.0.0.1:0".into());
    config.server.unix = with_unix.then(unique_socket_path);
    let unix = config.server.unix.clone();
    let server = Server::bind(&config, vec![]).await.unwrap();
    let tcp = format!("tcp://{}", server.tcp_addr().unwrap());
    let ws = format!("ws://{}/bus", server.ws_addr().unwrap());
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(server.run(async {
        let _ = stopped.await;
    }));
    Running {
        tcp,
        ws,
        unix,
        stop: Some(stop),
        task: Some(task),
    }
}

async fn echo(call: Call) {
    match call.action() {
        "echo" => {
            let payload = call.payload().to_vec();
            let _ = call.reply(payload).await;
        }
        "count" => {
            let n = call.payload().first().copied().unwrap_or(0);
            let mut call = call;
            for i in 0..n {
                let _ = call.stream_item(vec![i]).await;
            }
            let _ = call.end().await;
        }
        "boom" => {
            let _ = call.fail(ErrorCode::TargetFailed, "exploded", false).await;
        }
        "hang" => {
            let _call = call;
            std::future::pending::<()>().await;
        }
        "flood" => {
            let mut call = call;
            for i in 0..1_000u32 {
                if call.stream_item(i.to_be_bytes().to_vec()).await.is_err() {
                    return;
                }
            }
            let _ = call.end().await;
        }
        _ => {}
    }
}

fn echo_actions() -> Vec<ActionSpec> {
    ["echo", "count", "boom", "note", "hang", "flood"]
        .into_iter()
        .map(ActionSpec::new)
        .collect()
}

async fn wait_for_target(client: &Client, name: &str) {
    tokio::time::timeout(WAIT, async {
        loop {
            let targets = client.targets().await.unwrap();
            if targets.iter().any(|t| t.name == name && t.online) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("target never came online");
}

fn spawn_target(config: TargetConfig) -> JoinHandle<Result<(), ClientError>> {
    tokio::spawn(Target::new(config).run(echo))
}

#[tokio::test]
async fn request_stream_fire_and_failure_over_tcp() {
    let server = start(Config::default(), false).await;
    let target = spawn_target(TargetConfig::new(&server.tcp, "echo-1", echo_actions()));
    let client = Client::connect(&server.tcp, None).await.unwrap();
    wait_for_target(&client, "echo-1").await;
    let opts = SendOptions::default();

    let reply = client
        .request("echo-1", "echo", b"hello".to_vec(), &opts)
        .await
        .unwrap();
    assert_eq!(reply, b"hello");

    let mut items = client
        .stream("echo-1", "count", vec![3], &opts)
        .await
        .unwrap();
    let mut received = Vec::new();
    while let Some(item) = items.recv().await {
        received.push(item.unwrap());
    }
    assert_eq!(received, vec![vec![0], vec![1], vec![2]]);

    client
        .fire("echo-1", "note", b"x".to_vec(), &opts)
        .await
        .unwrap();

    let err = client
        .request("echo-1", "boom", vec![], &opts)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Remote(e) if e.code == ErrorCode::TargetFailed));

    let err = client
        .request("echo-1", "missing", vec![], &opts)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Remote(e) if e.code == ErrorCode::UnknownAction));

    target.abort();
    server.stop().await;
}

#[tokio::test]
async fn unanswered_call_fails_instead_of_hanging() {
    let server = start(Config::default(), false).await;
    let config = TargetConfig::new(&server.tcp, "silent", vec![ActionSpec::new("ignore")]);
    let target = tokio::spawn(Target::new(config).run(|call: Call| async move { drop(call) }));
    let client = Client::connect(&server.tcp, None).await.unwrap();
    wait_for_target(&client, "silent").await;

    let err = client
        .request("silent", "ignore", vec![], &SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Remote(e) if e.code == ErrorCode::TargetFailed));
    target.abort();
    server.stop().await;
}

#[tokio::test]
async fn websocket_and_unix_transports_interoperate() {
    let server = start(Config::default(), true).await;
    let socket = server.unix.clone().unwrap();
    let unix_url = format!("unix:{}", socket.display());
    let target = spawn_target(TargetConfig::new(&server.ws, "ws-target", echo_actions()));
    let client = Client::connect(&unix_url, None).await.unwrap();
    wait_for_target(&client, "ws-target").await;

    let reply = client
        .request(
            "ws-target",
            "echo",
            b"across".to_vec(),
            &SendOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(reply, b"across");

    target.abort();
    server.stop().await;
    assert!(
        !socket.exists(),
        "socket file should be removed on shutdown"
    );
}

#[tokio::test]
async fn events_reach_subscribers() {
    let server = start(Config::default(), false).await;
    let mut config = TargetConfig::new(&server.tcp, "emitter", vec![]);
    config.groups = vec!["fleet".into()];
    let target = Target::new(config);
    let handle = target.handle();
    let running = tokio::spawn(target.run(echo));

    let client = Client::connect(&server.tcp, None).await.unwrap();
    let mut events = client.events().unwrap();
    assert!(client.events().is_none());
    client.subscribe("group:fleet", "*").await.unwrap();
    wait_for_target(&client, "emitter").await;

    handle.emit("status", b"ok".to_vec()).await.unwrap();
    let event = tokio::time::timeout(WAIT, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.target, "emitter");
    assert_eq!(event.event, "status");
    assert_eq!(event.payload, b"ok");

    running.abort();
    server.stop().await;
}

#[tokio::test]
async fn tokens_are_required_and_scoped() {
    let config = Config {
        auth: AuthConfig {
            anonymous: None,
            tokens: vec![
                TokenConfig {
                    name: "game".into(),
                    sha256: hash_token("game-secret"),
                    grant: Grant {
                        caps: vec![],
                        register: vec!["game-*".into()],
                        send: vec![],
                        groups: vec![],
                    },
                },
                TokenConfig {
                    name: "ops".into(),
                    sha256: hash_token("ops-secret"),
                    grant: Grant {
                        caps: vec![],
                        register: vec![],
                        send: vec!["game-*".into()],
                        groups: vec![],
                    },
                },
            ],
        },
        ..Config::default()
    };
    let server = start(config, false).await;

    let err = Client::connect(&server.tcp, None).await.err().unwrap();
    assert!(matches!(err, ClientError::Refused(e) if e.code == ErrorCode::Unauthorized));
    let err = Client::connect(&server.tcp, Some("nope"))
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ClientError::Refused(e) if e.code == ErrorCode::Unauthorized));

    let mut wrong_name = TargetConfig::new(&server.tcp, "db", echo_actions());
    wrong_name.token = Some("game-secret".into());
    let result = tokio::time::timeout(WAIT, Target::new(wrong_name).run(echo))
        .await
        .unwrap();
    assert!(matches!(result, Err(ClientError::Refused(e)) if e.code == ErrorCode::Forbidden));

    let mut game = TargetConfig::new(&server.tcp, "game-1", echo_actions());
    game.token = Some("game-secret".into());
    let target = spawn_target(game);
    let ops = Client::connect(&server.tcp, Some("ops-secret"))
        .await
        .unwrap();
    wait_for_target(&ops, "game-1").await;
    let reply = ops
        .request("game-1", "echo", b"ok".to_vec(), &SendOptions::default())
        .await
        .unwrap();
    assert_eq!(reply, b"ok");

    target.abort();
    server.stop().await;
}

#[tokio::test]
async fn local_rules_run_without_a_bus() {
    let mut config = TargetConfig::new("tcp://127.0.0.1:1", "offline", vec![]);
    config.rules = vec![LocalRule {
        trigger: Trigger::Every(Duration::from_millis(20)),
        action: "heartbeat".into(),
        payload: b"beat".to_vec(),
    }];
    let (tx, mut rx) = mpsc::channel(8);
    let target = tokio::spawn(Target::new(config).run(move |call: Call| {
        let tx = tx.clone();
        async move {
            assert!(call.is_local());
            let _ = tx
                .send((call.action().to_string(), call.payload().to_vec()))
                .await;
        }
    }));
    for _ in 0..2 {
        let (action, payload) = tokio::time::timeout(WAIT, rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(action, "heartbeat");
        assert_eq!(payload, b"beat");
    }
    target.abort();
}

#[tokio::test]
async fn connected_rule_fires_after_registration() {
    let server = start(Config::default(), false).await;
    let mut config = TargetConfig::new(&server.tcp, "announcer", vec![]);
    config.rules = vec![LocalRule {
        trigger: Trigger::Connected,
        action: "on-connect".into(),
        payload: vec![],
    }];
    let (tx, mut rx) = mpsc::channel(8);
    let target = tokio::spawn(Target::new(config).run(move |call: Call| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(call.action().to_string()).await;
        }
    }));
    let action = tokio::time::timeout(WAIT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(action, "on-connect");
    target.abort();
    server.stop().await;
}

#[test]
fn example_config_is_valid_once_hashes_are_filled_in() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/sarca.example.toml");
    let text = std::fs::read_to_string(path).unwrap();
    let filled = text
        .replace(
            "replace-with-output-of-sarca-hash-token-0000000000000000000000000",
            &hash_token("a"),
        )
        .replace(
            "replace-with-output-of-sarca-hash-token-1111111111111111111111111",
            &hash_token("b"),
        );
    let config: Config = toml::from_str(&filled).unwrap();
    config.validate().unwrap();
    assert_eq!(config.auth.tokens.len(), 2);

    let unfilled: Config = toml::from_str(&text).unwrap();
    assert!(unfilled.validate().is_err());
}

#[tokio::test]
async fn target_rejects_deliveries_beyond_its_handler_limit() {
    let server = start(Config::default(), false).await;
    let mut config = TargetConfig::new(&server.tcp, "busy", echo_actions());
    config.max_concurrent = 1;
    let target = spawn_target(config);
    let client = Client::connect(&server.tcp, None).await.unwrap();
    wait_for_target(&client, "busy").await;

    let short = SendOptions {
        ttl: Some(Duration::from_millis(300)),
        ..SendOptions::default()
    };
    let hung = {
        let client = client.clone();
        let short = short.clone();
        tokio::spawn(async move { client.request("busy", "hang", Vec::new(), &short).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let err = client
        .request("busy", "echo", b"x".to_vec(), &SendOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Remote(e) if e.code == ErrorCode::Overloaded));

    let err = hung.await.unwrap().unwrap_err();
    assert!(matches!(err, ClientError::Remote(e) if e.code == ErrorCode::Expired));
    let reply = tokio::time::timeout(WAIT, async {
        loop {
            match client
                .request("busy", "echo", b"back".to_vec(), &SendOptions::default())
                .await
            {
                Ok(reply) => return reply,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("timed-out handler never released its slot");
    assert_eq!(reply, b"back");

    target.abort();
    server.stop().await;
}

#[tokio::test]
async fn stalled_stream_does_not_block_other_replies() {
    let server = start(Config::default(), false).await;
    let target = spawn_target(TargetConfig::new(&server.tcp, "firehose", echo_actions()));
    let client = Client::connect(&server.tcp, None).await.unwrap();
    wait_for_target(&client, "firehose").await;
    let opts = SendOptions::default();

    let mut items = client
        .stream("firehose", "flood", Vec::new(), &opts)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        while items.len() < 257 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stream never filled its buffer");

    let reply = tokio::time::timeout(
        WAIT,
        client.request("firehose", "echo", b"hi".to_vec(), &opts),
    )
    .await
    .expect("reply blocked behind a stalled stream")
    .unwrap();
    assert_eq!(reply, b"hi");

    let mut received = 0;
    let mut terminal = None;
    while let Some(item) = items.recv().await {
        match item {
            Ok(_) => received += 1,
            Err(err) => terminal = Some(err.code),
        }
    }
    assert_eq!(received, 256);
    assert_eq!(terminal, Some(ErrorCode::Overloaded));

    target.abort();
    server.stop().await;
}

#[tokio::test]
async fn stalled_reader_is_disconnected_and_frees_its_connection_slot() {
    let mut config = Config::default();
    config.limits.max_connections = 2;
    config.limits.max_outbound_bytes_per_session = 4 * 1024 * 1024;
    config.limits.handshake_timeout_ms = 200;
    let server = start(config, false).await;

    let target = Target::new(TargetConfig::new(&server.tcp, "source", echo_actions()));
    let handle = target.handle();
    let target = tokio::spawn(target.run(echo));
    tokio::time::timeout(WAIT, async {
        while !handle.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("target never registered");

    let (mut sink, mut stream) = Endpoint::parse(&server.tcp)
        .unwrap()
        .connect(DEFAULT_MAX_FRAME_BYTES)
        .await
        .unwrap();
    sink.send(Frame::Hello {
        client: "stalled".into(),
        token: None,
    })
    .await
    .unwrap();
    let next = tokio::time::timeout(WAIT, stream.next()).await;
    assert!(matches!(next, Ok(Some(Ok(Frame::Welcome { .. })))));
    sink.send(Frame::Subscribe {
        id: 1,
        target: "source".into(),
        event: "*".into(),
    })
    .await
    .unwrap();
    let next = tokio::time::timeout(WAIT, stream.next()).await;
    assert!(matches!(next, Ok(Some(Ok(Frame::Accepted { id: 1 })))));

    let blocked = tokio::time::timeout(
        Duration::from_millis(300),
        Client::connect(&server.tcp, None),
    )
    .await;
    assert!(blocked.is_err(), "connection limit was not enforced");

    tokio::time::timeout(WAIT, async {
        for _ in 0..32 {
            handle.emit("blob", vec![0; 1024 * 1024]).await.unwrap();
        }
    })
    .await
    .expect("emits blocked");

    let client = tokio::time::timeout(WAIT, Client::connect(&server.tcp, None))
        .await
        .expect("stalled connection still holds its slot")
        .unwrap();
    let reply = tokio::time::timeout(
        WAIT,
        client.request("source", "echo", b"alive".to_vec(), &SendOptions::default()),
    )
    .await
    .expect("request blocked")
    .unwrap();
    assert_eq!(reply, b"alive");

    drop((sink, stream));
    target.abort();
    tokio::time::timeout(WAIT, server.stop())
        .await
        .expect("server did not stop");
}

#[tokio::test]
async fn unix_socket_gets_its_mode_before_it_is_reachable() {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("sarca-mode-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir(&dir).unwrap();
    let socket = dir.join("bus.sock");
    let mut config = Config::default();
    config.server.tcp = Some("127.0.0.1:0".into());
    config.server.unix = Some(socket.clone());
    config.server.unix_mode = 0o640;
    let server = Server::bind(&config, vec![]).await.unwrap();

    let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o640);
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(entries, vec![std::ffi::OsString::from("bus.sock")]);
    assert!(tokio::net::UnixStream::connect(&socket).await.is_ok());

    drop(server);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn failed_write_disconnects_the_client_and_fails_pending_requests() {
    let server = start(Config::default(), false).await;
    let client = Client::connect(&server.tcp, None).await.unwrap();
    let oversized = vec![0; DEFAULT_MAX_FRAME_BYTES];
    let result = tokio::time::timeout(
        WAIT,
        client.request("t", "op", oversized, &SendOptions::default()),
    )
    .await
    .expect("request hung after its frame could not be written");
    assert!(matches!(result, Err(ClientError::Disconnected)));
    assert!(!client.is_connected());
    assert!(matches!(
        client.targets().await,
        Err(ClientError::Disconnected)
    ));
    server.stop().await;
}

#[tokio::test]
async fn aborted_target_closes_its_connection_and_handle() {
    let server = start(Config::default(), false).await;
    let target = Target::new(TargetConfig::new(&server.tcp, "worker", echo_actions()));
    let handle = target.handle();
    let running = tokio::spawn(target.run(echo));
    tokio::time::timeout(WAIT, async {
        while !handle.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("target never registered");

    running.abort();
    let _ = running.await;
    assert!(!handle.is_connected());
    assert!(matches!(
        handle.emit("status", Vec::new()).await,
        Err(ClientError::Disconnected)
    ));

    let client = Client::connect(&server.tcp, None).await.unwrap();
    tokio::time::timeout(WAIT, async {
        while client
            .targets()
            .await
            .unwrap()
            .iter()
            .any(|t| t.name == "worker" && t.online)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("aborted target is still registered as online");
    server.stop().await;
}
