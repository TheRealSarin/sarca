use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sarca::auth::hash_token;
use sarca::client::{Client, ClientError, SendOptions};
use sarca::config::{AuthConfig, Config, Grant, TokenConfig};
use sarca::server::Server;
use sarca::target::{Call, LocalRule, Target, TargetConfig, Trigger};
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
        _ => {}
    }
}

fn echo_actions() -> Vec<ActionSpec> {
    ["echo", "count", "boom", "note"]
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
                    },
                },
                TokenConfig {
                    name: "ops".into(),
                    sha256: hash_token("ops-secret"),
                    grant: Grant {
                        caps: vec![],
                        register: vec![],
                        send: vec!["game-*".into()],
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
