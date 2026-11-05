use std::time::Duration;

use sarca::auth::{Authenticator, Principal, hash_token};
use sarca::bus::{self, BusHandle, SessionId};
use sarca::config::{AuthConfig, Config, Grant, Rule, TokenConfig};
use sarca::wire::{ActionSpec, Envelope, ErrorCode, Frame, Mode, Retry};
use tokio::sync::mpsc;

struct Peer {
    bus: BusHandle,
    session: SessionId,
    rx: mpsc::Receiver<Frame>,
}

impl Peer {
    async fn connect(bus: &BusHandle, principal: Principal) -> Self {
        let (session, rx) = bus.connect(principal).await.unwrap();
        let mut peer = Self {
            bus: bus.clone(),
            session,
            rx,
        };
        assert!(matches!(peer.recv().await, Frame::Welcome { .. }));
        peer
    }

    async fn send(&self, frame: Frame) {
        self.bus.frame(self.session, frame).await.unwrap();
    }

    async fn recv(&mut self) -> Frame {
        tokio::time::timeout(Duration::from_secs(600), self.rx.recv())
            .await
            .expect("timed out waiting for frame")
            .expect("session closed")
    }

    async fn assert_silent(&mut self) {
        let result = tokio::time::timeout(Duration::from_millis(50), self.rx.recv()).await;
        assert!(result.is_err(), "unexpected frame: {result:?}");
    }

    async fn register(&mut self, name: &str, actions: &[&str], groups: &[&str]) {
        self.register_specs(
            name,
            actions.iter().map(|a| ActionSpec::new(*a)).collect(),
            groups,
        )
        .await;
    }

    async fn register_specs(&mut self, name: &str, actions: Vec<ActionSpec>, groups: &[&str]) {
        self.send(Frame::Register {
            target: name.into(),
            actions,
            groups: groups.iter().map(|g| g.to_string()).collect(),
        })
        .await;
        assert_eq!(
            self.recv().await,
            Frame::Registered {
                target: name.into()
            }
        );
    }

    async fn disconnect(self) {
        self.bus.disconnect(self.session).await;
    }
}

fn envelope(id: u64, target: &str, action: &str, payload: &[u8]) -> Envelope {
    Envelope {
        id,
        target: target.into(),
        action: action.into(),
        caps: vec![],
        ttl: 0,
        retry: Retry::default(),
        mode: Mode::Request,
        payload: payload.to_vec(),
    }
}

fn anyone() -> Principal {
    Authenticator::new(&AuthConfig {
        anonymous: Some(Grant::everything()),
        tokens: vec![],
    })
    .authenticate(None)
    .unwrap()
}

fn start(config: Config) -> BusHandle {
    bus::spawn(&config, vec![])
}

async fn expect_deliver(peer: &mut Peer) -> Envelope {
    match peer.recv().await {
        Frame::Deliver(env) => env,
        other => panic!("expected deliver, got {other:?}"),
    }
}

async fn expect_fail(peer: &mut Peer) -> (u64, ErrorCode) {
    match peer.recv().await {
        Frame::Fail { id, code, .. } => (id, code),
        other => panic!("expected fail, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn request_is_routed_and_reply_returns_with_caller_id() {
    let bus = start(Config::default());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("game-1", &["ping"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;

    caller
        .send(Frame::Send(envelope(77, "game-1", "ping", b"hi")))
        .await;
    let delivered = expect_deliver(&mut target).await;
    assert_eq!(delivered.payload, b"hi");
    assert_eq!(delivered.target, "game-1");
    assert!(delivered.ttl > 0);

    target
        .send(Frame::Reply {
            id: delivered.id,
            payload: b"pong".to_vec(),
        })
        .await;
    assert_eq!(
        caller.recv().await,
        Frame::Reply {
            id: 77,
            payload: b"pong".to_vec()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn messages_queue_while_offline_and_flush_on_register() {
    let bus = start(Config::default());
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller
        .send(Frame::Send(envelope(1, "late", "go", b"a")))
        .await;
    caller
        .send(Frame::Send(envelope(2, "late", "go", b"b")))
        .await;
    caller.assert_silent().await;

    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("late", &["go"], &[]).await;
    assert_eq!(expect_deliver(&mut target).await.payload, b"a");
    assert_eq!(expect_deliver(&mut target).await.payload, b"b");
}

#[tokio::test(start_paused = true)]
async fn queued_message_expires_after_ttl() {
    let bus = start(Config::default());
    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut env = envelope(5, "nobody", "go", b"");
    env.ttl = 1_000;
    caller.send(Frame::Send(env)).await;
    assert_eq!(expect_fail(&mut caller).await, (5, ErrorCode::Expired));

    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("nobody", &["go"], &[]).await;
    target.assert_silent().await;
}

#[tokio::test(start_paused = true)]
async fn in_flight_request_is_retried_after_target_disconnect() {
    let bus = start(Config::default());
    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut first = Peer::connect(&bus, anyone()).await;
    first.register("svc", &["work"], &[]).await;

    let mut env = envelope(9, "svc", "work", b"job");
    env.retry = Retry {
        max_attempts: 2,
        backoff_ms: 100,
    };
    caller.send(Frame::Send(env)).await;
    expect_deliver(&mut first).await;
    first.disconnect().await;

    let mut second = Peer::connect(&bus, anyone()).await;
    second.register("svc", &["work"], &[]).await;
    let retried = expect_deliver(&mut second).await;
    assert_eq!(retried.payload, b"job");
    second.disconnect().await;
    assert_eq!(expect_fail(&mut caller).await, (9, ErrorCode::Unavailable));
}

#[tokio::test(start_paused = true)]
async fn retryable_failures_back_off_then_give_up() {
    let bus = start(Config::default());
    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("flaky", &["op"], &[]).await;

    let mut env = envelope(3, "flaky", "op", b"");
    env.retry = Retry {
        max_attempts: 3,
        backoff_ms: 1_000,
    };
    caller.send(Frame::Send(env)).await;

    let mut last = tokio::time::Instant::now();
    let mut gaps = Vec::new();
    for _ in 0..3 {
        let delivered = expect_deliver(&mut target).await;
        let now = tokio::time::Instant::now();
        gaps.push((now - last).as_millis());
        last = now;
        target
            .send(Frame::Fail {
                id: delivered.id,
                code: ErrorCode::TargetFailed,
                message: "busy".into(),
                retryable: true,
            })
            .await;
    }
    assert!(gaps[1] >= 1_000 && gaps[1] < 1_100, "{gaps:?}");
    assert!(gaps[2] >= 2_000 && gaps[2] < 2_100, "{gaps:?}");
    assert_eq!(expect_fail(&mut caller).await, (3, ErrorCode::TargetFailed));
}

#[tokio::test(start_paused = true)]
async fn unknown_actions_are_rejected() {
    let bus = start(Config::default());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["known"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller
        .send(Frame::Send(envelope(1, "t", "nope", b"")))
        .await;
    assert_eq!(
        expect_fail(&mut caller).await,
        (1, ErrorCode::UnknownAction)
    );

    target.disconnect().await;
    caller
        .send(Frame::Send(envelope(2, "t", "nope", b"")))
        .await;
    assert_eq!(
        expect_fail(&mut caller).await,
        (2, ErrorCode::UnknownAction)
    );
}

#[tokio::test(start_paused = true)]
async fn fallback_rule_routes_to_backup_when_primary_is_down() {
    let config = Config {
        rules: vec![Rule {
            when_down: "primary".into(),
            route_to: vec!["backup".into()],
        }],
        ..Config::default()
    };
    let bus = start(config);
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller
        .send(Frame::Send(envelope(1, "primary", "op", b"queued")))
        .await;
    caller.assert_silent().await;

    let mut backup = Peer::connect(&bus, anyone()).await;
    backup.register("backup", &["op"], &[]).await;
    assert_eq!(expect_deliver(&mut backup).await.payload, b"queued");

    caller
        .send(Frame::Send(envelope(2, "primary", "op", b"direct")))
        .await;
    let delivered = expect_deliver(&mut backup).await;
    assert_eq!(delivered.payload, b"direct");
    assert_eq!(delivered.target, "backup");

    let mut primary = Peer::connect(&bus, anyone()).await;
    primary.register("primary", &["op"], &[]).await;
    caller
        .send(Frame::Send(envelope(3, "primary", "op", b"home")))
        .await;
    assert_eq!(expect_deliver(&mut primary).await.payload, b"home");
}

#[tokio::test(start_paused = true)]
async fn groups_round_robin_across_online_members() {
    let bus = start(Config::default());
    let mut a = Peer::connect(&bus, anyone()).await;
    a.register("a", &["op"], &["pool"]).await;
    let mut b = Peer::connect(&bus, anyone()).await;
    b.register("b", &["op"], &["pool"]).await;
    let caller = Peer::connect(&bus, anyone()).await;

    for id in 0..4 {
        caller
            .send(Frame::Send(envelope(id, "group:pool", "op", b"")))
            .await;
    }
    for _ in 0..2 {
        assert_eq!(expect_deliver(&mut a).await.target, "a");
        assert_eq!(expect_deliver(&mut b).await.target, "b");
    }
}

#[tokio::test(start_paused = true)]
async fn capabilities_are_enforced() {
    let ops_hash = hash_token("ops-token");
    let config = Config {
        auth: AuthConfig {
            anonymous: Some(Grant {
                caps: vec![],
                register: vec!["*".into()],
                send: vec!["*".into()],
            }),
            tokens: vec![TokenConfig {
                name: "ops".into(),
                sha256: ops_hash,
                grant: Grant {
                    caps: vec!["game.admin".into()],
                    register: vec![],
                    send: vec!["game-*".into()],
                },
            }],
        },
        ..Config::default()
    };
    let auth = Authenticator::new(&config.auth);
    let bus = start(config);
    let mut target = Peer::connect(&bus, auth.authenticate(None).unwrap()).await;
    target
        .register_specs(
            "game-1",
            vec![ActionSpec::with_caps("restart", ["game.admin"])],
            &[],
        )
        .await;
    let mut anon = Peer::connect(&bus, auth.authenticate(None).unwrap()).await;
    let mut ops = Peer::connect(&bus, auth.authenticate(Some("ops-token")).unwrap()).await;

    anon.send(Frame::Send(envelope(1, "game-1", "restart", b"")))
        .await;
    assert_eq!(expect_fail(&mut anon).await, (1, ErrorCode::Forbidden));

    let mut claimed = envelope(2, "game-1", "restart", b"");
    claimed.caps = vec!["game.admin".into()];
    anon.send(Frame::Send(claimed.clone())).await;
    assert_eq!(expect_fail(&mut anon).await, (2, ErrorCode::Forbidden));

    ops.send(Frame::Send(envelope(3, "game-1", "restart", b"")))
        .await;
    assert_eq!(expect_fail(&mut ops).await, (3, ErrorCode::Forbidden));

    ops.send(Frame::Send(claimed)).await;
    let delivered = expect_deliver(&mut target).await;
    assert_eq!(delivered.caps, vec!["game.admin"]);

    ops.send(Frame::Send(envelope(4, "db", "drop", b""))).await;
    assert_eq!(expect_fail(&mut ops).await, (4, ErrorCode::Forbidden));
}

#[tokio::test(start_paused = true)]
async fn fire_and_forget_is_accepted_without_reply() {
    let bus = start(Config::default());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["log"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut env = envelope(11, "t", "log", b"line");
    env.mode = Mode::Fire;
    caller.send(Frame::Send(env)).await;
    assert_eq!(caller.recv().await, Frame::Accepted { id: 11 });
    let delivered = expect_deliver(&mut target).await;
    target
        .send(Frame::Reply {
            id: delivered.id,
            payload: vec![],
        })
        .await;
    caller.assert_silent().await;
}

#[tokio::test(start_paused = true)]
async fn streams_forward_items_then_end() {
    let bus = start(Config::default());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["tail"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut env = envelope(4, "t", "tail", b"");
    env.mode = Mode::Stream;
    caller.send(Frame::Send(env)).await;
    let id = expect_deliver(&mut target).await.id;
    for chunk in [b"one".to_vec(), b"two".to_vec()] {
        target.send(Frame::StreamItem { id, payload: chunk }).await;
    }
    target.send(Frame::StreamEnd { id }).await;
    assert_eq!(
        caller.recv().await,
        Frame::StreamItem {
            id: 4,
            payload: b"one".to_vec()
        }
    );
    assert_eq!(
        caller.recv().await,
        Frame::StreamItem {
            id: 4,
            payload: b"two".to_vec()
        }
    );
    assert_eq!(caller.recv().await, Frame::StreamEnd { id: 4 });
}

#[tokio::test(start_paused = true)]
async fn subscribers_receive_target_and_group_events() {
    let bus = start(Config::default());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("game-1", &[], &["games"]).await;
    let mut direct = Peer::connect(&bus, anyone()).await;
    let mut group = Peer::connect(&bus, anyone()).await;
    direct
        .send(Frame::Subscribe {
            id: 1,
            target: "game-1".into(),
            event: "joined".into(),
        })
        .await;
    assert_eq!(direct.recv().await, Frame::Accepted { id: 1 });
    group
        .send(Frame::Subscribe {
            id: 2,
            target: "group:games".into(),
            event: "*".into(),
        })
        .await;
    assert_eq!(group.recv().await, Frame::Accepted { id: 2 });

    target
        .send(Frame::Emit {
            event: "joined".into(),
            payload: b"bob".to_vec(),
        })
        .await;
    target
        .send(Frame::Emit {
            event: "left".into(),
            payload: b"amy".to_vec(),
        })
        .await;
    let joined = Frame::Event {
        target: "game-1".into(),
        event: "joined".into(),
        payload: b"bob".to_vec(),
    };
    assert_eq!(direct.recv().await, joined);
    direct.assert_silent().await;
    assert_eq!(group.recv().await, joined);
    assert!(matches!(group.recv().await, Frame::Event { event, .. } if event == "left"));
}

#[tokio::test(start_paused = true)]
async fn queue_is_bounded() {
    let mut config = Config::default();
    config.limits.max_queue_per_target = 2;
    let bus = start(config);
    let mut caller = Peer::connect(&bus, anyone()).await;
    for id in 0..3 {
        caller
            .send(Frame::Send(envelope(id, "off", "op", b"")))
            .await;
    }
    assert_eq!(expect_fail(&mut caller).await, (2, ErrorCode::QueueFull));
}

#[tokio::test(start_paused = true)]
async fn duplicate_registration_is_refused() {
    let bus = start(Config::default());
    let mut first = Peer::connect(&bus, anyone()).await;
    first.register("solo", &["op"], &[]).await;
    let mut second = Peer::connect(&bus, anyone()).await;
    second
        .send(Frame::Register {
            target: "solo".into(),
            actions: vec![],
            groups: vec![],
        })
        .await;
    assert!(matches!(
        second.recv().await,
        Frame::Error {
            code: ErrorCode::NameTaken,
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn list_targets_reports_state() {
    let bus = start(Config::default());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["op"], &["g"]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller.send(Frame::ListTargets { id: 8 }).await;
    let Frame::Targets { id, targets } = caller.recv().await else {
        panic!("expected targets");
    };
    assert_eq!(id, 8);
    assert_eq!(targets.len(), 1);
    assert!(targets[0].online);
    assert_eq!(targets[0].groups, vec!["g"]);
}
