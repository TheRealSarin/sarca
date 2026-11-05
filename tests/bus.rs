use std::time::Duration;

use sarca::auth::{Authenticator, Principal, hash_token};
use sarca::bus::{self, BusHandle, SessionId};
use sarca::config::{AuthConfig, Config, Grant, RateLimit, Rule, TokenConfig};
use sarca::outbound::Outbound;
use sarca::wire::{self, ActionSpec, Envelope, ErrorCode, Frame, Mode, Retry};
use tokio::time::Instant;

const RECV_DEADLINE: Duration = Duration::from_secs(3600);
const SILENCE_WINDOW: Duration = Duration::from_millis(50);

struct Peer {
    bus: BusHandle,
    session: SessionId,
    rx: Outbound,
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
        tokio::time::timeout(RECV_DEADLINE, self.rx.recv())
            .await
            .expect("timed out waiting for frame")
            .expect("session closed")
    }

    async fn assert_silent(&mut self) {
        let result = tokio::time::timeout(SILENCE_WINDOW, self.rx.recv()).await;
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

    let start = Instant::now();
    let mut delivered_at = Vec::new();
    for _ in 0..3 {
        let delivered = expect_deliver(&mut target).await;
        delivered_at.push(Instant::now() - start);
        target
            .send(Frame::Fail {
                id: delivered.id,
                code: ErrorCode::TargetFailed,
                message: "busy".into(),
                retryable: true,
            })
            .await;
    }
    assert_eq!(
        delivered_at,
        [
            Duration::ZERO,
            Duration::from_millis(1_000),
            Duration::from_millis(3_000)
        ]
    );
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
                groups: vec![],
            }),
            tokens: vec![TokenConfig {
                name: "ops".into(),
                sha256: ops_hash,
                grant: Grant {
                    caps: vec!["game.admin".into()],
                    register: vec![],
                    send: vec!["game-*".into()],
                    groups: vec![],
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

#[tokio::test(start_paused = true)]
async fn partially_sent_stream_is_not_retried() {
    let bus = start(Config::default());
    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["tail"], &[]).await;

    let mut env = envelope(6, "t", "tail", b"");
    env.mode = Mode::Stream;
    env.retry = Retry {
        max_attempts: 5,
        backoff_ms: 0,
    };
    caller.send(Frame::Send(env)).await;
    let id = expect_deliver(&mut target).await.id;
    target
        .send(Frame::StreamItem {
            id,
            payload: b"one".to_vec(),
        })
        .await;
    assert!(matches!(
        caller.recv().await,
        Frame::StreamItem { id: 6, .. }
    ));
    target.disconnect().await;
    assert_eq!(expect_fail(&mut caller).await, (6, ErrorCode::Unavailable));
}

#[tokio::test(start_paused = true)]
async fn requests_of_a_disconnected_caller_are_dropped() {
    let bus = start(Config::default());
    let caller = Peer::connect(&bus, anyone()).await;
    caller
        .send(Frame::Send(envelope(1, "later", "op", b"")))
        .await;
    caller.disconnect().await;

    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("later", &["op"], &[]).await;
    target.assert_silent().await;
}

fn principal(name: &str, grant: Grant) -> Principal {
    let token = format!("{name}-token");
    Authenticator::new(&AuthConfig {
        anonymous: None,
        tokens: vec![TokenConfig {
            name: name.into(),
            sha256: hash_token(&token),
            grant,
        }],
    })
    .authenticate(Some(&token))
    .unwrap()
}

async fn expect_error(peer: &mut Peer) -> ErrorCode {
    match peer.recv().await {
        Frame::Error { code, .. } => code,
        other => panic!("expected error, got {other:?}"),
    }
}

fn subscribe(id: u64, target: &str, event: &str) -> Frame {
    Frame::Subscribe {
        id,
        target: target.into(),
        event: event.into(),
    }
}

#[tokio::test(start_paused = true)]
async fn joining_a_group_requires_a_group_grant() {
    let bus = start(Config::default());
    let grant = Grant {
        register: vec!["dev-*".into()],
        groups: vec!["dev".into()],
        ..Grant::default()
    };
    let mut intruder = Peer::connect(&bus, principal("dev", grant.clone())).await;
    intruder
        .send(Frame::Register {
            target: "dev-worker".into(),
            actions: vec![ActionSpec::new("deploy")],
            groups: vec!["dev".into(), "production".into()],
        })
        .await;
    assert_eq!(expect_error(&mut intruder).await, ErrorCode::Forbidden);

    let mut caller = Peer::connect(&bus, anyone()).await;
    let mut env = envelope(1, "group:production", "deploy", b"secret");
    env.ttl = 1_000;
    caller.send(Frame::Send(env)).await;
    assert_eq!(expect_fail(&mut caller).await, (1, ErrorCode::Expired));

    let mut worker = Peer::connect(&bus, principal("dev", grant)).await;
    worker.register("dev-worker", &["deploy"], &["dev"]).await;
}

#[tokio::test(start_paused = true)]
async fn pending_requests_are_bounded_by_count_and_bytes() {
    let mut config = Config::default();
    config.limits.max_pending = 3;
    config.limits.max_pending_per_principal = 2;
    config.limits.max_pending_bytes_per_principal = 1_500;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("slow", &["op"], &[]).await;
    let mut alice = Peer::connect(&bus, principal("alice", Grant::everything())).await;
    let mut bob = Peer::connect(&bus, principal("bob", Grant::everything())).await;

    alice
        .send(Frame::Send(envelope(1, "slow", "op", &[0; 2_000])))
        .await;
    assert_eq!(expect_fail(&mut alice).await, (1, ErrorCode::QuotaExceeded));

    alice
        .send(Frame::Send(envelope(2, "slow", "op", b"")))
        .await;
    alice
        .send(Frame::Send(envelope(3, "other", "op", b"")))
        .await;
    alice
        .send(Frame::Send(envelope(4, "slow", "op", b"")))
        .await;
    assert_eq!(expect_fail(&mut alice).await, (4, ErrorCode::QuotaExceeded));

    bob.send(Frame::Send(envelope(5, "slow", "op", b""))).await;
    bob.send(Frame::Send(envelope(6, "slow", "op", b""))).await;
    assert_eq!(expect_fail(&mut bob).await, (6, ErrorCode::Overloaded));

    let first = expect_deliver(&mut target).await;
    target
        .send(Frame::Reply {
            id: first.id,
            payload: vec![],
        })
        .await;
    assert!(matches!(alice.recv().await, Frame::Reply { id: 2, .. }));
    alice
        .send(Frame::Send(envelope(7, "slow", "op", b"")))
        .await;
    expect_deliver(&mut target).await;
    expect_deliver(&mut target).await;
    alice.assert_silent().await;
}

#[tokio::test(start_paused = true)]
async fn subscriptions_are_capped_per_session() {
    let mut config = Config::default();
    config.limits.max_subscriptions_per_session = 2;
    let bus = start(config);
    let mut peer = Peer::connect(&bus, anyone()).await;
    peer.send(subscribe(1, "t", "a")).await;
    peer.send(subscribe(2, "t", "b")).await;
    peer.send(subscribe(3, "t", "a")).await;
    peer.send(subscribe(4, "t", "c")).await;
    assert_eq!(peer.recv().await, Frame::Accepted { id: 1 });
    assert_eq!(peer.recv().await, Frame::Accepted { id: 2 });
    assert_eq!(peer.recv().await, Frame::Accepted { id: 3 });
    assert_eq!(expect_fail(&mut peer).await, (4, ErrorCode::QuotaExceeded));

    peer.send(Frame::Unsubscribe {
        target: "t".into(),
        event: "a".into(),
    })
    .await;
    peer.send(subscribe(5, "t", "c")).await;
    assert_eq!(peer.recv().await, Frame::Accepted { id: 5 });
}

#[tokio::test(start_paused = true)]
async fn subscriptions_are_capped_per_principal_across_sessions() {
    let mut config = Config::default();
    config.limits.max_subscriptions_per_principal = 1;
    let bus = start(config);
    let mut first = Peer::connect(&bus, anyone()).await;
    let mut second = Peer::connect(&bus, anyone()).await;
    first.send(subscribe(1, "t", "a")).await;
    assert_eq!(first.recv().await, Frame::Accepted { id: 1 });
    second.send(subscribe(2, "t", "b")).await;
    assert_eq!(
        expect_fail(&mut second).await,
        (2, ErrorCode::QuotaExceeded)
    );
    first.disconnect().await;
    second.send(subscribe(3, "t", "b")).await;
    assert_eq!(second.recv().await, Frame::Accepted { id: 3 });
}

#[tokio::test(start_paused = true)]
async fn offline_registrations_are_bounded_and_expire() {
    let mut config = Config::default();
    config.limits.max_targets_per_principal = 1;
    config.limits.offline_target_retention_ms = 10_000;
    let bus = start(config);

    let mut first = Peer::connect(&bus, anyone()).await;
    first.register("old", &["op"], &["pool"]).await;
    first.disconnect().await;

    let mut second = Peer::connect(&bus, anyone()).await;
    second
        .send(Frame::Register {
            target: "new".into(),
            actions: vec![],
            groups: vec![],
        })
        .await;
    assert_eq!(expect_error(&mut second).await, ErrorCode::QuotaExceeded);
    second.register("old", &["op"], &["pool"]).await;
    second.disconnect().await;

    tokio::time::sleep(Duration::from_millis(10_001)).await;
    let mut lister = Peer::connect(&bus, anyone()).await;
    lister.send(Frame::ListTargets { id: 1 }).await;
    let Frame::Targets { targets, .. } = lister.recv().await else {
        panic!("expected targets");
    };
    assert!(targets.is_empty(), "offline target was not forgotten");

    let mut third = Peer::connect(&bus, anyone()).await;
    third.register("new", &["op"], &["pool"]).await;
    lister
        .send(Frame::Send(envelope(2, "group:pool", "op", b"")))
        .await;
    assert_eq!(expect_deliver(&mut third).await.target, "new");
}

#[tokio::test(start_paused = true)]
async fn global_target_registry_is_bounded() {
    let mut config = Config::default();
    config.limits.max_targets = 1;
    let bus = start(config);
    let mut first = Peer::connect(&bus, principal("a", Grant::everything())).await;
    first.register("one", &[], &[]).await;
    let mut second = Peer::connect(&bus, principal("b", Grant::everything())).await;
    second
        .send(Frame::Register {
            target: "two".into(),
            actions: vec![],
            groups: vec![],
        })
        .await;
    assert_eq!(expect_error(&mut second).await, ErrorCode::Overloaded);
}

#[tokio::test(start_paused = true)]
async fn rate_limit_covers_non_send_operations() {
    let config = Config {
        rate_limit: Some(RateLimit {
            per_second: 0.001,
            burst: 1.0,
        }),
        ..Config::default()
    };
    let bus = start(config);

    let mut lister = Peer::connect(&bus, anyone()).await;
    lister.send(Frame::ListTargets { id: 1 }).await;
    assert!(matches!(lister.recv().await, Frame::Targets { id: 1, .. }));
    lister.send(Frame::ListTargets { id: 2 }).await;
    assert_eq!(expect_fail(&mut lister).await, (2, ErrorCode::RateLimited));

    let mut subscriber = Peer::connect(&bus, anyone()).await;
    subscriber.send(subscribe(3, "*", "*")).await;
    assert_eq!(subscriber.recv().await, Frame::Accepted { id: 3 });
    subscriber.send(subscribe(4, "t", "*")).await;
    assert_eq!(
        expect_fail(&mut subscriber).await,
        (4, ErrorCode::RateLimited)
    );

    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &[], &[]).await;
    target
        .send(Frame::Emit {
            event: "spam".into(),
            payload: vec![0; 1024],
        })
        .await;
    assert_eq!(expect_error(&mut target).await, ErrorCode::RateLimited);
    subscriber.assert_silent().await;
}

#[tokio::test(start_paused = true)]
async fn fire_and_forget_rejected_before_acceptance_reports_failure() {
    let mut config = Config::default();
    config.limits.max_queue_per_target = 1;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    target
        .register_specs(
            "t",
            vec![
                ActionSpec::with_caps("admin", ["root"]),
                ActionSpec::new("log"),
            ],
            &[],
        )
        .await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    let fire = |id: u64, target: &str, action: &str| {
        let mut env = envelope(id, target, action, b"");
        env.mode = Mode::Fire;
        Frame::Send(env)
    };

    caller.send(fire(1, "t", "nope")).await;
    assert_eq!(
        expect_fail(&mut caller).await,
        (1, ErrorCode::UnknownAction)
    );
    caller.send(fire(2, "t", "admin")).await;
    assert_eq!(expect_fail(&mut caller).await, (2, ErrorCode::Forbidden));

    caller.send(fire(3, "offline", "op")).await;
    assert_eq!(caller.recv().await, Frame::Accepted { id: 3 });
    caller.send(fire(4, "offline", "op")).await;
    assert_eq!(expect_fail(&mut caller).await, (4, ErrorCode::QueueFull));

    caller.send(fire(5, "t", "log")).await;
    assert_eq!(caller.recv().await, Frame::Accepted { id: 5 });
    expect_deliver(&mut target).await;
    caller.assert_silent().await;
}

#[tokio::test(start_paused = true)]
async fn capability_lists_are_bounded_validated_and_counted() {
    let mut config = Config::default();
    config.limits.max_pending_bytes_per_principal = 1_500;
    let bus = start(config);
    let mut caller = Peer::connect(&bus, anyone()).await;

    let mut too_many = envelope(1, "off", "op", b"");
    too_many.caps = vec!["a".into(); 65];
    caller.send(Frame::Send(too_many)).await;
    assert_eq!(expect_fail(&mut caller).await, (1, ErrorCode::Invalid));

    let mut empty = envelope(2, "off", "op", b"");
    empty.caps = vec![String::new()];
    caller.send(Frame::Send(empty)).await;
    assert_eq!(expect_fail(&mut caller).await, (2, ErrorCode::Invalid));

    let mut container_heavy = envelope(3, "off", "op", b"");
    container_heavy.caps = vec!["a".into(); 60];
    caller.send(Frame::Send(container_heavy)).await;
    assert_eq!(
        expect_fail(&mut caller).await,
        (3, ErrorCode::QuotaExceeded)
    );

    caller
        .send(Frame::Send(envelope(4, "off", "op", b"")))
        .await;
    caller.assert_silent().await;
}

#[tokio::test(start_paused = true)]
async fn registration_metadata_is_bounded_by_count_and_bytes() {
    let mut config = Config::default();
    config.limits.max_target_bytes_per_principal = 4_000;
    let bus = start(config);

    let mut peer = Peer::connect(&bus, anyone()).await;
    peer.send(Frame::Register {
        target: "t".into(),
        actions: vec![ActionSpec::with_caps("op", vec!["a"; 65])],
        groups: vec![],
    })
    .await;
    assert_eq!(expect_error(&mut peer).await, ErrorCode::Invalid);

    let long_cap = "c".repeat(50);
    peer.send(Frame::Register {
        target: "t".into(),
        actions: vec![ActionSpec::with_caps("op", vec![long_cap.as_str(); 60])],
        groups: vec![],
    })
    .await;
    assert_eq!(expect_error(&mut peer).await, ErrorCode::QuotaExceeded);

    peer.register("t", &["op"], &[]).await;
}

#[tokio::test(start_paused = true)]
async fn slow_subscriber_is_closed_once_its_byte_budget_is_spent() {
    let mut config = Config::default();
    config.limits.max_outbound_bytes_per_session = 3_000;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &[], &[]).await;
    let mut slow = Peer::connect(&bus, anyone()).await;
    slow.send(subscribe(1, "t", "*")).await;
    for _ in 0..5 {
        target
            .send(Frame::Emit {
                event: "blob".into(),
                payload: vec![0; 1_000],
            })
            .await;
    }
    target.send(Frame::Ping { nonce: 1 }).await;
    assert_eq!(target.recv().await, Frame::Pong { nonce: 1 });

    assert!(slow.rx.is_closed());
    assert_eq!(slow.recv().await, Frame::Accepted { id: 1 });
    let mut events = 0;
    while let Some(frame) = slow.rx.recv().await {
        assert!(matches!(frame, Frame::Event { .. }));
        events += 1;
    }
    assert!((1..5).contains(&events), "received {events} events");
}

#[tokio::test(start_paused = true)]
async fn kicked_session_signals_its_connection_to_close() {
    let bus = start(Config::default());
    let mut peer = Peer::connect(&bus, anyone()).await;
    assert!(!peer.rx.is_closed());
    peer.send(Frame::Welcome { session: 0 }).await;
    assert_eq!(expect_error(&mut peer).await, ErrorCode::Protocol);
    assert!(peer.rx.recv().await.is_none());
    assert!(peer.rx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn sessions_are_capped_per_principal() {
    let mut config = Config::default();
    config.limits.max_sessions_per_principal = 1;
    let bus = start(config);
    let first = Peer::connect(&bus, anyone()).await;
    let (_, mut refused) = bus.connect(anyone()).await.unwrap();
    assert!(matches!(
        refused.recv().await,
        Some(Frame::Error {
            code: ErrorCode::QuotaExceeded,
            ..
        })
    ));
    assert!(refused.recv().await.is_none());
    assert!(refused.is_closed());

    let _other = Peer::connect(&bus, principal("other", Grant::everything())).await;
    first.disconnect().await;
    let _again = Peer::connect(&bus, anyone()).await;
}

#[tokio::test(start_paused = true)]
async fn oversized_listing_returns_error_without_disconnecting() {
    let mut config = Config::default();
    config.limits.max_frame_bytes = 1024;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    let actions = (0..10)
        .map(|i| ActionSpec::new(format!("action-{i}-{}", "a".repeat(100))))
        .collect();
    target.register_specs("t", actions, &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller.send(Frame::ListTargets { id: 1 }).await;
    assert_eq!(expect_fail(&mut caller).await, (1, ErrorCode::Overloaded));
    caller.send(Frame::Ping { nonce: 42 }).await;
    assert_eq!(caller.recv().await, Frame::Pong { nonce: 42 });
}

#[tokio::test(start_paused = true)]
async fn backlog_larger_than_the_session_buffer_drains_in_order() {
    let mut config = Config::default();
    config.limits.session_buffer = 4;
    config.limits.max_queue_per_target = 10;
    let bus = start(config);
    let mut caller = Peer::connect(&bus, anyone()).await;
    for id in 0..10u8 {
        caller
            .send(Frame::Send(envelope(id.into(), "t", "op", &[id])))
            .await;
    }
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["op"], &[]).await;
    for id in 0..10u8 {
        let delivered = expect_deliver(&mut target).await;
        assert_eq!(delivered.payload, [id]);
        target
            .send(Frame::Reply {
                id: delivered.id,
                payload: vec![id],
            })
            .await;
        assert_eq!(
            caller.recv().await,
            Frame::Reply {
                id: id.into(),
                payload: vec![id],
            }
        );
    }
    assert!(!target.rx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn burst_beyond_a_live_target_buffer_waits_instead_of_closing_it() {
    let mut config = Config::default();
    config.limits.session_buffer = 2;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["op"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    for id in 0..6u8 {
        caller
            .send(Frame::Send(envelope(id.into(), "t", "op", &[id])))
            .await;
    }
    caller.assert_silent().await;
    for id in 0..6u8 {
        assert_eq!(expect_deliver(&mut target).await.payload, [id]);
    }
    assert!(!target.rx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn group_delivery_skips_a_member_with_no_room() {
    let mut config = Config::default();
    config.limits.session_buffer = 1;
    let bus = start(config);
    let mut a = Peer::connect(&bus, anyone()).await;
    a.register("a", &["op"], &["pool"]).await;
    let mut b = Peer::connect(&bus, anyone()).await;
    b.register("b", &["op"], &["pool"]).await;
    let caller = Peer::connect(&bus, anyone()).await;

    caller
        .send(Frame::Send(envelope(1, "a", "op", b"direct")))
        .await;
    caller
        .send(Frame::Send(envelope(2, "group:pool", "op", b"pooled")))
        .await;
    assert_eq!(expect_deliver(&mut b).await.payload, b"pooled");
    assert_eq!(expect_deliver(&mut a).await.payload, b"direct");
}

#[tokio::test(start_paused = true)]
async fn fallback_requires_a_send_grant_for_the_backup() {
    let config = Config {
        rules: vec![Rule {
            when_down: "primary".into(),
            route_to: vec!["backup".into()],
        }],
        ..Config::default()
    };
    let bus = start(config);
    let mut backup = Peer::connect(&bus, anyone()).await;
    backup.register("backup", &["op"], &[]).await;
    let narrow = Grant {
        send: vec!["primary".into()],
        ..Grant::default()
    };
    let mut caller = Peer::connect(&bus, principal("narrow", narrow)).await;
    caller
        .send(Frame::Send(envelope(1, "primary", "op", b"kept")))
        .await;
    caller.assert_silent().await;
    backup.assert_silent().await;

    let mut primary = Peer::connect(&bus, anyone()).await;
    primary.register("primary", &["op"], &[]).await;
    assert_eq!(expect_deliver(&mut primary).await.payload, b"kept");
}

#[tokio::test(start_paused = true)]
async fn outbound_budget_is_shared_by_a_principal_sessions() {
    let mut config = Config::default();
    config.limits.max_outbound_bytes_per_session = 4_096;
    config.limits.max_outbound_bytes_per_principal = 2_500;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &[], &[]).await;
    let mut slow = Vec::new();
    for _ in 0..2 {
        let mut peer = Peer::connect(&bus, principal("slow", Grant::everything())).await;
        peer.send(subscribe(1, "t", "*")).await;
        assert_eq!(peer.recv().await, Frame::Accepted { id: 1 });
        slow.push(peer);
    }
    let mut other = Peer::connect(&bus, principal("other", Grant::everything())).await;
    other.send(subscribe(1, "t", "*")).await;
    assert_eq!(other.recv().await, Frame::Accepted { id: 1 });

    for _ in 0..2 {
        target
            .send(Frame::Emit {
                event: "blob".into(),
                payload: vec![0; 1_000],
            })
            .await;
    }
    target.send(Frame::Ping { nonce: 1 }).await;
    assert_eq!(target.recv().await, Frame::Pong { nonce: 1 });

    for peer in &mut slow {
        assert!(peer.rx.is_closed());
        assert!(matches!(peer.rx.recv().await, Some(Frame::Event { .. })));
        assert!(peer.rx.recv().await.is_none());
    }
    for _ in 0..2 {
        assert!(matches!(other.recv().await, Frame::Event { .. }));
    }
    assert!(!other.rx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn message_that_can_never_fit_a_session_is_rejected() {
    let mut config = Config::default();
    config.limits.max_outbound_bytes_per_session = 4_096;
    let bus = start(config);
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["op"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller
        .send(Frame::Send(envelope(1, "t", "op", &[0; 4_000])))
        .await;
    assert_eq!(expect_fail(&mut caller).await, (1, ErrorCode::Invalid));
    target.assert_silent().await;
}

fn with_small_frames() -> Config {
    let mut config = Config::default();
    config.limits.max_frame_bytes = 1024;
    config
}

fn encoded_len(frame: &Frame) -> usize {
    wire::encode(frame).unwrap().len()
}

#[tokio::test(start_paused = true)]
async fn send_whose_delivery_could_exceed_the_frame_limit_is_rejected() {
    let bus = start(with_small_frames());
    let mut member = Peer::connect(&bus, anyone()).await;
    member.register(&"m".repeat(200), &["op"], &["pool"]).await;
    let mut direct = Peer::connect(&bus, anyone()).await;
    direct.register("d", &["op"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;

    let grouped = Frame::Send(envelope(1, "group:pool", "op", &[0; 800]));
    assert!(encoded_len(&grouped) <= 1024);
    caller.send(grouped).await;
    assert_eq!(expect_fail(&mut caller).await, (1, ErrorCode::Invalid));
    member.assert_silent().await;
    assert!(!member.rx.is_closed());

    caller
        .send(Frame::Send(envelope(2, "group:pool", "op", &[0; 500])))
        .await;
    let delivered = expect_deliver(&mut member).await;
    assert!(encoded_len(&Frame::Deliver(delivered)) <= 1024);

    caller
        .send(Frame::Send(envelope(3, "d", "op", &[0; 850])))
        .await;
    let delivered = expect_deliver(&mut direct).await;
    assert!(encoded_len(&Frame::Deliver(delivered)) <= 1024);
}

#[tokio::test(start_paused = true)]
async fn event_too_large_to_forward_is_refused_without_closing_subscribers() {
    let bus = start(with_small_frames());
    let name = "t".repeat(200);
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register(&name, &[], &[]).await;
    let mut subscriber = Peer::connect(&bus, anyone()).await;
    subscriber.send(subscribe(1, &name, "*")).await;
    assert_eq!(subscriber.recv().await, Frame::Accepted { id: 1 });

    let emit = Frame::Emit {
        event: "event".into(),
        payload: vec![0; 900],
    };
    assert!(encoded_len(&emit) <= 1024);
    target.send(emit).await;
    assert_eq!(expect_error(&mut target).await, ErrorCode::Invalid);
    subscriber.assert_silent().await;
    assert!(!subscriber.rx.is_closed());

    target
        .send(Frame::Emit {
            event: "event".into(),
            payload: vec![0; 600],
        })
        .await;
    let event = subscriber.recv().await;
    assert!(matches!(&event, Frame::Event { payload, .. } if payload.len() == 600));
    assert!(encoded_len(&event) <= 1024);
}

#[tokio::test(start_paused = true)]
async fn responses_that_grow_past_the_frame_limit_fail_instead_of_closing_the_caller() {
    let bus = start(with_small_frames());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["op"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;

    for mode in [Mode::Request, Mode::Stream] {
        let mut env = envelope(u64::MAX, "t", "op", b"");
        env.mode = mode;
        caller.send(Frame::Send(env)).await;
        let id = expect_deliver(&mut target).await.id;
        let response = |payload: Vec<u8>| match mode {
            Mode::Stream => Frame::StreamItem { id, payload },
            _ => Frame::Reply { id, payload },
        };
        let overhead = encoded_len(&response(vec![0; 300])) - 300;
        let largest = response(vec![0; 1024 - overhead]);
        assert_eq!(encoded_len(&largest), 1024);
        target.send(largest).await;
        assert_eq!(
            expect_fail(&mut caller).await,
            (u64::MAX, ErrorCode::TargetFailed)
        );
    }
    assert!(!caller.rx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn failure_message_too_large_to_relay_is_replaced() {
    let bus = start(with_small_frames());
    let mut target = Peer::connect(&bus, anyone()).await;
    target.register("t", &["op"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;
    caller
        .send(Frame::Send(envelope(u64::MAX, "t", "op", b"")))
        .await;
    let id = expect_deliver(&mut target).await.id;
    let fail = |message: String| Frame::Fail {
        id,
        code: ErrorCode::Rejected,
        message,
        retryable: false,
    };
    let overhead = encoded_len(&fail("m".repeat(300))) - 300;
    let largest = fail("m".repeat(1024 - overhead));
    assert_eq!(encoded_len(&largest), 1024);
    target.send(largest).await;
    match caller.recv().await {
        Frame::Fail {
            id, code, message, ..
        } => {
            assert_eq!((id, code), (u64::MAX, ErrorCode::Rejected));
            assert!(message.len() < 100, "relayed {} bytes", message.len());
        }
        other => panic!("expected fail, got {other:?}"),
    }
    assert!(!caller.rx.is_closed());
}

#[tokio::test(start_paused = true)]
async fn offline_primary_without_the_action_still_falls_back() {
    let config = Config {
        rules: vec![Rule {
            when_down: "primary".into(),
            route_to: vec!["backup".into()],
        }],
        ..Config::default()
    };
    let bus = start(config);
    let mut primary = Peer::connect(&bus, anyone()).await;
    primary.register("primary", &["old"], &[]).await;
    primary.disconnect().await;
    let mut backup = Peer::connect(&bus, anyone()).await;
    backup.register("backup", &["new"], &[]).await;
    let mut caller = Peer::connect(&bus, anyone()).await;

    caller
        .send(Frame::Send(envelope(1, "primary", "new", b"job")))
        .await;
    let delivered = expect_deliver(&mut backup).await;
    assert_eq!(delivered.target, "backup");
    assert_eq!(delivered.payload, b"job");
    backup
        .send(Frame::Reply {
            id: delivered.id,
            payload: vec![],
        })
        .await;
    assert!(matches!(caller.recv().await, Frame::Reply { id: 1, .. }));

    backup.disconnect().await;
    caller
        .send(Frame::Send(envelope(2, "primary", "new", b"")))
        .await;
    assert_eq!(
        expect_fail(&mut caller).await,
        (2, ErrorCode::UnknownAction)
    );
}
