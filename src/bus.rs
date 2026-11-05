use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::Instant;

use crate::auth::Principal;
use crate::config::{Config, Limits};
use crate::footprint;
use crate::hooks::{Hook, HookContext, LoggingHook, Operation, RateLimitHook};
use crate::outbound::{self, ByteBudget, OfferError, Outbound, OutboundSender};
use crate::wire::{
    ActionSpec, Envelope, ErrorCode, Frame, GROUP_PREFIX, MAX_NAME_LEN, Mode, TargetInfo,
    valid_name,
};

pub type SessionId = u64;

const MAX_ACTIONS: usize = 1024;
const MAX_GROUPS: usize = 64;
const MAX_CAPS: usize = 64;
const WILDCARD: &str = "*";
const DRAIN_INTERVAL: Duration = Duration::from_millis(10);

type Failure = (ErrorCode, String);

enum BusEvent {
    Connect {
        session: SessionId,
        principal: Principal,
        out: OutboundSender,
    },
    Frame {
        session: SessionId,
        frame: Frame,
        _charge: OwnedSemaphorePermit,
    },
    Disconnect {
        session: SessionId,
    },
}

#[derive(Clone)]
pub struct BusHandle {
    tx: mpsc::Sender<BusEvent>,
    next_session: Arc<AtomicU64>,
    session_buffer: usize,
    outbound_session_bytes: usize,
    outbound_principal_bytes: usize,
    outbound_principals: Arc<Mutex<HashMap<String, Arc<ByteBudget>>>>,
    outbound_bytes: Arc<ByteBudget>,
    ingress: Arc<Semaphore>,
    ingress_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("bus unavailable")]
pub struct BusClosed;

impl BusHandle {
    pub async fn connect(&self, principal: Principal) -> Result<(SessionId, Outbound), BusClosed> {
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        let principal_budget = self
            .outbound_principals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(principal.name.clone())
            .or_insert_with(|| ByteBudget::new(self.outbound_principal_bytes))
            .clone();
        let (out, rx) = outbound::channel(
            self.session_buffer,
            self.outbound_session_bytes,
            principal_budget,
            self.outbound_bytes.clone(),
        );
        self.tx
            .send(BusEvent::Connect {
                session,
                principal,
                out,
            })
            .await
            .map_err(|_| BusClosed)?;
        Ok((session, rx))
    }

    pub async fn frame(&self, session: SessionId, frame: Frame) -> Result<(), BusClosed> {
        let bytes = footprint::frame_bytes(&frame).min(self.ingress_bytes);
        let bytes = u32::try_from(bytes).unwrap_or(u32::MAX);
        let charge = self
            .ingress
            .clone()
            .acquire_many_owned(bytes)
            .await
            .map_err(|_| BusClosed)?;
        self.tx
            .send(BusEvent::Frame {
                session,
                frame,
                _charge: charge,
            })
            .await
            .map_err(|_| BusClosed)
    }

    pub async fn disconnect(&self, session: SessionId) {
        let _ = self.tx.send(BusEvent::Disconnect { session }).await;
    }
}

pub fn spawn(config: &Config, extra_hooks: Vec<Arc<dyn Hook>>) -> BusHandle {
    let mut hooks: Vec<Arc<dyn Hook>> = Vec::new();
    if let Some(limit) = config.rate_limit {
        hooks.push(Arc::new(RateLimitHook::new(limit)));
    }
    if config.log_messages {
        hooks.push(Arc::new(LoggingHook));
    }
    hooks.extend(extra_hooks);

    let rules = config
        .rules
        .iter()
        .map(|rule| (rule.when_down.clone(), rule.route_to.clone()))
        .collect();
    let (tx, rx) = mpsc::channel(4096);
    let bus = Bus {
        limits: config.limits.clone(),
        rules,
        hooks,
        sessions: HashMap::new(),
        targets: HashMap::new(),
        queues: HashMap::new(),
        pending: HashMap::new(),
        timers: BTreeMap::new(),
        groups: HashMap::new(),
        subs: HashMap::new(),
        usage: HashMap::new(),
        pending_bytes: 0,
        target_bytes: 0,
        kicks: Vec::new(),
        drains: HashSet::new(),
        outbound_fit: config
            .limits
            .max_outbound_bytes_per_session
            .min(config.limits.max_outbound_bytes_per_principal)
            .min(config.limits.max_outbound_bytes),
        next_id: 1,
        next_timer: 0,
    };
    tokio::spawn(bus.run(rx));
    BusHandle {
        tx,
        next_session: Arc::new(AtomicU64::new(1)),
        session_buffer: config.limits.session_buffer.max(1),
        outbound_session_bytes: config.limits.max_outbound_bytes_per_session,
        outbound_principal_bytes: config.limits.max_outbound_bytes_per_principal,
        outbound_principals: Arc::new(Mutex::new(HashMap::new())),
        outbound_bytes: ByteBudget::new(config.limits.max_outbound_bytes),
        ingress: Arc::new(Semaphore::new(config.limits.max_pending_bytes)),
        ingress_bytes: config.limits.max_pending_bytes,
    }
}

struct Session {
    principal: Principal,
    owner: Arc<str>,
    out: OutboundSender,
    target: Option<String>,
    inflight: HashSet<u64>,
    owned: HashSet<u64>,
    subs: HashSet<(String, String)>,
}

struct TargetEntry {
    session: Option<SessionId>,
    owner: Arc<str>,
    actions: Vec<ActionSpec>,
    groups: Vec<String>,
    bytes: usize,
    forget_key: Option<(Instant, u64)>,
}

#[derive(Default)]
struct Group {
    members: BTreeSet<String>,
    cursor: usize,
}

#[derive(Default)]
struct Usage {
    pending: usize,
    pending_bytes: usize,
    subs: usize,
    targets: usize,
    target_bytes: usize,
    sessions: usize,
}

impl TargetEntry {
    fn action(&self, name: &str) -> Option<&ActionSpec> {
        self.actions.iter().find(|spec| spec.name == name)
    }
}

#[derive(Clone, PartialEq, Eq)]
enum Stage {
    Queued(String),
    Waiting,
    InFlight(SessionId),
}

struct Pending {
    caller: SessionId,
    owner: Arc<str>,
    bytes: usize,
    caller_id: u64,
    envelope: Envelope,
    attempts: u32,
    max_attempts: u32,
    deadline: Instant,
    expire_key: (Instant, u64),
    retry_key: Option<(Instant, u64)>,
    streamed: bool,
    acknowledged: bool,
    stage: Stage,
}

enum Timer {
    Expire(u64),
    Retry(u64),
    Forget(String),
    Drain(String),
}

enum Route {
    Online(SessionId, String),
    Busy,
    Offline,
    Reject(ErrorCode, String),
}

enum Attempt {
    Done(bool),
    Wait { busy: bool },
}

struct Bus {
    limits: Limits,
    rules: HashMap<String, Vec<String>>,
    hooks: Vec<Arc<dyn Hook>>,
    sessions: HashMap<SessionId, Session>,
    targets: HashMap<String, TargetEntry>,
    queues: HashMap<String, VecDeque<u64>>,
    pending: HashMap<u64, Pending>,
    timers: BTreeMap<(Instant, u64), Timer>,
    groups: HashMap<String, Group>,
    subs: HashMap<(String, String), HashSet<SessionId>>,
    usage: HashMap<Arc<str>, Usage>,
    pending_bytes: usize,
    target_bytes: usize,
    kicks: Vec<SessionId>,
    drains: HashSet<String>,
    outbound_fit: usize,
    next_id: u64,
    next_timer: u64,
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn valid_route(key: &str) -> bool {
    match key.strip_prefix(GROUP_PREFIX) {
        Some(group) => valid_name(group),
        None => valid_name(key),
    }
}

fn valid_pattern(value: &str, check: fn(&str) -> bool) -> bool {
    value == WILDCARD || check(value)
}

fn retained_bytes(envelope: &Envelope) -> usize {
    footprint::envelope_bytes(envelope) + std::mem::size_of::<Pending>()
}

fn valid_caps(caps: &[String]) -> bool {
    caps.len() <= MAX_CAPS && caps.iter().all(|cap| valid_name(cap))
}

fn is_retryable(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::RateLimited | ErrorCode::Overloaded | ErrorCode::QuotaExceeded
    )
}

impl Bus {
    async fn run(mut self, mut rx: mpsc::Receiver<BusEvent>) {
        loop {
            let next_timer = self.timers.keys().next().map(|(at, _)| *at);
            tokio::select! {
                event = rx.recv() => match event {
                    Some(event) => self.handle(event),
                    None => break,
                },
                () = sleep_until(next_timer) => self.fire_timers(Instant::now()),
            }
            self.process_kicks();
        }
    }

    fn handle(&mut self, event: BusEvent) {
        match event {
            BusEvent::Connect {
                session,
                principal,
                out,
            } => self.on_connect(session, principal, out),
            BusEvent::Disconnect { session } => self.drop_session(session),
            BusEvent::Frame {
                session,
                frame,
                _charge,
            } => {
                if self.sessions.contains_key(&session) {
                    self.on_frame(session, frame);
                }
            }
        }
    }

    fn on_connect(&mut self, session: SessionId, principal: Principal, out: OutboundSender) {
        let owner: Arc<str> = Arc::from(principal.name.as_str());
        let open = self.usage_of(&owner).map_or(0, |usage| usage.sessions);
        if open >= self.limits.max_sessions_per_principal {
            tracing::warn!(session, principal = %owner, "session quota exceeded, refusing session");
            let frame = Frame::Error {
                code: ErrorCode::QuotaExceeded,
                message: "too many sessions for this principal".into(),
            };
            let _ = out.offer(footprint::frame_bytes(&frame), move || frame);
            out.close();
            return;
        }
        self.usage.entry(owner.clone()).or_default().sessions += 1;
        self.sessions.insert(
            session,
            Session {
                principal,
                owner,
                out,
                target: None,
                inflight: HashSet::new(),
                owned: HashSet::new(),
                subs: HashSet::new(),
            },
        );
        self.send_to(session, Frame::Welcome { session });
    }

    fn on_frame(&mut self, session: SessionId, frame: Frame) {
        match frame {
            Frame::Send(envelope) => self.on_send(session, envelope),
            Frame::Register {
                target,
                actions,
                groups,
            } => self.on_register(session, target, actions, groups),
            Frame::Reply { .. }
            | Frame::StreamItem { .. }
            | Frame::StreamEnd { .. }
            | Frame::Fail { .. } => self.on_target_response(session, frame),
            Frame::Subscribe { id, target, event } => self.on_subscribe(session, id, target, event),
            Frame::Unsubscribe { target, event } => self.unsubscribe(session, &(target, event)),
            Frame::Emit { event, payload } => self.on_emit(session, event, payload),
            Frame::ListTargets { id } => self.on_list(session, id),
            Frame::Ping { nonce } => {
                self.send_to(session, Frame::Pong { nonce });
            }
            Frame::Pong { .. } => {}
            other => {
                tracing::warn!(
                    session,
                    frame = other.kind(),
                    "unexpected frame, closing session"
                );
                self.send_to(
                    session,
                    Frame::Error {
                        code: ErrorCode::Protocol,
                        message: "unexpected frame".into(),
                    },
                );
                self.kicks.push(session);
            }
        }
    }

    fn send_to(&mut self, session: SessionId, frame: Frame) -> bool {
        let bytes = footprint::frame_bytes(&frame);
        self.send_lazy(session, bytes, move || frame)
    }

    fn send_lazy(
        &mut self,
        session: SessionId,
        bytes: usize,
        build: impl FnOnce() -> Frame,
    ) -> bool {
        let Some(entry) = self.sessions.get(&session) else {
            return false;
        };
        let Err(err) = entry.out.offer(bytes, build) else {
            return true;
        };
        if err != OfferError::Closed {
            tracing::warn!(session, reason = ?err, "session outbound buffer full, closing session");
        }
        self.kicks.push(session);
        false
    }

    fn session_error(&mut self, session: SessionId, code: ErrorCode, message: impl Into<String>) {
        self.send_to(
            session,
            Frame::Error {
                code,
                message: message.into(),
            },
        );
    }

    fn process_kicks(&mut self) {
        while let Some(session) = self.kicks.pop() {
            self.drop_session(session);
        }
    }

    fn add_timer(&mut self, at: Instant, timer: Timer) -> (Instant, u64) {
        let key = (at, self.next_timer);
        self.next_timer += 1;
        self.timers.insert(key, timer);
        key
    }

    fn fire_timers(&mut self, now: Instant) {
        while let Some(entry) = self.timers.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let (key, timer) = entry.remove_entry();
            match timer {
                Timer::Expire(id) => self.fail(id, ErrorCode::Expired, "ttl expired".into()),
                Timer::Forget(name) => self.forget_target(&name, key),
                Timer::Drain(name) => {
                    self.drains.remove(&name);
                    self.drain_queue(&name);
                }
                Timer::Retry(id) => {
                    let Some(pending) = self.pending.get_mut(&id) else {
                        continue;
                    };
                    if pending.retry_key != Some(key) {
                        continue;
                    }
                    pending.retry_key = None;
                    if pending.stage == Stage::Waiting {
                        self.dispatch(id);
                    }
                }
            }
        }
    }

    fn check_operation(&self, session: SessionId, operation: Operation<'_>) -> Result<(), Failure> {
        let Some(entry) = self.sessions.get(&session) else {
            return Err((ErrorCode::Protocol, "unknown session".into()));
        };
        let ctx = HookContext {
            session,
            principal: &entry.principal.name,
        };
        for hook in &self.hooks {
            hook.on_operation(&ctx, operation)
                .map_err(|rejection| (rejection.code, rejection.message))?;
        }
        Ok(())
    }

    fn usage_of(&self, owner: &str) -> Option<&Usage> {
        self.usage.get(owner)
    }

    fn check_pending_quota(&self, owner: &str, bytes: usize) -> Result<(), Failure> {
        let (count, used) = self
            .usage_of(owner)
            .map_or((0, 0), |usage| (usage.pending, usage.pending_bytes));
        if count >= self.limits.max_pending_per_principal
            || used.saturating_add(bytes) > self.limits.max_pending_bytes_per_principal
        {
            return Err((
                ErrorCode::QuotaExceeded,
                "pending request quota exceeded".into(),
            ));
        }
        if self.pending.len() >= self.limits.max_pending
            || self.pending_bytes.saturating_add(bytes) > self.limits.max_pending_bytes
        {
            return Err((
                ErrorCode::Overloaded,
                "bus has too many pending requests".into(),
            ));
        }
        Ok(())
    }

    fn validate_send(&self, session: SessionId, envelope: &mut Envelope) -> Result<(), Failure> {
        let Some(caller) = self.sessions.get(&session) else {
            return Err((ErrorCode::Protocol, "unknown session".into()));
        };
        if !valid_route(&envelope.target) || !valid_name(&envelope.action) {
            return Err((ErrorCode::Invalid, "invalid target or action name".into()));
        }
        if !valid_caps(&envelope.caps) {
            return Err((
                ErrorCode::Invalid,
                format!("at most {MAX_CAPS} valid capability names are allowed"),
            ));
        }
        if !caller.principal.may_address(&envelope.target) {
            return Err((
                ErrorCode::Forbidden,
                format!("not allowed to address '{}'", envelope.target),
            ));
        }
        if let Some(cap) = envelope
            .caps
            .iter()
            .find(|cap| !caller.principal.has_cap(cap))
        {
            return Err((
                ErrorCode::Forbidden,
                format!("capability '{cap}' not granted"),
            ));
        }
        let ctx = HookContext {
            session,
            principal: &caller.principal.name,
        };
        for hook in &self.hooks {
            hook.on_send(&ctx, envelope)
                .map_err(|rejection| (rejection.code, rejection.message))?;
        }
        if footprint::delivery_bytes(envelope, "") + MAX_NAME_LEN > self.outbound_fit {
            return Err((ErrorCode::Invalid, "message is too large to deliver".into()));
        }
        self.check_pending_quota(&caller.owner, retained_bytes(envelope))
    }

    fn reply_fail(&mut self, session: SessionId, id: u64, (code, message): Failure) {
        self.send_to(
            session,
            Frame::Fail {
                id,
                code,
                message,
                retryable: is_retryable(code),
            },
        );
    }

    fn on_send(&mut self, session: SessionId, mut envelope: Envelope) {
        let caller_id = envelope.id;
        if let Err(failure) = self.validate_send(session, &mut envelope) {
            return self.reply_fail(session, caller_id, failure);
        }
        let Some(owner) = self.sessions.get(&session).map(|s| s.owner.clone()) else {
            return;
        };

        envelope.ttl = match envelope.ttl {
            0 => self.limits.default_ttl_ms,
            ttl => ttl.min(self.limits.max_ttl_ms),
        };
        let max_attempts = envelope
            .retry
            .max_attempts
            .clamp(1, self.limits.max_attempts);
        envelope.retry.max_attempts = max_attempts;
        let mode = envelope.mode;
        let id = self.next_id;
        self.next_id += 1;
        let deadline = Instant::now() + Duration::from_millis(envelope.ttl);
        let expire_key = self.add_timer(deadline, Timer::Expire(id));
        let bytes = retained_bytes(&envelope);
        self.pending_bytes += bytes;
        let usage = self.usage.entry(owner.clone()).or_default();
        usage.pending += 1;
        usage.pending_bytes += bytes;
        self.pending.insert(
            id,
            Pending {
                caller: session,
                owner,
                bytes,
                caller_id,
                envelope,
                attempts: 0,
                max_attempts,
                deadline,
                expire_key,
                retry_key: None,
                streamed: false,
                acknowledged: false,
                stage: Stage::Waiting,
            },
        );
        if let Some(caller) = self.sessions.get_mut(&session) {
            caller.owned.insert(id);
        }

        let accepted = self.dispatch(id);
        if mode == Mode::Fire && accepted {
            if let Some(pending) = self.pending.get_mut(&id) {
                pending.acknowledged = true;
            }
            self.send_to(session, Frame::Accepted { id: caller_id });
        }
    }

    fn dispatch(&mut self, id: u64) -> bool {
        let Some(pending) = self.pending.get(&id) else {
            return false;
        };
        let key = pending.envelope.target.clone();
        if self.queues.get(&key).is_some_and(|queue| !queue.is_empty()) {
            return self.enqueue_waiting(id, key, true);
        }
        match self.attempt(id) {
            Attempt::Done(accepted) => accepted,
            Attempt::Wait { busy } => self.enqueue_waiting(id, key, busy),
        }
    }

    fn enqueue_waiting(&mut self, id: u64, key: String, drain: bool) -> bool {
        if !self.enqueue(id, key.clone()) {
            return false;
        }
        if drain {
            self.schedule_drain(key);
        }
        true
    }

    fn schedule_drain(&mut self, key: String) {
        if self.drains.insert(key.clone()) {
            self.add_timer(Instant::now() + DRAIN_INTERVAL, Timer::Drain(key));
        }
    }

    fn attempt(&mut self, id: u64) -> Attempt {
        let Some(pending) = self.pending.get(&id) else {
            return Attempt::Done(false);
        };
        let key = pending.envelope.target.clone();
        let action = pending.envelope.action.clone();
        let base = footprint::delivery_bytes(&pending.envelope, "");
        let caller = pending.caller;
        match self.resolve(&key, &action, base, caller) {
            Route::Online(session, name) => self.deliver(id, session, &name),
            Route::Busy => Attempt::Wait { busy: true },
            Route::Offline => Attempt::Wait { busy: false },
            Route::Reject(code, message) => {
                self.fail(id, code, message);
                Attempt::Done(false)
            }
        }
    }

    fn deliver(&mut self, id: u64, session: SessionId, name: &str) -> Attempt {
        let Some(pending) = self.pending.get(&id) else {
            return Attempt::Done(false);
        };
        let required = self
            .targets
            .get(name)
            .and_then(|entry| entry.action(&pending.envelope.action))
            .map(|spec| spec.caps.clone())
            .unwrap_or_default();
        if let Some(cap) = required
            .iter()
            .find(|cap| !pending.envelope.caps.contains(cap))
        {
            let message = format!("action requires capability '{cap}'");
            self.fail(id, ErrorCode::Forbidden, message);
            return Attempt::Done(false);
        }
        let Some(target) = self.sessions.get(&session) else {
            return Attempt::Wait { busy: true };
        };
        let bytes = footprint::delivery_bytes(&pending.envelope, name);
        let remaining = pending.deadline.saturating_duration_since(Instant::now());
        let ttl = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let offered = target.out.offer(bytes, || {
            let mut envelope = pending.envelope.clone();
            envelope.id = id;
            envelope.target = name.to_string();
            envelope.ttl = ttl;
            Frame::Deliver(envelope)
        });
        match offered {
            Ok(()) => {}
            Err(OfferError::FramesFull | OfferError::BytesFull) => {
                return Attempt::Wait { busy: true };
            }
            Err(OfferError::Closed) => {
                self.kicks.push(session);
                return Attempt::Wait { busy: true };
            }
            Err(OfferError::TooLarge) => {
                self.fail(
                    id,
                    ErrorCode::Invalid,
                    "message is too large to deliver".into(),
                );
                return Attempt::Done(false);
            }
        }
        let Some(pending) = self.pending.get_mut(&id) else {
            return Attempt::Done(false);
        };
        pending.attempts += 1;
        pending.stage = Stage::InFlight(session);
        if pending.envelope.mode == Mode::Fire {
            self.finish(id);
        } else if let Some(target) = self.sessions.get_mut(&session) {
            target.inflight.insert(id);
        }
        Attempt::Done(true)
    }

    fn enqueue(&mut self, id: u64, key: String) -> bool {
        let queue = self.queues.entry(key.clone()).or_default();
        if queue.len() >= self.limits.max_queue_per_target {
            self.fail(
                id,
                ErrorCode::QueueFull,
                format!("queue for '{key}' is full"),
            );
            return false;
        }
        queue.push_back(id);
        if let Some(pending) = self.pending.get_mut(&id) {
            pending.stage = Stage::Queued(key);
        }
        true
    }

    fn resolve(&mut self, key: &str, action: &str, base: usize, caller: SessionId) -> Route {
        match self.resolve_direct(key, action, base) {
            Route::Offline => {
                let backups = self.rules.get(key).cloned().unwrap_or_default();
                let mut busy = false;
                for backup in backups {
                    let allowed = self
                        .sessions
                        .get(&caller)
                        .is_some_and(|entry| entry.principal.may_address(&backup));
                    if !allowed {
                        continue;
                    }
                    match self.resolve_direct(&backup, action, base) {
                        route @ Route::Online(..) => return route,
                        Route::Busy => busy = true,
                        Route::Offline | Route::Reject(..) => {}
                    }
                }
                if busy { Route::Busy } else { Route::Offline }
            }
            route => route,
        }
    }

    fn has_room(&self, session: SessionId, bytes: usize) -> bool {
        self.sessions
            .get(&session)
            .is_some_and(|entry| entry.out.has_room(bytes))
    }

    fn resolve_direct(&mut self, key: &str, action: &str, base: usize) -> Route {
        if let Some(group_name) = key.strip_prefix(GROUP_PREFIX) {
            let Some(group) = self.groups.get(group_name) else {
                return Route::Offline;
            };
            let mut online_members = 0;
            let mut candidates: Vec<(&String, SessionId)> = Vec::new();
            for name in &group.members {
                let Some(entry) = self.targets.get(name) else {
                    continue;
                };
                let Some(session) = entry.session else {
                    continue;
                };
                online_members += 1;
                if entry.action(action).is_some() {
                    candidates.push((name, session));
                }
            }
            if candidates.is_empty() {
                return if online_members > 0 {
                    Route::Reject(
                        ErrorCode::UnknownAction,
                        format!("no member of '{key}' supports '{action}'"),
                    )
                } else {
                    Route::Offline
                };
            }
            let start = group.cursor;
            let chosen = (0..candidates.len()).find_map(|offset| {
                let (name, session) = candidates[start.wrapping_add(offset) % candidates.len()];
                self.has_room(session, base + name.len())
                    .then(|| (offset, session, name.clone()))
            });
            let Some((offset, session, name)) = chosen else {
                return Route::Busy;
            };
            if let Some(group) = self.groups.get_mut(group_name) {
                group.cursor = start.wrapping_add(offset).wrapping_add(1);
            }
            return Route::Online(session, name);
        }

        match self.targets.get(key) {
            None => Route::Offline,
            Some(entry) if entry.action(action).is_none() => Route::Reject(
                ErrorCode::UnknownAction,
                format!("'{key}' does not support '{action}'"),
            ),
            Some(entry) => match entry.session {
                Some(session) if self.has_room(session, base + key.len()) => {
                    Route::Online(session, key.to_string())
                }
                Some(_) => Route::Busy,
                None => Route::Offline,
            },
        }
    }

    fn finish(&mut self, id: u64) -> Option<Pending> {
        let pending = self.pending.remove(&id)?;
        self.timers.remove(&pending.expire_key);
        if let Some(key) = pending.retry_key {
            self.timers.remove(&key);
        }
        match &pending.stage {
            Stage::InFlight(session) => {
                if let Some(target) = self.sessions.get_mut(session) {
                    target.inflight.remove(&id);
                }
            }
            Stage::Queued(key) => {
                if let Some(queue) = self.queues.get_mut(key) {
                    queue.retain(|queued| *queued != id);
                    if queue.is_empty() {
                        self.queues.remove(key);
                    }
                }
            }
            Stage::Waiting => {}
        }
        if let Some(caller) = self.sessions.get_mut(&pending.caller) {
            caller.owned.remove(&id);
        }
        self.pending_bytes = self.pending_bytes.saturating_sub(pending.bytes);
        if let Some(usage) = self.usage.get_mut(&pending.owner) {
            usage.pending = usage.pending.saturating_sub(1);
            usage.pending_bytes = usage.pending_bytes.saturating_sub(pending.bytes);
        }
        Some(pending)
    }

    fn fail(&mut self, id: u64, code: ErrorCode, message: String) {
        let Some(pending) = self.finish(id) else {
            return;
        };
        if pending.envelope.mode == Mode::Fire && pending.acknowledged {
            tracing::debug!(id, ?code, ?message, "fire-and-forget message dropped");
            return;
        }
        self.send_to(
            pending.caller,
            Frame::Fail {
                id: pending.caller_id,
                code,
                message,
                retryable: false,
            },
        );
    }

    fn retry_or_fail(&mut self, id: u64, code: ErrorCode, message: String) -> bool {
        let Some(pending) = self.pending.get_mut(&id) else {
            return false;
        };
        if pending.streamed || pending.attempts >= pending.max_attempts {
            self.fail(id, code, message);
            return false;
        }
        if let Stage::InFlight(session) = pending.stage
            && let Some(target) = self.sessions.get_mut(&session)
        {
            target.inflight.remove(&id);
        }
        let exponent = pending.attempts.saturating_sub(1).min(20);
        let delay_ms = pending
            .envelope
            .retry
            .backoff_ms
            .saturating_mul(1u64 << exponent)
            .min(self.limits.max_backoff_ms);
        pending.stage = Stage::Waiting;
        let previous = pending.retry_key.take();
        tracing::debug!(
            id,
            attempts = pending.attempts,
            delay_ms,
            ?message,
            "retrying"
        );
        if let Some(previous) = previous {
            self.timers.remove(&previous);
        }
        let key = self.add_timer(
            Instant::now() + Duration::from_millis(delay_ms),
            Timer::Retry(id),
        );
        if let Some(pending) = self.pending.get_mut(&id) {
            pending.retry_key = Some(key);
        }
        true
    }

    fn on_target_response(&mut self, session: SessionId, frame: Frame) {
        let id = match &frame {
            Frame::Reply { id, .. }
            | Frame::StreamItem { id, .. }
            | Frame::StreamEnd { id }
            | Frame::Fail { id, .. } => *id,
            _ => return,
        };
        let Some(pending) = self.pending.get_mut(&id) else {
            return;
        };
        if pending.stage != Stage::InFlight(session) {
            return;
        }
        let is_stream = pending.envelope.mode == Mode::Stream;
        let caller = pending.caller;
        let caller_id = pending.caller_id;
        match frame {
            Frame::Reply { payload, .. } => {
                self.finish(id);
                self.send_to(
                    caller,
                    Frame::Reply {
                        id: caller_id,
                        payload,
                    },
                );
            }
            Frame::StreamItem { payload, .. } if is_stream => {
                pending.streamed = true;
                self.send_to(
                    caller,
                    Frame::StreamItem {
                        id: caller_id,
                        payload,
                    },
                );
            }
            Frame::StreamEnd { .. } if is_stream => {
                self.finish(id);
                self.send_to(caller, Frame::StreamEnd { id: caller_id });
            }
            Frame::Fail {
                code,
                message,
                retryable,
                ..
            } => {
                if retryable {
                    self.retry_or_fail(id, code, message);
                } else {
                    self.fail(id, code, message);
                }
            }
            _ => self.fail(
                id,
                ErrorCode::TargetFailed,
                "target sent a stream frame for a non-stream request".into(),
            ),
        }
    }

    fn on_register(
        &mut self,
        session: SessionId,
        target: String,
        mut actions: Vec<ActionSpec>,
        mut groups: Vec<String>,
    ) {
        let Some(entry) = self.sessions.get(&session) else {
            return;
        };
        if entry.target.is_some() {
            return self.session_error(session, ErrorCode::Protocol, "session already registered");
        }
        let names_ok = valid_name(&target)
            && !target.starts_with(GROUP_PREFIX)
            && actions.len() <= MAX_ACTIONS
            && groups.len() <= MAX_GROUPS
            && groups.iter().all(|g| valid_name(g))
            && actions
                .iter()
                .all(|a| valid_name(&a.name) && valid_caps(&a.caps));
        if !names_ok {
            return self.session_error(session, ErrorCode::Invalid, "invalid registration");
        }
        if !entry.principal.may_register(&target) {
            return self.session_error(
                session,
                ErrorCode::Forbidden,
                format!("not allowed to register '{target}'"),
            );
        }
        if let Some(group) = groups.iter().find(|g| !entry.principal.may_join(g)) {
            let message = format!("not allowed to join group '{group}'");
            return self.session_error(session, ErrorCode::Forbidden, message);
        }
        let owner = entry.owner.clone();
        if let Err((code, message)) =
            self.check_operation(session, Operation::Register { target: &target })
        {
            return self.session_error(session, code, message);
        }
        if self
            .targets
            .get(&target)
            .is_some_and(|t| t.session.is_some())
        {
            return self.session_error(
                session,
                ErrorCode::NameTaken,
                format!("'{target}' is already registered"),
            );
        }

        actions.sort_by(|a, b| a.name.cmp(&b.name));
        actions.dedup_by(|a, b| a.name == b.name);
        groups.sort();
        groups.dedup();
        let bytes = footprint::registration_bytes(&target, &actions, &groups);
        if let Err((code, message)) = self.check_target_quota(&owner, &target, bytes) {
            return self.session_error(session, code, message);
        }
        self.remove_target(&target);
        for group in &groups {
            self.groups
                .entry(group.clone())
                .or_default()
                .members
                .insert(target.clone());
        }
        let usage = self.usage.entry(owner.clone()).or_default();
        usage.targets += 1;
        usage.target_bytes += bytes;
        self.target_bytes += bytes;
        self.targets.insert(
            target.clone(),
            TargetEntry {
                session: Some(session),
                owner,
                actions,
                groups: groups.clone(),
                bytes,
                forget_key: None,
            },
        );
        if let Some(entry) = self.sessions.get_mut(&session) {
            entry.target = Some(target.clone());
        }
        tracing::info!(session, target = %target, "target registered");
        self.send_to(
            session,
            Frame::Registered {
                target: target.clone(),
            },
        );

        let mut keys: Vec<String> = std::iter::once(target)
            .chain(groups.iter().map(|g| format!("{GROUP_PREFIX}{g}")))
            .collect();
        let fallback_keys: Vec<String> = self
            .rules
            .iter()
            .filter(|(_, backups)| backups.iter().any(|b| keys.contains(b)))
            .map(|(primary, _)| primary.clone())
            .collect();
        keys.extend(fallback_keys);
        for key in keys {
            self.drain_queue(&key);
        }
    }

    fn check_target_quota(&self, owner: &str, target: &str, bytes: usize) -> Result<(), Failure> {
        let existing = self.targets.get(target);
        let replaced_bytes = existing.map_or(0, |entry| entry.bytes);
        let same_owner = existing.is_some_and(|entry| &*entry.owner == owner);
        let (owned, owned_bytes) = self
            .usage_of(owner)
            .map_or((0, 0), |usage| (usage.targets, usage.target_bytes));
        if !same_owner && owned >= self.limits.max_targets_per_principal {
            return Err((
                ErrorCode::QuotaExceeded,
                "target registration quota exceeded".into(),
            ));
        }
        let owner_released = if same_owner { replaced_bytes } else { 0 };
        if owned_bytes.saturating_sub(owner_released) + bytes
            > self.limits.max_target_bytes_per_principal
        {
            return Err((
                ErrorCode::QuotaExceeded,
                "target registration byte quota exceeded".into(),
            ));
        }
        if existing.is_none() && self.targets.len() >= self.limits.max_targets {
            return Err((ErrorCode::Overloaded, "target registry is full".into()));
        }
        if self.target_bytes.saturating_sub(replaced_bytes) + bytes > self.limits.max_target_bytes {
            return Err((
                ErrorCode::Overloaded,
                "target registry byte budget is full".into(),
            ));
        }
        Ok(())
    }

    fn remove_target(&mut self, name: &str) -> Option<TargetEntry> {
        let entry = self.targets.remove(name)?;
        if let Some(key) = entry.forget_key {
            self.timers.remove(&key);
        }
        if let Some(usage) = self.usage.get_mut(&entry.owner) {
            usage.targets = usage.targets.saturating_sub(1);
            usage.target_bytes = usage.target_bytes.saturating_sub(entry.bytes);
        }
        self.target_bytes = self.target_bytes.saturating_sub(entry.bytes);
        for group_name in &entry.groups {
            if let Some(group) = self.groups.get_mut(group_name) {
                group.members.remove(name);
                if group.members.is_empty() {
                    self.groups.remove(group_name);
                }
            }
        }
        Some(entry)
    }

    fn forget_target(&mut self, name: &str, key: (Instant, u64)) {
        let expired = self
            .targets
            .get(name)
            .is_some_and(|entry| entry.session.is_none() && entry.forget_key == Some(key));
        if expired {
            self.remove_target(name);
            tracing::info!(target = %name, "offline target forgotten");
        }
    }

    fn drain_queue(&mut self, key: &str) {
        let Some(queue) = self.queues.remove(key) else {
            return;
        };
        let mut kept = VecDeque::new();
        let mut busy = false;
        let mut items = queue.into_iter();
        while let Some(id) = items.next() {
            let Some(pending) = self.pending.get_mut(&id) else {
                continue;
            };
            pending.stage = Stage::Waiting;
            let Attempt::Wait { busy: full } = self.attempt(id) else {
                continue;
            };
            if let Some(pending) = self.pending.get_mut(&id) {
                pending.stage = Stage::Queued(key.to_string());
                kept.push_back(id);
            }
            if full {
                busy = true;
                kept.extend(items.by_ref().filter(|id| self.pending.contains_key(id)));
                break;
            }
        }
        if let Some(newer) = self.queues.remove(key) {
            kept.extend(newer);
        }
        if !kept.is_empty() {
            self.queues.insert(key.to_string(), kept);
        }
        if busy {
            self.schedule_drain(key.to_string());
        }
    }

    fn on_subscribe(&mut self, session: SessionId, id: u64, target: String, event: String) {
        let key = (target, event);
        let is_new = match self.admit_subscription(session, &key) {
            Ok(is_new) => is_new,
            Err(failure) => return self.reply_fail(session, id, failure),
        };
        if is_new {
            let Some(entry) = self.sessions.get_mut(&session) else {
                return;
            };
            entry.subs.insert(key.clone());
            let owner = entry.owner.clone();
            self.usage.entry(owner).or_default().subs += 1;
            self.subs.entry(key).or_default().insert(session);
        }
        self.send_to(session, Frame::Accepted { id });
    }

    fn admit_subscription(
        &self,
        session: SessionId,
        (target, event): &(String, String),
    ) -> Result<bool, Failure> {
        let Some(entry) = self.sessions.get(&session) else {
            return Err((ErrorCode::Protocol, "unknown session".into()));
        };
        if !valid_pattern(target, valid_route) || !valid_pattern(event, valid_name) {
            return Err((ErrorCode::Invalid, "invalid target or event name".into()));
        }
        if !entry.principal.may_address(target) {
            return Err((
                ErrorCode::Forbidden,
                format!("not allowed to address '{target}'"),
            ));
        }
        self.check_operation(session, Operation::Subscribe { target, event })?;
        if entry.subs.contains(&(target.clone(), event.clone())) {
            return Ok(false);
        }
        let principal_subs = self.usage_of(&entry.owner).map_or(0, |usage| usage.subs);
        if entry.subs.len() >= self.limits.max_subscriptions_per_session
            || principal_subs >= self.limits.max_subscriptions_per_principal
        {
            return Err((
                ErrorCode::QuotaExceeded,
                "subscription quota exceeded".into(),
            ));
        }
        Ok(true)
    }

    fn unsubscribe(&mut self, session: SessionId, key: &(String, String)) {
        let Some(entry) = self.sessions.get_mut(&session) else {
            return;
        };
        if !entry.subs.remove(key) {
            return;
        }
        let owner = entry.owner.clone();
        if let Some(usage) = self.usage.get_mut(&owner) {
            usage.subs = usage.subs.saturating_sub(1);
        }
        if let Some(subscribers) = self.subs.get_mut(key) {
            subscribers.remove(&session);
            if subscribers.is_empty() {
                self.subs.remove(key);
            }
        }
    }

    fn on_emit(&mut self, session: SessionId, event: String, payload: Vec<u8>) {
        let Some(name) = self.sessions.get(&session).and_then(|s| s.target.clone()) else {
            return self.session_error(session, ErrorCode::Protocol, "emit before register");
        };
        if !valid_name(&event) {
            return self.session_error(session, ErrorCode::Invalid, "invalid event name");
        }
        let operation = Operation::Emit {
            event: &event,
            payload_bytes: payload.len(),
        };
        if let Err((code, message)) = self.check_operation(session, operation) {
            return self.session_error(session, code, message);
        }
        let groups = self
            .targets
            .get(&name)
            .map(|entry| entry.groups.clone())
            .unwrap_or_default();
        let sources = std::iter::once(name.clone())
            .chain(groups.iter().map(|g| format!("{GROUP_PREFIX}{g}")))
            .chain(std::iter::once(WILDCARD.to_string()));
        let mut recipients = HashSet::new();
        for source in sources {
            for event_key in [event.as_str(), WILDCARD] {
                if let Some(subscribers) = self.subs.get(&(source.clone(), event_key.to_string())) {
                    recipients.extend(subscribers.iter().copied());
                }
            }
        }
        let frame = Frame::Event {
            target: name,
            event,
            payload,
        };
        let bytes = footprint::frame_bytes(&frame);
        for recipient in recipients {
            self.send_lazy(recipient, bytes, || frame.clone());
        }
    }

    fn on_list(&mut self, session: SessionId, id: u64) {
        if let Err(failure) = self.check_operation(session, Operation::ListTargets) {
            return self.reply_fail(session, id, failure);
        }
        let Some(entry) = self.sessions.get(&session) else {
            return;
        };
        // Check a conservative wire/memory bound before cloning registry metadata.
        let limit = self
            .limits
            .max_frame_bytes
            .min(self.limits.max_outbound_bytes_per_session);
        let mut estimated = 128usize;
        let mut targets = Vec::new();
        for (name, target) in &self.targets {
            if !entry.principal.may_address(name) {
                continue;
            }
            estimated = estimated.saturating_add(target.bytes.saturating_mul(2));
            if estimated > limit {
                return self.reply_fail(
                    session,
                    id,
                    (
                        ErrorCode::Overloaded,
                        "target listing exceeds response limit; narrow the principal's send grant"
                            .into(),
                    ),
                );
            }
            targets.push(TargetInfo {
                name: name.clone(),
                actions: target.actions.clone(),
                groups: target.groups.clone(),
                online: target.session.is_some(),
                queued: self.queues.get(name).map_or(0, VecDeque::len),
            });
        }
        targets.sort_by(|a, b| a.name.cmp(&b.name));
        let frame = Frame::Targets { id, targets };
        let fits = crate::wire::encode(&frame)
            .is_ok_and(|bytes| bytes.len() <= limit && crate::wire::validate_shape(&bytes).is_ok());
        if !fits {
            return self.reply_fail(
                session,
                id,
                (
                    ErrorCode::Overloaded,
                    "target listing exceeds response limit".into(),
                ),
            );
        }
        self.send_to(session, frame);
    }

    fn drop_session(&mut self, session: SessionId) {
        let Some(entry) = self.sessions.remove(&session) else {
            return;
        };
        entry.out.close();
        if let Some(usage) = self.usage.get_mut(&entry.owner) {
            usage.sessions = usage.sessions.saturating_sub(1);
        }
        for hook in &self.hooks {
            hook.on_disconnect(session);
        }
        if let Some(name) = &entry.target
            && self
                .targets
                .get(name)
                .is_some_and(|target| target.session == Some(session))
        {
            let retention = Duration::from_millis(self.limits.offline_target_retention_ms);
            let key = self.add_timer(Instant::now() + retention, Timer::Forget(name.clone()));
            if let Some(target) = self.targets.get_mut(name) {
                target.session = None;
                target.forget_key = Some(key);
            }
            tracing::info!(session, target = %name, "target offline");
        }
        if let Some(usage) = self.usage.get_mut(&entry.owner) {
            usage.subs = usage.subs.saturating_sub(entry.subs.len());
        }
        for id in entry.owned {
            self.finish(id);
        }
        for id in entry.inflight {
            if let Some(pending) = self.pending.get_mut(&id) {
                pending.stage = Stage::Waiting;
                self.retry_or_fail(id, ErrorCode::Unavailable, "target disconnected".into());
            }
        }
        for key in entry.subs {
            if let Some(subscribers) = self.subs.get_mut(&key) {
                subscribers.remove(&session);
                if subscribers.is_empty() {
                    self.subs.remove(&key);
                }
            }
        }
    }
}

#[cfg(test)]
mod ingress_tests {
    use super::*;
    #[tokio::test]
    async fn queued_frames_hold_and_release_ingress_budget() {
        let (tx, mut rx) = mpsc::channel(4);
        let bytes = footprint::frame_bytes(&Frame::Ping { nonce: 0 });
        let handle = BusHandle {
            tx,
            next_session: Arc::new(AtomicU64::new(1)),
            session_buffer: 1,
            outbound_session_bytes: 1024,
            outbound_principal_bytes: 1024,
            outbound_principals: Arc::new(Mutex::new(HashMap::new())),
            outbound_bytes: ByteBudget::new(1024),
            ingress: Arc::new(Semaphore::new(bytes)),
            ingress_bytes: bytes,
        };
        handle.frame(1, Frame::Ping { nonce: 0 }).await.unwrap();
        let second = handle.frame(1, Frame::Ping { nonce: 1 });
        tokio::pin!(second);
        let waited = tokio::time::timeout(Duration::from_millis(20), &mut second).await;
        assert!(
            waited.is_err(),
            "second frame should wait for ingress budget"
        );
        drop(rx.recv().await.unwrap());
        second.await.unwrap();
        drop(rx.recv().await.unwrap());
        let big = Frame::Emit {
            event: "e".into(),
            payload: vec![0; 4 * bytes],
        };
        handle.frame(1, big).await.unwrap();
        drop(rx.recv().await.unwrap());
    }
}
