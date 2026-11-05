# SARCA

Serialised Arbitrary Remote Control Autonomy. A Flow Construct Studios project.

SARCA is a lightweight, self-hosted remote-control bus. Callers (CLIs, dashboards,
scripts, AI agents, webhooks) send commands; targets (game instances, servers,
devices, services) execute them. Neither side knows about the other: both speak
SARCA to the bus, and targets dial out, so they need no inbound ports.

```
[callers] -> SARCA bus -> [targets]
             route, validate, authorise,
             serialise, queue, retry, log
```

- **Serialised**: one msgpack wire format for every command, result and event.
- **Arbitrary**: payloads are opaque bytes. Targets register the actions they support.
- **Remote control**: request/response, fire-and-forget and streaming over TCP,
  WebSocket or a unix socket.
- **Autonomy**: messages queue while targets are offline, retry with exponential
  backoff, expire after their TTL, and fall back to backup targets by rule.
  Targets can also run local rules (timers, on-connect, on-disconnect) without
  the bus.

## Build

Built and tested with Rust 1.99 (edition 2024).

```sh
cargo build --release
cargo test
```

The binary is `target/release/sarca`.

## Quick start

```sh
sarca serve                                    # 127.0.0.1:7878, anonymous full access
cargo run --example echo_target -- echo-1      # a demo target
sarca targets
sarca send echo-1 echo "hello"
sarca send echo-1 count 5 --stream
printf 'raw bytes' | sarca send echo-1 echo -
sarca subscribe group:examples                 # prints target, event, payload
```

Without `--config` the server accepts anonymous connections with full access on
localhost only, and logs a warning. Use a config file for anything else.

CLI connection settings come from `--url` / `SARCA_URL` (`tcp://host:port`,
`unix:/path` or `ws://host:port/path`) and `--token` / `SARCA_TOKEN`. Prefer the
environment variable for tokens so they do not appear in the process list.

`sarca send` options: `--fire`, `--stream`, `--cap <cap>` (repeatable),
`--ttl <ms>`, `--attempts <n>`, `--backoff <ms>`. Remote failures exit with
status 1 and print the error code.

## Configuration

See `sarca.example.toml`. Unknown keys are rejected. Sections:

- `[server]`: `tcp`, `ws`, `unix` listen addresses (at least one) and `unix_mode`
  (default `0o600`). An existing stale socket file is replaced; a live one is not.
- `[limits]`: frame size, per-target queue length, default and maximum TTL,
  maximum retry attempts and backoff, handshake timeout, per-session buffer.
- `[auth]`: `[auth.anonymous]` grant (absent means anonymous is refused) and
  `[[auth.tokens]]`. Tokens are stored as SHA-256 hashes: generate one with
  `printf '%s' "$TOKEN" | sarca hash-token`. A config file without an `[auth]`
  section refuses everyone.
- `[rate_limit]`: token bucket per session (`per_second`, `burst`).
- `log_messages`: log routing metadata for every send (never payloads).
- `[[rules]]`: `when_down = "name"`, `route_to = ["backup", "group:pool"]`.
  Backups are tried in order when the primary is offline; otherwise the message
  queues for the primary and is flushed when it, or a backup, registers.

### Grants

Each grant has three lists of glob patterns (`*` matches any run of characters):

- `caps`: capability scopes the principal may claim in an envelope.
- `register`: target names the principal may register as.
- `send`: targets (and `group:` names) the principal may send to, subscribe to
  and see in `targets`.

A target may require capabilities per action. A request reaches the target only
if the envelope claims every capability the action requires, and the bus has
verified the caller holds every claimed capability. Targets receive the verified
`caps` and can make finer decisions themselves.

## Delivery semantics

- Each send gets a TTL (default from config, capped by `max_ttl_ms`). When it
  runs out, a queued or in-flight request fails with `expired`.
- `retry.max_attempts` counts deliveries. A delivery is retried when the target
  disconnects mid-request or replies `fail` with `retryable: true`. The delay is
  `backoff_ms * 2^(attempt-1)`, capped by `max_backoff_ms`. Retries make
  delivery at-least-once, so actions should be idempotent where it matters.
- A stream that has already forwarded an item is never retried.
- Fire-and-forget gets an `accepted` reply once the bus has delivered or queued
  it. Later failures (expiry, full queue on retry) are logged, not reported.
- `group:<name>` round-robins across online members that support the action.
- If a caller disconnects, its outstanding requests are dropped.

## Wire protocol

Every frame is a msgpack **map** (named fields) with a string `type` field.

- TCP and unix socket: each frame is prefixed by a 4-byte big-endian length.
- WebSocket: each binary message is one frame. Text messages are rejected.
- Frames over `max_frame_bytes` close the connection.

Envelope fields (used by `send` and `deliver`):

| field     | type          | notes                                       |
|-----------|---------------|---------------------------------------------|
| `id`      | u64           | chosen by the caller; replies echo it       |
| `target`  | string        | target name or `group:<name>`               |
| `action`  | string        | registered action name                      |
| `caps`    | list<string>  | optional                                    |
| `ttl`     | u64 ms        | optional, 0 means server default            |
| `retry`   | map           | optional `{max_attempts, backoff_ms}`       |
| `mode`    | string        | `request` (default), `fire` or `stream`     |
| `payload` | bin           | optional, opaque                            |

Names use `[A-Za-z0-9-_./:]`, at most 256 bytes.

Session flow:

1. Client sends `hello {client, token?}`. Bus answers `welcome {session}` or
   `error {code, message}` and closes.
2. A target sends `register {target, actions: [{name, caps}], groups}` and gets
   `registered {target}` or `error`.
3. A caller sends `send <envelope>`. The bus forwards `deliver <envelope>` to
   the target with its own `id` and the remaining `ttl`.
4. The target answers that id with `reply {id, payload}`, or
   `stream_item {id, payload}`... `stream_end {id}`, or
   `fail {id, code, message, retryable}`. The bus relays these to the caller
   under the caller's original id.
5. `subscribe {id, target, event}` (`*` allowed for both) is answered with
   `accepted {id}` or `fail {id, ...}`. Targets publish with
   `emit {event, payload}`; subscribers receive `event {target, event, payload}`.
6. `list_targets {id}` returns `targets {id, targets}`. `ping {nonce}` returns
   `pong {nonce}`.

Error codes: `protocol`, `unauthorized`, `forbidden`, `invalid`, `name_taken`,
`unknown_action`, `queue_full`, `rate_limited`, `rejected`, `expired`,
`unavailable`, `target_failed`.

## Rust SDK

```rust
use sarca::client::{Client, SendOptions};

let client = Client::connect("tcp://127.0.0.1:7878", Some("token")).await?;
let reply = client.request("game-1", "status", Vec::new(), &SendOptions::default()).await?;
let mut items = client.stream("game-1", "logs", Vec::new(), &SendOptions::default()).await?;
client.subscribe("group:games", "*").await?;
let mut events = client.events().unwrap();
```

```rust
use sarca::target::{Call, Target, TargetConfig};
use sarca::wire::ActionSpec;

let config = TargetConfig::new("tcp://127.0.0.1:7878", "game-1", vec![ActionSpec::new("status")]);
Target::new(config)
    .run(|call: Call| async move {
        let _ = call.reply(b"ok".to_vec()).await;
    })
    .await?;
```

`Target::run` reconnects with backoff and re-registers on its own. It returns
only on fatal refusals (bad token, forbidden name, invalid registration). A
`Call` dropped without an answer reports `target_failed` to the caller.
`TargetConfig::rules` holds `LocalRule`s triggered by `Every(duration)`,
`Connected` or `Disconnected`; they call the same handler with a local `Call`
(`call.is_local()`), whether or not the bus is reachable. `target.handle()`
returns a `TargetHandle` for `emit`.

## Plugin hooks

Implement `sarca::hooks::Hook` and pass it to `Server::bind(&config, hooks)`:

```rust
fn on_send(&self, ctx: &HookContext<'_>, envelope: &mut Envelope) -> Result<(), Rejection>;
fn on_disconnect(&self, session: u64) {}
```

Hooks run in order on every send, after authorisation, and may rewrite the
envelope (payload transforms) or reject it. Built-ins: rate limiting and message
logging. Hooks run on the bus task, so they must be fast and must not block.

## Security notes

- There is no TLS. Bind to localhost or a private network, or put the TCP/WS
  listener behind a TLS-terminating proxy or tunnel. Tokens travel in clear text
  otherwise.
- Keep the config file readable only by the service user; it holds token hashes
  and grants.
- The unix socket defaults to mode `0600`.
