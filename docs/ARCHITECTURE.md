# Architecture

```text
Telegram private forum supergroup
          ↕ Bot API
portable Python bridge
   ↙              ↘
SQLite state      local media/ffmpeg
   ↘              ↙
owner-only Codex App Server socket
          ↕
Codex tasks with cwd == configured workspace
```

One bridge instance owns one bot update stream and one Telegram binding.
Codex tasks are scoped by their exact canonical working directory because the
App Server protocol does not expose a stable saved-project identifier.

## Durable state

SQLite records:

- Telegram group/user binding;
- Codex task ↔ Telegram Topic mappings;
- queued Telegram input and local voice/video/document references;
- inbound update completion;
- a bounded content-free quarantine record for an unexpected per-update
  handler failure, after which polling advances to later updates;
- outbound message/media delivery reservations;
- Topic-creation intents;
- approval cards and decisions;
- archive cards, restore tokens, and confirmed Topic deletion.

Telegram does not provide idempotency keys for Topic creation, messages, or
media uploads. The bridge therefore reserves side effects before network calls.
Ambiguous outcomes are surfaced for reconciliation and are not blindly
replayed.

Telegram documents use the same durable media lifecycle as voice and video:
the downloaded owner-only file remains referenced by a queued input and is
therefore protected from cache pruning until dispatch is resolved. Final-answer
attachments travel in the opposite direction only when Codex explicitly links
a safe file inside the configured workspace.

## Global turn capacity

`max_active_turns` optionally bounds simultaneous Codex turns for the whole
bridge instance. Zero preserves the original unlimited behavior. A positive
limit counts the union of turns started by the bridge and active turns observed
from another App Server client.

The start RPC is serialized, but its lock is not the capacity boundary. A slot
stays occupied until an authoritative terminal notification or a full thread
history read reports `completed`, `failed`, or `interrupted`. An interrupt
request alone does not release it. Pending inputs remain durable in SQLite,
and a global FIFO dispatcher starts work from any eligible Topic when a slot
opens. On restart, the bridge first reconstructs active-turn state from Codex,
then resumes queued dispatch, so startup order cannot temporarily exceed the
configured limit.

A `dispatching` queue reservation also occupies one slot per Topic. That state
means a start or steer RPC may have reached Codex even though its response was
lost. The reservation keeps blocking new cross-Topic starts across reconnects
until authoritative history either finds its client ID or satisfies the
guarded multi-read miss reconciliation and returns it to `pending`.

## Codex connection

Both macOS and Linux use the Codex CLI-managed local App Server daemon and its
owner-only Unix socket. A MacBook can additionally attach Codex Desktop to that
same daemon, allowing Telegram and Desktop to observe the same live turns and
approvals. A headless VPS simply has no Desktop peer.

No Codex port is exposed on the LAN or public internet.

## Platform layer

The Python bridge is shared. Only service registration differs:

- macOS: one keep-alive bridge LaunchAgent, one five-minute health
  LaunchAgent, and one thirty-minute online-backup LaunchAgent;
- Linux: one `Type=notify` systemd user service with a watchdog, plus bounded
  health and online-backup services/timers.

Each instance has its own configuration, virtual environment, state directory,
logs, database, and service names.

The bridge process treats its Telegram, Codex-connection, and progress
heartbeat loops as critical. If any one returns, is cancelled unexpectedly,
or raises, the supervisor cancels the remaining loops and exits. This gives
the external service manager an unambiguous failure signal and prevents a
process that is alive but no longer performing one of its core jobs.

The local `probe-local` path checks SQLite and the Unix-socket App Server
without retrieving the Telegram secret or consulting remote services. The
backup path uses SQLite's online backup API, validates schema and integrity,
fsyncs, then atomically publishes an owner-only snapshot. A per-instance
advisory lock serializes publication and retention across processes; the next
run safely removes strictly named owner-owned temporary files left by an
interrupted attempt. Remote outages are operational states, not local restart
signals.

## Optional media acceleration

Voice and video preparation may use a mutually authenticated HTTPS worker on a
trusted LAN. The bridge uploads only the source bytes plus a bounded media kind
and duration. The worker returns at most one audio artifact and three ordered
JPEG frames. The bridge verifies sizes and hashes, atomically publishes the
artifacts inside its own owner-only media directory, and gives Codex only those
local paths.

This worker does not own durable bridge state. A bounded queue and short result
cache make duplicate job submission idempotent, while the source file on the
bridge remains authoritative. The spool has a lifetime single-owner lock,
durable retryable jobs, atomic results, quota reservations, startup recovery,
and periodic TTL cleanup. Server and client use aggregate wall deadlines;
FFmpeg and the boot service have aligned file-size and process resource
limits.

Input pixels, single allocations, codec/filter threads, and output size are
bounded before and during FFmpeg work. Native macOS additionally polls public
libproc physical-footprint data for the FFmpeg process group and kills it above
512 MiB. This is best-effort because macOS resident-set limits are advisory;
Linux systemd `MemoryMax` is the strict cgroup boundary. A hard cap on Apple
hardware requires a capped Linux VM.

Infrastructure, transport, TLS, capacity, timeout, protocol, malformed-output,
and unsupported-capability failures use the local `MediaProcessor`. A terminal
invalid-media conclusion from the authenticated worker is intentionally
returned without a second local attempt. The worker is not a critical bridge
loop, readiness condition, watchdog input, or systemd dependency.

The worker TLS identity and operating-system account are deliberately unrelated
to any separate full-access main-user Codex SSH channel. Its private CA stays
offline between leaf issuance events, and certificate renewal is planned in
advance rather than weakening mTLS at expiry.
