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

- macOS: one long-lived LaunchAgent and one five-minute health LaunchAgent;
- Linux: one systemd user service plus a health service/timer.

Each instance has its own configuration, virtual environment, state directory,
logs, database, and service names.
