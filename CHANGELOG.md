# Changelog

## Unreleased

## 0.3.0 — 2026-07-30

- Supervise the three critical bridge loops as one failure domain. An
  unexpected return, cancellation, or exception now terminates the process
  instead of leaving a partially working service alive.
- Add native systemd readiness and watchdog notifications, restart the
  long-lived bridge after any unintended exit on Linux and macOS, remove the
  ineffective user-level network-online dependency, and bound health checks.
- Add `probe-local`, which verifies SQLite and the local Codex App Server
  without loading the Telegram token or depending on Telegram, OpenAI, DNS, or
  Internet availability.
- Add verified atomic SQLite online backups and install half-hour systemd or
  launchd backup jobs retaining 96 snapshots. Backup publication/pruning is
  serialized across processes, and a later run safely removes stale
  owner-owned temporary files after an interrupted attempt. Activation
  requires an initial successful backup.
- Make the installed five-minute health job use the token-free local probe;
  retain the full network-aware `doctor` for activation and explicit checks.
- Add an explicit `--codex-full-access` instance mode. New threads, resumed
  threads, and every Telegram-started turn pin `approval_policy=never` and
  `danger-full-access`, preventing restrictive or incomplete host defaults
  from causing repeated approval prompts. The default remains inherited from
  the host unless the operator makes this trust decision.
- Make manual `doctor` and protocol-version fallback follow the configured
  App Server socket's `CODEX_HOME`, so relocated CLI homes work outside their
  service-manager environment as well as inside it.
- Make `doctor` fail closed when managed App Server requirements disallow the
  requested approval/sandbox policy or select permission-profile mode that
  this bridge release does not yet send.
- Add an optional global `max_active_turns` guard for low-memory hosts.
  Cross-Topic work remains in the durable FIFO queue until an actual terminal
  Codex state frees capacity, including after bridge restart. Outcome-unknown
  dispatch reservations keep occupying capacity until history reconciliation.
- Generate Linux `WorkingDirectory=` values with directive-specific systemd
  syntax, so absolute workspace paths containing spaces or literal percent
  signs load and run without quoted-path or specifier-expansion failures.

## 0.2.0 — 2026-07-29

- Accept Telegram document messages as private, durable Codex mentioned-file
  inputs instead of silently forwarding only their captions.
- Sanitize inbound document names, preserve the 20 MB Telegram download limit,
  and keep queued files protected from media-cache pruning.
- Upload safe workspace files explicitly linked in final Codex answers as
  native Telegram attachments instead of leaving ordinary `docs/` links as
  text-only paths.

## 0.1.1 — 2026-07-28

- Normalize integer weekly-limit values before decimal formatting so the
  bridge behaves consistently on every supported Python 3.10–3.13 runtime.

## 0.1.0 — 2026-07-28

- Extracted the durable Codex↔Telegram bridge into a project-neutral package.
- Added isolated per-workspace instance configuration.
- Added macOS launchd and Linux systemd user-service installation.
- Added macOS Keychain, Proton Pass CLI, and owner-only file secret backends.
- Added a Codex-readable onboarding contract and reusable bootstrap skill.
- Preserved durable queue, delivery deduplication, archive/restore, approvals,
  media handling, progress cards, mode controls, and fail-closed protocol
  compatibility.
- Added macOS/Linux CI and portable installer/security regression tests.
