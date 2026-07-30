# Changelog

## Unreleased

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
