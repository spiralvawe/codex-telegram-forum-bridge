# Changelog

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
