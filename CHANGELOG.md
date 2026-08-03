# Changelog

## Unreleased

- Keep Telegram responsive on small Linux hosts during voice requests: local
  STT is now skipped, with native-audio fallback retained, while any Codex
  turn is active or available memory is below a conservative 450 MiB floor.
  Operators may raise that floor with
  `CODEX_TELEGRAM_LOCAL_STT_MIN_AVAILABLE_MEMORY_MIB`.

- Persist owner-only Telegram update-loop health and make a running bridge's
  `doctor` fail closed when inbound polling is stale, failed, or unobserved.
- Classify dropped HTTP connections as retryable network failures and switch
  to a short recovery poll after repeated long-poll failures, clearing stale
  transport state without operator intervention.
- Gate systemd watchdog heartbeats on repeated stale local polling failures so
  a half-working process is restarted, while external network outages do not
  cause restart loops.

## 0.4.2 — 2026-07-31

- Restore the result-only Telegram attachment boundary: local Markdown links,
  inline images, and native output citations are uploaded only when they
  resolve to safe regular files below the workspace `outputs/` directory.
- Keep technical and intermediate paths readable in final answers without
  uploading their contents to Telegram.

## 0.4.1 — 2026-07-31

- Record an owner-only deployment manifest containing the package version,
  deterministic package digest, and source commit when available.
- Make `doctor` fail closed when installed package files no longer match the
  recorded deployment, detecting direct runtime edits.
- Refuse downgrades and refuse different code under an unchanged version,
  requiring every behavior change to receive a version bump.

## 0.4.0 — 2026-07-30

- Add an opt-in, mutually authenticated LAN media worker for bounded FFmpeg
  preparation. The bridge validates and materializes returned artifacts
  locally and uses a retry circuit breaker. Infrastructure, transport,
  capacity, protocol, timeout, and unsupported-capability failures fall back
  to local FFmpeg; authenticated terminal invalid-media results deliberately
  do not repeat the same work locally. The worker is not a readiness or
  service dependency.
- Keep the worker identity and configuration separate from Telegram, Codex,
  password-manager, sudo, and full-access main-user SSH credentials.
- Add bounded TLS/request/processing/shutdown deadlines, a durable idempotent
  spool, atomic owner-only output, startup residue cleanup, fail-fast worker
  supervision, and restart-on-boot service templates for a dedicated account.
- Bound FFmpeg pixels, allocation size, codec/filter threads, output, and
  process-group physical footprint. Linux uses strict cgroup `MemoryMax`;
  native macOS uses a documented best-effort public-libproc watchdog because
  its resident-set service limit is advisory.
- Support explicit all-interface mTLS listeners for multi-interface/DHCP
  failover, with documented firewall scope, rotating sanitized worker logs,
  strict configuration schemas, resource-limited service definitions, and
  prepare-plus-activate local-only rollback.
- Install correctly from a wheel outside the source checkout, preserve the
  exact existing instance/runtime policy during media-worker updates, and
  require activation to verify, back up, reload, and restart the bridge.

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
