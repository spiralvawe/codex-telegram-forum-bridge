# Codex Telegram Forum Bridge

This repository contains a security-sensitive communication bridge.

For installation requests:

1. Read `SETUP_WITH_CODEX.md` completely.
2. Treat the requested target workspace as exact; never substitute the bridge
   repository itself unless the user explicitly wants to mirror it.
3. Run read-only preflight checks before installation.
4. Never ask the user to paste a Telegram bot token into Codex chat, a command
   argument, a repository file, or a log.
5. Use one unique bot token and one private forum supergroup per bridge
   instance.
6. Stop if the local Codex App Server protocol version is unsupported.
7. Do not expose numeric Telegram IDs, Codex thread IDs, Proton Pass item IDs,
   tokens, database contents, or credential-bearing URLs.
8. Run tests and `doctor` after every code or installation change.

For development:

- Preserve durable queue and outbound-delivery semantics.
- Treat ambiguous remote mutations as outcome-unknown; never blindly replay
  Topic creation, messages, media uploads, archive cards, or approvals.
- Keep runtime state outside the repository with owner-only permissions.
- Add a regression test for every behavioral fix.
