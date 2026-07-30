---
name: codex-telegram-bootstrap
description: Install and verify one isolated Codex workspace to Telegram forum bridge on macOS or Linux without exposing the bot token.
---

# Codex Telegram Bootstrap

Use this skill when the user wants to mirror one local Codex project into a
private Telegram forum supergroup.

1. Read the repository root `SETUP_WITH_CODEX.md` completely.
2. Confirm the exact target workspace.
3. Use one new bot and one new private forum supergroup for this instance.
4. Never request or print the bot token or private runtime identifiers.
5. Run preflight and the full tests.
6. Run `installer.py prepare` with macOS Keychain, Proton Pass CLI, or an
   explicitly accepted owner-only file backend.
7. Pause only for BotFather, group setup, secret-manager input, `/connect`, and
   a possible user-approved Codex Desktop restart.
8. Run bootstrap, activation, final `doctor`, a verified online backup, and a
   disposable smoke test.
9. Stop on unsupported protocol, ambiguous binding, unsafe permissions, test
   failure, or failed health.
10. For an unattended Linux node, verify user lingering, all installed
    service/timer units, the non-root privilege boundary, and one cold reboot.
    Treat UPS, storage, thermal, Wi-Fi, host watchdog, and encrypted off-host
    backup as separate machine-specific checks.
