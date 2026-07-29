# Security

The bridge copies project conversation data between Codex and Telegram. A
Telegram group member may therefore see prompts, visible progress, final
answers, and explicitly mirrored files from the linked workspace.

## Required deployment boundary

- Use a private Telegram forum supergroup.
- Use one bot token per project/bridge instance.
- Bind exactly one intended human administrator through `/connect`.
- Give the bot only the group permissions it needs, including Manage Topics.
- Keep Telegram IDs, Codex thread IDs, queued input, media, and the SQLite
  database outside the repository.
- Protect the Telegram account with strong authentication.

Inbound Telegram documents are untrusted input. They are filename- and
MIME-sanitized, bounded by the Bot API download limit, stored owner-only, and
passed to Codex as mentioned files; the bridge does not execute them.
Conversely, a safe local file is uploaded only when the final Codex answer
explicitly links it. Workspace containment, symlink, sensitive-path,
blocked-file-type, and size checks still apply.

## Bot token

Never put the bot token in:

- a prompt or Codex conversation;
- a command argument or shell history;
- Git, `.env`, project configuration, logs, or Telegram;
- a systemd unit or launchd plist.

Supported backends are macOS Keychain, Proton Pass CLI, and an owner-only local
file. The file backend is a fallback for unattended hosts where an encrypted
secret manager is unavailable.

## Approvals

Telegram approval buttons can authorize local Codex actions. The bridge binds
approvals to the exact user, group, Topic, message, and live server request.
Secret questions are not rendered in Telegram. Operators should still reserve
high-risk credentials and unfamiliar system changes for a trusted local
surface.

## Reporting

Do not include tokens, private identifiers, message/database dumps, or user
content in a public issue. Report security concerns privately to the repository
owner.
