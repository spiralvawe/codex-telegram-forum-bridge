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

An instance prepared with `--codex-full-access` deliberately bypasses these
approval prompts. The bridge sends `approval_policy=never` and
`danger-full-access` explicitly when it starts or resumes a thread and when it
starts every turn. Only enable this for a private group whose bound
administrator may act with all filesystem and network privileges of the
bridge's OS account. Operating-system permissions still apply; this flag does
not grant root access.

For an unattended full-access node, run the bridge as a dedicated non-root
account and keep administration in a separate account. Do not grant the
Telegram-facing runtime user unrestricted sudo: anyone who can issue accepted
commands in the bound group can exercise that user's filesystem and network
authority.

## Backup data

Operational SQLite snapshots contain the same private identifiers, queued
content, mappings, and message state as the live database. They stay
owner-only and outside the repository. Encrypt any copy that leaves the host,
authenticate the receiving device, pin its identity, and test restoration
without starting a second Telegram poller.

Local snapshots are intentionally not described as off-host disaster
recovery. They do not protect against total storage or machine loss.

## Reporting

Do not include tokens, private identifiers, message/database dumps, or user
content in a public issue. Report security concerns privately to the repository
owner.
