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

## Optional media worker

The media worker is a narrow FFmpeg service, not a remote shell or a remote
Codex runtime. Its protocol accepts only the versioned voice/video operations
and bounded binary input. It must run as a dedicated non-admin user with an
owner-only spool and TLS private key and no shell login. Linux permits only the
primary group; macOS additionally permits only the reviewed automatic
`everyone`, `localaccounts`, `_lpoperator`, and
`com.apple.sharepoint.group.N` groups. Administrative, remote-access, custom,
and root-equivalent groups are forbidden. Its root-owned
Python/package/FFmpeg runtime must be immutable to that user. Never place
Telegram, Codex, Home Assistant, Proton Pass, SSH, or sudo credentials in its
configuration.

Mutual TLS client material used by the bridge is also dedicated to this one
protocol. Do not reuse a main-user SSH key or the full-access Codex identity.
Compromise of the media worker must not grant login access to the Pi, the main
macOS account, the Codex workspace, or password-manager data.

The worker is optional by design. Infrastructure, authentication, transport,
capacity, timeout, protocol, malformed-output, and unsupported-capability
failures open a bounded client circuit and use local FFmpeg. An authenticated
terminal invalid-media result is intentionally not retried locally. Neither
case may stop the bridge, fail bridge readiness, or trigger a bridge
service-manager restart.

The worker spool, aggregate request deadline, output writes, queue, request and
processor concurrency, memory, file size, tasks/processes, and open files are
bounded. The worker's lifetime spool lock prevents two service instances from
cleaning or publishing into the same state tree. Do not weaken these controls
to make a failing installation start.

FFmpeg is restricted to 16,777,216 input pixels, 128 MiB per allocation, one
decoder/encoder/filter thread, bounded output, and a 512 MiB process-group
physical-footprint watchdog on macOS. A watchdog breach or monitoring failure
is terminal and is not retried on the Pi. The macOS watchdog remains
best-effort because public launchd/RLIMIT resident-set controls are advisory.
Only a Linux cgroup (`MemoryMax`) or a capped Linux VM gives a strict memory
limit. Native-Mac deployment therefore retains a denial-of-service risk and is
appropriate only for the private trusted-group boundary.

Use a one-purpose private CA and keep its private key offline. Track both leaf
certificate expiries, begin renewal validation 90 days before expiry, and
complete replacement by 60 days before expiry. Do not disable mTLS or hostname
verification as a renewal workaround.

## Reporting

Do not include tokens, private identifiers, message/database dumps, or user
content in a public issue. Report security concerns privately to the repository
owner.
