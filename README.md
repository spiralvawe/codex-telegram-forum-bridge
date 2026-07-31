# Codex ↔ Telegram Forum Bridge

Mirror every Codex task for one exact local project into its own Topic in a
private Telegram forum supergroup.

The bridge supports:

- macOS with Codex Desktop/CLI and launchd;
- Linux hosts, including ARM64 Raspberry Pi, with Codex CLI and a systemd
  user service;
- macOS Keychain, Proton Pass CLI, or an owner-only secret file;
- one isolated bot + supergroup + runtime database per project;
- text, voice, video context, files, visible progress, final answers,
  approvals, queueing, steer, archive/restore, mode controls, and health checks;
- supervised long-lived services and verified online SQLite snapshots on both
  macOS and Linux.

When a final Codex answer explicitly links a safe local file inside the target
workspace, the Bridge sends that file to the same Topic as a native Telegram
attachment. It does not require the file to live in a special output
directory. Files outside the workspace and secret-shaped, blocked, or oversized
paths remain ineligible.

The Telegram bot token and all private mappings remain outside the project and
outside this repository.

An ordinary Telegram document is downloaded into the owner-only media cache
under the existing 20 MB Bot API limit and reaches Codex as a native mentioned
file. Its sanitized filename is preserved, its caption remains the user's
instruction, and a queued document stays protected across bridge restarts.
The bridge never executes an inbound file.

> Status: alpha. Codex App Server is experimental. Unknown protocol versions
> fail closed instead of dispatching queued work.

## Fastest installation: let Codex do it

Clone this repository, open it in Codex, and say:

```text
Read SETUP_WITH_CODEX.md completely and install the bridge for
/absolute/path/to/my/project.
```

Codex will run the preflight, choose the platform-specific service manager,
prepare the isolated runtime, and pause only for the Telegram/secret-manager
steps that require the human owner.

Full onboarding: [SETUP_WITH_CODEX.md](SETUP_WITH_CODEX.md)

## Deployment model

Use this boundary for the first release:

```text
one project
  └── one exact workspace path
      └── one bridge instance
          ├── one Telegram bot token
          ├── one private forum supergroup
          ├── one owner-only SQLite database
          └── one launchd/systemd service
```

Do not run two bridge instances with the same bot token. Telegram long polling
has one update queue per bot, so consumers would race and lose each other's
updates.

## Manual command outline

Prepare a macOS instance:

```sh
python3 installer.py prepare \
  --workspace "/absolute/path/to/project" \
  --secret-backend macos-keychain
```

Prepare a Linux/VPS instance backed by Proton Pass:

```sh
python3 installer.py prepare \
  --workspace "/srv/projects/example" \
  --secret-backend proton-pass \
  --secret-reference "Codex Telegram Bot - example"
```

On a resource-constrained host such as Raspberry Pi 3B+, serialize Codex work
across all Telegram Topics:

```sh
python3 installer.py prepare \
  --workspace "/srv/projects/example" \
  --secret-backend file \
  --max-active-turns 1 \
  --codex-full-access
```

`max_active_turns=0` is the default and preserves unlimited cross-Topic
parallelism. A positive value remains occupied until Codex reports the turn as
completed, failed, or interrupted. Waiting inputs stay in the SQLite queue and
resume in global FIFO order after capacity becomes available. The same setting
can be supplied as `CODEX_TELEGRAM_MAX_ACTIVE_TURNS` before `prepare`, or as
`max_active_turns` in the owner-only JSON configuration. A start/steer request
whose result is unknown continues to occupy capacity across reconnects until
Codex history safely reconciles it.

`--codex-full-access` is an explicit trust decision. It makes every new,
resumed, and Telegram-started Codex turn use `approval_policy=never` and the
`danger-full-access` sandbox, even if the host Codex defaults are more
restrictive. Use it only for a private group whose bound administrator is
trusted to act with the same filesystem and network access as the service OS
user. Omitting it preserves the host Codex permission defaults.

The command prints the owner-only configuration path and the exact next
commands. Then:

1. configure/check the secret backend;
2. create the bot and private forum supergroup;
3. run `codex-telegram-bridge --config ... bootstrap --wait-seconds 900`;
4. send `/connect` in General from the intended administrator;
5. run `python3 installer.py activate --config ...`.

`activate` performs a first synchronization and requires a passing `doctor`
and a verified database snapshot before registering the long-lived bridge,
five-minute health service, and thirty-minute backup service.

To stop and unregister an instance without deleting its database or Telegram
Topics:

```sh
codex-telegram-bridge-installer deactivate --config ".../config.json"
```

## Runtime isolation

Each instance gets a deterministic name derived from the canonical workspace
path unless `--instance` is supplied.

Default state locations:

- macOS: `~/Library/Application Support/CodexTelegramBridge/<instance>/`
- Linux: `${XDG_DATA_HOME:-~/.local/share}/codex-telegram-bridge/<instance>/`

The state directory contains the installed virtual environment, configuration,
SQLite database, media cache, and bounded logs. It is created with mode `0700`;
the database and configuration are `0600`.

Moving a live instance to another machine is different from making a new
installation. Prefer a verified file produced by the `backup` command. Stop
the old service before cutover and never clone one state database into two
active consumers.

## Supported secret backends

### macOS Keychain

`installer.py configure-secret` invokes the native `security` prompt with `-w`
as the final option. The token is not placed in process arguments.

### Proton Pass CLI

Create a Proton Pass login item and put the bot token in its password field.
Authenticate `pass-cli` for the same OS user that owns the bridge, then use
either the item title or a `pass://...` URI as `--secret-reference`.

On an unattended VPS, verify the Proton Pass session and user service survive a
logout. Depending on the host policy, the administrator may need to enable
systemd user lingering.

### Owner-only file

This is a fallback. `installer.py configure-secret` writes the token through a
hidden prompt into an owner-only regular file outside the repository.

## Health and compatibility

`doctor` checks:

- workspace and owner-only runtime paths;
- Telegram identity, binding, administrator status, and Manage Topics;
- Codex CLI/App Server version equality and explicit compatibility;
- local socket ownership and protocol smoke;
- SQLite integrity, schema, queue age, unresolved delivery outcomes, and
  exact task↔Topic parity;
- ffmpeg and private media storage.
- the owner-only deployment manifest and deterministic installed-package
  digest, so direct runtime edits or incomplete installs fail closed.

Runtime code has one canonical path: change a branch, bump the patch version,
test, merge the PR, and install the exact clean merge commit. The installer
refuses an older version and refuses different package bytes under the same
version. Never patch `site-packages` or an installed runtime directly.

An unsupported Codex version keeps Telegram input durable but disables unsafe
dispatch until a tested bridge release is installed.

## Recovery and backups

The Linux service uses systemd readiness notification and a watchdog. An
unexpected exit of any critical internal loop terminates the process so the
service manager can restart the whole bridge instead of leaving a
half-working PID. Both systemd and launchd restart the bridge after any
unintended exit. Health checks are time-bounded.

Activation also installs a backup job. It takes a consistent snapshot while
the bridge is running through SQLite's online backup API, checks the schema
and full database integrity, writes owner-only files atomically, and retains
96 half-hour snapshots:

```sh
codex-telegram-bridge --config ".../config.json" backup --retention 96
```

Backup creation, publication, and pruning are serialized across processes.
The next successful run removes only strictly named owner-owned temporary
files left by an interrupted prior backup.

`probe-local` checks only SQLite and the local Codex App Server. It deliberately
does not read the Telegram token or treat Telegram, OpenAI, DNS, or Internet
outages as reasons to restart local services. The installed five-minute health
job uses this local probe; run the full network-aware `doctor` manually after
installation, updates, or recovery.

These snapshots protect against application and database failures on the same
host. They are not disaster recovery for a failed disk, stolen computer,
power event, or destroyed machine. Periodically copy a verified snapshot to a
second trusted device using authenticated transport and encryption at rest.
Never expose the live database or its backups publicly: both contain private
conversation and identifier data.

## Optional media worker

A low-power bridge host can optionally ask one trusted LAN worker to prepare
Telegram voice and video with FFmpeg. The worker is only an accelerator:

- the bridge host remains the sole owner of Telegram polling, SQLite, Codex,
  queue state, and every final `LocalInput`;
- documents, prompts, workspace paths, Telegram identifiers, bot tokens,
  Codex credentials, and password-manager data are never sent to the worker;
- mutual TLS authenticates both hosts and encrypts the media in transit;
- strict byte, artifact, queue, concurrency, and time limits bound every job;
- infrastructure, transport, TLS, capacity, timeout, protocol, malformed
  response, and unsupported-capability failures fall back to local FFmpeg;
- an authenticated terminal invalid-media result is returned directly and is
  intentionally not processed a second time on the resource-limited host.

The optional worker is never a systemd dependency and is not part of bridge
readiness or the local watchdog. Removing its configuration restores the exact
local-only behavior.

The worker identity must stay separate from any SSH channel that gives the
main macOS user or Codex full access. Do not reuse that user's account, SSH
keys, authorized-key rules, sudo authority, home directory, Codex login, or
password-manager session for the media worker.

The repository only renders reviewed boot-service definitions; it does not
install them. See [docs/MEDIA_WORKER.md](docs/MEDIA_WORKER.md) for the bounded
configuration contract, dedicated-account requirement, and rollback path.

Minimal deployment flow:

1. Approve a dedicated non-login account, a stable mDNS/DNS name and port, and
   a private one-purpose CA. Issue a server certificate whose SAN is the
   Pi-side `server_name`, plus a client certificate used only by this bridge.
   Keep the CA private key off both runtime hosts after issuance.
2. Install Python, this package, and FFmpeg in a root-owned location that the
   worker can execute but cannot modify. Put worker state, its `0600` config,
   and its server key in separate worker-owned paths. Copy only the client
   certificate/key and server CA to an owner-only Pi path.
3. Create the two closed-schema JSON files shown in
   [docs/MEDIA_WORKER.md](docs/MEDIA_WORKER.md), then validate and render:

```sh
sudo -u SERVICE_USER codex-telegram-media-worker \
  --config /absolute/path/to/worker.json probe-config

sudo -u SERVICE_USER codex-telegram-media-worker \
  --config /absolute/path/to/worker.json render-launchd \
  --service-user SERVICE_USER \
  --python-executable /root-owned/runtime/bin/python
```

Use `render-systemd` instead of `render-launchd` on Linux. Review the rendered
boot-service definition before installing it. macOS stdout/stderr are sent to
`/dev/null`; the worker writes a sanitized owner-only log under
`state_dir/logs`, rotated at 1 MiB with three backups. Linux also retains
bounded policy-controlled journal records.

FFmpeg is limited to one decoder, encoder, and filter thread, 16,777,216 input
pixels, 128 MiB per allocation, and bounded output. Native macOS also polls
the FFmpeg process group's physical footprint and kills it above 512 MiB.
That watchdog is best-effort: launchd's resident-set setting is advisory, not
a hard memory boundary. Linux `MemoryMax` is strict; a strict cap on Apple
hardware requires a capped Linux VM. Keep native-Mac worker concurrency at
one and expose it only to the private trusted bridge identity.

Rendering fails unless `SERVICE_USER` already exists, has a non-login shell,
has nonzero UID, and has no administrative, remote-access, custom, or
root-equivalent group membership. Linux requires primary-group-only
membership. macOS permits only the reviewed automatic `everyone`,
`localaccounts`, `_lpoperator`, and `com.apple.sharepoint.group.N` groups in
addition to the primary group.

After review, the macOS boot-level install is:

```sh
sudo install -o root -g wheel -m 0644 REVIEWED.plist \
  /Library/LaunchDaemons/com.codex.telegram-media-worker.plist
sudo launchctl bootstrap system \
  /Library/LaunchDaemons/com.codex.telegram-media-worker.plist
```

The corresponding Linux `systemctl enable --now` flow is documented in
[docs/MEDIA_WORKER.md](docs/MEDIA_WORKER.md).

On a multi-interface worker, an explicit `0.0.0.0` or `::` listener survives
interface and DHCP failover. It also listens on every matching interface, so
mutual TLS and a host/network firewall limited to trusted LAN clients are
mandatory. The Pi may connect by the stable certificate name rather than a
pinned lease.

After the worker is healthy, enable the client on the Pi:

```sh
codex-telegram-bridge-installer prepare \
  --workspace /absolute/path/to/the/project \
  --instance EXISTING_INSTANCE \
  --state-dir /absolute/path/to/existing/state \
  --media-worker-client-config /absolute/path/to/client.json

codex-telegram-bridge-installer activate \
  --config /absolute/path/to/existing/state/config.json
```

Stop the worker once and confirm the same harmless media request completes
through local FFmpeg before considering the deployment complete. Preserve the
exact existing `--instance` and `--state-dir`: `prepare` alone does not reload
the running service, and its returned `activate` command is mandatory.

Local-only rollback does not touch the worker host. It also requires prepare
and activate:

```sh
codex-telegram-bridge-installer prepare \
  --workspace /absolute/path/to/the/project \
  --instance EXISTING_INSTANCE \
  --state-dir /absolute/path/to/existing/state \
  --disable-media-worker

codex-telegram-bridge-installer activate \
  --config /absolute/path/to/existing/state/config.json
```

The installed console installer works outside a repository checkout; when
working from a clone, `python3 installer.py` is an equivalent entry point.
Track both leaf-certificate expiries, begin renewal checks 90 days before
expiry, complete replacement by 60 days before expiry, and keep the CA private
key offline between issuance events.

## Development

```sh
python3 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements.lock
.venv/bin/python -m unittest discover -s tests -v
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).
