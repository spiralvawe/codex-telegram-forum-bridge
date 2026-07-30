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
  approvals, queueing, steer, archive/restore, mode controls, and health checks.

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
before registering the long-lived bridge and five-minute health service.

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
installation. Stop the old service before transferring its database. Never
clone a live database into two active consumers.

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

An unsupported Codex version keeps Telegram input durable but disables unsafe
dispatch until a tested bridge release is installed.

## Development

```sh
python3 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements.lock
.venv/bin/python -m unittest discover -s tests -v
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).
