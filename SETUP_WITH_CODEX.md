# Install this bridge with Codex

This file is the installation contract for Codex. The human user should be
able to provide this repository and the absolute target-project path, then
follow only the Telegram and secret-manager prompts below.

## Non-negotiable rules

1. Read this file completely before running commands.
2. Confirm the exact target workspace with the user. Do not assume this bridge
   repository is the target.
3. Use one new Telegram bot token and one private forum supergroup for this
   project. Never reuse a token already consumed by another bridge.
4. Never ask the user to paste the bot token into Codex chat or place it in a
   command argument, repository file, `.env`, log, or Telegram message.
5. Do not print numeric Telegram IDs, Codex thread IDs, Proton Pass item IDs,
   database content, or credential-bearing URLs.
6. Stop on unsupported Codex protocol, failed tests, an unsafe runtime path, an
   ambiguous group binding, or a failed `doctor`.

## 1. Read-only preflight

Run:

```sh
uname -s
python3 --version
codex --version
codex app-server daemon version
command -v ffmpeg
```

On macOS, the Codex binary may instead be bundled at:

```text
/Applications/ChatGPT.app/Contents/Resources/codex
```

On a Linux host, also check:

```sh
systemctl --user --version
command -v pass-cli
pass-cli test
```

Do not display secret-manager item contents.

## 2. Run the repository tests

Create a repository-local development environment and run the complete suite:

```sh
python3 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements.lock
.venv/bin/python -m unittest discover -s tests -v
```

Do not continue after a test failure.

## 3. Prepare one isolated instance

Choose:

- `macos-keychain` on a normal MacBook;
- `proton-pass` on a VPS where `pass-cli test` succeeds;
- `file` only when the user explicitly accepts the owner-only-file fallback.

Run:

```sh
python3 installer.py prepare \
  --workspace "/absolute/path/to/target-project" \
  --secret-backend BACKEND \
  --codex-full-access
```

This installation contract assumes the owner wants Telegram to be a fully
trusted Codex control surface. `--codex-full-access` explicitly selects
`approval_policy=never` and `danger-full-access` for every new, resumed, and
Telegram-started turn. Before running it, state that Telegram commands will
have the same filesystem and network access as the bridge's OS user. If the
owner does not accept that boundary, omit the flag and explain that Codex may
request local or Telegram approvals.

For a host with limited memory, add `--max-active-turns 1`. This is a global
limit across every Telegram Topic in the instance, not a per-Topic limit.
Never substitute a shorter RPC-only semaphore: capacity must remain occupied
until the actual Codex turn reaches a completed, failed, or interrupted state.

For Proton Pass, add a title or opaque `pass://...` reference and optional
vault:

```sh
python3 installer.py prepare \
  --workspace "/absolute/path/to/target-project" \
  --secret-backend proton-pass \
  --secret-reference "Codex Telegram Bot - project-name" \
  --secret-vault "Work"
```

Record the returned configuration path. It contains no bot token, but it is
private runtime metadata and must not be committed.

On macOS, tell the user that Codex Desktop may need one manual restart after
the shared App Server daemon is bootstrapped. Never close or restart Desktop
without the user's consent.

## 4. Human Telegram checklist

Ask the user to do these steps:

1. In BotFather, create a new bot dedicated to this project.
2. Disable group privacy for this bot so ordinary Topic messages are delivered.
3. Create a new private Telegram supergroup.
4. Enable Topics/forum mode.
5. Add the bot as an administrator and enable Manage Topics.
6. Do not add untrusted members: Telegram becomes a second copy of the project
   conversation.

Do not request the user's numeric Telegram ID. `/connect` will bind the exact
sender after verifying that the sender is a group administrator.

## 5. Configure the token without showing it to Codex

For macOS Keychain or owner-only file, tell the user to run the returned
`installer.py configure-secret` command directly in their terminal and answer
the hidden native prompt.

For Proton Pass:

1. ask the user to create a login item whose password is the bot token;
2. ensure `pass-cli test` succeeds for the service-owning user;
3. run the returned `installer.py secret-check` command.

Only report backend availability. Never show the secret reference or value.

## 6. Bind Telegram

Run the returned bridge bootstrap command with a bounded wait:

```sh
.../codex-telegram-bridge --config ".../config.json" \
  bootstrap --wait-seconds 900
```

While it waits, ask the intended owner to send `/connect` in the General Topic.
Bootstrap must verify:

- the chat is a supergroup with forum Topics;
- the bot is an administrator with Manage Topics;
- the sender is a human group administrator;
- no previous binding exists.

## 7. Activate and verify

Run:

```sh
python3 installer.py activate --config ".../config.json"
```

Activation must:

1. synchronize the current workspace tasks;
2. require exact task↔Topic parity;
3. require a passing `doctor`;
4. create and verify an online database snapshot;
5. install/start launchd on macOS or a systemd user service on Linux;
6. install/start a five-minute local-only health check and a thirty-minute
   backup job.

Then run `doctor` one more time through the installed CLI and report only
sanitized counts and statuses.

## 8. Smoke test

Use a disposable Codex task/Topic. Verify:

- Telegram text starts one Codex turn;
- a harmless Telegram CSV or PDF reaches Codex as a file together with its
  caption;
- visible progress and the final answer return to the same Topic;
- a harmless workspace file explicitly linked in the final answer arrives as
  a native Telegram attachment;
- a second synchronization creates no duplicate Topic;
- restart of the bridge service preserves the queue and mappings.
- one manually requested `backup --retention 96` succeeds and its output file
  remains owner-only;
- when `max_active_turns` is positive, a task in another Topic remains queued
  until the current turn actually terminates.
- when `--codex-full-access` is enabled, harmless workspace, outside-workspace,
  and outbound-network probes complete without an approval request, while an
  OS-level path forbidden to the service user remains forbidden.

Do not use a physical actuator, production deployment, payment, credential
change, or other high-impact action as a smoke test.

## 9. Unattended Linux host check

For a headless Linux node, verify after activation:

```sh
loginctl show-user "$USER" -p Linger
systemctl --user is-active codex-telegram-bridge-INSTANCE.service
systemctl --user is-active codex-telegram-bridge-INSTANCE-health.timer
systemctl --user is-active codex-telegram-bridge-INSTANCE-backup.timer
```

The bridge should run as a dedicated non-root account. If administration is
needed, use a separate administrator account; do not give the Telegram-facing
runtime user unrestricted sudo. Reboot the host once and repeat `doctor`
before declaring an unattended installation complete.

The portable installer supervises the application and produces local verified
snapshots. UPS power, storage health, host/network watchdogs, Wi-Fi policy,
thermal management, and encrypted off-host backup are machine-specific and
must be assessed separately. A same-disk snapshot does not survive loss of the
host or storage device.

## 10. Optional media worker

Configure the LAN media worker only when the owner explicitly requests it.
Read `docs/MEDIA_WORKER.md` completely. Before issuing certificates or loading
a boot service, obtain approval for the dedicated non-login account, stable
DNS/mDNS name, port, CA lifetime/storage, retention period, and system paths.

Never reuse the main user's SSH/Codex identity or put Telegram, Codex, Home
Assistant, Proton Pass, sudo, or shell-login credentials in the worker. Run the
worker renderer and `probe-config` as the verified service account. On Linux,
require primary-group-only membership. On macOS, permit only the reviewed
automatic `everyone`, `localaccounts`, `_lpoperator`, and
`com.apple.sharepoint.group.N` groups; reject administrative, remote-access,
custom, and root-equivalent membership on every platform. Keep
Python/package/FFmpeg immutable to that account and retain the generated
process, memory, file-size, open-file, restart, and spool limits.

On native macOS keep worker concurrency at one. Verify that the FFmpeg command
retains the pixel, single-allocation, decoder/encoder/filter-thread, output,
and 512 MiB physical-footprint watchdog controls. State explicitly that
launchd's resident-set setting is advisory and the watchdog is best-effort;
only Linux `MemoryMax` or a capped Linux VM provides a strict memory boundary.
Accept the remaining native-Mac denial-of-service risk only for a private,
trusted Telegram group.

When changing a prepared bridge, read its current configuration and pass the
same absolute workspace, `--instance`, and `--state-dir` to `prepare`. Never
derive a new instance for an existing Telegram bot:

```sh
codex-telegram-bridge-installer prepare \
  --workspace "/exact/existing/workspace" \
  --instance EXISTING_INSTANCE \
  --state-dir "/exact/existing/state" \
  --media-worker-client-config "/owner-only/client.json"
```

Run the exact `activate --config` command returned by `prepare`. Preparation
does not reload an already running service; activation performs `doctor`,
creates a verified backup, and restarts/reloads the service. The same two-step
prepare-plus-activate rule applies to `--disable-media-worker`.

After activation, stop the worker and prove that one harmless media request
completes through local FFmpeg; then restart the worker and repeat it remotely.
Transport, TLS, capacity, timeout, protocol, malformed-output, and unsupported
capability failures must fall back locally. An authenticated terminal
`MediaProcessingError` is intentionally not retried locally.

Record both leaf-certificate expiries, check for renewal starting 90 days
before expiry, and complete replacement by 60 days before expiry. Do not
hardcode one deployment's expiry in this universal repository. Keep the CA
private key offline between issuance events. A worker failure or certificate
maintenance event must never fail bridge readiness or restart the bridge.
