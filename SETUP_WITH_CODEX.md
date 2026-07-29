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

On a Linux VPS, also check:

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
  --secret-backend BACKEND
```

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
4. install/start launchd on macOS or a systemd user service on Linux;
5. install/start a five-minute health check.

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

Do not use a physical actuator, production deployment, payment, credential
change, or other high-impact action as a smoke test.
