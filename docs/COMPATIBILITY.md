# Compatibility

Codex App Server is experimental. A release explicitly lists the Codex
CLI/App Server versions it has tested.

The bridge requires:

- Python 3.10 or newer;
- `websocket-client` at the pinned version in `requirements.lock`;
- ffmpeg for voice and video;
- a Codex CLI that provides `app-server daemon bootstrap`, `start`, and
  `version`;
- a local owner-only Unix App Server socket;
- Telegram Bot API access;
- launchd on macOS or a systemd user manager on Linux.

Unknown Codex versions fail closed. Telegram polling may continue accepting
durable input, but dispatch and unsafe controls remain unavailable until a
tested release updates the compatibility set.

## Adding a Codex version

Do not merely append a version string. Validate at minimum:

1. daemon bootstrap/start/version;
2. initialize and protocol version;
3. thread list/read/start/archive/unarchive;
4. turn start/steer/interrupt;
5. model list, thread settings read/update, and rate-limit read;
6. notification and server-request shapes;
7. text, image, audio, mentioned-file, approval, and question inputs;
8. complete regression suite;
9. a disposable end-to-end Telegram smoke test.
