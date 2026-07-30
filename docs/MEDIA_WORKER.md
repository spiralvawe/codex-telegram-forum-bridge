# Optional media worker

This component moves only bounded FFmpeg preparation of inbound Telegram
voice, video, and video notes from a low-power bridge host to one trusted LAN
machine. It is an accelerator, not a second bridge.

The Pi remains authoritative for Telegram polling, durable queue state,
SQLite, Codex, the project workspace, and final local inputs. Infrastructure,
transport, TLS, capacity, timeout, protocol, malformed-response, and unsupported
worker-capability failures open the client circuit and use local FFmpeg. An
authenticated terminal `invalid_audio`, `invalid_video`, or `invalid_media`
result is intentionally returned as `MediaProcessingError` without repeating
the same expensive work on the Pi. The bridge service must never depend on
worker startup or health.

## Security boundary

Run the worker under a dedicated non-admin operating-system account. Its
account, home, state directory, TLS identity, and package runtime must be
separate from the main macOS user that has full-access Codex or SSH. Never give
the worker:

- an SSH key, shell login, sudo rule, or authorized-key entry;
- Telegram, Codex, Home Assistant, or Proton Pass credentials;
- access to the bridge database, project workspace, or the main user's home.

Use a dedicated private CA and client certificate for this protocol. Do not
reuse an SSH key or a general-purpose internal CA. The server certificate needs
a SAN matching the configured `server_name`. Keep the CA private key offline
after issuance; it must not remain on the worker or Pi.

The Python environment, installed package, and FFmpeg binary should be
root-owned and non-writable by the worker. The state directory, worker config,
TLS server key, and sanitized rotating log are worker-owned. Only the dedicated
client certificate/key and server CA belong on the Pi.

Required ownership:

| Object | Owner | Mode / policy |
|---|---|---|
| Python/package/FFmpeg runtime | root | worker-readable/executable, not worker-writable |
| Worker state and `logs/` | dedicated worker | `0700` |
| Worker JSON, server cert/key, client CA | dedicated worker | `0600` |
| CA private key | offline operator storage | never installed on either runtime host |
| Rendered boot-service definition | root | `0644`, reviewed before load |
| Pi client JSON, client cert/key, server CA | bridge account | `0600` |

The service renderers require an explicit `--service-user`, reject privileged
shared accounts, verify that the account exists with a non-login shell, and
reject administrative, remote-access, custom, or root-equivalent group
membership. Linux requires only the primary group. macOS additionally permits
only its reviewed automatic groups: `everyone`, `localaccounts`,
`_lpoperator`, and `com.apple.sharepoint.group.N`. The renderers produce a
boot-level LaunchDaemon or system service; they only print definitions, so
installation remains a separate reviewed action.

## Worker configuration

The worker configuration is a `0600` JSON file owned by the dedicated worker
account. Every field is required and the schema is closed:

```json
{
  "listen_host": "0.0.0.0",
  "listen_port": 9443,
  "state_dir": "/var/lib/codex-telegram-media-worker",
  "ffmpeg_binary": "/Library/Application Support/CodexTelegramMediaWorker/bin/ffmpeg",
  "tls_server_cert": "/etc/codex-telegram-media-worker/server.crt",
  "tls_server_key": "/etc/codex-telegram-media-worker/server.key",
  "tls_client_ca": "/etc/codex-telegram-media-worker/client-ca.crt",
  "queue_capacity": 2,
  "concurrency": 1,
  "request_timeout_seconds": 30,
  "processing_timeout_seconds": 120,
  "shutdown_timeout_seconds": 20,
  "retention_seconds": 86400
}
```

`listen_host` must be one literal private/local address or the explicit
all-interface wildcard `0.0.0.0`/`::`. A wildcard is the resilient choice for a
worker with multiple LAN interfaces or changing DHCP leases, but it also binds
future non-LAN interfaces. Mutual TLS remains mandatory; additionally limit the
port to trusted LAN clients with the host/network firewall. The Pi can connect
by a stable mDNS/DNS name whose SAN is in the server certificate. State must be
an owner-only directory outside credential and bridge paths. TLS files must be
distinct owner-only regular files outside mutable state.
Uploaded source media remains in the bounded spool until TTL/quota cleanup, so
the retention value is also a privacy decision. The internal owner-only log is
rotated at 1 MiB with three backups and never records paths or payloads.

The spool is single-owner locked before cleanup or listener bind. Source,
reserved output, metadata overhead, queue depth, request threads, processing
threads, and wall-clock deadlines are bounded. Duplicate jobs are replayed
idempotently without consuming a second quota reservation. Retryable jobs
survive a worker restart; terminal and expired jobs are pruned at startup and
by idle housekeeping. FFmpeg receives a per-output file-size limit aligned
with the server reservation, a 16,777,216-pixel decoder ceiling, a 128 MiB
single-allocation ceiling, and one decoder, encoder, and filter thread. On
macOS a public `libproc` watchdog polls the complete FFmpeg process group's
physical footprint and kills it above 512 MiB; failure to monitor a live job
also fails closed. A memory-budget result is terminal and is not repeated on
the Pi.

Linux `MemoryMax` is a strict service-cgroup cap. macOS
`ResidentSetSize`/`RLIMIT_RSS` is only a memory-pressure hint, so the libproc
watchdog is best-effort rather than a hard kernel boundary. A strict memory
guarantee on Apple hardware requires a Linux cgroup inside a capped VM. The
native-Mac residual denial-of-service risk is accepted only for a private,
trusted Telegram group. File size, process/task count, and open files are also
bounded. These controls are defense in depth, not permission to place the
spool on an unbounded or unreliable volume.

Homebrew's normal `/opt/homebrew/bin/ffmpeg` is commonly a symlink owned by the
main administrator and is deliberately rejected when the worker runs under a
different account. Point `ffmpeg_binary` at a root-owned, non-writable regular
launcher in the managed runtime; that launcher may execute the approved stable
Homebrew target. Do not make the worker own or modify the launcher.

Validate without starting a listener:

```sh
codex-telegram-media-worker \
  --config /absolute/path/to/worker.json \
  probe-config
```

Render a macOS LaunchDaemon while executing as the dedicated account:

```sh
sudo -u _codexmedia codex-telegram-media-worker \
  --config /absolute/path/to/worker.json \
  render-launchd \
  --service-user _codexmedia \
  --python-executable /absolute/path/to/python \
  > /tmp/com.codex.telegram-media-worker.plist
```

Render a Linux system service while executing as the dedicated account:

```sh
sudo -u codexmedia codex-telegram-media-worker \
  --config /absolute/path/to/worker.json \
  render-systemd \
  --service-user codexmedia \
  --python-executable /absolute/path/to/python \
  > /tmp/codex-telegram-media-worker.service
```

Review the output before any administrator installs or enables it. The Python
runtime must be readable/executable by the worker account and must not live in
the main user's private home. The generated service disables Python bytecode
writes and applies restart plus resource controls. Keep worker `concurrency`
at `1` on native macOS. Do not remove those controls to work around a failed
probe.

Only after that review, install one boot-level definition:

```sh
# macOS
sudo install -o root -g wheel -m 0644 \
  /tmp/com.codex.telegram-media-worker.plist \
  /Library/LaunchDaemons/com.codex.telegram-media-worker.plist
sudo launchctl bootstrap system \
  /Library/LaunchDaemons/com.codex.telegram-media-worker.plist

# Linux
sudo install -o root -g root -m 0644 \
  /tmp/codex-telegram-media-worker.service \
  /etc/systemd/system/codex-telegram-media-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now codex-telegram-media-worker.service
```

Do not run both platform blocks. A failed bootstrap must be diagnosed with the
owner-only worker log plus `probe-config`; do not weaken file modes or move the
service into the main user's account.

## Bridge client configuration

The Pi-side connection file is also owner-only:

```json
{
  "host": "codex-media-worker.local",
  "port": 9443,
  "server_name": "codex-media-worker.local",
  "ca_certificate": "/var/lib/codex-telegram-bridge/media-worker/server-ca.crt",
  "client_certificate": "/var/lib/codex-telegram-bridge/media-worker/client.crt",
  "client_key": "/var/lib/codex-telegram-bridge/media-worker/client.key",
  "request_timeout_seconds": 30,
  "processing_timeout_seconds": 180,
  "failure_threshold": 3,
  "cooldown_seconds": 300
}
```

The `host` is the connection address; `server_name` is independently verified
against the server certificate. Examples below use the installed console
installer, which works outside a source checkout. From a repository checkout,
`python3 installer.py` can be substituted.

For an existing bridge, reuse its exact workspace, `instance_id`, and
`state_dir`; do not let `prepare` derive a second instance:

```sh
codex-telegram-bridge-installer prepare \
  --workspace /absolute/path/to/the/project \
  --instance EXISTING_INSTANCE \
  --state-dir /absolute/path/to/existing/state \
  --media-worker-client-config /absolute/path/to/client.json
```

Re-running `prepare` without either media-worker option preserves the current
setting. `prepare` writes/upgrades the private runtime and configuration, but
does not reload an already running service. Run the exact `activate --config`
command returned by `prepare`; activation runs synchronization, `doctor`, and
a verified backup before installing or restarting the service:

```sh
codex-telegram-bridge-installer activate \
  --config /absolute/path/to/existing/state/config.json
```

To return to the exact local-only path, prepare the same instance and then
activate the returned configuration:

```sh
codex-telegram-bridge-installer prepare \
  --workspace /absolute/path/to/the/project \
  --instance EXISTING_INSTANCE \
  --state-dir /absolute/path/to/existing/state \
  --disable-media-worker

codex-telegram-bridge-installer activate \
  --config /absolute/path/to/existing/state/config.json
```

Deployment still requires explicit choices for the dedicated account name,
reserved LAN address, port, certificate names/lifetime, and system paths.
Those choices must be approved before installing a service or issuing keys.

## Certificate lifecycle

Record the expiry of both leaf certificates in the operator's maintenance
system without putting certificate private keys or deployment-specific dates
in this universal runbook. Begin validation and renewal work 90 days before
expiry and complete replacement no later than 60 days before expiry. Keep the
CA private key offline between issuance events; temporarily retrieve it only
in a controlled operator environment, never on the Pi or worker.

Install renewed leaf certificates and keys atomically with the ownership and
modes above, restart the worker, and verify all three paths: authenticated
remote processing, rejection without the client certificate, and local Pi
fallback while the worker is stopped. A certificate alert is an advance
maintenance signal, not a reason to disable hostname verification or mutual
TLS.
