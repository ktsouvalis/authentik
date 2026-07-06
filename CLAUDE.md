# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

`ak-monitor` — standalone operational tooling for a UoP/ESDA-Lab **Authentik HA cluster** (Authentik + Patroni/PostgreSQL + etcd + HAProxy + keepalived/VIP + nginx, 3 nodes per site). Nothing here runs *inside* the cluster; every script connects out to the cluster over HTTP/SSH from an operator's workstation. There is no build step, package, or test suite — these are three independent, config-driven CLI/TUI scripts:

- `monitor.py` — Textual TUI dashboard, polls all services every `refresh_interval` seconds.
- `logs_viewer.py` — Textual TUI (or `--save` plain-text mode) that pulls warn/error logs from every node over SSH (`docker logs` or `journalctl` depending on service type).
- `import_users.py` — one-shot CLI that bulk-imports/updates Authentik users from a CSV via the Authentik REST API.

## Running

```bash
pip install -r requirements.txt

python monitor.py                          # uses ./config.yml
python monitor.py custom_config.yml        # positional arg, NOT --config

python3 logs_viewer.py                     # TUI mode
python3 logs_viewer.py --config config.yml --last 12
python3 logs_viewer.py --save cluster_logs # writes cluster_logs.log, no TUI

python3 import_users.py users.csv --group "Lab Members" --dry-run
```

Note the config-flag inconsistency: `monitor.py` takes the config path as a bare positional arg, while `logs_viewer.py` and `import_users.py` use `--config`. Don't "fix" this to be consistent without checking both call sites — it's a pre-existing quirk, not a bug to silently unify.

No automated tests exist. There's no linter/formatter config either — match the surrounding style (no type hints beyond simple annotations, `Optional[...]` from `typing`, f-strings, Rich markup like `[bold green]...[/]` for terminal color).

## Config files (`config.yml`)

Everything is driven by one YAML file per site (`config.yml`, `config_esda.yml` are real, gitignored site configs; `config.yml.example` is the tracked template — always update the example, not just the real files, when adding a new config key). Key sections: `nodes:` (per-service IP/name lists — `authentik`, `patroni`, `etcd`, `haproxy`), `ports:`, `credentials:`, `keepalived:` (VIP failover priorities), `services:` (drives `logs_viewer.py`'s node×service matrix), `authentik.url` (used by `import_users.py`).

`*.yml` and `*.csv` are gitignored — only `config.yml.example` is force-tracked. When editing config shape, update all three yaml files (`config.yml`, `config_esda.yml`, `config.yml.example`) even though the first two aren't tracked, since they're the actual working configs on this machine.

## monitor.py architecture

Single-file Textual app. The pattern repeats per service and is the thing to copy when adding a new panel:

1. A `check_<service>_node(node) -> dict` function does one blocking network call (`requests`, `psycopg2`, etc.) per node and always returns a dict with at least `ip`, `name`, `ok` — never raises, catches its own exceptions and returns an `ok: False` sentinel shape.
2. `action_refresh_now` (a `@work(thread=True)` method) fans these out via one shared `ThreadPoolExecutor`, `.result()`s them all, then hands the whole batch to `self.call_from_thread(self._apply_updates, ...)`.
3. `_apply_updates` pushes each result list into its panel's `data` reactive, then recomputes a `<service>_fail` count and folds it into `all_failures` for the top-level status dot.
4. A `<Service>Panel(Static)` class renders `self.data` into Rich-markup text via `render_content()`, triggered by `watch_data`.

Patroni is special: its results are awaited *before* the rest of the batch because `check_replication_slots`/`check_patroni_history` need the primary's IP, which is only known after `check_patroni_node` resolves.

`_fmt_lag`, `_fmt_bytes`, `failures_to_dot` are shared formatting helpers — reuse them for new panels rather than duplicating bar/threshold logic. `_UNICODE`/`_BULLET` control whether status dots render as `●` or `*` (auto-detected from locale, overridable via `unicode_bullets` in config) — some Proxmox CTs lack UTF-8 locales.

A Redis + Redis Sentinel panel/check pair existed here previously; it was removed when Redis Sentinel was dropped from the stack. If cluster architecture changes again, grep git history (`git log -p -- monitor.py`) rather than assuming the current panel set is final.

## import_users.py vs history

`mass_import.py` (an earlier, ESDA-lab-specific script with a hardcoded user list) was removed in favor of `import_users.py`, which is CSV-driven, reads its Authentik URL/token from `config.yml`, and handles both create and update-existing-email flows. Any future bulk-import work should extend `import_users.py`, not resurrect the hardcoded-list pattern.
