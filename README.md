# ak-monitor

A pair of TUI tools for the **Authentik HA Cluster**:

| Script | Purpose |
|---|---|
| `monitor.py` | Real-time dashboard — polls every 20 s, one panel per service |
| `logs_viewer.py` | Log viewer — fetches warnings/errors from all nodes via SSH |

Built with [Textual](https://textual.textualize.io/). No agents, no daemons — runs from any workstation that can reach the cluster network (or VXLAN interface).

---

## What it monitors

| Panel | How |
|---|---|
| **VIP / keepalived** | HTTP to HAProxy stats on VIP — confirms VIP is reachable |
| **HAProxy backends** | Parses `/stats;csv` — shows per-backend UP/DOWN count per node |
| **PostgreSQL / Patroni** | `GET http://<node>:8008/` — role (LEADER/REPLICA), state, timeline |
| **etcd** | `GET http://<node>:2379/health` + `/v2/stats/self` — health + leader |
| **Authentik** | `/-/health/live/` and `/-/health/ready/` — server + worker per node |

---

## Color coding

| Indicator | Meaning |
|---|---|
| ${\color{green}●}$ Green | Service is up and in primary/active/leader role |
| ${\color{gray}●}$ Grey | Service is up but in backup/replica/follower role (healthy, non-primary) |
| ${\color{yellow}●}$ Yellow | Degraded — partial backends UP |
| ${\color{red}●}$ Red | Service is down or unreachable |
| ${\color{green}●}$ Top banner green | All services across all nodes are healthy |
| ${\color{red}●}$ Top banner red | One or more services are down |

---

## Requirements

- Python 3.11+
- Network access to all cluster node IPs (direct or via VXLAN)
- HAProxy stats endpoint enabled (port 9000 by default)
- Patroni REST API accessible (port 8008)
- etcd HTTP API accessible (port 2379)
- Authentik HTTPS accessible on port 9443 per node

---

## Installation
1. Clone the repo and set up a Python environment:
```bash
git clone <repo> ak-monitor
cd ak-monitor
```
2. Create and activate a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```
or

```bash
conda create -n ak-monitor python=3.11
conda activate ak-monitor
```
3. Install dependencies:
```bash
pip install -r requirements.txt
```

---

## Configuration

All settings are driven by configuration yaml variables. No config files to edit.

```bash
cp config.yml.example config.yml
nano config.yml     # fill in your IPs, passwords, node names
```

Load the default config file and run:

```bash
python monitor.py
```
or specify a custom config file:

```bash
python monitor.py --config custom_config.yaml
```

---

## Key bindings (monitor.py)

| Key | Action |
|---|---|
| `R` | Force immediate refresh |
| `Q` | Quit |
| `Ctrl+P` | Pallette |

---

## log viewer (logs_viewer.py)

Collects warnings and errors from the last 24 hours across every service and node via SSH. Bare-metal services are read from `journald`; containerised services are read from `docker logs`.

The minimum severity to include is configurable via `--level` (`error`, `warning` — default, `info`, `debug`); each level includes everything at or above it in severity (e.g. `info` includes info/warning/error). `debug` disables filtering entirely and returns every line.

### TUI mode

Node tabs across the top; service sub-tabs within each node. Results stream in per service as SSH calls complete.

```bash
python3 logs_viewer.py
python3 logs_viewer.py --config custom_config.yml
python3 logs_viewer.py --level error
```

Key bindings:

| Key | Action |
|---|---|
| `R` | Re-fetch all logs |
| `Q` | Quit |

### Save mode

Fetches all logs and writes a structured plain-text `.log` file — no TUI is shown. Progress is printed to stdout as each result arrives. The `.log` extension is appended automatically if omitted.

```bash
python3 logs_viewer.py --save cluster_logs
# writes: cluster_logs.log
python3 logs_viewer.py --save cluster_logs --level info
```

Output format:

```
Authentik HA Cluster — Log Report
Fetched:  2026-04-28 15:30:00
Scope:    last 24h, warning and above
================================================================================

NODE: ak-node-1  (10.99.97.71)
================================================================================

  SERVICE: Auth Server  [docker: authentik-server-1]
  ────────────────────────────────────────────────────────────
  2026-04-28 14:01:33 WARNING  …
  2026-04-28 14:22:11 ERROR    …

  SERVICE: Patroni  [systemd: patroni]
  ────────────────────────────────────────────────────────────
  (no warnings or errors in the last 24h)
…
```

### Configuring services

The list of services to poll is defined in `config.yml` under the `services:` key. Each entry specifies a display label, which node group it runs on, whether it is a Docker container or a systemd unit, and the container/unit name.

```yaml
services:
  - label: "Auth Server"
    nodes: authentik       # key from the nodes: or keepalived: sections
    type: docker
    container: "authentik-server-1"

  - label: "Patroni"
    nodes: patroni
    type: systemd
    unit: "patroni"
```

`nodes` must match one of the keys already present in the `nodes:` map (or `keepalived`). Services can be added, removed, or renamed here without touching the code.

### Additional requirements for logs_viewer.py

- SSH access to all cluster nodes (username + password in `config.yml` under `ssh:`)
- Docker CLI available on each node (`docker logs`)
- `systemd`/`journalctl` available on nodes running bare-metal services

---

## Importing users

`import_users.py` bulk-imports users into Authentik from a CSV file (columns: surname, name, and an email column — the header just needs to contain `@` somewhere). It reads the Authentik URL and API token from `config.yml`, creates missing users, updates emails on existing ones, and optionally adds everyone to a group.

```bash
python3 import_users.py users.csv
python3 import_users.py users.csv --group "Lab Members"
python3 import_users.py users.csv --dry-run
python3 import_users.py users.csv --config config.site-b.yml
```

---

## Notes

- Authentik TLS verification is disabled (`verify=False`) since the backends use self-signed or internal certs on port 9443. This is intentional and scoped to health check requests only.
