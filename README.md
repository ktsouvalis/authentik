# ak-monitor

Real-time TUI dashboard for the **Authentik HA Cluster** stack at the University of Peloponnese.  
Monitors all services on one screen: keepalived VIP, Patroni/PostgreSQL, etcd, HAProxy, Redis, Redis Sentinel, and Authentik backends.

Built with [Textual](https://textual.textualize.io/). No agents, no daemons — runs from any workstation that can reach the cluster network (or VXLAN interface).

---

## What it monitors

| Panel | How |
|---|---|
| **VIP / keepalived** | HTTP to HAProxy stats on VIP — confirms VIP is reachable |
| **HAProxy backends** | Parses `/stats;csv` — shows per-backend UP/DOWN count per node |
| **PostgreSQL / Patroni** | `GET http://<node>:8008/` — role (LEADER/REPLICA), state, timeline |
| **etcd** | `GET http://<node>:2379/health` + `/v2/stats/self` — health + leader |
| **Redis** | `INFO replication` via redis-py — MASTER/REPLICA, slave count, master link |
| **Redis Sentinel** | `SENTINEL masters` — master IP, replica count, sentinel quorum |
| **Authentik** | `/-/health/live/` and `/-/health/ready/` — server + worker per node |

---

## Color coding

| Color | Meaning |
|---|---|
| **Green ●** | Service is up and in primary/active/leader role |
| **Dim grey ●** | Service is up but in backup/replica/follower role (healthy, non-primary) |
| **Yellow ●** | Degraded — partial backends UP or sentinel flags set |
| **Red ●** | Service is down or unreachable |
| **Top banner green** | All services across all nodes are healthy |
| **Top banner red** | One or more services are down |

---

## Requirements

- Python 3.11+
- Network access to all cluster node IPs (direct or via VXLAN)
- HAProxy stats endpoint enabled (port 9000 by default)
- Patroni REST API accessible (port 8008)
- etcd HTTP API accessible (port 2379)
- Redis and Sentinel ports reachable (6379, 26379)
- Authentik HTTPS accessible on port 9443 per node

---

## Installation

```bash
git clone <repo> ak-monitor
cd ak-monitor

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

---

## Configuration

All settings are driven by environment variables. No config files to edit.

```bash
cp .env.example .env
nano .env          # fill in your IPs, passwords, node names
```

Load the env and run:

```bash
set -a && source .env && set +a
python monitor.py
```

Or inline for a one-liner:

```bash
env $(cat .env | grep -v '^#' | xargs) python monitor.py
```

---

## Key bindings

| Key | Action |
|---|---|
| `R` | Force immediate refresh |
| `Q` | Quit |

---

## Multi-site / VXLAN usage

Each site gets its own `.env` file with its local IPs:

```
.env.site-a    # Patras  — VXLAN 10.10.1.x
.env.site-b    # Tripoli — VXLAN 10.10.2.x
.env.site-c    # Sparta  — VXLAN 10.10.3.x
```

Run per site:

```bash
env $(cat .env.site-b | grep -v '^#' | xargs) python monitor.py
```

Or open three terminal tabs, one per site.

The `SITE_NAME` variable controls the label shown in the TUI title bar so you always know which cluster you're looking at.

---

## Notes

- Authentik TLS verification is disabled (`verify=False`) since the backends use self-signed or internal certs on port 9443. This is intentional and scoped to health check requests only.
- The Sentinel check connects directly to each Sentinel node and reads `SENTINEL masters` — no VIP needed for Sentinel.
- `NODE_NAMES` is optional. If omitted, raw IPs are displayed. Format: `IP=name` pairs comma-separated.
