# ak-monitor

Real-time TUI dashboard for **Authentik HA Cluster** stack .
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

| Indicator | Meaning |
|---|---|
| ${\color{green}●}$ Green | Service is up and in primary/active/leader role |
| ${\color{gray}●}$ Grey | Service is up but in backup/replica/follower role (healthy, non-primary) |
| ${\color{yellow}●}$ Yellow | Degraded — partial backends UP or sentinel flags set |
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
- Redis and Sentinel ports reachable (6379, 26379)
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
cp config.example.yml config.yml
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

## Key bindings

| Key | Action |
|---|---|
| `R` | Force immediate refresh |
| `Q` | Quit |
| `Ctrl+P` | Pallette |

---

## Notes

- Authentik TLS verification is disabled (`verify=False`) since the backends use self-signed or internal certs on port 9443. This is intentional and scoped to health check requests only.
- The Sentinel check connects directly to each Sentinel node and reads `SENTINEL masters` — no VIP needed for Sentinel.
- `NODE_NAMES` is optional. If omitted, raw IPs are displayed. Format: `IP=name` pairs comma-separated.
