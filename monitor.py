"""
UoP Authentik HA Cluster Monitor
---------------------------------
Real-time TUI dashboard for the full Authentik HA stack.
All connection details read from config.yml (or a path passed as first argument).

Usage:
    python monitor.py                        # uses config.yml in current dir
    python monitor.py config.site-b.yml     # use a specific config file
"""

import re
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

import yaml
import requests
import redis as redis_lib
import urllib3

from textual.app import App, ComposeResult
from textual.widgets import Static, Footer
from textual.reactive import reactive
from textual.timer import Timer
from textual import work
from textual.containers import Horizontal

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------

def load_config(path: str = "config.yml") -> dict:
    if not os.path.exists(path):
        print(f"[ERROR] Config file not found: {path}")
        print(f"        Copy config.example.yml to {path} and fill in your values.")
        sys.exit(1)
    with open(path) as f:
        return yaml.safe_load(f)


CONFIG_PATH = sys.argv[1] if len(sys.argv) > 1 else "config.yml"
CFG = load_config(CONFIG_PATH)

# Convenience accessors
SITE_NAME         = CFG.get("site_name", "Authentik HA Cluster")
REFRESH_INTERVAL  = int(CFG.get("refresh_interval", 30))
HTTP_TIMEOUT      = int(CFG.get("http_timeout", 4))
REDIS_TIMEOUT     = float(CFG.get("redis_timeout", 3))
VIP               = CFG.get("vip", "")

NODES             = CFG.get("nodes", {})
PORTS             = CFG.get("ports", {})
CREDS             = CFG.get("credentials", {})
SENTINEL_CFG      = CFG.get("sentinel", {})
KA_CFG            = CFG.get("keepalived", {})

# Node lists
AK_NODES      = NODES.get("authentik", [])
PATRONI_NODES = NODES.get("patroni", [])
ETCD_NODES    = NODES.get("etcd", [])
HAPROXY_NODES = NODES.get("haproxy", [])
REDIS_NODES   = NODES.get("redis", [])
KA_NODES      = KA_CFG.get("nodes", [])
TRACK_WEIGHT  = int(KA_CFG.get("track_weight", -20))

# Ports
P_AUTHENTIK   = int(PORTS.get("authentik", 9443))
P_PATRONI     = int(PORTS.get("patroni", 8008))
P_ETCD        = int(PORTS.get("etcd", 2379))
P_HAPROXY     = int(PORTS.get("haproxy_stats", 9000))
P_REDIS       = int(PORTS.get("redis", 6379))
P_SENTINEL    = int(PORTS.get("sentinel", 26379))
P_NGINX_STATUS = int(PORTS.get("nginx_status", 8080))

# Credentials
HAPROXY_USER        = CREDS.get("haproxy_stats_user", "admin")
HAPROXY_PASS        = CREDS.get("haproxy_stats_pass", "")
REDIS_PASS          = CREDS.get("redis_password", "")
AUTHENTIK_API_TOKEN = CREDS.get("authentik_api_token", "")
PG_USER             = CREDS.get("postgres_user", "postgres")
PG_PASS             = CREDS.get("postgres_password", "")
SENTINEL_NAME       = SENTINEL_CFG.get("master_name", "mymaster")

P_POSTGRES          = int(PORTS.get("postgres", 5432))
SLOT_WARN_BYTES     = 100 * 1024 * 1024   # 100 MB — yellow
SLOT_CRIT_BYTES     = 1024 ** 3           # 1 GB  — red


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def check_keepalived_node(node: dict) -> dict:
    """
    Hit https://<node_ip>/monitor — returns JSON with node name.
    Also checks if this node currently holds the VIP by hitting https://<VIP>/monitor
    and comparing the returned node name.
    Infers nginx up/down and calculates effective priority.
    """
    ip   = node["ip"]
    name = node.get("name", ip)
    base = int(node.get("base_priority", 100))

    # Check nginx on this node directly
    nginx_up = False
    try:
        r = requests.get(f"https://{ip}/monitor",
                         timeout=HTTP_TIMEOUT, verify=False)
        nginx_up = r.status_code == 200
    except Exception:
        nginx_up = False

    effective_priority = base if nginx_up else base + TRACK_WEIGHT

    return {
        "ip": ip,
        "name": name,
        "nginx_up": nginx_up,
        "base_priority": base,
        "effective_priority": effective_priority,
    }


def check_vip_holder() -> dict:
    """
    Hit https://<VIP>/monitor — the node that responds is the current MASTER.
    Returns the node name/ip that holds the VIP, or None if VIP is unreachable.
    """
    try:
        r = requests.get(f"https://{VIP}/monitor",
                         timeout=HTTP_TIMEOUT, verify=False)
        if r.status_code == 200:
            data = r.json()
            return {
                "reachable": True,
                "holder_name": data.get("node", "?"),
                "holder_ip": data.get("ip", "?"),
            }
    except Exception:
        pass
    return {"reachable": False, "holder_name": None, "holder_ip": None}


def check_patroni_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = requests.get(f"http://{ip}:{P_PATRONI}/", timeout=HTTP_TIMEOUT)
        data = r.json()
        raw_role = data.get("role", "unknown")
        # Patroni 4.x uses "primary"; older versions used "master"
        is_leader = raw_role in ("primary", "master", "standby_leader")
        role = "primary" if is_leader else "replica"
        tl = data.get("timeline")
        tl_str = str(tl) if tl is not None else "—"
        # replication_state is top-level on replicas in newer Patroni
        repl_state = data.get("replication_state", "")
        state = repl_state if repl_state else data.get("state", "unknown")
        # Replication lag: only meaningful on replicas
        lag_bytes = None
        if not is_leader:
            xlog = data.get("xlog", {})
            received = xlog.get("received_location")
            replayed = xlog.get("replayed_location")
            if received is not None and replayed is not None:
                lag_bytes = max(0, received - replayed)
        # A leader should be "running"; a replica should be "streaming".
        # Anything else (e.g. stuck "starting" after a timeline mismatch /
        # crash loop) is a real problem even though Patroni still answers.
        healthy = (
            (is_leader and state == "running")
            or (not is_leader and state == "streaming")
        )
        return {
            "ip": ip,
            "name": node.get("name", ip),
            "ok": True,
            "role": role,
            "state": state,
            "healthy": healthy,
            "timeline": tl_str,
            "pending_restart": data.get("pending_restart", False),
            "lag_bytes": lag_bytes,
        }
    except Exception:
        return {
            "ip": ip, "name": node.get("name", ip),
            "ok": False, "role": "down", "state": "unreachable",
            "healthy": False,
            "timeline": "—", "pending_restart": False,
            "lag_bytes": None,
        }


def check_patroni_history(ip: str) -> dict:
    """
    Fetch /history from the primary and return the most recent timeline switch.
    Returns {} if the endpoint is unreachable or history is empty.
    """
    try:
        r = requests.get(f"http://{ip}:{P_PATRONI}/history", timeout=HTTP_TIMEOUT)
        entries = r.json()
        if not entries:
            return {}
        last = entries[-1]
        # Patroni format: [timeline_id, switchpoint_lsn, reason, timestamp?]
        return {
            "timeline": last[0] if len(last) > 0 else "?",
            "reason":    last[2] if len(last) > 2 else "unknown",
            "timestamp": last[3] if len(last) > 3 else None,
        }
    except Exception:
        return {}


def check_replication_slots(ip: str) -> Optional[list]:
    """
    Query pg_replication_slots on the primary for slot name, type, active status,
    and WAL lag. Returns None when credentials are missing or psycopg2 unavailable.
    """
    if not PG_PASS or not _HAS_PSYCOPG2:
        return None
    try:
        conn = psycopg2.connect(
            host=ip, port=P_POSTGRES,
            user=PG_USER, password=PG_PASS,
            dbname="postgres",
            connect_timeout=HTTP_TIMEOUT,
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT slot_name, slot_type, active,
                CASE
                    WHEN slot_type = 'logical'
                        THEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                    ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                END AS lag_bytes
            FROM pg_replication_slots
            ORDER BY lag_bytes DESC NULLS LAST
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "name":      row[0],
                "type":      row[1],
                "active":    row[2],
                "lag_bytes": int(row[3]) if row[3] is not None else 0,
            }
            for row in rows
        ]
    except Exception:
        return None


def check_etcd_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = requests.get(f"http://{ip}:{P_ETCD}/health", timeout=HTTP_TIMEOUT)
        healthy = r.json().get("health") in (True, "true")
    except Exception:
        return {"ip": ip, "name": node.get("name", ip),
                "ok": False, "leader": False, "raft_term": "?", "db_kb": 0}
    # etcd 3.5+ v3 API — POST /v3/maintenance/status
    # member_id == leader means this node is the leader
    is_leader = False
    raft_term = "?"
    db_kb = 0
    try:
        r2 = requests.post(
            f"http://{ip}:{P_ETCD}/v3/maintenance/status",
            json={}, timeout=HTTP_TIMEOUT,
        )
        d2 = r2.json()
        member_id = d2.get("header", {}).get("member_id", "")
        leader_id = d2.get("leader", "")
        is_leader = bool(member_id and leader_id and member_id == leader_id)
        raft_term = d2.get("raftTerm", "?")
        db_kb     = int(d2.get("dbSizeInUse", 0)) // 1024
    except Exception:
        pass
    return {
        "ip": ip, "name": node.get("name", ip),
        "ok": healthy, "leader": is_leader,
        "raft_term": raft_term, "db_kb": db_kb,
    }


def check_haproxy_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = requests.get(
            f"http://{ip}:{P_HAPROXY}/stats;csv",
            auth=(HAPROXY_USER, HAPROXY_PASS),
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            return {"ip": ip, "name": node.get("name", ip), "ok": False, "backends": {}}

        backends      = {}
        backend_stats = {}
        for line in r.text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split(",")
            if len(parts) < 18:
                continue
            pxname, svname, status = parts[0], parts[1], parts[17]
            if svname == "FRONTEND":
                continue
            if svname == "BACKEND":
                def _int(col):
                    return int(parts[col]) if len(parts) > col and parts[col].strip().isdigit() else 0
                backend_stats[pxname] = {"rate": _int(33), "errs": _int(43)}
                continue
            backends.setdefault(pxname, []).append({"server": svname, "status": status})

        return {"ip": ip, "name": node.get("name", ip), "ok": True, "backends": backends, "backend_stats": backend_stats}
    except Exception:
        return {"ip": ip, "name": node.get("name", ip), "ok": False, "backends": {}}


def check_redis_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = redis_lib.Redis(
            host=ip, port=P_REDIS,
            password=REDIS_PASS or None,
            socket_timeout=REDIS_TIMEOUT,
            socket_connect_timeout=REDIS_TIMEOUT,
        )
        info_repl = r.info("replication")
        info_mem  = r.info("memory")
        r.close()
        result = {
            "ip": ip, "name": node.get("name", ip), "ok": True,
            "role": info_repl.get("role", "unknown"),
            "connected_slaves": info_repl.get("connected_slaves", 0),
            "master_host": info_repl.get("master_host"),
            "master_link_status": info_repl.get("master_link_status"),
            "mem_used": info_mem.get("used_memory", 0),
            "mem_max":  info_mem.get("maxmemory", 0),
        }
        if info_repl.get("role") == "master":
            master_offset = info_repl.get("master_repl_offset", 0)
            replica_lags: dict = {}
            for key, val in info_repl.items():
                if not key.startswith("slave"):
                    continue
                if isinstance(val, dict):
                    slave_ip = val.get("ip")
                    slave_offset = int(val.get("offset", master_offset))
                elif isinstance(val, str):
                    parts = dict(p.split("=") for p in val.split(",") if "=" in p)
                    slave_ip = parts.get("ip")
                    slave_offset = int(parts.get("offset", master_offset))
                else:
                    continue
                if slave_ip:
                    replica_lags[slave_ip] = max(0, master_offset - slave_offset)
            result["replica_lags"] = replica_lags
        return result
    except Exception:
        return {
            "ip": ip, "name": node.get("name", ip), "ok": False,
            "role": "down", "connected_slaves": 0,
            "master_host": None, "master_link_status": None,
            "mem_used": 0, "mem_max": 0,
        }


def check_sentinel_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        s = redis_lib.Redis(
            host=ip, port=P_SENTINEL,
            socket_timeout=REDIS_TIMEOUT,
            socket_connect_timeout=REDIS_TIMEOUT,
        )
        masters = s.execute_command("SENTINEL", "masters")
        s.close()
        if masters:
            m = masters[0]
            if isinstance(m, list):
                d = {}
                for i in range(0, len(m) - 1, 2):
                    k = m[i].decode() if isinstance(m[i], bytes) else m[i]
                    v = m[i+1].decode() if isinstance(m[i+1], bytes) else m[i+1]
                    d[k] = v
                flags = d.get("flags", "")
                ok = not any(f in flags for f in ("s_down", "o_down", "disconnected"))
                if "o_down" in flags:
                    master_status, status_color = "odown", "bold red"
                elif "s_down" in flags:
                    master_status, status_color = "sdown", "yellow"
                elif "disconnected" in flags:
                    master_status, status_color = "disconnected", "yellow"
                else:
                    master_status, status_color = "ok", "green"
                return {
                    "ip": ip, "name": node.get("name", ip),
                    "ok": True, "sentinel_ok": ok,
                    "master_ip": d.get("ip", "?"),
                    "master_port": d.get("port", "?"),
                    "num_slaves": d.get("num-slaves", "?"),
                    "num_sentinels": d.get("num-other-sentinels", "?"),
                    "master_status": master_status,
                    "status_color": status_color,
                    "flags": flags,
                }
        return {
            "ip": ip, "name": node.get("name", ip),
            "ok": True, "sentinel_ok": True,
            "master_ip": "?", "master_port": "?",
            "num_slaves": "?", "num_sentinels": "?",
            "master_status": "ok", "status_color": "green", "flags": "",
        }
    except Exception:
        return {
            "ip": ip, "name": node.get("name", ip),
            "ok": False, "sentinel_ok": False,
            "master_ip": "?", "master_port": "?",
            "num_slaves": "?", "num_sentinels": "?",
            "master_status": "?", "status_color": "dim white", "flags": "",
        }


def check_authentik_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = requests.get(f"https://{ip}:{P_AUTHENTIK}/-/health/live/",
                         timeout=HTTP_TIMEOUT, verify=False)
        server_ok = r.status_code in (200, 204)
    except Exception:
        server_ok = False
    return {"ip": ip, "name": node.get("name", ip),
            "server_ok": server_ok}


def check_authentik_task_queue() -> dict:
    """
    Poll /api/v3/tasks/tasks/status/ for background task health.
    Rejected/errored tasks (email, outpost sync, policy cache…) don't affect
    /health/ready but cause silent UX failures.
    """
    if not AUTHENTIK_API_TOKEN:
        return {"ok": None, "error": "no token configured"}
    try:
        r = requests.get(
            f"https://{VIP}:{P_AUTHENTIK}/api/v3/tasks/tasks/status/",
            headers={"Authorization": f"Bearer {AUTHENTIK_API_TOKEN}"},
            timeout=HTTP_TIMEOUT,
            verify=False,
        )
        if r.status_code in (401, 403):
            return {"ok": False, "error": "unauthorized — token needs superuser permissions"}
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        d = r.json()
        return {
            "ok":        True,
            "queued":    d.get("queued",    0),
            "running":   d.get("running",   0),
            "rejected":  d.get("rejected",  0),
            "error":     d.get("error",     0),
            "warning":   d.get("warning",   0),
            "done":      d.get("done",      0),
            "consumed":  d.get("consumed",  0),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def check_authentik_workers() -> dict:
    """
    /api/v3/tasks/workers/ — workers currently connected via the broker
    heartbeat. Same number as the admin 'Workers' widget, and the ONLY
    reliable signal a worker is actually consuming tasks. The :9080
    /-/health/live/ probe is the Rust/axum liveness server and stays 200
    even when the dramatiq consumer is dead — never judge worker health from it.

    worker_id format is "<uuid>@<hostname>", so we map each connection back
    to its node and flag any node with zero connected workers.
    """
    expected = [n.get("name", n["ip"]) for n in AK_NODES]
    if not AUTHENTIK_API_TOKEN:
        return {"ok": None, "error": "no API token configured",
                "count": 0, "expected": len(expected),
                "present": [], "missing": expected, "mismatched": []}
    try:
        r = requests.get(
            f"https://{VIP}:{P_AUTHENTIK}/api/v3/tasks/workers/",
            headers={"Authorization": f"Bearer {AUTHENTIK_API_TOKEN}"},
            timeout=HTTP_TIMEOUT, verify=False,
        )
        if r.status_code in (401, 403):
            return {"ok": False, "error": "unauthorized — token needs admin perms",
                    "count": 0, "expected": len(expected),
                    "present": [], "missing": expected, "mismatched": []}
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}",
                    "count": 0, "expected": len(expected),
                    "present": [], "missing": expected, "mismatched": []}

        workers = r.json() if isinstance(r.json(), list) else []
        present, mismatched = [], []
        for w in workers:
            wid  = w.get("worker_id", "")
            node = wid.split("@", 1)[1] if "@" in wid else wid
            present.append(node)
            if w.get("version_matching") is False:
                mismatched.append(node)

        missing = [n for n in expected if n not in present]
        return {
            "ok": True, "error": None,
            "count": len(workers), "expected": len(expected),
            "present": present, "missing": missing, "mismatched": mismatched,
        }
    except Exception as e:
        return {"ok": False, "error": str(e),
                "count": 0, "expected": len(expected),
                "present": [], "missing": expected, "mismatched": []}

def check_nginx_status(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = requests.get(f"http://{ip}:{P_NGINX_STATUS}/nginx_status",
                         timeout=2)
        if r.status_code != 200:
            raise ValueError(f"status {r.status_code}")
        text    = r.text
        active  = int(re.search(r"Active connections:\s+(\d+)", text).group(1))
        reading = int(re.search(r"Reading:\s+(\d+)",            text).group(1))
        writing = int(re.search(r"Writing:\s+(\d+)",            text).group(1))
        waiting = int(re.search(r"Waiting:\s+(\d+)",            text).group(1))
        return {
            "ip": ip, "name": node.get("name", ip), "ok": True,
            "active": active, "reading": reading,
            "writing": writing, "waiting": waiting,
        }
    except Exception:
        return {"ip": ip, "name": node.get("name", ip), "ok": False,
                "active": 0, "reading": 0, "writing": 0, "waiting": 0}


# ---------------------------------------------------------------------------
# Dot indicators
# ---------------------------------------------------------------------------

def _terminal_supports_unicode() -> bool:
    # Config override takes priority
    if CFG.get("unicode_bullets") is not None:
        return bool(CFG["unicode_bullets"])
    # Python 3.7+ defaults sys.stdout.encoding to utf-8 regardless of locale,
    # so check the locale env vars which reflect the actual terminal capability.
    import locale
    for var in ("LC_ALL", "LC_CTYPE", "LANG"):
        val = os.environ.get(var, "")
        if val and "utf" in val.lower():
            return True
    try:
        if "utf" in (locale.getpreferredencoding(False) or "").lower():
            return True
    except Exception:
        pass
    return False

_UNICODE = _terminal_supports_unicode()
_BULLET = "●" if _UNICODE else "*"

OK   = f"[bold green]{_BULLET}[/]"
DOWN = f"[bold red]{_BULLET}[/]"
WARN = f"[bold yellow]{_BULLET}[/]"
GREY = f"[dim white]{_BULLET}[/]"


def failures_to_dot(failures: int) -> str:
    """Return a coloured dot based on how many nodes have failed (out of 3)."""
    if failures >= 3:
        return DOWN
    elif failures >= 2:
        return WARN
    return OK


# ---------------------------------------------------------------------------
# Panels
# ---------------------------------------------------------------------------

class KeepalivedPanel(Static):
    data: reactive[dict] = reactive({})

    def render_content(self) -> str:
        d = self.data
        if not d:
            return "  [dim]Checking...[/]"

        vip_d    = d.get("vip", {})
        nodes    = d.get("nodes", [])
        reachable = vip_d.get("reachable", False)
        holder_name = vip_d.get("holder_name")

        vip_str = (
            f"{OK} VIP [bold cyan]{VIP}[/]  →  MASTER: [bold green]{holder_name}[/]"
            if reachable
            else f"{DOWN} VIP [bold cyan]{VIP}[/]  →  [bold red]UNREACHABLE[/]"
        )

        lines = [f"  {vip_str}", ""]

        for node in nodes:
            name     = node["name"]
            nginx_up = node["nginx_up"]
            base     = node["base_priority"]
            eff      = node["effective_priority"]
            is_master = reachable and holder_name == name

            if is_master:
                state_str = "[bold green]MASTER[/]"
                d_dot = OK
                name_fmt = f"[bold green]{name:<14}[/]"
            elif nginx_up:
                state_str = "[dim white]BACKUP[/]"
                d_dot = GREY
                name_fmt = f"[dim white]{name:<14}[/]"
            else:
                state_str = "[bold red]FAULT[/]"
                d_dot = DOWN
                name_fmt = f"[bold red]{name:<14}[/]"

            prio_str = f"priority=[cyan]{eff}[/]"
            if not nginx_up:
                prio_str += f" [dim white](base {base} {TRACK_WEIGHT})[/]"

            lines.append(f"  {d_dot} {name_fmt} {state_str}  {prio_str}")

        return "\n".join(lines)

    def watch_data(self, data: dict) -> None:
        self.update(self.render_content())


_MB = 1024 * 1024

PATRONI_LAG_WARN = 1   * _MB   # yellow ≥ 1 MB
PATRONI_LAG_CRIT = 100 * _MB   # red    ≥ 100 MB
REDIS_LAG_WARN   = 1   * _MB   # yellow ≥ 1 MB
REDIS_LAG_CRIT   = 10  * _MB   # red    ≥ 10 MB


def _fmt_lag(lag_bytes: Optional[int],
             warn_bytes: int = PATRONI_LAG_WARN,
             crit_bytes: int = PATRONI_LAG_CRIT) -> str:
    """Format replication lag bytes into a coloured Rich string."""
    if lag_bytes is None:
        return ""
    if lag_bytes < 1024:
        val_str = f"{lag_bytes}B"
    elif lag_bytes < _MB:
        val_str = f"{lag_bytes // 1024}KB"
    else:
        val_str = f"{lag_bytes // _MB}MB"

    if lag_bytes >= crit_bytes:
        color = "bold red"
    elif lag_bytes >= warn_bytes:
        color = "yellow"
    else:
        color = "cyan"
    return f"  lag=[{color}]{val_str}[/]"


def _fmt_mem_bar(used: int, maxmem: int, width: int = 10) -> str:
    """Format Redis memory usage as a coloured % bar."""
    if maxmem <= 0:
        if used < 1024 * 1024:
            human = f"{used // 1024}KB"
        elif used < 1024 ** 3:
            human = f"{used // (1024 * 1024)}MB"
        else:
            human = f"{used / (1024 ** 3):.1f}GB"
        return f"  mem=[cyan]{human}[/][dim]/∞[/]" if _UNICODE else f"  mem=[cyan]{human}[/][dim]/no-limit[/]"
    pct    = min(100, used * 100 // maxmem)
    filled = pct * width // 100
    bar    = ("█" * filled + "░" * (width - filled)) if _UNICODE else ("=" * filled + "-" * (width - filled))
    color  = "bold red" if pct >= 90 else ("yellow" if pct >= 70 else "green")
    return f"  mem=[{color}]{pct}%[/] [{color}]{bar}[/]"


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n // 1024}KB"
    if n < 1024 ** 3:
        return f"{n // (1024 * 1024)}MB"
    return f"{n / (1024 ** 3):.1f}GB"


class PatroniPanel(Static):
    data: reactive[dict] = reactive({})

    def render_content(self) -> str:
        d = self.data
        if not d or "nodes" not in d:
            return "  [dim]Checking...[/]"

        # Build slot suffix for the primary line (only warn/crit slots shown)
        slots = d.get("slots")
        slot_suffix = ""
        if slots:
            warn_parts = []
            for s in slots:
                lag  = s.get("lag_bytes", 0) or 0
                act  = s["active"]
                if not act and lag >= SLOT_CRIT_BYTES:
                    color = "bold red"
                elif not act or lag >= SLOT_WARN_BYTES:
                    color = "yellow"
                else:
                    continue
                warn_parts.append(
                    f"[{color}]{s['name']}[/]([{color}]{_fmt_bytes(lag)}[/])"
                )
            if warn_parts:
                slot_suffix = "  [dim]slots:[/] " + "  ".join(warn_parts)

        lines = []
        for node in d["nodes"]:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]UNREACHABLE[/]")
                continue
            role  = node["role"]
            state = node["state"]
            tl    = node["timeline"]
            pend  = " [yellow](restart pending)[/]" if node.get("pending_restart") else ""
            is_leader = role in ("primary", "master")
            healthy   = node.get("healthy", is_leader and state == "running")
            if is_leader:
                d_dot = OK if healthy else WARN
            else:
                d_dot = GREY if healthy else WARN
            role_str  = "[bold green]LEADER[/]" if is_leader else "[dim white]REPLICA[/]"
            nfmt      = f"[bold green]{name:<14}[/]" if is_leader else f"[dim white]{name:<14}[/]"
            lag_val   = node.get("lag_bytes")
            lag_str   = "" if is_leader else (_fmt_lag(lag_val, PATRONI_LAG_WARN, PATRONI_LAG_CRIT) if lag_val else "")
            stuck_str = "" if healthy else "  [bold yellow]⚠ stuck (not streaming)[/]"
            extra     = (slot_suffix + pend) if is_leader else pend
            lines.append(
                f"  {d_dot} {nfmt} {role_str}  "
                f"state=[cyan]{state}[/]  TL=[cyan]{tl}[/]{lag_str}{extra}{stuck_str}"
            )

        return "\n".join(lines)

    def watch_data(self, data: dict) -> None:
        self.update(self.render_content())


class EtcdPanel(Static):
    data: reactive[list] = reactive([])

    def render_content(self) -> str:
        if not self.data:
            return "  [dim]Checking...[/]"
        lines = []
        for node in self.data:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]UNREACHABLE[/]")
                continue
            term  = node.get("raft_term", "?")
            db_kb = node.get("db_kb", 0)
            if node["leader"]:
                lines.append(
                    f"  {OK} [bold green]{name:<14}[/] [bold green]LEADER[/]  "
                    f"term=[cyan]{term}[/]  db=[cyan]{db_kb}KB[/]"
                )
            else:
                lines.append(
                    f"  {GREY} [dim white]{name:<14}[/] [dim white]FOLLOWER[/]  "
                    f"term=[cyan]{term}[/]  db=[cyan]{db_kb}KB[/]"
                )
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())


class HAProxyPanel(Static):
    data: reactive[list] = reactive([])

    def render_content(self) -> str:
        if not self.data:
            return "  [dim]Checking...[/]"
        lines = []
        for node in self.data:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]STATS UNREACHABLE[/]")
                continue
            backends      = node.get("backends", {})
            backend_stats = node.get("backend_stats", {})
            any_zero      = False
            parts         = []
            for pxname, servers in backends.items():
                ups   = sum(1 for s in servers if s["status"] == "UP")
                total = len(servers)
                # 0/N is a real problem; partial (e.g. 1/3 on primary) is by design
                if ups == 0:
                    any_zero = True
                stats   = backend_stats.get(pxname, {})
                rate    = stats.get("rate", 0)
                errs    = stats.get("errs", 0)
                err_str = f"[red]5xx:{errs}[/]" if errs > 0 else "[dim]5xx:0[/]"
                # only name specific servers when the whole backend is dark
                if ups == 0:
                    down_servers = [s["server"] for s in servers if s["status"] != "UP"]
                    down_str = "".join(f" [red]↓{s}[/]" for s in down_servers)
                else:
                    down_str = ""
                parts.append(f"[cyan]{pxname}[/]: {ups}/{total}{down_str} {rate}/s {err_str}")
            summary = "  ".join(parts) if parts else "[dim]no backends[/]"
            d_dot   = DOWN if any_zero else OK
            color   = "red" if any_zero else "green"
            lines.append(f"  {d_dot} [bold {color}]{name:<14}[/] {summary}")
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())


class RedisPanel(Static):
    data: reactive[list] = reactive([])

    def render_content(self) -> str:
        if not self.data:
            return "  [dim]Checking...[/]"
        replica_lags: dict = {}
        for node in self.data:
            if node.get("role") == "master" and "replica_lags" in node:
                replica_lags = node["replica_lags"]
                break
        lines = []
        for node in self.data:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]UNREACHABLE[/]")
                continue
            if node["role"] == "master":
                slaves = node.get("connected_slaves", 0)
                mem_str = _fmt_mem_bar(node.get("mem_used", 0), node.get("mem_max", 0))
                lines.append(
                    f"  {OK} [bold green]{name:<14}[/] [bold green]MASTER[/]  "
                    f"slaves=[cyan]{slaves}[/]{mem_str}"
                )
            else:
                mhost = node.get("master_host", "?")
                mlink = node.get("master_link_status", "?")
                link_str = f"[green]{mlink}[/]" if mlink == "up" else f"[red]{mlink}[/]"
                lag = replica_lags.get(node["ip"])
                lag_str = _fmt_lag(lag, REDIS_LAG_WARN, REDIS_LAG_CRIT) if lag else ""
                lines.append(
                    f"  {GREY} [dim white]{name:<14}[/] [dim white]REPLICA[/]  "
                    f"master=[cyan]{mhost}[/]  link={link_str}{lag_str}"
                )
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())


class SentinelPanel(Static):
    data: reactive[list] = reactive([])

    def render_content(self) -> str:
        if not self.data:
            return "  [dim]Checking...[/]"
        lines = []
        for node in self.data:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]UNREACHABLE[/]")
                continue
            mstatus = node.get("master_status", "?")
            scolor  = node.get("status_color", "dim white")
            if mstatus == "odown":
                d_dot, ncolor = DOWN, "bold red"
            elif mstatus in ("sdown", "disconnected"):
                d_dot, ncolor = WARN, "yellow"
            else:
                d_dot, ncolor = OK, "bold green"
            mip        = node.get("master_ip", "?")
            mport      = node.get("master_port", "?")
            status_str = f"  status=[{scolor}]{mstatus}[/]" if mstatus != "ok" else ""
            lines.append(
                f"  {d_dot} [{ncolor}]{name:<14}[/] "
                f"master=[cyan]{mip}:{mport}[/]  "
                f"replicas=[cyan]{node.get('num_slaves','?')}[/]  "
                f"sentinels=[cyan]{node.get('num_sentinels','?')}[/]{status_str}"
            )
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())


class AuthentikPanel(Static):
    data: reactive[list] = reactive([])

    def render_content(self) -> str:
        if not self.data:
            return "  [dim]Checking...[/]"
        lines = []
        for node in self.data:
            name = node["name"]
            sv   = node.get("server_ok", False)
            sv_str   = f"{OK} [green]server[/]" if sv else f"{DOWN} [red]server[/]"
            overall = OK if sv else DOWN
            lines.append(f"  {overall} {name:<14} {sv_str}")
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())

class WorkersPanel(Static):
    data: reactive[dict] = reactive({})

    def render_content(self) -> str:
        d = self.data
        if not d:
            return "  [dim]Checking...[/]"
        if d.get("ok") is None:
            return f"  {GREY} [dim]No API token — set credentials.authentik_api_token[/]"
        if d.get("ok") is False:
            return f"  {DOWN} [bold red]ERROR:[/] {d.get('error','unknown')}"

        count, expected = d["count"], d["expected"]
        missing, mism   = d["missing"], d["mismatched"]
        if missing:
            dot, color, state = DOWN, "red", f"[bold red]{count}/{expected}[/] — missing: " + ", ".join(missing)
        elif mism:
            dot, color, state = WARN, "yellow", f"[yellow]{count}/{expected}[/] — version mismatch: " + ", ".join(mism)
        else:
            dot, color, state = OK, "green", f"[bold green]{count}/{expected} connected[/]"
        present = "  ".join(f"[cyan]{n}[/]" for n in d["present"]) or "[dim]none[/]"
        return f"  {dot} [{color}]workers[/] {state}\n    [dim]present:[/] {present}"

    def watch_data(self, data: dict) -> None:
        self.update(self.render_content())

class WorkerQueuePanel(Static):
    data: reactive[dict] = reactive({})

    def render_content(self) -> str:
        d = self.data
        if not d:
            return "  [dim]Checking...[/]"

        ok = d.get("ok")
        if ok is None:
            return f"  {GREY} [dim]No API token configured — set credentials.authentik_api_token in config.yml[/]"
        if ok is False:
            return f"  {DOWN} [bold red]ERROR:[/] {d.get('error', 'unknown')}"

        error    = d.get("error",    0)
        warning  = d.get("warning",  0)
        rejected = d.get("rejected", 0)
        running  = d.get("running",  0)
        queued   = d.get("queued",   0)
        done     = d.get("done",     0)

        if error:
            dot   = DOWN
            color = "red"
            state = f"[bold red]{error} error(s)[/]"
        elif rejected:
            dot   = WARN
            color = "yellow"
            state = f"[yellow]{rejected} rejected[/]"
        elif warning:
            dot   = WARN
            color = "yellow"
            state = f"[yellow]{warning} warning(s)[/]"
        else:
            dot   = OK
            color = "green"
            state = "[bold green]healthy[/]"

        parts = [f"  {dot} [{color}]worker tasks[/] {state}"]
        parts.append(
            f"    [dim]running=[/][cyan]{running}[/]  "
            f"[dim]queued=[/][cyan]{queued}[/]  "
            f"[dim]rejected=[/]"
            + (f"[yellow]{rejected}[/]" if rejected else f"[cyan]{rejected}[/]")
            + f"  [dim]done=[/][cyan]{done}[/]"
        )
        return "\n".join(parts)

    def watch_data(self, data: dict) -> None:
        self.update(self.render_content())


class NginxPanel(Static):
    data: reactive[list] = reactive([])

    def render_content(self) -> str:
        if not self.data:
            return "  [dim]Checking...[/]"
        lines = []
        ok_nodes     = [n for n in self.data if n["ok"]]
        max_active   = max((n["active"] for n in ok_nodes), default=0)
        for node in self.data:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]UNREACHABLE[/]")
                continue
            active  = node["active"]
            reading = node["reading"]
            writing = node["writing"]
            waiting = node["waiting"]
            busiest = max_active > 1 and active == max_active
            dot          = OK
            name_fmt     = f"[bold green]{name:<14}[/]" if busiest else f"{name:<14}"
            active_fmt   = f"[bold green]{active}[/]" if busiest else f"[bold cyan]{active}[/]"
            lines.append(
                f"  {dot} {name_fmt} "
                f"active={active_fmt}  "
                f"[dim]R=[/][cyan]{reading}[/]  "
                f"[dim]W=[/][cyan]{writing}[/]  "
                f"[dim]Wait=[/][cyan]{waiting}[/]"
            )
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())


class StatusBar(Static):
    last_refresh: reactive[str] = reactive("")
    status_dot:   reactive[str] = reactive(GREY)

    def render_content(self) -> str:
        ts = self.last_refresh or "—"
        return (
            f"  {self.status_dot}    "
            f"[dim]Last refresh: {ts}   "
            f"Auto-refresh: {REFRESH_INTERVAL}s   "
            f"Config: {CONFIG_PATH}[/]"
        )

    def watch_last_refresh(self, _: str) -> None:
        self.update(self.render_content())

    def watch_status_dot(self, _: str) -> None:
        self.update(self.render_content())


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------

CSS = """
Screen {
    background: #0d1117;
    color: #e6edf3;
}

#title {
    content-align: center middle;
    background: #161b22;
    color: #58a6ff;
    text-style: bold;
    height: 1;
    padding: 0 2;
}

#statusbar {
    height: 1;
    content-align: center middle;
    padding: 0 2;
    margin-bottom: 1;
}

.panel {
    border: solid #30363d;
    border-title-color: #58a6ff;
    border-title-style: bold;
    padding: 0 1;
    margin: 0 1 1 1;
    height: auto;
    background: #161b22;
    width: 1fr;
}

Footer {
    background: #161b22;
    color: #8b949e;
}
"""


class ClusterMonitor(App):
    CSS = CSS
    TITLE = SITE_NAME
    BINDINGS = [
        ("r", "refresh_now", "Refresh"),
        ("q", "quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        yield Static(f"  {GREY}  {SITE_NAME}", id="title")
        yield StatusBar(id="statusbar")

        yield KeepalivedPanel("  [dim]Checking...[/]",
                              id="panel-keepalived", classes="panel")
        yield NginxPanel("  [dim]Checking...[/]",
                         id="panel-nginx", classes="panel")

        yield AuthentikPanel("  [dim]Checking...[/]",
                             id="panel-authentik", classes="panel")
        yield WorkersPanel("  [dim]Checking...[/]",
                           id="panel-workers", classes="panel")
        yield WorkerQueuePanel("  [dim]Checking...[/]",
                               id="panel-worker-queue", classes="panel")

        yield HAProxyPanel("  [dim]Checking...[/]",
                           id="panel-haproxy", classes="panel")

        with Horizontal(id="row-mid"):
            yield PatroniPanel("  [dim]Checking...[/]",
                               id="panel-patroni", classes="panel")
            yield EtcdPanel("  [dim]Checking...[/]",
                            id="panel-etcd", classes="panel")

        with Horizontal(id="row-redis"):
            yield RedisPanel("  [dim]Checking...[/]",
                             id="panel-redis", classes="panel")
            yield SentinelPanel("  [dim]Checking...[/]",
                                id="panel-sentinel", classes="panel")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#panel-keepalived").border_title  = f" {GREY}  VIP / KEEPALIVED / NGINX  "
        self.query_one("#panel-nginx").border_title       = f" {GREY}  NGINX CONNECTIONS  "
        self.query_one("#panel-patroni").border_title     = f" {GREY}  POSTGRESQL / PATRONI  "
        self.query_one("#panel-etcd").border_title        = f" {GREY}  ETCD CLUSTER  "
        self.query_one("#panel-redis").border_title       = f" {GREY}  REDIS  "
        self.query_one("#panel-sentinel").border_title    = f" {GREY}  REDIS SENTINEL  "
        self.query_one("#panel-haproxy").border_title        = f" {GREY}  HAPROXY BACKENDS  "
        self.query_one("#panel-authentik").border_title     = f" {GREY}  AUTHENTIK BACKENDS  "
        self.query_one("#panel-worker-queue").border_title  = f" {GREY}  AUTHENTIK WORKER QUEUE  "
        self.query_one("#panel-workers").border_title       = f" {GREY}  AUTHENTIK WORKERS  "

        self.set_interval(REFRESH_INTERVAL, self.action_refresh_now)
        self.action_refresh_now()

    @work(thread=True)
    def action_refresh_now(self) -> None:
        with ThreadPoolExecutor(max_workers=32) as ex:
            f_vip          = ex.submit(check_vip_holder)
            f_ka           = [ex.submit(check_keepalived_node, n) for n in KA_NODES]
            f_patroni      = [ex.submit(check_patroni_node,   n) for n in PATRONI_NODES]
            f_etcd         = [ex.submit(check_etcd_node,      n) for n in ETCD_NODES]
            f_haproxy      = [ex.submit(check_haproxy_node,   n) for n in HAPROXY_NODES]
            f_redis        = [ex.submit(check_redis_node,     n) for n in REDIS_NODES]
            f_sentinel     = [ex.submit(check_sentinel_node,  n) for n in REDIS_NODES]
            f_authentik    = [ex.submit(check_authentik_node, n) for n in AK_NODES]
            f_nginx        = [ex.submit(check_nginx_status,   n) for n in KA_NODES]
            f_worker_queue = ex.submit(check_authentik_task_queue)
            f_workers      = ex.submit(check_authentik_workers)

            # Patroni results needed first to identify the primary for dependent queries
            patroni_data = [f.result() for f in f_patroni]
            primary = next(
                (n for n in patroni_data if n["ok"] and n["role"] in ("primary", "master")),
                None,
            )
            primary_ip = primary["ip"] if primary else None

            f_history = ex.submit(check_patroni_history,   primary_ip) if primary_ip else None
            f_slots   = ex.submit(check_replication_slots, primary_ip) if primary_ip else None

            vip_data          = f_vip.result()
            ka_data           = [f.result() for f in f_ka]
            etcd_data         = [f.result() for f in f_etcd]
            haproxy_data      = [f.result() for f in f_haproxy]
            redis_data        = [f.result() for f in f_redis]
            sentinel_data     = [f.result() for f in f_sentinel]
            authentik_data    = [f.result() for f in f_authentik]
            nginx_data        = [f.result() for f in f_nginx]
            workers_data      = f_workers.result()
            worker_queue_data = f_worker_queue.result()

            history_data = f_history.result() if f_history else {}
            slots_data   = f_slots.result()   if f_slots   else None

        ts = datetime.now().strftime("%H:%M:%S")

        self.call_from_thread(
            self._apply_updates,
            vip_data, ka_data,
            patroni_data, history_data, slots_data,
            etcd_data, haproxy_data,
            redis_data, sentinel_data, authentik_data,
            nginx_data, worker_queue_data, workers_data, ts,
        )

    def _apply_updates(
        self, vip_data, ka_data,
        patroni_data, history_data, slots_data,
        etcd_data, haproxy_data,
        redis_data, sentinel_data, authentik_data,
        nginx_data, worker_queue_data, workers_data, ts,
    ):
        self.query_one("#panel-keepalived", KeepalivedPanel).data = {
            "vip": vip_data, "nodes": ka_data
        }
        self.query_one("#panel-patroni", PatroniPanel).data = {
            "nodes":   patroni_data,
            "history": history_data,
            "slots":   slots_data,
        }
        self.query_one("#panel-etcd",     EtcdPanel).data     = etcd_data
        self.query_one("#panel-haproxy",  HAProxyPanel).data  = haproxy_data
        self.query_one("#panel-redis",    RedisPanel).data    = redis_data
        self.query_one("#panel-sentinel", SentinelPanel).data = sentinel_data
        self.query_one("#panel-authentik",   AuthentikPanel).data    = authentik_data
        self.query_one("#panel-nginx",       NginxPanel).data        = nginx_data
        self.query_one("#panel-worker-queue",WorkerQueuePanel).data  = worker_queue_data
        self.query_one("#panel-workers", WorkersPanel).data = workers_data

        # --- per-service failure counts (out of 3 nodes) ---
        ka_fail       = sum(1 for n in ka_data       if not n["nginx_up"])
        patroni_fail = sum(
            1 for n in patroni_data
            if not n["ok"] or not n.get("healthy", True)
        )
        etcd_fail     = sum(1 for n in etcd_data     if not n["ok"])
        haproxy_fail  = sum(1 for n in haproxy_data  if not n["ok"])
        redis_fail    = sum(1 for n in redis_data    if not n["ok"])
        sentinel_fail = sum(1 for n in sentinel_data if not n["ok"])
        authentik_fail = sum(
            1 for n in authentik_data if not (n["server_ok"])
        )
        wq = worker_queue_data
        wq_fail = (
            1 if wq.get("ok") is False
            else (1 if wq.get("error", 0) > 0 else 0)
        )
        workers_fail = 1 if workers_data.get("ok") is False or workers_data.get("missing") else 0

        all_failures = [
            ka_fail, patroni_fail, etcd_fail, haproxy_fail,
            redis_fail, sentinel_fail, authentik_fail, wq_fail, workers_fail
        ]

        # --- update border titles with per-service dot ---
        nginx_fail = sum(1 for n in nginx_data if not n["ok"])

        self.query_one("#panel-keepalived").border_title = (
            f" {failures_to_dot(ka_fail)}  VIP / KEEPALIVED / NGINX  "
        )
        self.query_one("#panel-nginx").border_title = (
            f" {failures_to_dot(nginx_fail)}  NGINX CONNECTIONS  "
        )
        cluster_tl = next(
            (n["timeline"] for n in patroni_data if n["ok"] and n["role"] in ("primary", "master")),
            None,
        )
        tl_suffix = f"  TL={cluster_tl}" if cluster_tl else ""
        if history_data:
            raw_ts = history_data.get("timestamp") or ""
            if raw_ts:
                try:
                    ts_display = datetime.fromisoformat(raw_ts).strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    ts_display = raw_ts.replace("T", " ")[:16]
            else:
                ts_display = "unknown time"
            hist_tl = history_data.get("timeline", "?")
            hist_reason = str(history_data.get("reason", "unknown"))[:40]
            failover_part = f"  (last failover TL {hist_tl}  {ts_display}  →  {hist_reason})"
        else:
            failover_part = ""
        self.query_one("#panel-patroni").border_title = (
            f" {failures_to_dot(patroni_fail)}  POSTGRESQL / PATRONI{tl_suffix}{failover_part}  "
        )
        self.query_one("#panel-etcd").border_title = (
            f" {failures_to_dot(etcd_fail)}  ETCD CLUSTER  "
        )
        self.query_one("#panel-redis").border_title = (
            f" {failures_to_dot(redis_fail)}  REDIS  "
        )
        self.query_one("#panel-sentinel").border_title = (
            f" {failures_to_dot(sentinel_fail)}  REDIS SENTINEL  "
        )
        self.query_one("#panel-haproxy").border_title = (
            f" {failures_to_dot(haproxy_fail)}  HAPROXY BACKENDS  "
        )
        self.query_one("#panel-authentik").border_title = (
            f" {failures_to_dot(authentik_fail)}  AUTHENTIK BACKENDS  "
        )
        wq_dot = (
            GREY if wq.get("ok") is None
            else DOWN if wq.get("ok") is False or wq.get("error", 0) > 0
            else WARN if wq.get("rejected", 0) > 0 or wq.get("warning", 0) > 0
            else OK
        )
        self.query_one("#panel-worker-queue").border_title = (
            f" {wq_dot}  AUTHENTIK WORKER QUEUE  "
        )

        wkr = workers_data
        workers_dot = (
            GREY if wkr.get("ok") is None
            else DOWN if wkr.get("ok") is False or wkr.get("missing")
            else WARN if wkr.get("mismatched")
            else OK
        )
        self.query_one("#panel-workers").border_title = (
            f" {workers_dot}  AUTHENTIK WORKERS  "
        )

        # --- central dot in title bar ---
        if any(f >= 3 for f in all_failures):
            central_dot = DOWN
        elif any(f >= 2 for f in all_failures):
            central_dot = WARN
        else:
            central_dot = OK
        self.query_one("#title").update(f"  {central_dot}  {SITE_NAME}")

        sb = self.query_one("#statusbar", StatusBar)
        sb.status_dot   = central_dot
        sb.last_refresh = ts


if __name__ == "__main__":
    ClusterMonitor().run()
