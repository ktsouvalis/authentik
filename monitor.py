"""
UoP Authentik HA Cluster Monitor
---------------------------------
Real-time TUI dashboard for the full Authentik HA stack.
All connection details read from config.yml (or a path passed as first argument).

Usage:
    python monitor.py                        # uses config.yml in current dir
    python monitor.py config.site-b.yml     # use a specific config file
"""

import sys
import os
from datetime import datetime
from typing import Optional

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

# Credentials
HAPROXY_USER  = CREDS.get("haproxy_stats_user", "admin")
HAPROXY_PASS  = CREDS.get("haproxy_stats_pass", "")
REDIS_PASS    = CREDS.get("redis_password", "")
SENTINEL_NAME = SENTINEL_CFG.get("master_name", "mymaster")


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
        return {
            "ip": ip,
            "name": node.get("name", ip),
            "ok": True,
            "role": role,
            "state": state,
            "timeline": tl_str,
            "pending_restart": data.get("pending_restart", False),
        }
    except Exception:
        return {
            "ip": ip, "name": node.get("name", ip),
            "ok": False, "role": "down", "state": "unreachable",
            "timeline": "—", "pending_restart": False,
        }


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

        backends = {}
        for line in r.text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split(",")
            if len(parts) < 18:
                continue
            pxname, svname, status = parts[0], parts[1], parts[17]
            if svname in ("FRONTEND", "BACKEND"):
                continue
            backends.setdefault(pxname, []).append({"server": svname, "status": status})

        return {"ip": ip, "name": node.get("name", ip), "ok": True, "backends": backends}
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
        info = r.info("replication")
        r.close()
        return {
            "ip": ip, "name": node.get("name", ip), "ok": True,
            "role": info.get("role", "unknown"),
            "connected_slaves": info.get("connected_slaves", 0),
            "master_host": info.get("master_host"),
            "master_link_status": info.get("master_link_status"),
        }
    except Exception:
        return {
            "ip": ip, "name": node.get("name", ip), "ok": False,
            "role": "down", "connected_slaves": 0,
            "master_host": None, "master_link_status": None,
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
                return {
                    "ip": ip, "name": node.get("name", ip),
                    "ok": True, "sentinel_ok": ok,
                    "master_ip": d.get("ip", "?"),
                    "master_port": d.get("port", "?"),
                    "num_slaves": d.get("num-slaves", "?"),
                    "num_sentinels": d.get("num-other-sentinels", "?"),
                    "flags": flags,
                }
        return {
            "ip": ip, "name": node.get("name", ip),
            "ok": True, "sentinel_ok": True,
            "master_ip": "?", "master_port": "?",
            "num_slaves": "?", "num_sentinels": "?", "flags": "",
        }
    except Exception:
        return {
            "ip": ip, "name": node.get("name", ip),
            "ok": False, "sentinel_ok": False,
            "master_ip": "?", "master_port": "?",
            "num_slaves": "?", "num_sentinels": "?", "flags": "",
        }


def check_authentik_node(node: dict) -> dict:
    ip = node["ip"]
    try:
        r = requests.get(f"https://{ip}:{P_AUTHENTIK}/-/health/live/",
                         timeout=HTTP_TIMEOUT, verify=False)
        server_ok = r.status_code in (200, 204)
    except Exception:
        server_ok = False
    try:
        r2 = requests.get(f"https://{ip}:{P_AUTHENTIK}/-/health/ready/",
                          timeout=HTTP_TIMEOUT, verify=False)
        worker_ok = r2.status_code in (200, 204)
    except Exception:
        worker_ok = False
    return {"ip": ip, "name": node.get("name", ip),
            "server_ok": server_ok, "worker_ok": worker_ok}


# ---------------------------------------------------------------------------
# Dot indicators
# ---------------------------------------------------------------------------

OK   = "[bold green]●[/]"
DOWN = "[bold red]●[/]"
WARN = "[bold yellow]●[/]"
GREY = "[dim white]●[/]"


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


class PatroniPanel(Static):
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
            role  = node["role"]
            state = node["state"]
            tl    = node["timeline"]
            pend  = " [yellow](restart pending)[/]" if node.get("pending_restart") else ""
            is_leader = role in ("primary", "master")
            d_dot     = OK if is_leader else GREY
            role_str  = "[bold green]LEADER[/]" if is_leader else "[dim white]REPLICA[/]"
            nfmt      = f"[bold green]{name:<14}[/]" if is_leader else f"[dim white]{name:<14}[/]"
            lines.append(
                f"  {d_dot} {nfmt} {role_str}  "
                f"state=[cyan]{state}[/]  TL=[cyan]{tl}[/]{pend}"
            )
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
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
            backends  = node.get("backends", {})
            any_zero  = False
            parts     = []
            for pxname, servers in backends.items():
                ups   = sum(1 for s in servers if s["status"] == "UP")
                total = len(servers)
                # 0/N is a real problem; partial (e.g. 1/3 on primary) is by design
                if ups == 0:
                    any_zero = True
                parts.append(f"[cyan]{pxname}[/]: {ups}/{total}")
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
        lines = []
        for node in self.data:
            name = node["name"]
            if not node["ok"]:
                lines.append(f"  {DOWN} [bold red]{name:<14}[/] [red]UNREACHABLE[/]")
                continue
            if node["role"] == "master":
                slaves = node.get("connected_slaves", 0)
                lines.append(
                    f"  {OK} [bold green]{name:<14}[/] [bold green]MASTER[/]  "
                    f"slaves=[cyan]{slaves}[/]"
                )
            else:
                mhost = node.get("master_host", "?")
                mlink = node.get("master_link_status", "?")
                link_str = f"[green]{mlink}[/]" if mlink == "up" else f"[red]{mlink}[/]"
                lines.append(
                    f"  {GREY} [dim white]{name:<14}[/] [dim white]REPLICA[/]  "
                    f"master=[cyan]{mhost}[/]  link={link_str}"
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
            sok      = node.get("sentinel_ok", False)
            d_dot    = OK if sok else WARN
            color    = "green" if sok else "yellow"
            lines.append(
                f"  {d_dot} [bold {color}]{name:<14}[/] "
                f"master=[cyan]{node.get('master_ip','?')}[/]  "
                f"replicas=[cyan]{node.get('num_slaves','?')}[/]  "
                f"sentinels=[cyan]{node.get('num_sentinels','?')}[/]"
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
            wk   = node.get("worker_ok", False)
            sv_str   = f"{OK} [green]server[/]" if sv else f"{DOWN} [red]server[/]"
            wk_str   = f"{OK} [green]worker[/]" if wk else f"{DOWN} [red]worker[/]"
            overall  = OK if (sv and wk) else (WARN if (sv or wk) else DOWN)
            color    = "green" if (sv and wk) else ("yellow" if (sv or wk) else "red")
            lines.append(f"  {overall} [bold {color}]{name:<14}[/] {sv_str}   {wk_str}")
        return "\n".join(lines)

    def watch_data(self, data: list) -> None:
        self.update(self.render_content())


class StatusBar(Static):
    last_refresh: reactive[str]  = reactive("")
    overall_ok:   reactive[bool] = reactive(True)

    def render_content(self) -> str:
        ts = self.last_refresh or "—"
        if self.overall_ok:
            banner = "[bold green on dark_green]  ✔  ALL SYSTEMS OPERATIONAL  [/]"
        else:
            banner = "[bold white on red]  ✖  DEGRADED — CHECK PANELS BELOW  [/]"
        return (
            f"{banner}    "
            f"[dim]Last refresh: {ts}   "
            f"Auto-refresh: {REFRESH_INTERVAL}s   "
            f"Config: {CONFIG_PATH}[/]"
        )

    def watch_last_refresh(self, _: str) -> None:
        self.update(self.render_content())

    def watch_overall_ok(self, _: bool) -> None:
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
        yield Static(f"  ⬡  {SITE_NAME}", id="title")
        yield StatusBar(id="statusbar")

        yield KeepalivedPanel("  [dim]Checking...[/]",
                              id="panel-keepalived", classes="panel")

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

        yield HAProxyPanel("  [dim]Checking...[/]",
                           id="panel-haproxy", classes="panel")
        yield AuthentikPanel("  [dim]Checking...[/]",
                             id="panel-authentik", classes="panel")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#panel-keepalived").border_title  = "  VIP / KEEPALIVED / NGINX  "
        self.query_one("#panel-patroni").border_title     = "  POSTGRESQL / PATRONI  "
        self.query_one("#panel-etcd").border_title        = "  ETCD CLUSTER  "
        self.query_one("#panel-redis").border_title       = "  REDIS  "
        self.query_one("#panel-sentinel").border_title    = "  REDIS SENTINEL  "
        self.query_one("#panel-haproxy").border_title     = "  HAPROXY BACKENDS  "
        self.query_one("#panel-authentik").border_title   = "  AUTHENTIK BACKENDS  "

        self.set_interval(REFRESH_INTERVAL, self.action_refresh_now)
        self.action_refresh_now()

    @work(thread=True)
    def action_refresh_now(self) -> None:
        # Keepalived — VIP holder + per-node nginx/priority
        vip_data   = check_vip_holder()
        ka_data    = [check_keepalived_node(n) for n in KA_NODES]

        patroni_data   = [check_patroni_node(n)  for n in PATRONI_NODES]
        etcd_data      = [check_etcd_node(n)     for n in ETCD_NODES]
        haproxy_data   = [check_haproxy_node(n)  for n in HAPROXY_NODES]
        redis_data     = [check_redis_node(n)    for n in REDIS_NODES]
        sentinel_data  = [check_sentinel_node(n) for n in REDIS_NODES]
        authentik_data = [check_authentik_node(n) for n in AK_NODES]

        all_ok = (
            vip_data.get("reachable", False)
            and all(n["nginx_up"]                    for n in ka_data)
            and all(n["ok"]                          for n in patroni_data)
            and all(n["ok"]                          for n in etcd_data)
            and all(n["ok"]                          for n in haproxy_data)
            and all(n["ok"]                          for n in redis_data)
            and all(n["ok"] and n["sentinel_ok"]     for n in sentinel_data)
            and all(n["server_ok"] and n["worker_ok"] for n in authentik_data)
        )

        ts = datetime.now().strftime("%H:%M:%S")

        self.call_from_thread(
            self._apply_updates,
            vip_data, ka_data,
            patroni_data, etcd_data, haproxy_data,
            redis_data, sentinel_data, authentik_data,
            all_ok, ts,
        )

    def _apply_updates(
        self, vip_data, ka_data,
        patroni_data, etcd_data, haproxy_data,
        redis_data, sentinel_data, authentik_data,
        all_ok, ts,
    ):
        self.query_one("#panel-keepalived", KeepalivedPanel).data = {
            "vip": vip_data, "nodes": ka_data
        }
        self.query_one("#panel-patroni",  PatroniPanel).data  = patroni_data
        self.query_one("#panel-etcd",     EtcdPanel).data     = etcd_data
        self.query_one("#panel-haproxy",  HAProxyPanel).data  = haproxy_data
        self.query_one("#panel-redis",    RedisPanel).data    = redis_data
        self.query_one("#panel-sentinel", SentinelPanel).data = sentinel_data
        self.query_one("#panel-authentik",AuthentikPanel).data = authentik_data

        sb = self.query_one("#statusbar", StatusBar)
        sb.overall_ok    = all_ok
        sb.last_refresh  = ts


if __name__ == "__main__":
    ClusterMonitor().run()
