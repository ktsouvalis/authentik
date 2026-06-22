#!/usr/bin/env python3
"""
logs_viewer.py — Authentik HA cluster log viewer (warnings + errors, last 24h)

TUI mode (default):
  Outer tabs: one per node.  Inner tabs: one per service on that node.

Save mode (--save <file>):
  Fetches logs and writes a plain-text report; no TUI shown.

Usage:
  conda run -n authentik python3 logs_viewer.py [--config config.yml]
  conda run -n authentik python3 logs_viewer.py --save cluster_logs.txt
"""
import getpass
import os
import re
import sys
import argparse
import yaml
import paramiko
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from rich.markup import escape
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, TabbedContent, TabPane, RichLog
from textual import work

MAX_LINES = 500

CSS = """\
Screen { background: $surface; }
TabbedContent { height: 1fr; }
TabPane { padding: 0; }
RichLog {
    height: 1fr;
    margin: 0 1 1 1;
    border: round $primary-darken-2;
    background: $surface-darken-1;
    scrollbar-gutter: stable;
}
"""


def _slug(text):
    return re.sub(r"[^a-z0-9]", "-", text.lower())


def _node_tab_id(node_name):
    return f"ntab-{_slug(node_name)}"


def _svc_tab_id(node_name, label):
    return f"stab-{_slug(node_name)}-{_slug(label)}"


def _log_id(node_name, label):
    return f"log-{_slug(node_name)}-{_slug(label)}"


def get_nodes(config, key):
    if key == "keepalived":
        return config["keepalived"]["nodes"]
    return config["nodes"][key]


def load_services(config):
    """Return list of (label, nodes_key, src_type, identifier) from config['services']."""
    result = []
    for svc in config.get("services", []):
        identifier = svc.get("container") or svc.get("unit", "")
        result.append((svc["label"], svc["nodes"], svc["type"], identifier))
    return result


def build_node_map(config, services):
    """Return ordered dict: node_name -> {ip, services: [(label, src_type, identifier)]}"""
    node_map = {}
    for label, nodes_key, src_type, identifier in services:
        for node in get_nodes(config, nodes_key):
            name = node["name"]
            if name not in node_map:
                node_map[name] = {"ip": node["ip"], "services": []}
            node_map[name]["services"].append((label, src_type, identifier))
    return node_map


def docker_log_cmd(container, hours):
    return (
        f"docker logs --since {hours}h {container} 2>&1"
        f" | grep -iE '(WARN|WARNING|ERROR|CRITICAL|FATAL|CRIT)'"
        f" | tail -{MAX_LINES}"
    )


def systemd_log_cmd(unit, hours):
    return (
        f"journalctl -u {unit} --since '{hours} hours ago'"
        f" --no-pager -p warning -o short-iso | tail -{MAX_LINES}"
    )


def load_ssh_key(key_file):
    path = os.path.expanduser(key_file)
    for key_class in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
        try:
            return key_class.from_private_key_file(path)
        except paramiko.PasswordRequiredException:
            passphrase = getpass.getpass(f"SSH key passphrase for {key_file}: ")
            for kc in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
                try:
                    return kc.from_private_key_file(path, password=passphrase)
                except paramiko.SSHException:
                    continue
            raise RuntimeError(f"Could not load SSH key with the provided passphrase: {key_file}")
        except paramiko.SSHException:
            continue
    raise RuntimeError(f"Could not load SSH key: {key_file}")


def ssh_run(ip, user, pkey, cmd, timeout=30):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=user, pkey=pkey, timeout=10, auth_timeout=10)
    try:
        _, stdout, _ = client.exec_command(cmd, timeout=timeout)
        return stdout.read().decode("utf-8", errors="replace").strip()
    finally:
        client.close()


def colorize(line):
    safe = escape(line)
    ll = line.lower()
    if any(w in ll for w in ("error", "critical", "fatal", "crit")):
        return f"[bold red]{safe}[/bold red]"
    if "warn" in ll:
        return f"[yellow]{safe}[/yellow]"
    return safe


# ─── Save mode ───────────────────────────────────────────────────────────────

def run_save(config, services, filename, hours):
    node_map = build_node_map(config, services)
    ssh_user = config["ssh"]["username"]
    ssh_key = load_ssh_key(config["ssh"]["key_file"])

    tasks = [
        (node_name, info["ip"], label, src_type, identifier)
        for node_name, info in node_map.items()
        for label, src_type, identifier in info["services"]
    ]

    results: dict[tuple, tuple] = {}

    def fetch_one(task):
        node_name, ip, label, src_type, identifier = task
        cmd = docker_log_cmd(identifier, hours) if src_type == "docker" else systemd_log_cmd(identifier, hours)
        try:
            output = ssh_run(ip, ssh_user, ssh_key, cmd)
            return (node_name, label), output, None
        except Exception as exc:
            return (node_name, label), "", str(exc)

    total = len(tasks)
    print(f"Fetching logs from {len(node_map)} nodes ({total} service queries)…")
    with ThreadPoolExecutor(max_workers=16) as pool:
        for fut in as_completed(pool.submit(fetch_one, t) for t in tasks):
            key, output, error = fut.result()
            results[key] = (output, error)
            node_name, label = key
            if error:
                status = "SSH error"
            elif output:
                status = f"{len(output.splitlines())} lines"
            else:
                status = "clean"
            print(f"  [{len(results)}/{total}] {node_name} / {label}: {status}")

    with open(filename, "w") as f:
        f.write("Authentik HA Cluster — Log Report\n")
        f.write(f"Fetched:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Scope:    last {hours}h, warnings and errors only\n")
        f.write("=" * 80 + "\n")

        for node_name, info in node_map.items():
            f.write(f"\nNODE: {node_name}  ({info['ip']})\n")
            f.write("=" * 80 + "\n")
            for label, src_type, identifier in info["services"]:
                f.write(f"\n  SERVICE: {label}  [{src_type}: {identifier}]\n")
                f.write("  " + "─" * 60 + "\n")
                output, error = results.get((node_name, label), ("", "not fetched"))
                if error:
                    f.write(f"  SSH error: {error}\n")
                elif not output:
                    f.write("  (no warnings or errors in the last 24h)\n")
                else:
                    for line in output.splitlines():
                        f.write(f"  {line}\n")

    print(f"\nSaved → {filename}")


# ─── TUI mode ────────────────────────────────────────────────────────────────

class LogsApp(App):
    CSS = CSS
    TITLE = "Authentik HA – Logs (warn/error)"
    BINDINGS = [
        ("r", "refresh_logs", "Refresh"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, config, services, hours):
        super().__init__()
        self.config = config
        self.ssh_user = config["ssh"]["username"]
        self.ssh_key = load_ssh_key(config["ssh"]["key_file"])
        self._hours = hours
        self._node_map = build_node_map(config, services)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with TabbedContent():
            for node_name, info in self._node_map.items():
                with TabPane(node_name, id=_node_tab_id(node_name)):
                    with TabbedContent():
                        for label, *_ in info["services"]:
                            with TabPane(label, id=_svc_tab_id(node_name, label)):
                                yield RichLog(
                                    id=_log_id(node_name, label),
                                    markup=True,
                                    highlight=False,
                                    wrap=True,
                                )
        yield Footer()

    def on_mount(self):
        self.action_refresh_logs()

    def action_refresh_logs(self):
        for node_name, info in self._node_map.items():
            for label, *_ in info["services"]:
                rl = self.query_one(f"#{_log_id(node_name, label)}", RichLog)
                rl.clear()
                rl.write("[dim]Fetching…[/dim]")
        self._fetch_all()

    @work(thread=True, exclusive=True)
    def _fetch_all(self):
        tasks = [
            (node_name, info["ip"], label, src_type, identifier)
            for node_name, info in self._node_map.items()
            for label, src_type, identifier in info["services"]
        ]

        def fetch_one(task):
            node_name, ip, label, src_type, identifier = task
            cmd = docker_log_cmd(identifier, self._hours) if src_type == "docker" else systemd_log_cmd(identifier, self._hours)
            try:
                output = ssh_run(ip, self.ssh_user, self.ssh_key, cmd)
                return node_name, label, output, None
            except Exception as exc:
                return node_name, label, "", str(exc)

        with ThreadPoolExecutor(max_workers=16) as pool:
            for fut in as_completed(pool.submit(fetch_one, t) for t in tasks):
                node_name, label, output, error = fut.result()
                self.call_from_thread(self._write_service, node_name, label, output, error)

        self.call_from_thread(self._mark_done)

    def _write_service(self, node_name, label, output, error):
        rl = self.query_one(f"#{_log_id(node_name, label)}", RichLog)
        rl.clear()
        if error:
            rl.write(f"[bold red]SSH error: {escape(error)}[/bold red]")
        elif not output:
            rl.write("[dim italic](no warnings or errors in the last 24h)[/dim italic]")
        else:
            for line in output.splitlines():
                rl.write(colorize(line))

    def _mark_done(self):
        self.sub_title = f"Last fetched: {datetime.now().strftime('%H:%M:%S')}"


# ─── Entry point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Authentik HA cluster log viewer")
    parser.add_argument("--config", default="config.yml", help="Path to config file")
    parser.add_argument("--last", type=int, default=24, metavar="HOURS", help="Hours of logs to fetch (default: 24)")
    parser.add_argument("--save", metavar="FILE", help="Save logs to FILE as plain text (no TUI)")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    services = load_services(config)
    if not services:
        print("Error: no services defined in config. Add a 'services:' section.", file=sys.stderr)
        sys.exit(1)

    if args.save:
        save_path = args.save if args.save.endswith(".log") else args.save + ".log"
        run_save(config, services, save_path, args.last)
    else:
        LogsApp(config, services, args.last).run()


if __name__ == "__main__":
    main()
