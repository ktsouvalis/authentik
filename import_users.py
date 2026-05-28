#!/usr/bin/env python3
"""Import users from a CSV file into Authentik.

Usage:
    python3 import_users.py users.csv
    python3 import_users.py users.csv --group "Lab Members"
    python3 import_users.py users.csv --dry-run
    python3 import_users.py users.csv --config config.site-b.yml
"""

import argparse
import csv
import re
import sys

import requests
import yaml


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def clean_email(raw: str) -> str:
    """Strip whitespace and fix double-@ typos."""
    return re.sub(r"@{2,}", "@", raw.strip())


def parse_csv(path: str) -> list[dict]:
    """Parse a CSV with columns #, Surname (or Surename), Name, email@<domain>.

    Skips rows with missing or malformed emails. Handles the email column
    regardless of what the header domain is (matches by @ presence).
    """
    users = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        email_col = next((c for c in reader.fieldnames or [] if "@" in c), None)
        if email_col is None:
            print("ERROR: could not find an email column in the CSV header", file=sys.stderr)
            sys.exit(1)

        for row in reader:
            raw_email = row.get(email_col, "").strip()
            if not raw_email:
                continue

            email = clean_email(raw_email)
            if email.count("@") != 1:
                print(f"  WARN  skipping malformed email: {raw_email!r}")
                continue

            surname = (row.get("Surname") or row.get("Surename") or "").strip()
            given = row.get("Name", "").strip()
            username = email.split("@")[0]

            users.append(
                {
                    "username": username,
                    "name": f"{given} {surname}".strip(),
                    "email": email,
                }
            )

    return users


class AuthentikClient:
    def __init__(self, url: str, token: str):
        self.base = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def get_group_pk(self, name: str) -> str | None:
        resp = self.session.get(f"{self.base}/api/v3/core/groups/", params={"name": name})
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return results[0]["pk"] if results else None

    def find_user(self, username: str) -> dict | None:
        resp = self.session.get(f"{self.base}/api/v3/core/users/", params={"username": username})
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return results[0] if results else None

    def update_user(self, pk: int, email: str) -> requests.Response:
        return self.session.patch(
            f"{self.base}/api/v3/core/users/{pk}/",
            json={"email": email},
        )

    def create_user(self, username: str, name: str, email: str) -> requests.Response:
        return self.session.post(
            f"{self.base}/api/v3/core/users/",
            json={
                "username": username,
                "name": name,
                "email": email,
                "is_active": True,
                "path": "users",
                "type": "internal",
            },
        )

    def add_to_group(self, group_pk: str, user_pk: int) -> None:
        resp = self.session.post(
            f"{self.base}/api/v3/core/groups/{group_pk}/add_user/",
            json={"pk": user_pk},
        )
        resp.raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import users from CSV into Authentik")
    parser.add_argument("csv", help="Path to the CSV file")
    parser.add_argument(
        "--config", default="config.yml", help="Config file path (default: config.yml)"
    )
    parser.add_argument(
        "--group", default=None, help="Authentik group name to add every user to"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be imported without making any changes",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)

    url = cfg.get("authentik", {}).get("url", "").rstrip("/")
    token = cfg.get("credentials", {}).get("authentik_api_token", "")

    if not url:
        print("ERROR: authentik.url is not set in config", file=sys.stderr)
        sys.exit(1)
    if not token:
        print("ERROR: credentials.authentik_api_token is not set in config", file=sys.stderr)
        sys.exit(1)

    users = parse_csv(args.csv)
    print(f"Parsed {len(users)} users from {args.csv}")

    client = AuthentikClient(url, token)

    group_pk: str | None = None
    if args.group:
        group_pk = client.get_group_pk(args.group)
        if group_pk is None:
            print(f"ERROR: group {args.group!r} not found in Authentik", file=sys.stderr)
            sys.exit(1)
        print(f"Target group: {args.group!r}  pk={group_pk}")

    created = updated = skipped = errors = 0

    for u in users:
        existing = client.find_user(u["username"])

        if existing:
            pk = existing["pk"]
            if existing.get("email", "").lower() != u["email"].lower():
                if args.dry_run:
                    print(f"  UPD   {u['username']!r} — would update email {existing.get('email')!r} → {u['email']!r}")
                    updated += 1
                else:
                    resp = client.update_user(pk, u["email"])
                    if resp.status_code == 200:
                        print(f"  UPD   {u['username']!r} — email updated (pk={pk})")
                        updated += 1
                    else:
                        print(f"  ERR   {u['username']!r} — update failed HTTP {resp.status_code}: {resp.text[:300]}")
                        errors += 1
            else:
                print(f"  SKIP  {u['username']!r} — no change (pk={pk})")
                skipped += 1
            if group_pk and not args.dry_run:
                try:
                    client.add_to_group(group_pk, pk)
                    print(f"        → added to group {args.group!r}")
                except requests.HTTPError as exc:
                    print(f"        → failed to add to group: {exc}")
            elif group_pk and args.dry_run:
                print(f"        → [dry-run] would add to group {args.group!r}")
            continue

        if args.dry_run:
            print(f"  NEW   {u['username']!r} — would be created  ({u['name']}, {u['email']})")
            if group_pk:
                print(f"        → [dry-run] would add to group {args.group!r}")
            created += 1
            continue

        resp = client.create_user(u["username"], u["name"], u["email"])
        if resp.status_code == 201:
            pk = resp.json().get("pk")
            print(f"  OK    {u['username']!r} — created (pk={pk})")
            if group_pk and pk:
                try:
                    client.add_to_group(group_pk, pk)
                    print(f"        → added to group {args.group!r}")
                except requests.HTTPError as exc:
                    print(f"        → failed to add to group: {exc}")
            created += 1
        else:
            print(f"  ERR   {u['username']!r} — HTTP {resp.status_code}: {resp.text[:300]}")
            errors += 1

    suffix = " (dry-run)" if args.dry_run else ""
    print(f"\nDone{suffix}: {created} created, {updated} updated, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
