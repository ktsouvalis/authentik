#!/usr/bin/env python3
"""
Authentik Mass Import — ESDA Lab Researchers
Creates users, sets name/username/email, creates 'Researchers' group, adds all users.

Usage:
    python3 authentik_import_researchers.py --url https://auth.esdalab.ece.uop.gr --token <api_token>
    python3 authentik_import_researchers.py --url https://auth.esdalab.ece.uop.gr --token <api_token> --dry-run
"""

import argparse
import json
import sys
import requests

# ---------------------------------------------------------------------------
# User data (to be replaced. users will be imported with csv in the future)
# ---------------------------------------------------------------------------


USERS = [
            # {"username": "k.tsouvalis", "name": "Tsouvalis Konstantinos", "email": "ktsouvalis@go.uop.gr"},
        ]

GROUP_NAME = " "

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    s.verify = False  # adjust if your cert is trusted
    return s


def get_or_create_group(session: requests.Session, base_url: str, dry_run: bool) -> str | None:
    """Returns group pk (str). Creates group if it doesn't exist."""
    r = session.get(f"{base_url}/api/v3/core/groups/", params={"name": GROUP_NAME})
    r.raise_for_status()
    results = r.json()["results"]

    if results:
        pk = results[0]["pk"]
        print(f"  [group] '{GROUP_NAME}' already exists (pk={pk})")
        return pk

    if dry_run:
        print(f"  [group] DRY-RUN: would create group '{GROUP_NAME}'")
        return "dry-run-group-pk"

    r = session.post(f"{base_url}/api/v3/core/groups/", json={"name": GROUP_NAME})
    r.raise_for_status()
    pk = r.json()["pk"]
    print(f"  [group] Created '{GROUP_NAME}' (pk={pk})")
    return pk


def user_exists(session: requests.Session, base_url: str, username: str) -> dict | None:
    r = session.get(f"{base_url}/api/v3/core/users/", params={"username": username})
    r.raise_for_status()
    results = r.json()["results"]
    return results[0] if results else None


def create_user(session: requests.Session, base_url: str, user: dict, dry_run: bool) -> str | None:
    """Returns user pk (str) or None on failure."""
    username = user["username"]
    payload = {
        "username": username,
        "name": user["name"],
        "is_active": True,
        "type": "internal",
    }
    if user["email"]:
        payload["email"] = user["email"]

    if dry_run:
        email_display = user["email"] or "(no email)"
        print(f"  [user]  DRY-RUN: would create {username} / {user['name']} / {email_display}")
        return "dry-run-user-pk"

    r = session.post(f"{base_url}/api/v3/core/users/", json=payload)
    if r.status_code == 400:
        print(f"  [user]  ERROR creating {username}: {r.text}")
        return None
    r.raise_for_status()
    pk = r.json()["pk"]
    email_display = user["email"] or "(no email)"
    print(f"  [user]  Created {username} / {user['name']} / {email_display} (pk={pk})")
    return pk


def add_user_to_group(session: requests.Session, base_url: str, group_pk: str, user_pk: str, username: str, dry_run: bool):
    if dry_run:
        print(f"  [group] DRY-RUN: would add {username} to '{GROUP_NAME}'")
        return

    r = session.post(
        f"{base_url}/api/v3/core/groups/{group_pk}/add_user/",
        json={"pk": int(user_pk)},
    )
    if r.status_code in (200, 204):
        print(f"  [group] Added {username} to '{GROUP_NAME}'")
    else:
        print(f"  [group] ERROR adding {username} to group: {r.status_code} {r.text}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Authentik mass import — ESDA Lab Researchers")
    parser.add_argument("--url",     required=True, help="Authentik base URL, e.g. https://auth.esdalab.ece.uop.gr")
    parser.add_argument("--token",   required=True, help="Authentik API token (Admin > System > API Tokens)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without making any changes")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")
    session = make_session(args.token)

    if args.dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    # Suppress InsecureRequestWarning if verify=False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 1. Get or create group
    print(f"[1/3] Resolving group '{GROUP_NAME}'...")
    group_pk = get_or_create_group(session, base_url, args.dry_run)
    if not group_pk:
        print("Failed to get/create group. Aborting.")
        sys.exit(1)

    # 2. Create users
    print(f"\n[2/3] Creating {len(USERS)} users...")
    created = []   # (pk, username)
    skipped = []   # username

    for user in USERS:
        username = user["username"]
        existing = user_exists(session, base_url, username)
        if existing:
            print(f"  [user]  SKIP {username} — already exists (pk={existing['pk']})")
            created.append((str(existing["pk"]), username))
            continue

        pk = create_user(session, base_url, user, args.dry_run)
        if pk:
            created.append((pk, username))
        else:
            skipped.append(username)

    # 3. Add all to group
    print(f"\n[3/3] Adding users to group '{GROUP_NAME}'...")
    for pk, username in created:
        add_user_to_group(session, base_url, group_pk, pk, username, args.dry_run)

    # Summary
    print(f"\n{'=== DRY RUN COMPLETE ===' if args.dry_run else '=== IMPORT COMPLETE ==='}")
    print(f"  Processed : {len(USERS)}")
    print(f"  Created   : {len([p for p, _ in created])}")
    print(f"  Skipped   : {len(skipped)}")
    if skipped:
        print(f"  Failed    : {', '.join(skipped)}")
    if args.dry_run:
        print("\nRe-run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()