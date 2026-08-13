#!/usr/bin/env python3
"""
Backfill Invoice.client_name so it matches the real email on the 3x-ui panel.

Why this is needed
------------------
Regular-user services are created on the panel as ``{name}_{invoice_id}`` (see
``provision_new`` in tasks.py), but for a long time the invoice's
``client_name`` was left as the bare ``{name}`` the user typed. "My Services"
(``my_plans_content`` in bot.py) only shows a service when the stored
``client_name`` exactly equals an email that exists on the panel — so those
services became **invisible** even though they work fine and the sub link was
delivered.

This is a thin CLI wrapper. The actual matching algorithm lives in
``src/services/reconcile.py`` (shared with the in-bot admin feature and the
per-user quick-fix). Here we just parse flags, print a human report, and call
into that module. It is scheme-agnostic: it works whether the panel suffix is
the invoice id (``Home_42``) or a timestamp (``Home_1782704904``).

Grouping matches how the bot assigns clients:
  * reseller services  -> panel group ``str(reseller_id)``
  * everyone else      -> panel group ``str(telegram_user_id)``

Safety
------
  * DRY RUN by default. Pass ``--apply`` to write changes.
  * Never points client_name at an email that does not exist on the panel
    (every proposed value is verified against the panel first).
  * Never appends a suffix to an already-suffixed name; accumulated names
    (``Name_id_id_id...`` from repeated redos) are normalized to ``Name_id``.
  * Skips invoices that already match; safe to re-run (idempotent).
  * Ambiguous cases are reported, never guessed.
  * ``--delete-orphans`` only removes panel clients that share an invoice's base
    name but are NOT any invoice's current service (and never an AMBIGUOUS
    candidate). It is opt-in and dry-run unless combined with ``--apply``.
  * Runs against whatever ``DATABASE_URL`` / ``PANEL_URL`` point to.

Usage
-----
  python backfill_client_names.py                  # dry run, all users
  python backfill_client_names.py --apply          # apply the client_name repairs
  python backfill_client_names.py --user 12345     # limit to one tg/reseller id
  python backfill_client_names.py --apply --verbose

  # Redos of stuck invoices left duplicate panel clients behind. List them:
  python backfill_client_names.py --delete-orphans            # dry run (prints calls)
  python backfill_client_names.py --delete-orphans --apply    # actually delete them

In Docker (code is baked into the image, so bind-mount the script):
  docker compose run --rm -v "$PWD/backfill_client_names.py:/app/backfill_client_names.py" \
      bot python backfill_client_names.py
  docker compose run --rm -v "$PWD/backfill_client_names.py:/app/backfill_client_names.py" \
      bot python backfill_client_names.py --apply
"""
import os

# Import-time side effects: database.py runs migrations unless this is set.
# A backfill should not alter schema, so opt out before importing it. This must
# happen before importing src.services.reconcile (which imports database).
os.environ.setdefault("SKIP_DB_INIT", "true")

from dotenv import load_dotenv
load_dotenv()  # so DATABASE_URL / PANEL_URL / REDIS_URL work when run directly

import argparse
import asyncio

from database import engine
from xui_client import XUIClient
from src.services.reconcile import compute_reconcile, to_plan, apply_plan


def parse_args():
    p = argparse.ArgumentParser(description="Repair invisible invoices by matching client_name to the panel email.")
    p.add_argument("--apply", action="store_true", help="Write changes (default: dry run).")
    p.add_argument("--user", type=str, default=None, help="Limit to a single telegram_user_id or reseller_id.")
    p.add_argument("--verbose", action="store_true", help="Also list skipped (already-correct) invoices.")
    p.add_argument("--delete-orphans", dest="delete_orphans", action="store_true",
                   help="Delete leftover panel clients from redos (dry run unless combined with --apply).")
    return p.parse_args()


async def main():
    args = parse_args()

    print("=" * 72)
    print("Backfill invoice.client_name -> real panel email")
    print(f"  Database : {engine.url.render_as_string(hide_password=True)}")
    print(f"  Panel    : {os.getenv('PANEL_URL')}")
    print(f"  Mode     : {'APPLY (writing changes)' if args.apply else 'DRY RUN (no changes)'}")
    if args.user:
        print(f"  Filter   : id == {args.user}")
    print("=" * 72)

    xui = XUIClient()
    try:
        result = await compute_reconcile(xui, args.user)
        records = result["records"]
        if not records:
            print("No COMPLETE client-bearing invoices found. Nothing to do.")
            return

        fixes = result["fixes"]
        ambiguous = result["ambiguous"]
        not_found = result["not_found"]
        skipped = result["skipped"]
        orphan_targets = result["orphans"]

        print(f"Scanning {len(records)} invoice(s) across {result['groups']} panel group(s)...\n")

        # ---- Report ----
        if fixes:
            print(f"── {len(fixes)} invoice(s) to REPAIR ──")
            for r in fixes:
                _, new_email, how = r["result"]
                print(f"  #{r['id']:>5}  group={r['gid']:<14} {r['name']!r} -> {new_email!r}  [{how}]")
            print()

        # Leftover panel clients from redos: not referenced by any invoice.
        if orphan_targets:
            print(f"── ⚠ {len(orphan_targets)} ORPHAN panel client(s) from redos ──")
            for o in orphan_targets:
                tag = f"   (leftover from #{o['src']})" if o["src"] else ""
                print(f"  xui.delete_client({o['email']!r}){tag}")
            print("\n  ^ these share an invoice's base name but are NOT any invoice's")
            print("    current service, so they are safe to delete. To execute:")
            flt = f" --user {args.user}" if args.user else ""
            print("    docker compose run --rm -v \"$PWD/backfill_client_names.py:/app/backfill_client_names.py\" \\")
            print(f"        bot python backfill_client_names.py --delete-orphans --apply{flt}")
            print()

        if ambiguous:
            print(f"── {len(ambiguous)} AMBIGUOUS (skipped — fix manually) ──")
            for r in ambiguous:
                print(f"  #{r['id']:>5}  group={r['gid']:<14} {r['name']!r} -> candidates: {r['result'][1]}")
            print()

        if not_found:
            print(f"── {len(not_found)} NOT ON PANEL (skipped — likely deleted) ──")
            for r in not_found:
                print(f"  #{r['id']:>5}  group={r['gid']:<14} {r['name']!r}")
            print()

        if args.verbose and skipped:
            print(f"── {len(skipped)} already correct ──")
            for r in skipped:
                print(f"  #{r['id']:>5}  {r['name']!r}")
            print()

        print("Summary: "
              f"{len(fixes)} to fix, {len(ambiguous)} ambiguous, "
              f"{len(not_found)} not-on-panel, {len(skipped)} already-correct, "
              f"{len(orphan_targets)} orphan client(s).")

        # ---- Apply name fixes ----
        if fixes and args.apply:
            n = await apply_plan(to_plan(fixes))
            print(f"\n✅ Updated {n} invoice(s).")
            print(f"🧹 Cleared cache for {len({r['gid'] for r in fixes})} group(s).")
        elif fixes:
            print("\nDRY RUN — no name changes written. Re-run with --apply to repair the above.")

        # ---- Delete orphan panel clients (opt-in) ----
        if args.delete_orphans:
            if not orphan_targets:
                print("\nNo orphan panel clients to delete.")
            elif args.apply:
                print(f"\nDeleting {len(orphan_targets)} orphan panel client(s)...")
                ok = 0
                for o in orphan_targets:
                    try:
                        await xui.delete_client(o["email"])
                        print(f"  🗑  deleted {o['email']!r}")
                        ok += 1
                    except Exception as ex:
                        print(f"  ❌ {o['email']!r}: {ex}")
                print(f"✅ Deleted {ok}/{len(orphan_targets)} orphan client(s).")
            else:
                print("\nDRY RUN — orphans listed above were NOT deleted. Add --apply to execute.")
    finally:
        await xui.close()


if __name__ == "__main__":
    asyncio.run(main())
