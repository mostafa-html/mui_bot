#!/usr/bin/env python3
"""
Migration Script: Fix Mismatched Client Names in Invoices (PostgreSQL Version)
---------------------------------------------------------
This script fixes invoices where the stored client_name does not match
the actual email on the X-UI panel.

HOW TO USE:
1. DRY-RUN mode to see what will be fixed:
   python3 fix_client_names.py --dry-run

2. Run for real:
   python3 fix_client_names.py
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the correct Postgres database variable
try:
    from src.config import DATABASE_URL, XUI_HOST, XUI_USERNAME, XUI_PASSWORD
except ImportError as e:
    print(f"❌ Error: Could not import config variables. {e}")
    sys.exit(1)

import requests
try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("❌ Error: 'sqlalchemy' is required. Your container should already have it.")
    sys.exit(1)

def get_xui_session():
    """Login to X-UI and return session object."""
    session = requests.Session()
    login_url = f"{XUI_HOST}/login"
    payload = {"username": XUI_USERNAME, "password": XUI_PASSWORD}

    try:
        resp = session.post(login_url, data=payload, timeout=10)
        if resp.status_code == 200 and resp.json().get("success"):
            return session
        else:
            print(f"❌ X-UI Login failed: {resp.text}")
            return None
    except Exception as e:
        print(f"❌ Error connecting to X-UI: {e}")
        return None

def get_all_clients_from_panel(session):
    """Fetch all clients from X-UI panel."""
    if not session:
        return []

    url = f"{XUI_HOST}/xui/inbound/list"
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success"):
                clients = []
                for inbound in data.get("obj", []):
                    settings = inbound.get("settings", "")
                    if isinstance(settings, str):
                        import json
                        try:
                            settings = json.loads(settings)
                        except:
                            continue
                    
                    client_list = settings.get("clients", [])
                    for c in client_list:
                        email = c.get("email", "")
                        if email:
                            clients.append(email)
                return clients
        return []
    except Exception as e:
        print(f"❌ Error fetching clients: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="Fix mismatched client names in Postgres invoices.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed without applying changes")
    parser.add_argument("--force", action="store_true", help="Re-check and update even if already processed")
    args = parser.parse_args()

    print("🔍 Starting Client Name Migration (PostgreSQL)...")
    
    # Remove async driver prefix if present so we can use standard SQLAlchemy
    sync_db_url = DATABASE_URL.replace("+asyncpg", "")
    try:
        engine = create_engine(sync_db_url)
    except Exception as e:
        print(f"❌ Failed to initialize database engine: {e}")
        sys.exit(1)

    # Step 1: Get all COMPLETED invoices
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, user_id, client_name, status
                FROM invoices
                WHERE status = 'COMPLETED' AND client_name IS NOT NULL
            """))
            invoices = result.fetchall()
    except Exception as e:
        print(f"❌ Error querying database: {e}")
        sys.exit(1)

    print(f"📄 Found {len(invoices)} completed invoices with client names.")

    # Step 2: Get actual emails from X-UI Panel
    print("📡 Fetching current clients from X-UI panel...")
    session = get_xui_session()
    panel_emails = set()
    if session:
        panel_emails = set(get_all_clients_from_panel(session))
        print(f"✅ Found {len(panel_emails)} clients on X-UI panel.")
    else:
        print("❌ Could not connect to X-UI panel. Aborting.")
        sys.exit(1)

    fixed_count = 0
    error_count = 0
    no_change_count = 0

    for invoice in invoices:
        invoice_id = invoice[0]
        db_client_name = invoice[2]

        if db_client_name in panel_emails:
            if not args.force:
                no_change_count += 1
                continue

        potential_email = f"{db_client_name}_{invoice_id}"

        if potential_email in panel_emails:
            print(f"🔧 Mismatch found: Invoice #{invoice_id}")
            print(f"   DB Name: '{db_client_name}' (Not on panel)")
            print(f"   Panel Name: '{potential_email}' (Exists on panel)")

            if args.dry_run:
                print(f"   ⏭️  [DRY-RUN] Would update DB to: '{potential_email}'")
                fixed_count += 1
            else:
                try:
                    with engine.begin() as update_conn:
                        update_conn.execute(
                            text("UPDATE invoices SET client_name = :client_name WHERE id = :id"),
                            {"client_name": potential_email, "id": invoice_id}
                        )
                    print(f"   ✅ Updated DB to: '{potential_email}'")
                    fixed_count += 1
                except Exception as e:
                    print(f"   ❌ Error updating invoice {invoice_id}: {e}")
                    error_count += 1
        else:
            no_change_count += 1

    print("\n" + "="*40)
    print("📊 Migration Summary")
    print("="*40)
    if args.dry_run:
        print(f"ℹ️  DRY-RUN MODE: No changes were saved.")
        print(f"🔍 Invoices that WOULD be fixed: {fixed_count}")
    else:
        print(f"✅ Successfully fixed: {fixed_count} invoices")
        print(f"⚠️  Skipped/No Action: {no_change_count} invoices")

    if error_count > 0:
        print(f"❌ Errors encountered: {error_count}")
    print("="*40)

if __name__ == "__main__":
    main()
