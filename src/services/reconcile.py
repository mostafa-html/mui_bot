"""
Shared reconcile logic: repair ``Invoice.client_name`` so it matches the real
3x-ui panel email, making services that were stored under a bare name visible
again in "My Services".

Why this is needed
------------------
Regular-user services are created on the panel as ``{name}_{invoice_id}`` (see
``provision_new`` in tasks.py), but for a long time the invoice's
``client_name`` was left as the bare ``{name}`` the user typed. ``my_plans_content``
(bot.py) only shows a service when the stored ``client_name`` exactly equals an
email that exists on the panel — so those services became **invisible** even
though they work fine and the sub link was delivered.

This module is the single source of truth for the reconcile algorithm. It is
imported by:
  * ``backfill_client_names.py`` — the CLI wrapper (dry-run / --apply / --user / --delete-orphans)
  * ``tasks.py``                 — the ``reconcile_client_names`` Celery task (admin "repair all" button)
  * ``bot.py``                   — the per-user quick-fix handler

It performs NO import-time side effects (no ``load_dotenv``, no ``SKIP_DB_INIT``):
the caller sets up the environment, exactly like ``src/services/reseller.py``. It
also never closes the ``XUIClient`` it is handed — the caller owns that lifecycle.
"""
import os
import logging
from collections import defaultdict

from sqlalchemy import inspect as sa_inspect, update

from database import SessionLocal, Invoice, engine

logger = logging.getLogger(__name__)

# Action types that provision a *personal* panel client (i.e. have a real email).
# Pack purchases / custom receipts / manual receipts never create a client, so
# there is nothing to match for them.
CLIENT_ACTION_TYPES = {
    "NEW", "RENEW", "TOPUP", "TRIAL", "REFERRAL_REWARD",
    "RESELLER_NEW", "RESELLER_RENEW", "RESELLER_TOPUP",
}


def load_records(user_filter=None):
    """Load COMPLETE, client-bearing invoices as plain dicts.

    Selects columns individually (and only reseller_id when the column exists)
    so the code also runs against older schema snapshots. ``user_filter`` limits
    to a single telegram_user_id or reseller_id (matched as a string).
    """
    has_reseller = "reseller_id" in {c["name"] for c in sa_inspect(engine).get_columns("invoices")}
    records = []
    with SessionLocal() as db:
        cols = [Invoice.id, Invoice.telegram_user_id, Invoice.client_name, Invoice.action_type]
        if has_reseller:
            cols.insert(2, Invoice.reseller_id)
        q = db.query(*cols).filter(
            Invoice.status == "COMPLETE",
            Invoice.client_name.isnot(None),
            Invoice.client_name != "",
        ).order_by(Invoice.id.asc())
        for row in q:
            if has_reseller:
                iid, uid, resid, name, act = row
            else:
                iid, uid, name, act = row
                resid = None
            if (act or "").upper() not in CLIENT_ACTION_TYPES:
                continue
            gid = str(resid) if resid else str(uid)
            if user_filter is not None and str(user_filter) not in (str(uid), str(resid or "")):
                continue
            records.append({"id": iid, "uid": uid, "resid": resid, "name": name, "act": act, "gid": gid})
    return records


def _canonical_base(name, invoice_id):
    """Recover the base name by stripping any trailing ``_{invoice_id}`` suffixes.

    The legacy redo bug (``provision_new`` before the idempotency fix) appended
    ``_{invoice_id}`` on every re-provision, so an invoice can be stored as
    ``Alikargar_528_528_528...``. The one correct panel email is the *single*
    suffix ``Alikargar_528``. Returns ``(base, depth)`` where ``depth`` is how
    many suffixes were stripped (0 = normal bare name, >=1 = accumulated).
    """
    suffix = f"_{invoice_id}"
    base, depth = name, 0
    while base.endswith(suffix) and len(base) > len(suffix):
        base = base[: -len(suffix)]
        depth += 1
    return base, depth


async def resolve_group(xui, gid, items, group_orphans=None):
    """Decide, for each invoice in one panel group, what its client_name should be.

    Sets item['result'] to one of:
      ('SKIP', email)              already correct (points at a real panel email)
      ('FIX', email, how)          repair proposal; how = 'invoice_id' | 'accumulated' | 'prefix'
      ('NOT_FOUND', None)          no matching email on the panel (deleted?)
      ('AMBIGUOUS', [candidates])  more than one possible email; needs a human

    Two passes: pass 1 resolves exact matches and the deterministic
    ``{base}_{id}`` reconstruction and *claims* those emails; pass 2 does prefix
    matching only against what pass 1 left free. Claiming first stops a bare-name
    invoice (e.g. #569 'Mehdi') from stealing another invoice's specific email
    (#620 -> 'Mehdi_620'). It also never appends a suffix to an accumulated name —
    it strips back to the base and targets the single-suffix canonical email.
    """
    try:
        panel_emails = await xui.get_group_emails(gid)
    except Exception as e:
        logger.warning(f"reconcile: could not fetch panel group {gid}: {e}")
        for it in items:
            it["result"] = ("NOT_FOUND", None)
        return

    panel_set = set(panel_emails)
    claimed = set()  # emails locked to an invoice during this run (one each)

    # ---- Pass 1: exact + deterministic single-suffix reconstruction.
    unresolved = []
    for it in items:
        name, iid = it["name"], it["id"]
        base, depth = _canonical_base(name, iid)
        it["_base"], it["_depth"] = base, depth

        # (a) Already points at a real panel email -> visible, leave it alone.
        if name in panel_set:
            it["result"] = ("SKIP", name)
            claimed.add(name)
            continue

        # (b) Canonical single-suffix email {base}_{id}. Handles both the normal
        #     bare-name case (depth 0) and accumulated names (depth >= 1) without
        #     ever appending another suffix.
        canonical = f"{base}_{iid}"
        if canonical in claimed:
            unresolved.append(it)
            continue
        hit = canonical in panel_set
        if not hit and (it.get("act") or "").upper() == "NEW":
            # Only provision_new uses the {base}_{id} scheme, so a network probe
            # is meaningful only for NEW. Skipping it for timestamp schemes avoids
            # wasting retry backoff on the many deleted trial/reward clients; the
            # pass-2 prefix match (free, from the group listing) still covers them.
            try:
                info = await xui.get_client_full(canonical)
                hit = info is not None and "client" in info
            except Exception:
                hit = False
        if hit:
            if canonical == name:
                # The probe confirms the current name is a real panel client
                # (just not group-assigned, so it wasn't in the listing above).
                # It already matches — visible via the get_client_full fallback.
                it["result"] = ("SKIP", canonical)
            else:
                how = "accumulated" if depth >= 1 else "invoice_id"
                it["result"] = ("FIX", canonical, how)
            claimed.add(canonical)
            continue

        unresolved.append(it)

    # ---- Pass 2: prefix match for anything left (timestamp scheme, odd suffixes).
    taken = set(claimed)
    for it in unresolved:
        base = it["_base"]
        cands = sorted(e for e in panel_emails
                       if e.startswith(base + "_") and e not in taken)
        if len(cands) == 1:
            it["result"] = ("FIX", cands[0], "prefix")
            taken.add(cands[0])
        elif not cands:
            it["result"] = ("NOT_FOUND", None)
        else:
            it["result"] = ("AMBIGUOUS", cands)

    # ---- Orphan detection (status-independent, so it still works on a second
    # run after names were applied). An orphan is a panel client that shares a
    # base with one of this group's invoices but is NOT any invoice's kept email
    # and NOT an unresolved AMBIGUOUS candidate. Panel groups are per-user, so a
    # leftover here can only belong to this group's invoices.
    if group_orphans is not None:
        protected = set()
        for it in items:
            res = it["result"]
            if res[0] in ("FIX", "SKIP"):
                protected.add(res[1])
            elif res[0] == "AMBIGUOUS":
                protected.update(res[1])
        bases = {it["_base"] for it in items}
        group_orphans[gid] = sorted(
            e for e in panel_emails
            if e not in protected and any(e.startswith(b + "_") for b in bases)
        )


async def compute_reconcile(xui, user_filter=None):
    """Run the full reconcile scan and return a structured result (no writes).

    Returns a dict::

        {
          "records":   [...],   # every scanned invoice dict (each carries ['result'])
          "groups":    int,     # number of panel groups scanned
          "fixes":     [...],   # invoices needing a client_name repair
          "ambiguous": [...],   # >1 candidate email; needs a human
          "not_found": [...],   # no matching email on the panel (deleted?)
          "skipped":   [...],   # already correct
          "orphans":   [ {"gid","email","src"}, ... ],  # leftover panel clients (CLI only)
        }

    Does not write anything and does not close ``xui`` (the caller owns it).
    """
    empty = {"records": [], "groups": 0, "fixes": [], "ambiguous": [],
             "not_found": [], "skipped": [], "orphans": []}
    records = load_records(user_filter)
    if not records:
        return empty

    groups = defaultdict(list)
    for r in records:
        groups[r["gid"]].append(r)

    group_orphans = {}
    for gid, items in groups.items():
        await resolve_group(xui, gid, items, group_orphans)

    fixes, ambiguous, not_found, skipped = [], [], [], []
    for r in records:
        kind = r["result"][0]
        (fixes if kind == "FIX" else
         ambiguous if kind == "AMBIGUOUS" else
         not_found if kind == "NOT_FOUND" else
         skipped).append(r)

    # Flatten orphans; attach a plausible source invoice (longest base match).
    orphan_targets = []
    for gid, emails in group_orphans.items():
        if not emails:
            continue
        grp_bases = sorted({(r["_base"], r["id"]) for r in records if r["gid"] == gid},
                           key=lambda t: len(t[0]), reverse=True)
        for e in emails:
            src = next((iid for b, iid in grp_bases if e.startswith(b + "_")), None)
            orphan_targets.append({"gid": gid, "email": e, "src": src})

    return {"records": records, "groups": len(groups), "fixes": fixes,
            "ambiguous": ambiguous, "not_found": not_found, "skipped": skipped,
            "orphans": orphan_targets}


def to_plan(fixes):
    """Convert the ``fixes`` list from compute_reconcile into a minimal,
    JSON-serializable apply plan: ``[{"id", "email", "gid"}, ...]``.

    Used to stash a dry-run result in Redis (so the admin's tap-to-apply button
    can apply it without re-scanning) and to feed :func:`apply_plan`.
    """
    return [{"id": r["id"], "email": r["result"][1], "gid": r["gid"]} for r in fixes]


async def apply_plan(plan):
    """Write ``client_name`` for each planned fix and clear the bot's per-user caches.

    ``plan`` is a list of ``{"id", "email", "gid"}`` dicts (see :func:`to_plan`).
    Returns the number of invoices updated.

    Clears BOTH ``user_emails:{gid}`` and ``service_status:{gid}`` so repaired
    services appear immediately — mirroring ``bot.invalidate_user_service_cache``.
    (Do NOT route this through ``tasks.invalidate_cache``, which drops only the
    ``user_emails`` key and would leave a stale ``service_status`` entry.)
    """
    if not plan:
        return 0

    with SessionLocal() as db:
        for item in plan:
            db.execute(update(Invoice).where(Invoice.id == item["id"]).values(client_name=item["email"]))
        db.commit()

    affected = {item["gid"] for item in plan}
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        try:
            import redis.asyncio as redis
            rc = redis.Redis.from_url(redis_url, decode_responses=True)
            try:
                for gid in affected:
                    await rc.delete(f"user_emails:{gid}")
                    await rc.delete(f"service_status:{gid}")
            finally:
                await rc.aclose()
        except Exception as e:
            logger.warning(f"reconcile: cache clear skipped: {e}")

    return len(plan)
