#!/usr/bin/env python3
"""
CB Hub Jeeves — Automation Worker
Runs every 40 minutes via GitHub Actions.

Flow:
1. Pull new disputes from Metabase (cards 17862 + 16937)
2. Upsert to Supabase (new = tracker_status IS NULL)
3. Group by ticket, classify group status
4. For new groups: apply correct ZD macro by reason + service + language
5. Monitor D+7 → send "Sin Respuesta" if expired
6. Monitor pending→completed transitions → send new email
"""

import os, json, time, logging, requests, urllib3
from datetime import datetime, timedelta, timezone
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Config from env ──────────────────────────────────────
MB_URL   = os.environ.get("METABASE_URL", "https://jeeves.metabaseapp.com")
MB_KEY   = os.environ["METABASE_API_KEY"]
SB_URL   = os.environ.get("SUPABASE_URL", "https://vykfbnrrvtnvcvjudlef.supabase.co")
SB_KEY   = os.environ["SUPABASE_KEY"]
ZD_URL   = os.environ.get("ZENDESK_URL", "https://jeeves.zendesk.com")
ZD_TOKEN = os.environ["ZENDESK_TOKEN"]

# Look-back window for Metabase pull
DAYS_BACK = int(os.environ.get("DAYS_BACK", "7"))

FRAUD_REASONS = {"transaction_not_recognised", "transaction_not_authorised"}

# ── Macro map (reason|service → id, name) ────────────────
MACRO_MAP = {
    "transaction_not_recognised|tutuka":        ("34157979161745", "ES-CB-Mas informacion(Fraude)"),
    "transaction_not_recognised|tutukaColombia":("34157979161745", "ES-CB-Mas informacion(Fraude)"),
    "transaction_not_recognised|tutukaBrazil":  ("34672299903505", "PT - CB - Mais informações (Fraude)"),
    "transaction_not_recognised|stripeUS":      ("40191880769681", "ENG - CB - More infos (Fraud)"),
    "transaction_not_recognised|stripeEUR":     ("40191880769681", "ENG - CB - More infos (Fraud)"),
    "transaction_not_recognised|stripeUK":      ("40191880769681", "ENG - CB - More infos (Fraud)"),
    "transaction_not_recognised|marqetaCanada": ("40191880769681", "ENG - CB - More infos (Fraud)"),
    "transaction_not_authorised|tutuka":        ("34157979161745", "ES-CB-Mas informacion(Fraude)"),
    "transaction_not_authorised|tutukaBrazil":  ("34672299903505", "PT - CB - Mais informações (Fraude)"),
    "transaction_not_authorised|stripeUS":      ("36397014861073", "ENG - CB - More infos (Fraud)"),
    "duplicate_transaction|tutuka":             ("25683815899409", "ES-CB-Mas informacion(No Fraude)"),
    "duplicate_transaction|tutukaBrazil":       ("34672535484689", "PT - CB - Mais informações (Não Fraude)"),
    "duplicate_transaction|stripeUS":           ("34816519253009", "ENG - CB - More infos (No Fraud)"),
    "goods_not_received|tutuka":                ("25683815899409", "ES-CB-Mas informacion(No Fraude)"),
    "goods_not_received|tutukaBrazil":          ("34672535484689", "PT - CB - Mais informações (Não Fraude)"),
    "goods_not_received|stripeUS":              ("34816519253009", "ENG - CB - More infos (No Fraud)"),
    "transaction_cancelled_still_charged|tutuka":    ("25683815899409", "ES-CB-Mas informacion(No Fraude)"),
    "transaction_cancelled_still_charged|tutukaBrazil": ("34672535484689", "PT - CB - Mais informações (Não Fraude)"),
    "transaction_cancelled_still_charged|stripeUS":  ("34816519253009", "ENG - CB - More infos (No Fraud)"),
    "service_not_as_described|tutuka":          ("25683815899409", "ES-CB-Mas informacion(No Fraude)"),
    "service_not_as_described|tutukaBrazil":    ("34672535484689", "PT - CB - Mais informações (Não Fraude)"),
    "service_not_as_described|stripeUS":        ("34816519253009", "ENG - CB - More infos (No Fraud)"),
    "other|tutuka":                             ("25683815899409", "ES-CB-Mas informacion(No Fraude)"),
    "other|stripeUS":                           ("34816519253009", "ENG - CB - More infos (No Fraud)"),
    "pending_transaction|tutuka":               ("25684352015249", "ES - CB - Transacción Pendiente"),
    "pending_transaction|tutukaBrazil":         ("34909690315025", "PT - CB - Transação pendente"),
    "pending_transaction|stripeUS":             ("38654879962001", "ENG - CB - Pending transaction"),
    "dispute_refunded|tutuka":                  ("32360064602257", "ES - CB - Reembolsada"),
    "dispute_refunded|tutukaBrazil":            ("35871087728785", "PT - CB - Reembolsada"),
    "dispute_refunded|stripeUS":                ("35352783772689", "ENG - CB - Refunded"),
    "dispute_failed|tutuka":                    ("38972215773713", "ES - CB - Revertida"),
    "dispute_failed|tutukaBrazil":              ("38972253588881", "PT - CB - Revertida"),
    "dispute_failed|stripeUS":                  ("38972287112209", "ENG - CB - Reversed"),
    "auto_close_no_response|tutuka":            ("25684243452049", "ES - CB - Sin Respuesta del Cliente"),
    "auto_close_no_response|tutukaColombia":    ("25684243452049", "ES - CB - Sin Respuesta del Cliente"),
    "auto_close_no_response|tutukaBrazil":      ("35353188515473", "PT - CB - Sem Resposta do Cliente"),
    "auto_close_no_response|stripeUS":          ("35353124583185", "ENG - CB - No response from customer"),
    "mixed_status|tutuka":                      ("40191076305681", "ES-CB-Mas informacion (Completed y Pending)"),
    "mixed_status|tutukaColombia":              ("40191076305681", "ES-CB-Mas informacion (Completed y Pending)"),
    "mixed_status|tutukaBrazil":                ("40191448261137", "PT - CB - Mais informações (Completed e Pending)"),
    "mixed_status|stripeUS":                    ("40191837319825", "ENG - CB - More infos (Completed and Pending)"),
}

def get_macro(reason: str, service: str) -> Optional[tuple]:
    return MACRO_MAP.get(f"{reason}|{service}") or MACRO_MAP.get(f"{reason}|tutuka")

# ── Helpers ──────────────────────────────────────────────
def mb_headers():
    return {"x-api-key": MB_KEY, "Content-Type": "application/json"}

def sb_headers(extra=None):
    h = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": "application/json"}
    if extra:
        h.update(extra)
    return h

def zd_headers():
    return {"Content-Type": "application/json", "Authorization": "Basic " + __import__('base64').b64encode(ZD_TOKEN.encode()).decode()}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def detect_service(row: dict) -> str:
    svc = (row.get("cardServiceType") or row.get("card_service_type") or "").lower()
    curr = (row.get("Currency") or row.get("currency") or "").upper()
    if "tutukabrazil" in svc or curr == "BRL": return "tutukaBrazil"
    if "tutukacolombia" in svc or curr == "COP": return "tutukaColombia"
    if "tutuka" in svc or row.get("fundingSource") == "LOC": return "tutuka"
    if "stripeeur" in svc: return "stripeEUR"
    if "stripeuk" in svc: return "stripeUK"
    if "stripe" in svc: return "stripeUS"
    if "marqeta" in svc: return "marqetaCanada"
    return "tutuka"

def tx_month(date_str: str) -> str:
    try:
        d = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return d.strftime("%b%y")
    except:
        return datetime.now().strftime("%b%y")

def group_status(txs: list) -> str:
    """Hierarchy: any completed → completed; any pending → pending; else by majority."""
    statuses = {(t.get("transaction_status") or "").lower() for t in txs}
    if "completed" in statuses: return "completed"
    if "authorized" in statuses or "pending" in statuses or "processing" in statuses: return "pending"
    if statuses <= {"refunded"}: return "refunded"
    if statuses <= {"failed", "reversed", "declined"}: return "failed"
    return "unknown"

def zd_status_for_group(gs: str) -> str:
    if gs == "completed": return "pending"
    if gs == "pending": return "on-hold"
    return "solved"


# ── Metabase ─────────────────────────────────────────────
def mb_card(card_id: int, params: dict = {}) -> list:
    body = {"parameters": [
        {"type": "category", "target": ["variable", ["template-tag", k]], "value": str(v)}
        for k, v in params.items() if v
    ]}
    r = requests.post(
        f"{MB_URL}/api/card/{card_id}/query",
        headers=mb_headers(), json=body, timeout=60, verify=False
    )
    r.raise_for_status()
    data = r.json()
    cols = [c["name"] for c in (data.get("data", {}).get("cols") or [])]
    return [dict(zip(cols, row)) for row in (data.get("data", {}).get("rows") or [])]

def pull_metabase() -> list:
    date_from = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    date_to   = datetime.now().strftime("%Y-%m-%d")
    log.info(f"Pulling Metabase {date_from} → {date_to}")
    txs = []
    try:
        sf   = mb_card(17862, {"Date_From": date_from, "Date_To": date_to})
        log.info(f"  SF disputes: {len(sf)}")
        txs.extend([(r, True) for r in sf])
    except Exception as e:
        log.error(f"MB card 17862 error: {e}")
    try:
        cred = mb_card(16937, {})
        log.info(f"  Credit disputes: {len(cred)}")
        txs.extend([(r, False) for r in cred])
    except Exception as e:
        log.error(f"MB card 16937 error: {e}")
    return txs

def normalize_tx(row: dict, is_sf: bool) -> Optional[dict]:
    card = str(row.get("card_number") or row.get("cardnumber") or "????")
    tx_id = str(row.get("id") or row.get("dispute_id") or "")
    if not tx_id:
        return None
    ticket = str(row.get("zendesk_ticket") or row.get("ticket") or "")
    if not ticket:
        return None
    svc = detect_service(row)
    date_str = str(row.get("transactionDateTime") or row.get("date") or "")
    return {
        "id": tx_id,
        "ticket_id": ticket,
        "month": tx_month(date_str),
        "org_id": str(row.get("org_id") or ""),
        "org_name": str(row.get("name") or ""),
        "cardholder": " ".join(filter(None, [row.get("firstname"), row.get("lastname")])) or str(row.get("name") or ""),
        "email": str(row.get("email") or ""),
        "last4": card[-4:],
        "card_type": str(row.get("card_type") or "physical"),
        "wallet_id": str(row.get("wallet_reference") or ""),
        "stripe_id": str(row.get("stripeTransactionId") or ""),
        "self_funded_ref": str(row.get("selfFundedTransactionReferenceId") or ""),
        "is_self_funded": is_sf or bool(row.get("selfFundedTransactionReferenceId")),
        "merchant": str(row.get("merchant_name") or ""),
        "amount": _float(row.get("baseTransactionAmount")),
        "currency": str(row.get("Currency") or row.get("currency") or ""),
        "amount_usd": _float(row.get("amount_USD")),
        "transaction_date": date_str,
        "auth_id": str(row.get("auth_Id") or ""),
        "arn": str(row.get("ARN") or ""),
        "mcc": str(row.get("mcc") or ""),
        "funding_source": str(row.get("fundingSource") or ""),
        "card_service_type": svc,
        "transaction_source": str(row.get("transactionSource") or ""),
        "details_chargeback": str(row.get("Details_Chargeback") or ""),
        "is_3ds": str(row.get("3DS") or ""),
        "acquirer_location": str(row.get("acquirer_location") or ""),
        "dispute_reason": str(row.get("dispute_reason") or "other"),
        "transaction_status": str(row.get("transactionStatus") or ""),
        "refund_found": str(row.get("refund_found") or "N"),
        "refund_amount": _float(row.get("refund_amount")),
        "refund_date": str(row.get("refund_date") or ""),
        "refund_merchant": str(row.get("refund_clean_merchant_name") or ""),
        # tracker_status intentionally NOT set here — new rows get NULL
        # which is how the hub identifies them as "new"
    }

def _float(v) -> Optional[float]:
    try:
        f = float(v)
        return None if f != f else f  # NaN → None
    except:
        return None


# ── Supabase ─────────────────────────────────────────────
def sb_get(table: str, params: str = "") -> list:
    r = requests.get(
        f"{SB_URL}/rest/v1/{table}?{params}",
        headers=sb_headers(), timeout=30, verify=False
    )
    r.raise_for_status()
    return r.json()

def sb_upsert(table: str, rows: list, batch: int = 100):
    if not rows:
        return
    # Get all keys from all rows, normalize every row to have same keys
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    normalized = [{k: r.get(k, None) for k in all_keys} for r in rows]

    ok = 0
    for i in range(0, len(normalized), batch):
        chunk = normalized[i:i+batch]
        r = requests.post(
            f"{SB_URL}/rest/v1/{table}",
            headers=sb_headers({"Prefer": "resolution=merge-duplicates,return=minimal"}),
            json=chunk, timeout=30, verify=False
        )
        if r.status_code in (200, 201):
            ok += len(chunk)
        else:
            log.error(f"SB upsert {table} batch {i}: {r.status_code} {r.text[:200]}")
    log.info(f"  Upserted {ok}/{len(normalized)} → {table}")

def sb_update(table: str, id_val: str, patch: dict):
    r = requests.patch(
        f"{SB_URL}/rest/v1/{table}?id=eq.{requests.utils.quote(id_val)}",
        headers=sb_headers({"Prefer": "return=minimal"}),
        json=patch, timeout=15, verify=False
    )
    return r.ok

def sb_log(action: str, ticket_id: str = None, tx_id: str = None, **kwargs):
    row = {"action": action, "ticket_id": ticket_id, "transaction_id": tx_id,
           "created_at": now_iso(), **kwargs}
    requests.post(
        f"{SB_URL}/rest/v1/automation_log",
        headers=sb_headers({"Prefer": "return=minimal"}),
        json=row, timeout=10, verify=False
    )

# ── Zendesk ──────────────────────────────────────────────
def zd_apply_macro(ticket_id: str, macro_id: str) -> bool:
    """Apply macro: get preview → apply comment/status to ticket."""
    try:
        # Get macro preview
        r = requests.get(
            f"{ZD_URL}/api/v2/tickets/{ticket_id}/macros/{macro_id}/apply.json",
            headers=zd_headers(), timeout=15, verify=False
        )
        if not r.ok:
            log.error(f"ZD macro preview {macro_id} on {ticket_id}: {r.status_code} {r.text[:100]}")
            return False
        result = r.json().get("result", {})
        ticket_patch = result.get("ticket", {})
        payload = {"ticket": {}}
        if "comment" in ticket_patch:
            payload["ticket"]["comment"] = ticket_patch["comment"]
        if "status" in ticket_patch:
            payload["ticket"]["status"] = ticket_patch["status"]
        if not payload["ticket"]:
            return True  # No changes needed

        r2 = requests.put(
            f"{ZD_URL}/api/v2/tickets/{ticket_id}.json",
            headers=zd_headers(), json=payload, timeout=15, verify=False
        )
        if not r2.ok:
            log.error(f"ZD ticket update {ticket_id}: {r2.status_code} {r2.text[:100]}")
        return r2.ok
    except Exception as e:
        log.error(f"ZD macro error: {e}")
        return False

def zd_set_status(ticket_id: str, status: str) -> bool:
    try:
        r = requests.put(
            f"{ZD_URL}/api/v2/tickets/{ticket_id}.json",
            headers=zd_headers(),
            json={"ticket": {"status": status}},
            timeout=15, verify=False
        )
        return r.ok
    except:
        return False

def zd_add_tag(ticket_id: str, tag: str):
    try:
        # Get current tags first
        r = requests.get(f"{ZD_URL}/api/v2/tickets/{ticket_id}.json",
                         headers=zd_headers(), timeout=10, verify=False)
        if r.ok:
            current = r.json().get("ticket", {}).get("tags", [])
            if tag not in current:
                requests.put(
                    f"{ZD_URL}/api/v2/tickets/{ticket_id}/tags.json",
                    headers=zd_headers(),
                    json={"tags": current + [tag]},
                    timeout=10, verify=False
                )
    except:
        pass

def zd_needs_attention(ticket_id: str) -> bool:
    """Returns True if ticket is OPEN and last public comment is from requester."""
    try:
        r1 = requests.get(f"{ZD_URL}/api/v2/tickets/{ticket_id}.json",
                          headers=zd_headers(), timeout=10, verify=False)
        if not r1.ok:
            return False
        ticket = r1.json().get("ticket", {})
        if ticket.get("status") != "open":
            return False
        requester_id = ticket.get("requester_id")
        r2 = requests.get(f"{ZD_URL}/api/v2/tickets/{ticket_id}/comments.json",
                          headers=zd_headers(), timeout=10, verify=False)
        if not r2.ok:
            return False
        comments = r2.json().get("comments", [])
        pub = [c for c in comments if c.get("public")]
        if not pub:
            return False
        return pub[-1].get("author_id") == requester_id
    except:
        return False


# ── Main automation logic ────────────────────────────────
def process_group(ticket_id: str, txs: list, group: dict):
    """Handle one ticket group — send emails, update statuses."""
    svc = detect_service(txs[0]) if txs else "tutuka"
    gs = group_status(txs)
    email_sent = bool(group.get("primary_email_sent_at"))
    is_mixed = (
        any((t.get("transaction_status") or "").lower() == "completed" for t in txs) and
        any((t.get("transaction_status") or "").lower() in ("authorized", "pending", "processing") for t in txs)
    )

    # ── Step A: Send primary email if not sent yet ──
    if not email_sent:
        if gs == "refunded":
            macro_key = f"dispute_refunded|{svc}"
        elif gs == "failed":
            macro_key = f"dispute_failed|{svc}"
        elif gs == "pending":
            macro_key = f"pending_transaction|{svc}"
        elif is_mixed:
            macro_key = f"mixed_status|{svc}"
        elif gs == "completed":
            # All fraud or all non-fraud?
            all_fraud = all(t.get("dispute_reason") in FRAUD_REASONS for t in txs)
            reason = txs[0].get("dispute_reason", "other")
            if all_fraud:
                macro_key = f"transaction_not_recognised|{svc}"
            else:
                macro_key = f"{reason}|{svc}"
        else:
            log.info(f"  Ticket {ticket_id}: gs={gs}, skipping")
            return

        macro = get_macro(macro_key.split("|")[0], svc)
        if not macro:
            log.warning(f"  No macro for {macro_key}")
            return

        macro_id, macro_name = macro
        log.info(f"  Ticket {ticket_id}: sending macro '{macro_name}' (gs={gs})")
        ok = zd_apply_macro(ticket_id, macro_id)

        if ok:
            d7 = None
            zd_st = zd_status_for_group(gs)
            if gs in ("completed",) or is_mixed:
                d7 = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")

            # Update group in Supabase
            sb_upsert("ticket_groups", [{
                "id": ticket_id,
                "org_id": group.get("org_id", ""),
                "org_name": group.get("org_name", ""),
                "month": group.get("month", datetime.now().strftime("%b%y")),
                "group_status": gs,
                "zd_status": zd_st,
                "zd_ticket": ticket_id,
                "primary_email_sent_at": now_iso(),
                "d7_deadline": d7,
                "last_macro_id": macro_id,
                "last_macro_name": macro_name,
                "last_macro_sent_at": now_iso(),
            }])

            # Update tracker_status on each tx
            new_status = {
                "completed": "Needs Response",
                "pending": "Pending",
                "refunded": "Refunded",
                "failed": "Trx failed",
            }.get(gs, "Needs Response")

            for tx in txs:
                sb_update("transactions", tx["id"], {"tracker_status": new_status})

            sb_log("primary_email", ticket_id, notes=f"macro={macro_name} gs={gs} d7={d7}")
            log.info(f"  ✓ Ticket {ticket_id}: macro sent, tracker={new_status}")

    # ── Step B: Check D+7 expiry ──
    else:
        d7_str = group.get("d7_deadline")
        if d7_str:
            try:
                d7 = datetime.strptime(d7_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) > d7:
                    # Check ZD: still open + no client reply?
                    needs = zd_needs_attention(ticket_id)
                    current_zd = group.get("zd_status", "")
                    if not needs and current_zd not in ("solved",):
                        macro = get_macro("auto_close_no_response", svc)
                        if macro:
                            macro_id, macro_name = macro
                            ok = zd_apply_macro(ticket_id, macro_id)
                            if ok:
                                for tx in txs:
                                    if (tx.get("tracker_status") or "") == "Needs Response":
                                        sb_update("transactions", tx["id"],
                                                  {"tracker_status": "Closed - No response from customer"})
                                sb_upsert("ticket_groups", [{
                                    **group,
                                    "zd_status": "solved",
                                    "last_macro_id": macro_id,
                                    "last_macro_name": macro_name,
                                    "last_macro_sent_at": now_iso(),
                                }])
                                sb_log("d7_auto_close", ticket_id, notes=f"macro={macro_name}")
                                log.info(f"  ✓ Ticket {ticket_id}: D+7 closed")
            except ValueError:
                pass

        # ── Step C: Detect pending → completed transitions ──
        for tx in txs:
            prev = (tx.get("tracker_status") or "").lower()
            curr = (tx.get("transaction_status") or "").lower()
            if prev == "pending" and curr == "completed":
                reason = tx.get("dispute_reason", "other")
                is_fraud = reason in FRAUD_REASONS
                macro_key_reason = "transaction_not_recognised" if is_fraud else reason
                macro = get_macro(macro_key_reason, svc)
                if macro:
                    macro_id, macro_name = macro
                    ok = zd_apply_macro(ticket_id, macro_id)
                    if ok:
                        d7_new = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                        sb_update("transactions", tx["id"], {"tracker_status": "Needs Response"})
                        sb_upsert("ticket_groups", [{
                            **group,
                            "d7_deadline": d7_new,
                            "zd_status": "pending",
                            "last_macro_id": macro_id,
                            "last_macro_name": macro_name,
                            "last_macro_sent_at": now_iso(),
                        }])
                        sb_log("pending_to_completed", ticket_id, tx["id"],
                               notes=f"macro={macro_name} new_d7={d7_new}")
                        log.info(f"  ✓ Tx {tx['id']}: pending→completed email sent")

        # ── Step D: Refunded/failed transitions ──
        for tx in txs:
            curr = (tx.get("transaction_status") or "").lower()
            prev = (tx.get("tracker_status") or "").lower()
            if curr in ("refunded",) and prev not in ("refunded", "rejected - refunded by merchant"):
                macro = get_macro("dispute_refunded", svc)
                if macro:
                    zd_apply_macro(ticket_id, macro[0])
                    sb_update("transactions", tx["id"], {"tracker_status": "Rejected - Refunded by merchant"})
                    sb_log("refunded", ticket_id, tx["id"])
                    log.info(f"  ✓ Tx {tx['id']}: refunded")
            elif curr in ("failed", "reversed", "declined") and prev not in ("trx failed",):
                macro = get_macro("dispute_failed", svc)
                if macro:
                    zd_apply_macro(ticket_id, macro[0])
                    sb_update("transactions", tx["id"], {"tracker_status": "Trx failed"})
                    sb_log("failed", ticket_id, tx["id"])
                    log.info(f"  ✓ Tx {tx['id']}: failed/reversed")


# ── Entry point ──────────────────────────────────────────
def main():
    log.info("═" * 55)
    log.info("CB Hub Worker starting")
    log.info("═" * 55)
    start = time.time()

    # 1. Pull from Metabase
    raw = pull_metabase()
    txs_to_upsert = []
    for row, is_sf in raw:
        tx = normalize_tx(row, is_sf)
        if tx:
            txs_to_upsert.append(tx)
    log.info(f"Metabase: {len(txs_to_upsert)} valid transactions")

    # 2. Upsert new txs to Supabase (only fields present — tracker_status stays NULL for new)
    if txs_to_upsert:
        sb_upsert("transactions", txs_to_upsert)

    # 3. Load ONLY new transactions (tracker_status IS NULL = came fresh from Metabase)
    # Historical data imported from Sheets has tracker_status set → ignored automatically
    new_txs = sb_get("transactions", "select=*&tracker_status=is.null&order=ticket_id.asc&limit=1000")
    log.info(f"New transactions to process (tracker_status NULL): {len(new_txs)}")

    if not new_txs:
        log.info("No new transactions to process — nothing to do")
        return

    all_active = {t["id"]: t for t in new_txs}

    # 4. Load ticket groups
    groups_raw = sb_get("ticket_groups", "select=*&limit=2000")
    groups = {g["id"]: g for g in groups_raw}

    # 5. Group transactions by ticket
    by_ticket: dict[str, list] = {}
    for tx in all_active.values():
        tid = tx.get("ticket_id", "")
        if not tid or tid.startswith("NOTICKET"):
            continue
        if tid not in by_ticket:
            by_ticket[tid] = []
        by_ticket[tid].append(tx)

    log.info(f"Processing {len(by_ticket)} ticket groups")

    # 6. Process each group — only valid integer ZD ticket IDs
    processed = 0
    for ticket_id, txs in by_ticket.items():
        try:
            # Skip malformed IDs (e.g. "153162.0" from old Sheets import)
            clean_id = str(ticket_id).replace(".0", "").strip()
            if not clean_id.isdigit():
                log.info(f"  Skipping invalid ticket_id: {ticket_id}")
                continue
            ticket_id = clean_id  # use clean version for ZD calls
            group = groups.get(ticket_id, groups.get(ticket_id + ".0", {"id": ticket_id, "org_id": txs[0].get("org_id", ""), "org_name": txs[0].get("org_name", ""), "month": txs[0].get("month", "")}))
            process_group(ticket_id, txs, group)
            processed += 1
            time.sleep(0.3)  # Be kind to ZD rate limits
        except Exception as e:
            log.error(f"Error processing ticket {ticket_id}: {e}")

    elapsed = round(time.time() - start, 1)
    log.info(f"═" * 55)
    log.info(f"Done — {processed} groups processed in {elapsed}s")
    log.info(f"═" * 55)

if __name__ == "__main__":
    main()
