"""
Negative Keyword Review + Winners Scale App
Run: python3 review_app.py
Open: http://localhost:5050
"""

from flask import Flask, render_template_string, jsonify, request, make_response
import requests, json, time, gzip, threading, os, io, csv, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# ── CONFIG ────────────────────────────────────────────────────────────────────
CLIENT_ID     = os.getenv("AMAZON_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("AMAZON_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("AMAZON_REFRESH_TOKEN", "")

PROFILES = {
    os.getenv("MENHOOD_PROFILE_1", "3016627615357133"): "Account A (Postpaid)",
    os.getenv("MENHOOD_PROFILE_2", "154697331411051"):  "Account B (Prepaid)",
}
EU_API    = "https://advertising-api-eu.amazon.com"
MIN_SPEND = 50    # ₹ — minimum spend to surface a term

IST = timezone(timedelta(hours=5, minutes=30))

def date_range_30d():
    """Return (start, end) as a rolling 30-day window ending yesterday in IST.
    Cache key stability is handled separately by _range_key() and does not
    depend on this window being fixed."""
    today = datetime.now(IST)
    end   = today - timedelta(days=1)   # yesterday (last full day of data)
    start = end - timedelta(days=29)    # 30 days total
    return start, end

# ── CACHE ─────────────────────────────────────────────────────────────────────
CACHE_FILE  = f"/tmp/negkw_cache_s{MIN_SPEND}.json"
DEBUG_FILE  = "/tmp/negkw_debug.json"
_PG_KEY     = f"negkw_cache_s{MIN_SPEND}"

def _pg_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return None
    try:
        return psycopg2.connect(db_url, connect_timeout=5)
    except Exception as e:
        print(f"PG_CONN error: {e}", flush=True)
        return None

def _range_key():
    """Stable weekly cache key — the date of the most recent Sunday (including today if Sunday).
    This is consistent Sun–Sat so the cron (Sunday evening) and Mon–Sat reads always agree."""
    today = datetime.now(IST).date()
    # weekday(): Mon=0 … Sun=6  →  days since last Sunday: Mon=1, Tue=2, …, Sat=6, Sun=0
    days_since_sunday = (today.weekday() + 1) % 7
    last_sunday = today - timedelta(days=days_since_sunday)
    return last_sunday.strftime('%Y-%m-%d')

def _pg_get():
    conn = _pg_conn()
    if not conn:
        print("PG_GET: DATABASE_URL not set — skipping DB", flush=True)
        return None
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT cache_data FROM negkw_cache WHERE cache_key = %s", (_PG_KEY,))
            row = cur.fetchone()
            if row:
                print("PG_GET: cache hit", flush=True)
                return row["cache_data"]
            print("PG_GET: cache miss", flush=True)
    except Exception as e:
        print(f"PG_GET error: {e}", flush=True)
    finally:
        conn.close()
    return None

def _pg_set(data):
    conn = _pg_conn()
    if not conn:
        print("PG_SET: DATABASE_URL not set — skipping DB", flush=True)
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO negkw_cache (cache_key, cache_data, saved_at)
                   VALUES (%s, %s, NOW())
                   ON CONFLICT (cache_key)
                   DO UPDATE SET cache_data = EXCLUDED.cache_data, saved_at = NOW()""",
                (_PG_KEY, json.dumps(data))
            )
        print("PG_SET: saved to DB", flush=True)
    except Exception as e:
        print(f"PG_SET error: {e}", flush=True)
    finally:
        conn.close()

_pg_synced = False

def _ensure_has_exact(result):
    """Backfill has_exact on each term using kw_data (handles old cache entries)."""
    kw_data = result.get("kw_data", {})
    for t in result.get("terms", []):
        if "has_exact" not in t:
            pid = t.get("profile", "")
            term_lower = t.get("searchTerm", "").lower().strip()
            exact_list = kw_data.get(pid, {}).get("EXACT", [])
            t["has_exact"] = term_lower in exact_list
    return result


def load_cache():
    global _pg_synced
    rk = _range_key()

    # 1. File cache — require matching key so stale local files are rejected fast
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        result = data.get("result", {})
        if data.get("week_key") == rk and result.get("count", 0) > 0 and result.get("kw_data"):
            if not _pg_synced:
                _pg_synced = True
                threading.Thread(target=_pg_set, args=(data,), daemon=True).start()
            return _ensure_has_exact(result)
    except Exception:
        pass

    # 2. PostgreSQL cache — accept any valid data regardless of key format.
    #    This handles the one-time migration when _range_key() format changes.
    #    Re-saves with the current key so file + PG are consistent from now on.
    data = _pg_get()
    if data:
        pg_result = data.get("result", {})
        if pg_result.get("count", 0) > 0 and pg_result.get("kw_data"):
            updated = {"week_key": rk, "result": pg_result}
            try:
                with open(CACHE_FILE, "w") as f:
                    json.dump(updated, f)
            except Exception:
                pass
            if not _pg_synced:
                _pg_synced = True
                threading.Thread(target=_pg_set, args=(updated,), daemon=True).start()
            return _ensure_has_exact(pg_result)
    return None

def save_cache(result):
    if not result or result.get("count", 0) == 0:
        print("save_cache: skipping — 0 terms, not overwriting good cache", flush=True)
        return
    payload = {"week_key": _range_key(), "result": result}
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(payload, f)
    except Exception:
        pass
    _pg_set(payload)

# ── API HELPERS ────────────────────────────────────────────────────────────────
def get_token():
    if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
        raise RuntimeError(
            "Missing Amazon Ads credentials. Set AMAZON_CLIENT_ID, "
            "AMAZON_CLIENT_SECRET, and AMAZON_REFRESH_TOKEN."
        )
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
    })
    return r.json()["access_token"]

def fetch_all_terms(token, profile_id, debug_log=None):
    """Fetch ALL search terms with spend >= MIN_SPEND. Frontend splits negatives vs winners."""
    def log(msg):
        if debug_log is not None:
            debug_log.append(f"[{profile_id}] {msg}")

    start, end = date_range_30d()
    log(f"Date range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    headers = {"Amazon-Advertising-API-ClientId": CLIENT_ID,
               "Amazon-Advertising-API-Scope": profile_id,
               "Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "name": f"NegKW Review {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
        "startDate": start.strftime("%Y-%m-%d"), "endDate": end.strftime("%Y-%m-%d"),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS", "groupBy": ["searchTerm"],
            "columns": ["campaignId","campaignName","adGroupId","adGroupName",
                        "searchTerm","impressions","clicks","cost","purchases7d","sales7d"],
            "reportTypeId": "spSearchTerm", "timeUnit": "SUMMARY", "format": "GZIP_JSON",
        }
    }
    rr = requests.post(f"{EU_API}/reporting/reports", headers=headers, json=body)
    rr_json = rr.json()
    log(f"Report request status={rr.status_code} body={json.dumps(rr_json)[:300]}")
    rid = rr_json.get("reportId")
    if not rid and rr.status_code == 425:
        detail = rr_json.get("detail", "")
        rid = detail.split(":")[-1].strip() if ":" in detail else None
        if rid:
            log(f"Duplicate report — reusing existing reportId: {rid}")
        else:
            log(f"ERROR: 425 but could not parse reportId from detail: {detail!r}")
            return []
    if not rid:
        log("ERROR: no reportId — stopping")
        return []

    duplicate_report = rr.status_code == 425
    for attempt in range(240):
        if not (attempt == 0 and duplicate_report):
            time.sleep(5)
        rs = requests.get(f"{EU_API}/reporting/reports/{rid}", headers=headers).json()
        log(f"Poll {attempt+1}: status={rs.get('status')}")
        if rs.get("status") == "COMPLETED":
            raw_bytes = requests.get(rs["url"]).content
            data = json.loads(gzip.decompress(raw_bytes))
            log(f"Report downloaded: {len(data)} total rows")
            if data:
                log(f"Sample row keys: {list(data[0].keys())}")
            spends = sorted([row.get("cost", 0) for row in data], reverse=True)
            log(f"Top 10 spends: {spends[:10]}")
            log(f"Rows with spend >= {MIN_SPEND}: {sum(1 for s in spends if s >= MIN_SPEND)}")

            terms = []
            for row in data:
                spend  = row.get("cost", 0)
                if spend < MIN_SPEND:
                    continue
                sales  = row.get("sales7d", 0)
                orders = int(row.get("purchases7d", 0))
                clicks = int(row.get("clicks", 0))
                acos   = spend / sales if sales > 0 else None
                terms.append({
                    "id":           f"{row.get('campaignId')}_{row.get('adGroupId')}_{row.get('searchTerm','')}",
                    "searchTerm":   row.get("searchTerm", ""),
                    "campaignId":   str(row.get("campaignId", "")),
                    "campaignName": row.get("campaignName", ""),
                    "adGroupId":    str(row.get("adGroupId", "")),
                    "adGroupName":  row.get("adGroupName", ""),
                    "spend":        round(spend, 0),
                    "sales":        round(sales, 0),
                    "orders":       orders,
                    "clicks":       clicks,
                    "acos":         round(acos * 100, 1) if acos else None,
                    "cpc":          round(spend / clicks, 2) if clicks > 0 else None,
                    "profile":      profile_id,
                })
            log(f"Terms with spend >= ₹{MIN_SPEND}: {len(terms)}")
            return sorted(terms, key=lambda x: x["spend"], reverse=True)
        elif rs.get("status") == "FAILED":
            log(f"Report FAILED: {json.dumps(rs)[:300]}")
            return []
    log("Timed out waiting for report")
    return []

def fetch_keywords_for_profile(token, profile_id):
    """Fetch all enabled/paused SP keywords. Returns {EXACT:[...], PHRASE:[...], BROAD:[...]}."""
    headers = {
        "Amazon-Advertising-API-ClientId": CLIENT_ID,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
        "Content-Type": "application/vnd.spKeyword.v3+json",
        "Accept":       "application/vnd.spKeyword.v3+json",
    }
    kw_sets = {"EXACT": set(), "PHRASE": set(), "BROAD": set()}
    next_token = None
    while True:
        body = {
            "maxResults": 1000,
            "stateFilter": {"include": ["ENABLED", "PAUSED"]},
        }
        if next_token:
            body["nextToken"] = next_token
        try:
            r = requests.post(
                f"{EU_API}/sp/keywords/list",
                headers=headers,
                json=body,
                timeout=30,
            )
        except Exception as e:
            print(f"fetch_keywords [{profile_id}] error: {e}", flush=True)
            break
        if r.status_code >= 400:
            print(f"fetch_keywords [{profile_id}] {r.status_code}: {r.text[:200]}", flush=True)
            break
        resp = r.json()
        batch = resp.get("keywords", [])
        for kw in batch:
            mt = kw.get("matchType", "").upper()
            # Strip modified-broad + prefixes and normalise whitespace
            raw = kw.get("keywordText", "").lower().strip()
            kt  = " ".join(raw.replace("+", " ").split())
            if mt in kw_sets and kt:
                kw_sets[mt].add(kt)
        next_token = resp.get("nextToken")
        if not next_token:
            break
    result = {k: list(v) for k, v in kw_sets.items()}
    total = sum(len(v) for v in result.values())
    print(f"fetch_keywords [{profile_id}]: SP keywords list API fetched {total} keywords", flush=True)
    return result

# ── BACKGROUND FETCH ──────────────────────────────────────────────────────────
_lock      = threading.Lock()
_status    = {"state": "idle", "result": None, "error": None, "started_at": None}
_debug_log = []
_pg_synced = False

def _do_fetch():
    global _status, _debug_log
    fetch_log = [f"Fetch started at {datetime.utcnow().isoformat()}Z",
                 f"MIN_SPEND={MIN_SPEND}"]
    try:
        token = get_token()
        fetch_log.append("Token acquired OK")
        all_terms = []
        kw_data   = {}

        def fetch_profile(pid, label):
            terms = fetch_all_terms(token, pid, debug_log=fetch_log)
            for t in terms:
                t["accountLabel"] = label
            kws = fetch_keywords_for_profile(token, pid)
            return terms, pid, kws

        with ThreadPoolExecutor(max_workers=len(PROFILES)) as executor:
            futures = {executor.submit(fetch_profile, pid, label): label
                       for pid, label in PROFILES.items()}
            for future in as_completed(futures):
                terms, pid, kws = future.result()
                all_terms.extend(terms)
                kw_data[pid] = kws

        all_terms.sort(key=lambda x: x["spend"], reverse=True)

        # Pre-compute exact keyword coverage per term (server-side, authoritative)
        for t in all_terms:
            pid = t.get("profile", "")
            term_lower = t.get("searchTerm", "").lower().strip()
            exact_list = kw_data.get(pid, {}).get("EXACT", [])
            t["has_exact"] = term_lower in exact_list

        # Compute waste using default 30% threshold for cached KPI
        wasted = sum(t["spend"] for t in all_terms
                     if t["orders"] == 0 or (t["acos"] and t["acos"] > 30))
        start, end = date_range_30d()
        date_label = f"{start.strftime('%d %b')} – {end.strftime('%d %b %Y')} (30 days)"
        result = {
            "terms":       all_terms,
            "total_waste": wasted,
            "count":       len(all_terms),
            "date":        date_label,
            "cached":      False,
            "kw_data":     kw_data,
        }
        save_cache(result)
        fetch_log.append(f"Done — {len(all_terms)} terms total, ₹{wasted:.0f} default waste")
        with open(DEBUG_FILE, "w") as f:
            json.dump(fetch_log, f)
        with _lock:
            _status["state"]  = "ready"
            _status["result"] = result
            _debug_log        = fetch_log
    except Exception as e:
        fetch_log.append(f"EXCEPTION: {e}")
        with open(DEBUG_FILE, "w") as f:
            json.dump(fetch_log, f)
        with _lock:
            _status["state"] = "error"
            _status["error"] = str(e)
            _debug_log       = fetch_log

def _start_fetch(force=False):
    with _lock:
        if not force and _status["state"] == "ready" and _status["result"]:
            if _status["result"].get("count", 0) > 0:
                return "ready", _status["result"]
        if _status["state"] == "loading":
            return "loading", None
        _status["state"]      = "loading"
        _status["result"]     = None
        _status["error"]      = None
        _status["started_at"] = time.time()
    threading.Thread(target=_do_fetch, daemon=True).start()
    return "loading", None

# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    resp = make_response(render_template_string(HTML))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/api/fetch")
def api_fetch():
    force = request.args.get("force") == "true"
    if not force:
        cached = load_cache()
        if cached:
            with _lock:
                _status["state"]  = "ready"
                _status["result"] = cached
            cached["cached"] = True
            return jsonify({"status": "ready", **cached})
    state, result = _start_fetch(force=force)
    if state == "ready":
        result["cached"] = True
        return jsonify({"status": "ready", **result})
    return jsonify({"status": "loading"})

@app.route("/api/status")
def api_status():
    with _lock:
        state      = _status["state"]
        result     = _status["result"]
        started_at = _status["started_at"]
        error      = _status["error"]
    if state == "ready" and result:
        result["cached"] = True
        return jsonify({"status": "ready", **result})
    if state == "error":
        return jsonify({"status": "error", "error": error})
    cached = load_cache()
    if cached:
        with _lock:
            _status["state"]  = "ready"
            _status["result"] = cached
        cached["cached"] = True
        return jsonify({"status": "ready", **cached})
    elapsed = int(time.time() - started_at) if started_at else 0
    return jsonify({"status": "loading", "elapsed": elapsed})

@app.route("/api/debug")
def api_debug():
    with _lock:
        mem_log = list(_debug_log)
        state   = _status["state"]
        err     = _status["error"]
    disk_log = []
    try:
        with open(DEBUG_FILE) as f:
            disk_log = json.load(f)
    except Exception:
        pass
    cached = load_cache()
    return jsonify({
        "state":        state,
        "error":        err,
        "cache_file":   CACHE_FILE,
        "cache_exists": cached is not None,
        "cache_count":  cached.get("count") if cached else None,
        "min_spend":    MIN_SPEND,
        "week":         _range_key(),
        "log":          disk_log or mem_log,
    })

@app.route("/api/prefetch")
def api_prefetch():
    # Always force a fresh fetch — this endpoint is called by the weekly cron
    # and must refresh data regardless of current cache state.
    state, result = _start_fetch(force=True)
    if state == "ready":
        return jsonify({"status": "refreshed", "count": result.get("count", 0)})
    return jsonify({"status": "started", "message": "Background fetch triggered"})

def send_email_notification(subject: str, html_body: str):
    """Send email via Gmail SMTP. Requires GMAIL_USER and GMAIL_APP_PASSWORD env vars."""
    gmail_user = os.getenv("GMAIL_USER", "")
    gmail_pass = os.getenv("GMAIL_APP_PASSWORD", "")
    to_email   = os.getenv("NOTIFY_EMAIL", "rajeshwadhwa28@gmail.com")

    if not gmail_user or not gmail_pass:
        print("EMAIL: GMAIL_USER or GMAIL_APP_PASSWORD not set — skipping notification", flush=True)
        return

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = gmail_user
        msg["To"]      = to_email
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, to_email, msg.as_string())
        print(f"EMAIL: sent '{subject}' to {to_email}", flush=True)
    except Exception as e:
        print(f"EMAIL ERROR: {e}", flush=True)


def _build_negatives_email(approved: list, added: int, errors: int) -> str:
    """Build HTML email summary for applied negatives."""
    now_ist     = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")
    total_spend = sum(float(t.get("spend", 0)) for t in approved)

    rows = ""
    for t in approved[:50]:
        rows += f"""
        <tr>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;">{t.get('searchTerm','')}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;">{t.get('campaignName','')[:40]}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;color:#ef4444;font-weight:bold;">₹{float(t.get('spend',0)):,.0f}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;">{t.get('accountLabel','')}</td>
        </tr>"""

    more = f"<p style='color:#6b7280;font-size:13px;'>...and {len(approved)-50} more terms</p>" if len(approved) > 50 else ""

    return f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:700px;margin:0 auto;">
      <div style="background:#0D0D0D;padding:32px 40px;border-radius:12px 12px 0 0;">
        <h1 style="color:#fff;font-size:22px;margin:0;">🚫 Negatives Applied — Menhood</h1>
        <p style="color:#9ca3af;margin:8px 0 0;font-size:14px;">{now_ist}</p>
      </div>
      <div style="background:#fff;border:1px solid #e5e7eb;padding:32px 40px;">
        <div style="display:flex;gap:24px;margin-bottom:28px;flex-wrap:wrap;">
          <div style="background:#f9fafb;border-radius:10px;padding:16px 24px;flex:1;min-width:120px;">
            <div style="font-size:28px;font-weight:800;color:#ef4444;">{added}</div>
            <div style="font-size:12px;color:#6b7280;margin-top:4px;">Negatives Added</div>
          </div>
          <div style="background:#f9fafb;border-radius:10px;padding:16px 24px;flex:1;min-width:120px;">
            <div style="font-size:28px;font-weight:800;color:#FF6B35;">₹{total_spend:,.0f}</div>
            <div style="font-size:12px;color:#6b7280;margin-top:4px;">Monthly Spend Stopped</div>
          </div>
          <div style="background:#f9fafb;border-radius:10px;padding:16px 24px;flex:1;min-width:120px;">
            <div style="font-size:28px;font-weight:800;color:{'#10b981' if errors==0 else '#f59e0b'};">{errors}</div>
            <div style="font-size:12px;color:#6b7280;margin-top:4px;">Errors</div>
          </div>
        </div>
        <h3 style="font-size:14px;font-weight:700;color:#111;margin-bottom:12px;">Terms negated</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#0D0D0D;color:#fff;">
              <th style="padding:8px 10px;text-align:left;">Search Term</th>
              <th style="padding:8px 10px;text-align:left;">Campaign</th>
              <th style="padding:8px 10px;text-align:left;">Spend</th>
              <th style="padding:8px 10px;text-align:left;">Account</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        {more}
      </div>
      <div style="background:#f9fafb;border:1px solid #e5e7eb;border-top:0;padding:16px 40px;border-radius:0 0 12px 12px;">
        <p style="color:#9ca3af;font-size:12px;margin:0;">
          Sent by NegAuto · Menhood Ads Keyword Review ·
          <a href="https://neg-reviewde.replit.app" style="color:#FF6B35;">Open app</a>
        </p>
      </div>
    </div>"""


@app.route("/api/apply", methods=["POST"])
def api_apply():
    data     = request.json
    approved = data.get("terms", [])
    dry_run  = data.get("dry_run", True)

    if dry_run:
        return jsonify({"status": "dry_run", "would_add": len(approved),
                        "terms": [t["searchTerm"] for t in approved]})

    token = get_token()
    results = {"success": [], "errors": []}
    by_profile = {}
    for t in approved:
        by_profile.setdefault(t["profile"], []).append(t)

    for profile_id, terms in by_profile.items():
        headers = {"Amazon-Advertising-API-ClientId": CLIENT_ID,
                   "Amazon-Advertising-API-Scope": profile_id,
                   "Authorization": f"Bearer {token}",
                   "Content-Type": "application/vnd.spNegativeKeyword.v3+json",
                   "Accept":        "application/vnd.spNegativeKeyword.v3+json"}
        payload = [{"campaignId": t["campaignId"], "adGroupId": t["adGroupId"],
                    "keywordText": t["searchTerm"], "matchType": "NEGATIVE_EXACT",
                    "state": "ENABLED"} for t in terms]
        for i in range(0, len(payload), 1000):
            r = requests.post(f"{EU_API}/sp/negativeKeywords",
                              headers=headers, json={"negativeKeywords": payload[i:i+1000]})
            rd = r.json()
            results["success"].extend(rd.get("negativeKeywords", {}).get("success", []))
            results["errors"].extend(rd.get("negativeKeywords", {}).get("error", []))

    added  = len(results["success"])
    errors = len(results["errors"])

    if added > 0:
        subject   = f"🚫 {added} negatives applied — ₹{sum(float(t.get('spend',0)) for t in approved):,.0f} spend stopped"
        html_body = _build_negatives_email(approved, added, errors)
        threading.Thread(target=send_email_notification, args=(subject, html_body),
                         daemon=True).start()

    return jsonify({"status": "applied", "added": added,
                    "errors": errors, "detail": results})

@app.route("/api/add_keywords", methods=["POST"])
def api_add_keywords():
    """Create exact-match SP keywords from winning search terms."""
    data        = request.json
    terms       = data.get("terms", [])
    default_bid = float(data.get("bid", 10.0))

    token   = get_token()
    results = {"success": [], "errors": []}
    by_profile = {}
    for t in terms:
        by_profile.setdefault(t["profile"], []).append(t)

    for profile_id, pts in by_profile.items():
        headers = {
            "Amazon-Advertising-API-ClientId": CLIENT_ID,
            "Amazon-Advertising-API-Scope":    profile_id,
            "Authorization":                   f"Bearer {token}",
            "Content-Type": "application/vnd.spKeyword.v3+json",
            "Accept":       "application/vnd.spKeyword.v3+json",
        }
        payload = [
            {"campaignId": t["campaignId"], "adGroupId": t["adGroupId"],
             "keywordText": t["searchTerm"], "matchType": "EXACT",
             "state": "ENABLED", "bid": float(t.get("bid", default_bid))}
            for t in pts
        ]
        for i in range(0, len(payload), 1000):
            r = requests.post(f"{EU_API}/sp/keywords",
                              headers=headers, json={"keywords": payload[i:i+1000]},
                              timeout=30)
            if r.status_code >= 400:
                results["errors"].append({"msg": f"{r.status_code}: {r.text[:200]}"})
                continue
            rd = r.json()
            results["success"].extend(rd.get("keywords", {}).get("success", []))
            results["errors"].extend(rd.get("keywords", {}).get("error", []))

    return jsonify({"status": "done", "added": len(results["success"]),
                    "errors": len(results["errors"]), "detail": results})


# ── SELF-TARGET HELPERS ────────────────────────────────────────────────────────

def _fetch_product_ads_for_profile(token, profile_id, profile_label):
    """Fetch all ENABLED product ads (ASINs) for a profile via pagination."""
    headers = {
        "Amazon-Advertising-API-ClientId": CLIENT_ID,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
        "Content-Type": "application/vnd.spProductAd.v3+json",
        "Accept":       "application/vnd.spProductAd.v3+json",
    }
    ads, next_token = [], None
    while True:
        body = {"maxResults": 100, "stateFilter": {"include": ["ENABLED"]}}
        if next_token:
            body["nextToken"] = next_token
        r = requests.post(f"{EU_API}/sp/productAds/list", headers=headers,
                          json=body, timeout=30)
        if r.status_code >= 400:
            return ads, f"Product ads {r.status_code}: {r.text[:200]}"
        data = r.json()
        for ad in data.get("productAds", []):
            ad["profileLabel"] = profile_label
            ad["profile"]      = profile_id
            ads.append(ad)
        next_token = data.get("nextToken")
        if not next_token:
            break

    campaign_ids = sorted({str(ad.get("campaignId")) for ad in ads if ad.get("campaignId")})
    ad_group_ids = sorted({str(ad.get("adGroupId")) for ad in ads if ad.get("adGroupId")})

    def _fetch_name_map(kind, ids):
        if not ids:
            return {}
        if kind == "campaign":
            endpoint = f"{EU_API}/sp/campaigns/list"
            ctype    = "application/vnd.spCampaign.v3+json"
            arr_key, id_filter_key, id_key, name_key = "campaigns", "campaignIdFilter", "campaignId", "name"
        else:
            endpoint = f"{EU_API}/sp/adGroups/list"
            ctype    = "application/vnd.spAdGroup.v3+json"
            arr_key, id_filter_key, id_key, name_key = "adGroups", "adGroupIdFilter", "adGroupId", "name"
        local_headers = {
            "Amazon-Advertising-API-ClientId": CLIENT_ID,
            "Amazon-Advertising-API-Scope":    profile_id,
            "Authorization":                   f"Bearer {token}",
            "Content-Type": ctype, "Accept": ctype,
        }
        out = {}
        for i in range(0, len(ids), 1000):
            chunk = ids[i:i+1000]
            body  = {"maxResults": 1000,
                     "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                     id_filter_key: {"include": chunk}}
            nt = None
            while True:
                req_body = dict(body)
                if nt:
                    req_body["nextToken"] = nt
                rr = requests.post(endpoint, headers=local_headers, json=req_body, timeout=30)
                if rr.status_code >= 400:
                    print(f"{kind} name lookup [{profile_id}] {rr.status_code}: {rr.text[:200]}", flush=True)
                    break
                payload = rr.json()
                for obj in payload.get(arr_key, []):
                    oid = str(obj.get(id_key, ""))
                    if oid:
                        out[oid] = obj.get(name_key, "")
                nt = payload.get("nextToken")
                if not nt:
                    break
        return out

    campaign_name_map = _fetch_name_map("campaign", campaign_ids)
    ad_group_name_map = _fetch_name_map("adGroup", ad_group_ids)
    for ad in ads:
        ad["campaignName"] = campaign_name_map.get(str(ad.get("campaignId", "")), "")
        ad["adGroupName"]  = ad_group_name_map.get(str(ad.get("adGroupId", "")), "")

    return ads, ""


def _winner_pairs_from_cache():
    """Return set of (profile_id, campaign_id, ad_group_id) tuples with spend>0 and orders>0."""
    cached = load_cache() or {}
    pairs = set()
    for t in cached.get("terms", []):
        try:
            spend  = float(t.get("spend", 0) or 0)
            orders = int(t.get("orders", 0) or 0)
        except Exception:
            continue
        if spend > 0 and orders > 0:
            pairs.add((
                str(t.get("profile", "")),
                str(t.get("campaignId", "")),
                str(t.get("adGroupId", "")),
            ))
    return pairs


def _create_self_target_campaigns(token, profile_id, asins, bid, daily_budget):
    """For each ASIN: campaign → ad group → product ad → asinSameAs target."""
    today_str = datetime.now(IST).strftime("%Y%m%d")
    results   = {"success": [], "errors": []}
    base_hdrs = {
        "Amazon-Advertising-API-ClientId": CLIENT_ID,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
    }

    for asin in asins:
        # Step 1: Campaign
        r = requests.post(
            f"{EU_API}/sp/campaigns",
            headers={**base_hdrs, "Content-Type": "application/vnd.spCampaign.v3+json",
                                  "Accept":       "application/vnd.spCampaign.v3+json"},
            json={"campaigns": [{"name": f"SP|Self-Target|{asin}",
                                 "targetingType": "MANUAL", "state": "ENABLED",
                                 "budget": {"budgetType": "DAILY", "budget": daily_budget},
                                 "startDate": today_str}]},
            timeout=30)
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "campaign", "msg": r.text[:200]})
            continue
        camp = r.json().get("campaigns", {})
        ok   = camp.get("success", [])
        if not ok:
            results["errors"].append({"asin": asin, "step": "campaign",
                                      "msg": str(camp.get("error", "unknown"))})
            continue
        campaign_id = ok[0]["campaignId"]

        # Step 2: Ad group
        r = requests.post(
            f"{EU_API}/sp/adGroups",
            headers={**base_hdrs, "Content-Type": "application/vnd.spAdGroup.v3+json",
                                  "Accept":       "application/vnd.spAdGroup.v3+json"},
            json={"adGroups": [{"name": f"Self|{asin}", "campaignId": campaign_id,
                                "defaultBid": bid, "state": "ENABLED"}]},
            timeout=30)
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "adGroup", "msg": r.text[:200]})
            continue
        ag  = r.json().get("adGroups", {})
        aok = ag.get("success", [])
        if not aok:
            results["errors"].append({"asin": asin, "step": "adGroup",
                                      "msg": str(ag.get("error", "unknown"))})
            continue
        ad_group_id = aok[0]["adGroupId"]

        # Step 3: Product ad
        r = requests.post(
            f"{EU_API}/sp/productAds",
            headers={**base_hdrs, "Content-Type": "application/vnd.spProductAd.v3+json",
                                  "Accept":       "application/vnd.spProductAd.v3+json"},
            json={"productAds": [{"campaignId": campaign_id, "adGroupId": ad_group_id,
                                  "asin": asin, "state": "ENABLED"}]},
            timeout=30)
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "productAd", "msg": r.text[:200]})
            continue

        # Step 4: asinSameAs target
        r = requests.post(
            f"{EU_API}/sp/targets",
            headers={**base_hdrs,
                     "Content-Type": "application/vnd.spTargetingClause.v3+json",
                     "Accept":       "application/vnd.spTargetingClause.v3+json"},
            json={"targetingClauses": [{"campaignId": campaign_id, "adGroupId": ad_group_id,
                                        "state": "ENABLED", "bid": bid,
                                        "expression": [{"type": "asinSameAs", "value": asin}],
                                        "expressionType": "MANUAL"}]},
            timeout=30)
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "target", "msg": r.text[:200]})
            continue
        tgt = r.json().get("targetingClauses", {})
        if tgt.get("error"):
            results["errors"].append({"asin": asin, "step": "target",
                                      "msg": str(tgt["error"])})
        else:
            results["success"].append({"asin": asin, "campaignId": campaign_id,
                                       "adGroupId": ad_group_id})
    return results


@app.route("/api/self_target/asins")
def api_self_target_asins():
    """Return all active product ad ASINs across both profiles."""
    try:
        token = get_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    all_ads, errors = [], []
    for pid, label in PROFILES.items():
        ads, err = _fetch_product_ads_for_profile(token, pid, label)
        all_ads.extend(ads)
        if err:
            errors.append(err)

    # Step 1: From the FULL unfiltered list, build the set of ASINs already in any
    # self-targeting campaign. Must happen before winner_pairs filter strips those rows out.
    # Matches: "SP|Self-Target|..." (our format) and "Self PT..." (Rajesh's existing experiments).
    def _is_self_targeting(name):
        n = (name or "").lower()
        return n.startswith("sp|self-target|") or "self pt" in n

    already_targeted = {
        (ad.get("profile", ""), ad.get("asin", ""))
        for ad in all_ads
        if _is_self_targeting(ad.get("campaignName", "")) and ad.get("asin")
    }

    # Step 2: Filter to ad groups that produced real winners (spend>0 & orders>0).
    # Falls back to all enabled product ads if cache is empty.
    winner_pairs = _winner_pairs_from_cache()
    if winner_pairs:
        filtered = []
        for ad in all_ads:
            key = (str(ad.get("profile", "")),
                   str(ad.get("campaignId", "")),
                   str(ad.get("adGroupId", "")))
            if key in winner_pairs:
                filtered.append(ad)
        all_ads = filtered
    else:
        errors.append("No winner pairs in cache — showing all enabled product ads.")

    # Step 3: Remove any ASIN that already has self-targeting anywhere (cross-campaign).
    pre_filter = len(all_ads)
    all_ads = [
        ad for ad in all_ads
        if (ad.get("profile", ""), ad.get("asin", "")) not in already_targeted
    ]
    excluded_count = pre_filter - len(all_ads)
    if excluded_count:
        errors.append(f"Excluded {len(already_targeted)} ASINs already in self-targeting campaigns.")

    seen, unique = set(), []
    for ad in all_ads:
        key = (ad["profile"], ad.get("asin", ""))
        if key not in seen and ad.get("asin"):
            seen.add(key)
            unique.append({
                "asin":         ad["asin"],
                "sku":          ad.get("sku", ""),
                "profile":      ad["profile"],
                "profileLabel": ad["profileLabel"],
                "campaignName": ad.get("campaignName", ""),
                "adGroupName":  ad.get("adGroupName", ""),
            })

    return jsonify({"asins": unique, "errors": errors})


@app.route("/api/self_target/create", methods=["POST"])
def api_self_target_create():
    """Create self-targeting campaigns for the given ASINs."""
    data         = request.json
    items        = data.get("items", [])   # [{asin, profile}]
    bid          = float(data.get("bid", 5.0))
    daily_budget = float(data.get("daily_budget", 100.0))

    if not items:
        return jsonify({"error": "No ASINs provided"}), 400

    try:
        token = get_token()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    by_profile = {}
    for it in items:
        by_profile.setdefault(it["profile"], []).append(it["asin"])

    all_success, all_errors = [], []
    for pid, asins in by_profile.items():
        res = _create_self_target_campaigns(token, pid, asins, bid, daily_budget)
        all_success.extend(res["success"])
        all_errors.extend(res["errors"])

    return jsonify({"status": "done", "created": len(all_success),
                    "errors": len(all_errors), "detail": {"success": all_success,
                                                           "errors": all_errors}})


# ── HTML TEMPLATE ──────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Menhood Ads Review</title>
<style>
:root{--n:#0f172a;--b:#e2e8f0;--bg:#f8fafc;--r:#dc2626;--g:#16a34a;--y:#d97706;--bl:#2563eb;--pu:#7c3aed}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--n);font-size:13px}
.page{max-width:1200px;margin:0 auto;padding:24px 20px 48px}
.header{background:var(--n);color:#fff;padding:18px 24px;border-radius:12px;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
.header h1{font-size:17px;font-weight:800}
.header .sub{font-size:11px;color:#94a3b8;margin-top:2px}
.btn{display:inline-flex;align-items:center;gap:5px;padding:8px 18px;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;border:none;transition:.15s}
.btn-primary{background:var(--bl);color:#fff}.btn-primary:hover{background:#1d4ed8}
.btn-danger{background:var(--r);color:#fff}.btn-danger:hover{background:#b91c1c}
.btn-green{background:var(--g);color:#fff}.btn-green:hover{background:#15803d}
.btn-ghost{background:#f1f5f9;color:var(--n);border:1px solid var(--b)}
.btn:disabled{opacity:.4;cursor:not-allowed}
/* Controls bar */
.controls-bar{display:flex;align-items:center;gap:16px;margin-bottom:16px;flex-wrap:wrap}
.threshold-wrap{display:flex;align-items:center;gap:8px;background:#fff;border:1px solid var(--b);border-radius:10px;padding:8px 14px}
.threshold-wrap label{font-size:11px;font-weight:700;color:#64748b;white-space:nowrap}
.threshold-wrap input[type=number]{width:72px;border:1px solid var(--b);border-radius:6px;padding:4px 8px;font-size:13px;font-weight:700;text-align:center;outline:none}
.threshold-wrap input[type=number]:focus{border-color:var(--bl)}
.threshold-hint{font-size:10px;color:#94a3b8}
/* KPIs */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}
.kpi{background:#fff;border:1px solid var(--b);border-radius:10px;padding:12px 14px;text-align:center}
.kpi .big{font-size:20px;font-weight:800;margin-bottom:1px}
.kpi .lbl{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.5px}
.red{color:var(--r)} .green{color:var(--g)} .blue{color:var(--bl)} .amber{color:var(--y)} .pu{color:var(--pu)}
/* Tabs */
.tabs{display:flex;gap:0;border-bottom:2px solid var(--b);margin-bottom:16px}
.tab-btn{padding:10px 20px;font-size:13px;font-weight:700;cursor:pointer;border:none;background:none;color:#64748b;border-bottom:3px solid transparent;margin-bottom:-2px;transition:.15s}
.tab-btn.active{color:var(--bl);border-bottom-color:var(--bl)}
.tab-panel{display:none}.tab-panel.active{display:block}
/* Toolbar */
.toolbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:8px}
.toolbar-left{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.sel-count{font-size:12px;color:#64748b;font-weight:600}
/* Table */
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid var(--b);border-radius:10px;overflow:hidden}
th{padding:8px 10px;text-align:left;font-weight:700;color:#64748b;border-bottom:2px solid var(--b);font-size:11px;background:#f8fafc;white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid #f1f5f9;font-size:12px;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr.unchecked{opacity:.4}
tr:hover td{background:#fafafa}
.mono{font-family:'SF Mono',Consolas,monospace;font-size:11px}
.num{text-align:right;font-variant-numeric:tabular-nums}
.center{text-align:center}
.acos-bad{color:var(--r);font-weight:700}
.acos-high{color:var(--y);font-weight:700}
.acos-good{color:var(--g);font-weight:700}
.acos-inf{color:#94a3b8;font-style:italic}
.badge{font-size:10px;font-weight:700;padding:2px 7px;border-radius:20px}
.badge-zero{background:#fef2f2;color:#dc2626}
.badge-high{background:#fffbeb;color:#d97706}
.badge-brand{background:#7c3aed;color:#fff}
.badge-winner{background:#dcfce7;color:#16a34a}
tr.brand-row td{background:#fefce8!important}
.cv{font-size:13px;text-align:center}
/* Coverage legend */
.cov-ok{color:var(--g)}
.cov-no{color:#cbd5e1}
/* Winners bid bar */
.winners-controls{display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap}
.bid-wrap{display:flex;align-items:center;gap:6px;background:#fff;border:1px solid var(--b);border-radius:8px;padding:6px 12px}
.bid-wrap label{font-size:11px;font-weight:700;color:#64748b}
.bid-wrap input[type=number]{width:80px;border:1px solid var(--b);border-radius:6px;padding:4px 8px;font-size:13px;font-weight:700;outline:none}
.bid-wrap input[type=number]:focus{border-color:var(--g)}
/* Toast */
.toast{position:fixed;bottom:24px;right:24px;background:var(--n);color:#fff;padding:14px 20px;border-radius:10px;font-size:13px;font-weight:600;display:none;z-index:999;max-width:380px}
.loading{text-align:center;padding:60px;color:#94a3b8;font-size:14px}
.spinner{display:inline-block;width:22px;height:22px;border:3px solid #e2e8f0;border-top-color:var(--bl);border-radius:50%;animation:spin .8s linear infinite;margin-right:8px;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}
.empty-state{text-align:center;padding:48px;color:#64748b}
.empty-state .ico{font-size:32px;margin-bottom:8px}
/* Bid modal */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:1000;display:none;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:#fff;border-radius:14px;padding:28px;max-width:820px;width:95%;max-height:88vh;display:flex;flex-direction:column;gap:16px;box-shadow:0 20px 60px rgba(0,0,0,.25)}
.modal-header{display:flex;justify-content:space-between;align-items:flex-start}
.modal-header h2{font-size:16px;font-weight:800}
.modal-header .sub{font-size:11px;color:#64748b;margin-top:3px}
.modal-close{background:none;border:none;font-size:20px;cursor:pointer;color:#94a3b8;padding:0 4px;line-height:1}
.modal-bulk{display:flex;align-items:center;gap:10px;background:#f8fafc;border:1px solid var(--b);border-radius:8px;padding:10px 14px}
.modal-bulk label{font-size:11px;font-weight:700;color:#64748b;white-space:nowrap}
.modal-bulk input[type=number]{width:80px;border:1px solid var(--b);border-radius:6px;padding:4px 8px;font-size:13px;font-weight:700;outline:none}
.modal-bulk input[type=number]:focus{border-color:var(--g)}
.modal-scroll{overflow-y:auto;flex:1;border:1px solid var(--b);border-radius:8px}
.modal-scroll table{border:none;border-radius:0}
.modal-scroll th{position:sticky;top:0;z-index:1}
.modal-foot{display:flex;justify-content:flex-end;gap:10px;padding-top:4px}
</style>
</head>
<body>
<div class="page">

<!-- HEADER -->
<div class="header">
  <div>
    <h1>🎯 Menhood Ads — Keyword Review</h1>
    <div class="sub">Negative review + Winners scaling · Search term data last 30 days</div>
  </div>
  <div style="display:flex;gap:8px;align-items:center">
    <button class="btn btn-ghost" onclick="loadData(false)" id="refresh-btn">🔄 Load Data</button>
    <button class="btn btn-ghost" onclick="loadData(true)" id="force-btn" style="font-size:11px;color:#64748b">↻ Force Refresh</button>
  </div>
</div>

<!-- CONTROLS BAR -->
<div class="controls-bar">
  <div class="threshold-wrap">
    <label>ACoS Threshold %</label>
    <input type="number" id="acos-threshold" value="30" min="1" max="500" step="5" onchange="applyThreshold()">
    <span class="threshold-hint">↑ Negatives · ↓ Winners</span>
  </div>
  <span id="date-badge" style="font-size:11px;color:#64748b;background:#fff;border:1px solid var(--b);border-radius:8px;padding:6px 12px"></span>
</div>

<!-- KPI ROW -->
<div class="kpi-row">
  <div class="kpi"><div class="big red"  id="kpi-negs">—</div><div class="lbl">🚫 Negatives</div></div>
  <div class="kpi"><div class="big amber" id="kpi-waste">—</div><div class="lbl">Wasted Spend</div></div>
  <div class="kpi"><div class="big green" id="kpi-wins">—</div><div class="lbl">🚀 Winners</div></div>
  <div class="kpi"><div class="big blue"  id="kpi-rev" style="font-size:14px">—</div><div class="lbl">Winners Revenue</div></div>
</div>

<!-- TABS -->
<div class="tabs">
  <button class="tab-btn active" onclick="switchTab(1,this)" id="tab-btn-1">🚫 Negative Review <span id="tab1-count"></span></button>
  <button class="tab-btn" onclick="switchTab(2,this)" id="tab-btn-2">🚀 Scale Winners <span id="tab2-count"></span></button>
  <button class="tab-btn" onclick="switchTab(3,this)" id="tab-btn-3">🎯 Self-Target</button>
</div>

<!-- TAB 1: NEGATIVE REVIEW -->
<div class="tab-panel active" id="tab-1">
  <div class="toolbar">
    <div class="toolbar-left">
      <span class="sel-count" id="sel-count"></span>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="btn btn-ghost" onclick="toggleAll(true)">✅ Select All</button>
      <button class="btn btn-ghost" onclick="toggleAll(false)">☐ Deselect All</button>
      <button class="btn btn-danger" onclick="applyNegatives()" id="apply-btn" disabled>
        🚫 Apply Negatives
      </button>
    </div>
  </div>
  <div id="neg-table-container">
    <div class="loading"><span class="spinner"></span>Loading…</div>
  </div>
</div>

<!-- TAB 2: SCALE WINNERS -->
<div class="tab-panel" id="tab-2">
  <div class="winners-controls">
    <div class="bid-wrap">
      <label>Bid per keyword ₹</label>
      <input type="number" id="bid-input" value="10" min="1" max="9999" step="1">
    </div>
    <button class="btn btn-ghost" onclick="toggleAllWinners(true)">✅ Select All Gaps</button>
    <button class="btn btn-ghost" onclick="toggleAllWinners(false)">☐ Deselect All</button>
    <button class="btn btn-green" onclick="createExactKeywords()" id="create-btn" disabled>
      ➕ Create Exact Keywords
    </button>
    <button class="btn btn-ghost" onclick="exportWinnersCSV()" id="csv-btn" style="display:none">
      📥 Export CSV
    </button>
  </div>
  <div id="win-table-container">
    <div class="loading"><span class="spinner"></span>Loading…</div>
  </div>
</div>

<!-- TAB 3: SELF-TARGET -->
<div class="tab-panel" id="tab-3">
  <div class="winners-controls" style="flex-wrap:wrap;gap:10px">
    <div class="bid-wrap">
      <label>Bid per target ₹</label>
      <input type="number" id="st-bid" value="5" min="1" max="9999" step="1">
    </div>
    <div class="bid-wrap">
      <label>Daily budget ₹</label>
      <input type="number" id="st-budget" value="100" min="10" max="99999" step="10">
    </div>
    <button class="btn btn-ghost" onclick="loadSelfTargetAsins()">🔄 Refresh ASINs</button>
    <button class="btn btn-ghost" onclick="stToggleAll(true)">✅ Select All</button>
    <button class="btn btn-ghost" onclick="stToggleAll(false)">☐ Deselect All</button>
    <button class="btn btn-green" id="st-create-btn" onclick="showStPreview()" disabled>
      🎯 Preview &amp; Create
    </button>
  </div>

  <!-- Manual ASIN entry -->
  <div style="background:#fff;border:1px solid var(--b);border-radius:10px;padding:14px 18px;margin:12px 0;display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap">
    <div>
      <label style="font-size:11px;font-weight:700;color:#64748b;display:block;margin-bottom:4px">Add ASIN manually</label>
      <input id="st-manual-asin" type="text" placeholder="B0XXXXXXXX"
             style="padding:7px 10px;border:1px solid var(--b);border-radius:6px;font-size:13px;width:180px">
    </div>
    <div>
      <label style="font-size:11px;font-weight:700;color:#64748b;display:block;margin-bottom:4px">Account</label>
      <select id="st-manual-profile" style="padding:7px 10px;border:1px solid var(--b);border-radius:6px;font-size:13px">
        <option value="" disabled selected>— choose —</option>
      </select>
    </div>
    <button class="btn btn-ghost" onclick="stAddManual()" style="margin-bottom:1px">➕ Add</button>
  </div>

  <div id="st-table-container">
    <div class="loading"><span class="spinner"></span>Loading ASINs…</div>
  </div>
</div>

</div><!-- .page -->

<!-- SELF-TARGET PREVIEW MODAL -->
<div class="modal-overlay" id="st-modal" onclick="closeStModal(event)">
  <div class="modal" style="max-width:700px">
    <div class="modal-header">
      <div>
        <h2>🎯 Self-Target Preview</h2>
        <div class="sub">Review what will be created — 1 campaign + 1 ad group + 1 product ad + 1 target per ASIN</div>
      </div>
      <button class="modal-close" onclick="document.getElementById('st-modal').classList.remove('open')">✕</button>
    </div>
    <div style="padding:0 20px 8px;color:#64748b;font-size:12px">
      Bid: <strong id="st-preview-bid">—</strong> &nbsp;·&nbsp; Daily budget: <strong id="st-preview-budget">—</strong>
    </div>
    <div style="overflow-y:auto;max-height:420px;padding:0 20px 16px">
      <table id="st-preview-table" style="font-size:12px">
        <thead><tr>
          <th>ASIN</th>
          <th>Campaign name</th>
          <th>Ad group</th>
          <th>Target type</th>
          <th>Account</th>
        </tr></thead>
        <tbody id="st-preview-body"></tbody>
      </table>
    </div>
    <div style="padding:12px 20px 20px;display:flex;gap:10px;justify-content:flex-end">
      <button class="btn btn-ghost" onclick="document.getElementById('st-modal').classList.remove('open')">Cancel</button>
      <button class="btn btn-green" id="st-confirm-btn" onclick="createSelfTargets()">🎯 Create Campaigns</button>
    </div>
  </div>
</div>

<!-- BID MODAL -->
<div class="modal-overlay" id="bid-modal" onclick="closeBidModal(event)">
  <div class="modal">
    <div class="modal-header">
      <div>
        <h2>➕ Set Bids — Create Exact Keywords</h2>
        <div class="sub">Review each keyword and set individual bids before going live · These will be added to the same ad group where each term was found</div>
      </div>
      <button class="modal-close" onclick="document.getElementById('bid-modal').classList.remove('open')">✕</button>
    </div>
    <div class="modal-bulk">
      <label>Set all bids to ₹</label>
      <input type="number" id="bulk-bid" min="1" max="9999" step="1" placeholder="e.g. 15" oninput="applyBulkBid(this.value)">
      <span style="font-size:11px;color:#94a3b8">— or adjust each row individually below</span>
    </div>
    <div class="modal-scroll">
      <table>
        <thead><tr>
          <th>Search Term</th>
          <th>Campaign / Ad Group</th>
          <th class="center">Clicks</th>
          <th class="center">Orders</th>
          <th class="center">ACoS</th>
          <th class="center">Account</th>
          <th class="center" style="width:110px">Bid ₹</th>
        </tr></thead>
        <tbody id="modal-rows"></tbody>
      </table>
    </div>
    <div class="modal-foot">
      <button class="btn btn-ghost" onclick="document.getElementById('bid-modal').classList.remove('open')">Cancel</button>
      <button class="btn btn-green" onclick="confirmCreateKeywords()" id="confirm-create-btn">✅ Confirm &amp; Create</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
let allTerms   = [];   // all terms with spend >= MIN_SPEND
let kwData     = {};   // {profileId: {EXACT:[...], PHRASE:[...], BROAD:[...]}}
let negTerms   = [];   // computed from allTerms + threshold
let winTerms   = [];   // computed from allTerms + threshold
let negChecked = {};   // id → bool (negatives tab)
let winOverrides = {};  // id → bool: only EXPLICIT user toggles; default derived from coverage

let _pollTimer = null;
const BRAND_KEYWORDS = ['menhood'];

function isBrand(term) {
  const t = (term || '').toLowerCase();
  return BRAND_KEYWORDS.some(b => t.includes(b));
}

function getThreshold() {
  return parseFloat(document.getElementById('acos-threshold').value) || 30;
}

// ── Coverage check ─────────────────────────────────────────────────────────
// Normalise a keyword: strip modified-broad + signs, collapse whitespace
function normKw(kw) {
  return (kw || '').replace(/\+/g, ' ').trim().replace(/\s+/g, ' ');
}

// Word-stem helper: "trimmer" matches "trimmers", "beard" matches "beards", etc.
function wMatch(a, b) {
  if (a === b) return true;
  if (b.startsWith(a) && b.length <= a.length + 3) return true;
  if (a.startsWith(b) && a.length <= b.length + 3) return true;
  return false;
}

function checkCoverage(term, profileId) {
  const t = (term || '').toLowerCase().trim();
  const tWords = t.split(/\s+/);
  const sets = kwData[profileId] || {EXACT: [], PHRASE: [], BROAD: []};

  // Exact: search term == keyword exactly
  const exact = sets.EXACT.some(kw => kw && normKw(kw) === t);

  // Phrase: all keyword words appear consecutively in the search term (stem-aware)
  const phrase = sets.PHRASE.some(kw => {
    if (!kw) return false;
    const kwWords = normKw(kw).split(/\s+/).filter(Boolean);
    if (!kwWords.length) return false;
    for (let i = 0; i <= tWords.length - kwWords.length; i++) {
      if (kwWords.every((w, j) => wMatch(w, tWords[i + j]))) return true;
    }
    return false;
  });

  // Broad: every keyword word appears somewhere in the search term (any order, stem-aware)
  // Using normKw to strip + signs from old modified-broad keywords in the cache
  const broad = sets.BROAD.some(kw => {
    if (!kw) return false;
    const kwWords = normKw(kw).split(/\s+/).filter(Boolean);
    if (!kwWords.length) return false;
    return kwWords.every(kw_w => tWords.some(t_w => wMatch(kw_w, t_w)));
  });

  return {exact, phrase, broad};
}

// Default checked state: checked unless the term already has exact coverage.
// winOverrides stores only explicit user clicks and trumps the default.
function isWinChecked(t) {
  if (t.id in winOverrides) return winOverrides[t.id];
  // Prefer server-computed coverage (always correct); fall back to client-side check
  if (typeof t.has_exact !== 'undefined') return !t.has_exact;
  const cov = checkCoverage(t.searchTerm, t.profile);
  return !cov.exact;  // checked = true when exact coverage is MISSING
}

// ── Split terms into negatives vs winners based on threshold ────────────────
function splitTerms() {
  const thr = getThreshold();
  negTerms = allTerms.filter(t => t.orders === 0 || (t.acos !== null && t.acos > thr));
  winTerms = allTerms.filter(t => t.orders > 0 && t.clicks >= 3 && t.acos !== null && t.acos <= thr);
  // Initialise checked state for any new terms
  negTerms.forEach(t => {
    if (!(t.id in negChecked)) negChecked[t.id] = !isBrand(t.searchTerm);
  });
  // winOverrides stores only explicit user toggles; checked default is computed from coverage
}

function applyThreshold() {
  if (!allTerms.length) return;
  splitTerms();
  updateKPIs();
  renderNegTable();
  renderWinTable();
}

// ── KPIs ───────────────────────────────────────────────────────────────────
function updateKPIs() {
  const waste = negTerms.reduce((s, t) => s + t.spend, 0);
  const rev   = winTerms.reduce((s, t) => s + t.sales, 0);
  document.getElementById('kpi-negs').textContent  = negTerms.length;
  document.getElementById('kpi-waste').textContent = '₹' + waste.toLocaleString('en-IN', {maximumFractionDigits:0});
  document.getElementById('kpi-wins').textContent  = winTerms.length;
  document.getElementById('kpi-rev').textContent   = '₹' + rev.toLocaleString('en-IN', {maximumFractionDigits:0});
  document.getElementById('tab1-count').textContent = `(${negTerms.length})`;
  document.getElementById('tab2-count').textContent = `(${winTerms.length})`;
}

// ── NEGATIVE REVIEW TABLE ─────────────────────────────────────────────────
function renderNegTable() {
  const selected = negTerms.filter(t => negChecked[t.id]);
  const selWaste = selected.reduce((s, t) => s + t.spend, 0);
  document.getElementById('sel-count').textContent =
    `${selected.length} selected · ₹${selWaste.toLocaleString('en-IN',{maximumFractionDigits:0})} to remove`;
  document.getElementById('apply-btn').disabled = selected.length === 0;

  if (!negTerms.length) {
    document.getElementById('neg-table-container').innerHTML =
      '<div class="empty-state"><div class="ico">✅</div>No flagged terms at this threshold — account looks clean!</div>';
    return;
  }

  const thr = getThreshold();
  const rows = negTerms.map(t => {
    const brand = isBrand(t.searchTerm);
    const checked = negChecked[t.id];
    const rowCls = brand ? 'brand-row' : (checked ? '' : 'unchecked');
    const acos_str = t.acos === null
      ? '<span class="acos-inf">∞ (0 orders)</span>'
      : t.acos >= thr ? `<span class="acos-bad">${t.acos.toFixed(0)}%</span>`
      : `<span class="acos-high">${t.acos.toFixed(0)}%</span>`;
    const badge = t.orders === 0
      ? '<span class="badge badge-zero">0 orders</span>'
      : '<span class="badge badge-high">High ACoS</span>';
    const brandBadge = brand ? ' <span class="badge badge-brand">Brand</span>' : '';
    const chk = brand ? 'disabled' : (checked ? 'checked' : '');
    return `<tr class="${rowCls}">
      <td class="center"><input type="checkbox" ${chk} onchange="toggleNeg('${t.id}')" style="width:15px;height:15px;cursor:pointer"></td>
      <td class="mono">${escHtml(t.searchTerm)}${brandBadge}</td>
      <td style="font-size:11px;color:#64748b;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(t.campaignName)}</td>
      <td style="font-size:11px;color:#94a3b8">${escHtml(t.adGroupName)}</td>
      <td class="num red" style="font-weight:700">₹${t.spend.toLocaleString('en-IN')}</td>
      <td class="num" style="color:#64748b">${cpcStr(t)}</td>
      <td class="num">${t.clicks ?? 0}</td>
      <td class="num">${t.orders}</td>
      <td class="center">${acos_str}</td>
      <td class="center">${badge}</td>
      <td class="center" style="font-size:10px;color:#94a3b8">${t.accountLabel}</td>
    </tr>`;
  }).join('');

  document.getElementById('neg-table-container').innerHTML = `
    <table>
      <thead><tr>
        <th class="center" style="width:36px">✓</th>
        <th>Search Term</th><th>Campaign</th><th>Ad Group</th>
        <th style="text-align:right">Spend</th>
        <th style="text-align:right">CPC</th>
        <th style="text-align:right">Clicks</th>
        <th style="text-align:right">Orders</th>
        <th class="center">ACoS</th>
        <th class="center">Reason</th>
        <th class="center">Account</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function toggleNeg(id) {
  negChecked[id] = !negChecked[id];
  renderNegTable();
}

function toggleAll(val) {
  negTerms.forEach(t => { if (!isBrand(t.searchTerm)) negChecked[t.id] = val; });
  renderNegTable();
}

async function applyNegatives() {
  const selected = negTerms.filter(t => negChecked[t.id]);
  if (!selected.length) return;
  const confirmed = confirm(`Add ${selected.length} terms as NEGATIVE EXACT match?\\n\\nThis cannot be undone. Continue?`);
  if (!confirmed) return;

  const btn = document.getElementById('apply-btn');
  btn.disabled = true; btn.textContent = '⏳ Applying...';

  const r = await fetch('/api/apply', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({terms: selected, dry_run: false})
  });
  const d = await r.json();
  showToast(d.status === 'applied'
    ? `✅ ${d.added} negatives added successfully!`
    : `❌ Error: ${JSON.stringify(d)}`);
  btn.textContent = '🚫 Apply Negatives';
  btn.disabled = selected.length === 0;
}

// ── WINNERS TABLE ──────────────────────────────────────────────────────────
function renderWinTable() {
  const hasKw = Object.keys(kwData).length > 0;
  const selectedCount = winTerms.filter(t => isWinChecked(t)).length;
  const createBtn = document.getElementById('create-btn');
  const csvBtn    = document.getElementById('csv-btn');
  createBtn.disabled = selectedCount === 0;
  if (csvBtn) csvBtn.style.display = winTerms.length ? 'inline-flex' : 'none';

  if (!winTerms.length) {
    document.getElementById('win-table-container').innerHTML =
      '<div class="empty-state"><div class="ico">🎯</div>No winners at this threshold.<br>Try raising the ACoS % or force-refreshing data.</div>';
    return;
  }

  const noKwNote = hasKw ? '' :
    '<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 14px;margin-bottom:10px;font-size:12px;color:#92400e">⚠️ Keyword coverage data not available — force-refresh to load it.</div>';

  const rows = winTerms.map(t => {
    const cov = hasKw ? checkCoverage(t.searchTerm, t.profile) : {exact:false,phrase:false,broad:false};
    const checked = isWinChecked(t);
    const cvCell = (v) => v
      ? '<span class="cov-ok cv" title="Already targeted">✅</span>'
      : '<span class="cov-no cv" title="Gap — not targeted">❌</span>';
    return `<tr>
      <td class="center"><input type="checkbox" ${checked?'checked':''} onchange="toggleWin('${t.id}')" style="width:15px;height:15px;cursor:pointer"></td>
      <td class="mono">${escHtml(t.searchTerm)}</td>
      <td style="font-size:11px;color:#64748b;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(t.campaignName)}</td>
      <td style="font-size:11px;color:#94a3b8">${escHtml(t.adGroupName)}</td>
      <td class="num">${t.clicks}</td>
      <td class="num" style="color:#64748b">${cpcStr(t)}</td>
      <td class="num">${t.orders}</td>
      <td class="num">₹${t.sales.toLocaleString('en-IN')}</td>
      <td class="center"><span class="acos-good">${t.acos.toFixed(0)}%</span></td>
      <td>${cvCell(cov.exact)}</td>
      <td>${cvCell(cov.phrase)}</td>
      <td>${cvCell(cov.broad)}</td>
      <td class="center" style="font-size:10px;color:#94a3b8">${t.accountLabel}</td>
    </tr>`;
  }).join('');

  document.getElementById('win-table-container').innerHTML = noKwNote + `
    <table>
      <thead><tr>
        <th class="center" style="width:36px">✓</th>
        <th>Search Term</th><th>Campaign</th><th>Ad Group</th>
        <th style="text-align:right">Clicks</th>
        <th style="text-align:right">CPC</th>
        <th style="text-align:right">Orders</th>
        <th style="text-align:right">Revenue</th>
        <th class="center">ACoS</th>
        <th class="center" title="Exact match coverage">EXACT</th>
        <th class="center" title="Phrase match coverage">PHRASE</th>
        <th class="center" title="Broad match coverage">BROAD</th>
        <th class="center">Account</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function toggleWin(id) {
  const t = winTerms.find(x => x.id === id);
  winOverrides[id] = !isWinChecked(t);
  renderWinTable();
}

function toggleAllWinners(val) {
  if (val) {
    // "Select All Gaps": only check terms that DON'T already have exact coverage
    winTerms.forEach(t => {
      const cov = checkCoverage(t.searchTerm, t.profile);
      winOverrides[t.id] = !cov.exact;
    });
  } else {
    // "Deselect All": clear all overrides and let coverage-based default apply,
    // but also explicitly uncheck everything so nothing is selected
    winTerms.forEach(t => { winOverrides[t.id] = false; });
  }
  renderWinTable();
}

function createExactKeywords() {
  const selected = winTerms.filter(t => isWinChecked(t));
  if (!selected.length) return;
  const defaultBid = parseFloat(document.getElementById('bid-input').value) || 10;

  // Build modal rows
  const rows = selected.map((t, i) => `
    <tr>
      <td class="mono" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(t.searchTerm)}</td>
      <td style="font-size:11px;color:#64748b">
        <div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:220px">${escHtml(t.campaignName)}</div>
        <div style="color:#94a3b8;margin-top:1px">${escHtml(t.adGroupName)}</div>
      </td>
      <td class="center">${t.clicks}</td>
      <td class="center">${t.orders}</td>
      <td class="center"><span class="acos-good">${t.acos.toFixed(0)}%</span></td>
      <td class="center" style="font-size:10px;color:#94a3b8">${t.accountLabel}</td>
      <td class="center">
        <input type="number" class="modal-bid-input" data-idx="${i}"
               value="${defaultBid}" min="1" max="9999" step="1"
               style="width:80px;border:1px solid var(--b);border-radius:6px;padding:4px 8px;font-size:13px;font-weight:700;text-align:center;outline:none">
      </td>
    </tr>`).join('');

  document.getElementById('modal-rows').innerHTML = rows;
  document.getElementById('bulk-bid').value = defaultBid;
  document.getElementById('bid-modal').classList.add('open');
}

function closeBidModal(e) {
  // Close only if clicking the dark overlay itself (not the modal card)
  if (e.target === document.getElementById('bid-modal')) {
    document.getElementById('bid-modal').classList.remove('open');
  }
}

function applyBulkBid(val) {
  const v = parseFloat(val);
  if (!v || v < 1) return;
  document.querySelectorAll('.modal-bid-input').forEach(inp => inp.value = v);
}

async function confirmCreateKeywords() {
  const selected = winTerms.filter(t => isWinChecked(t));
  const inputs   = document.querySelectorAll('.modal-bid-input');
  const termsWithBids = selected.map((t, i) => ({
    ...t,
    bid: parseFloat(inputs[i]?.value) || 10,
  }));

  const btn = document.getElementById('confirm-create-btn');
  btn.disabled = true; btn.textContent = '⏳ Creating...';

  try {
    const r = await fetch('/api/add_keywords', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({terms: termsWithBids})
    });
    const d = await r.json();
    document.getElementById('bid-modal').classList.remove('open');
    if (d.errors > 0) {
      showToast(`⚠️ ${d.added} created · ${d.errors} errors`);
    } else {
      showToast(`✅ ${d.added} exact keywords created!`);
    }
  } catch(e) {
    showToast('❌ Error: ' + e.message);
  }

  btn.disabled = false; btn.textContent = '✅ Confirm & Create';
  document.getElementById('create-btn').disabled = winTerms.filter(t => isWinChecked(t)).length === 0;
}

function exportWinnersCSV() {
  if (!winTerms.length) return;
  const hasKw = Object.keys(kwData).length > 0;
  const headers = ['searchTerm','campaignName','adGroupName','clicks','orders','sales','acos','accountLabel','exact','phrase','broad'];
  const rows = winTerms.map(t => {
    const cov = hasKw ? checkCoverage(t.searchTerm, t.profile) : {exact:'',phrase:'',broad:''};
    return [
      t.searchTerm, t.campaignName, t.adGroupName,
      t.clicks, t.orders, t.sales, t.acos,
      t.accountLabel,
      hasKw ? (cov.exact?'yes':'no') : '',
      hasKw ? (cov.phrase?'yes':'no') : '',
      hasKw ? (cov.broad?'yes':'no') : '',
    ].map(v => `"${String(v ?? '').replace(/"/g,'""')}"`).join(',');
  });
  const csv = [headers.join(','), ...rows].join('\\n');
  const blob = new Blob([csv], {type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'menhood_winners.csv';
  a.click();
}

// ── TAB SWITCHING ──────────────────────────────────────────────────────────
let stLoaded = false;
function switchTab(n, btn) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + n).classList.add('active');
  btn.classList.add('active');
  if (n === 3 && !stLoaded) { stLoaded = true; loadSelfTargetAsins(); }
}

// ── DATA LOADING ───────────────────────────────────────────────────────────
function setLoadingMsg(msg) {
  document.getElementById('neg-table-container').innerHTML =
    `<div class="loading"><span class="spinner"></span>${msg}</div>`;
  document.getElementById('win-table-container').innerHTML =
    `<div class="loading"><span class="spinner"></span>${msg}</div>`;
}

function setButtons(loading) {
  document.getElementById('refresh-btn').disabled = loading;
  const fb = document.getElementById('force-btn');
  if (fb) fb.disabled = loading;
}

function displayData(d) {
  allTerms = d.terms || [];
  kwData   = d.kw_data || {};
  const dateEl = document.getElementById('date-badge');
  dateEl.textContent = '📅 ' + d.date;
  if (d.cached !== undefined) {
    const badge = d.cached
      ? '<span style="background:#dcfce7;color:#16a34a;font-size:10px;padding:1px 6px;border-radius:8px;font-weight:700;margin-left:6px">CACHED</span>'
      : '<span style="background:#eff6ff;color:#2563eb;font-size:10px;padding:1px 6px;border-radius:8px;font-weight:700;margin-left:6px">LIVE</span>';
    dateEl.innerHTML = '📅 ' + d.date + badge;
  }
  setButtons(false);
  // winOverrides is intentionally NOT reset on data load — isWinChecked() derives defaults from coverage
  splitTerms();
  updateKPIs();
  renderNegTable();
  renderWinTable();
}

async function pollStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    if (d.status === 'ready') {
      clearTimeout(_pollTimer);
      displayData(d);
    } else if (d.status === 'error') {
      clearTimeout(_pollTimer);
      setLoadingMsg(`❌ Fetch error: ${d.error || 'Unknown error'}`);
      setButtons(false);
    } else {
      const elapsed = d.elapsed || 0;
      const mins = Math.floor(elapsed / 60), secs = elapsed % 60;
      const t = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
      setLoadingMsg(`Pulling data from Amazon Ads API… (${t} elapsed — usually 3–6 min)`);
      _pollTimer = setTimeout(pollStatus, 5000);
    }
  } catch(e) {
    _pollTimer = setTimeout(pollStatus, 5000);
  }
}

async function loadData(force=false) {
  clearTimeout(_pollTimer);
  setButtons(true);
  setLoadingMsg(force ? 'Requesting fresh data from Amazon Ads API…' : 'Loading…');

  try {
    const r = await fetch(force ? '/api/fetch?force=true' : '/api/fetch');
    const d = await r.json();
    if (d.status === 'ready') {
      displayData(d);
    } else if (d.status === 'loading') {
      setLoadingMsg('Pulling data from Amazon Ads API… (0s elapsed — usually 3–6 min)');
      _pollTimer = setTimeout(pollStatus, 5000);
    } else {
      setLoadingMsg(`❌ ${d.error || 'Unknown error'}`);
      setButtons(false);
    }
  } catch(e) {
    setLoadingMsg('❌ Could not reach server');
    setButtons(false);
  }
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 5000);
}

function escHtml(s) {
  return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function cpcStr(t) {
  // Use stored cpc if present, otherwise compute from spend/clicks (handles old cache)
  const v = (t.cpc != null) ? t.cpc : (t.clicks > 0 ? t.spend / t.clicks : null);
  return v != null ? `₹${v.toFixed(2)}` : '—';
}

loadData();

// ── SELF-TARGET TAB ───────────────────────────────────────────────────────────
function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
let stAsins    = [];    // [{asin, sku, campaignName, adGroupName, profile, profileLabel, checked, manual}]
let stProfiles = {};    // pid → label

async function loadSelfTargetAsins() {
  document.getElementById('st-table-container').innerHTML =
    '<div class="loading"><span class="spinner"></span>Fetching active ASINs from Amazon…</div>';
  document.getElementById('st-create-btn').disabled = true;

  try {
    const r = await fetch('/api/self_target/asins');
    const d = await r.json();
    if (d.error) throw new Error(d.error);

    stProfiles = {};
    stAsins = d.asins.map(a => {
      stProfiles[a.profile] = a.profileLabel;
      return {...a, checked: true, manual: false};
    });

    _populateProfileSelect();
    renderStTable();
    if (d.errors && d.errors.length)
      showToast('⚠️ ' + d.errors.join(' | '));
  } catch(e) {
    document.getElementById('st-table-container').innerHTML =
      `<div class="loading" style="color:red">Error: ${e.message}</div>`;
  }
}

function _populateProfileSelect() {
  const sel = document.getElementById('st-manual-profile');
  sel.innerHTML = '<option value="" disabled selected>— choose —</option>';
  for (const [pid, label] of Object.entries(stProfiles))
    sel.innerHTML += `<option value="${pid}">${label}</option>`;
}

function stAddManual() {
  const asin = document.getElementById('st-manual-asin').value.trim().toUpperCase();
  const pid  = document.getElementById('st-manual-profile').value;
  if (!asin || !pid) { showToast('Enter an ASIN and choose an account'); return; }
  if (!/^B0[A-Z0-9]{8}$/.test(asin)) { showToast('ASIN format looks wrong — should be B0 + 8 chars'); return; }
  if (stAsins.find(a => a.asin === asin && a.profile === pid)) {
    showToast('That ASIN is already in the list'); return;
  }
  stAsins.push({asin, sku: '', campaignName: '', adGroupName: '',
                profile: pid, profileLabel: stProfiles[pid] || pid, checked: true, manual: true});
  document.getElementById('st-manual-asin').value = '';
  renderStTable();
}

function stToggleAll(val) {
  stAsins.forEach(a => a.checked = val);
  renderStTable();
}

function stToggle(i) {
  stAsins[i].checked = !stAsins[i].checked;
  renderStTable();
}

function stRemove(i) {
  stAsins.splice(i, 1);
  renderStTable();
}

function renderStTable() {
  const sel = stAsins.filter(a => a.checked).length;
  document.getElementById('st-create-btn').disabled = sel === 0;

  if (!stAsins.length) {
    document.getElementById('st-table-container').innerHTML =
      '<div class="loading" style="color:#64748b">No active ASINs found. Add manually above or click Refresh.</div>';
    return;
  }

  const rows = stAsins.map((a, i) => `
    <tr class="${a.checked ? '' : 'unchecked'}">
      <td class="center"><input type="checkbox" ${a.checked?'checked':''} onchange="stToggle(${i})" style="width:16px;height:16px;cursor:pointer"></td>
      <td class="mono" style="font-weight:700">${a.asin}</td>
      <td class="mono" style="font-size:11px;color:#64748b">${escHtml(a.sku || '—')}</td>
      <td style="font-size:11px;color:#64748b;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(a.campaignName || '—')}</td>
      <td style="font-size:11px;color:#94a3b8;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escHtml(a.adGroupName || '—')}</td>
      <td style="font-size:11px;color:#64748b">${a.profileLabel}</td>
      <td class="center" style="font-size:11px;color:#94a3b8">${a.manual ? '✏️ Manual' : '🔄 Auto'}</td>
      <td class="center">
        <button onclick="stRemove(${i})" style="background:none;border:none;cursor:pointer;color:#dc2626;font-size:14px;padding:2px 6px" title="Remove">✕</button>
      </td>
    </tr>`).join('');

  document.getElementById('st-table-container').innerHTML = `
    <div style="font-size:12px;color:#64748b;margin-bottom:8px">${stAsins.length} ASINs loaded · ${sel} selected</div>
    <table>
      <thead><tr>
        <th class="center" style="width:40px">✓</th>
        <th>ASIN</th>
        <th>SKU</th>
        <th>Campaign</th>
        <th>Ad Group</th>
        <th>Account</th>
        <th class="center">Source</th>
        <th class="center" style="width:50px"></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function showStPreview() {
  const selected = stAsins.filter(a => a.checked);
  if (!selected.length) return;
  const bid    = parseFloat(document.getElementById('st-bid').value) || 5;
  const budget = parseFloat(document.getElementById('st-budget').value) || 100;
  document.getElementById('st-preview-bid').textContent    = '₹' + bid;
  document.getElementById('st-preview-budget').textContent = '₹' + budget + '/day';
  const rows = selected.map(a => `<tr>
    <td class="mono" style="font-weight:700">${a.asin}</td>
    <td style="font-size:11px">SP|Self-Target|${a.asin}</td>
    <td style="font-size:11px">Self|${a.asin}</td>
    <td style="font-size:11px;color:#2563eb">asinSameAs</td>
    <td style="font-size:11px;color:#64748b">${a.profileLabel}</td>
  </tr>`).join('');
  document.getElementById('st-preview-body').innerHTML = rows;
  document.getElementById('st-modal').classList.add('open');
}

function closeStModal(e) {
  if (e.target === document.getElementById('st-modal'))
    document.getElementById('st-modal').classList.remove('open');
}

async function createSelfTargets() {
  const selected = stAsins.filter(a => a.checked);
  if (!selected.length) return;
  const bid    = parseFloat(document.getElementById('st-bid').value) || 5;
  const budget = parseFloat(document.getElementById('st-budget').value) || 100;

  const btn = document.getElementById('st-confirm-btn');
  btn.disabled = true;
  btn.textContent = '⏳ Creating…';

  try {
    const r = await fetch('/api/self_target/create', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        items: selected.map(a => ({asin: a.asin, profile: a.profile})),
        bid, daily_budget: budget
      })
    });
    const d = await r.json();
    document.getElementById('st-modal').classList.remove('open');

    if (d.errors > 0)
      showToast(`⚠️ ${d.created} created, ${d.errors} failed. Check console for details.`);
    else
      showToast(`✅ ${d.created} self-targeting campaign(s) created!`);

    // Uncheck successfully created ASINs
    const done = new Set((d.detail.success || []).map(s => s.asin));
    stAsins.forEach(a => { if (done.has(a.asin)) a.checked = false; });
    renderStTable();
  } catch(e) {
    showToast('❌ Error: ' + e.message);
  }
  btn.disabled = false;
  btn.textContent = '🎯 Create Campaigns';
}
</script>
</body>
</html>"""

# ── STARTUP PRE-WARM ──────────────────────────────────────────────────────────
_PREFETCH_LOCK = "/tmp/negkw_prefetch.lock"

def _startup_prefetch():
    """On startup: fetch Amazon data in background so it's ready when users arrive."""
    time.sleep(5)
    if load_cache():
        return
    try:
        fd = os.open(_PREFETCH_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        return
    try:
        _start_fetch()
        for _ in range(120):
            time.sleep(15)
            with _lock:
                done = _status["state"] in ("ready", "error")
            if done or load_cache():
                break
    finally:
        try:
            os.remove(_PREFETCH_LOCK)
        except Exception:
            pass

threading.Thread(target=_startup_prefetch, daemon=True).start()

if __name__ == "__main__":
    print("\n🚀 Menhood Ads Review App")
    print("   Open: http://localhost:5050\n")
    app.run(port=5050, debug=False)
