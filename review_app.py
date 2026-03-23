"""
Negative Keyword Review App
Run: python3 review_app.py
Open: http://localhost:5050
Rajesh reviews flagged terms, unchecks any to keep, clicks Apply.
"""

from flask import Flask, render_template_string, jsonify, request
import requests, json, time, gzip, threading, os
from datetime import datetime, timedelta

app = Flask(__name__)

# ── CONFIG ────────────────────────────────────────────────────────────────────
CLIENT_ID     = os.getenv("AMAZON_ADS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("AMAZON_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("AMAZON_ADS_REFRESH_TOKEN", "")

PROFILES = {
    "3016627615357133": "Account A (Postpaid)",
    "154697331411051":  "Account B (Prepaid)",
}
EU_API     = "https://advertising-api-eu.amazon.com"
MIN_SPEND  = 500
MAX_ACOS   = 0.30
LOOKBACK   = 30

# ── API HELPERS ───────────────────────────────────────────────────────────────
def get_token():
    if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
        raise RuntimeError(
            "Missing Amazon Ads credentials. Set AMAZON_ADS_CLIENT_ID, "
            "AMAZON_ADS_CLIENT_SECRET, and AMAZON_ADS_REFRESH_TOKEN."
        )
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
    })
    return r.json()["access_token"]

def fetch_flagged_terms(token, profile_id):
    end   = datetime.today() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK)
    headers = {"Amazon-Advertising-API-ClientId": CLIENT_ID,
               "Amazon-Advertising-API-Scope": profile_id,
               "Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "name": f"NegKW Review {end.strftime('%Y-%m-%d')}",
        "startDate": start.strftime("%Y-%m-%d"), "endDate": end.strftime("%Y-%m-%d"),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS", "groupBy": ["searchTerm"],
            "columns": ["campaignId","campaignName","adGroupId","adGroupName",
                        "searchTerm","impressions","clicks","cost","purchases7d","sales7d"],
            "reportTypeId": "spSearchTerm", "timeUnit": "SUMMARY", "format": "GZIP_JSON",
        }
    }
    rr = requests.post(f"{EU_API}/reporting/reports", headers=headers, json=body)
    rid = rr.json().get("reportId")
    if not rid: return []

    for _ in range(30):
        time.sleep(10)
        rs = requests.get(f"{EU_API}/reporting/reports/{rid}", headers=headers).json()
        if rs.get("status") == "COMPLETED":
            data = json.loads(gzip.decompress(requests.get(rs["url"]).content))
            flagged = []
            for row in data:
                spend  = row.get("cost", 0)
                sales  = row.get("sales7d", 0)
                orders = int(row.get("purchases7d", 0))
                acos   = spend / sales if sales > 0 else None
                if spend >= MIN_SPEND and (orders == 0 or (acos and acos >= MAX_ACOS)):
                    flagged.append({
                        "id":           f"{row.get('campaignId')}_{row.get('adGroupId')}_{row.get('searchTerm','')}",
                        "searchTerm":   row.get("searchTerm", ""),
                        "campaignId":   str(row.get("campaignId", "")),
                        "campaignName": row.get("campaignName", ""),
                        "adGroupId":    str(row.get("adGroupId", "")),
                        "adGroupName":  row.get("adGroupName", ""),
                        "spend":        round(spend, 0),
                        "sales":        round(sales, 0),
                        "orders":       orders,
                        "acos":         round(acos * 100, 0) if acos else None,
                        "profile":      profile_id,
                    })
            return sorted(flagged, key=lambda x: x["spend"], reverse=True)
        elif rs.get("status") == "FAILED":
            return []
    return []

# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/fetch")
def api_fetch():
    token = get_token()
    all_terms = []
    for pid, label in PROFILES.items():
        terms = fetch_flagged_terms(token, pid)
        for t in terms:
            t["accountLabel"] = label
        all_terms.extend(terms)
    total_waste = sum(t["spend"] for t in all_terms)
    return jsonify({"terms": all_terms, "total_waste": total_waste,
                    "count": len(all_terms), "date": datetime.today().strftime("%d %b %Y")})

@app.route("/api/apply", methods=["POST"])
def api_apply():
    data     = request.json
    approved = data.get("terms", [])  # only checked terms
    dry_run  = data.get("dry_run", True)

    if dry_run:
        return jsonify({"status": "dry_run", "would_add": len(approved),
                        "terms": [t["searchTerm"] for t in approved]})

    token = get_token()
    results = {"success": [], "errors": []}

    # Group by profile
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

    return jsonify({"status": "applied", "added": len(results["success"]),
                    "errors": len(results["errors"]), "detail": results})

# ── HTML TEMPLATE ─────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Negative Keyword Review</title>
<style>
:root{--n:#0f172a;--b:#e2e8f0;--bg:#f8fafc;--r:#dc2626;--g:#16a34a;--y:#d97706;--bl:#2563eb}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--n);font-size:13px}
.page{max-width:1100px;margin:0 auto;padding:24px 20px 48px}
.header{background:var(--n);color:#fff;padding:20px 28px;border-radius:12px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:18px;font-weight:800}
.header .sub{font-size:11px;color:#94a3b8;margin-top:3px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:9px 20px;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;border:none;transition:.15s}
.btn-primary{background:var(--bl);color:#fff}
.btn-primary:hover{background:#1d4ed8}
.btn-danger{background:var(--r);color:#fff}
.btn-danger:hover{background:#b91c1c}
.btn-ghost{background:#f1f5f9;color:var(--n);border:1px solid var(--b)}
.btn:disabled{opacity:.4;cursor:not-allowed}
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
.kpi{background:#fff;border:1px solid var(--b);border-radius:10px;padding:14px;text-align:center}
.kpi .big{font-size:22px;font-weight:800;margin-bottom:2px}
.kpi .lbl{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.5px}
.red{color:var(--r)} .green{color:var(--g)} .blue{color:var(--bl)} .amber{color:var(--y)}
.toolbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-wrap:wrap;gap:8px}
.toolbar-left{display:flex;align-items:center;gap:8px}
.sel-count{font-size:12px;color:#64748b;font-weight:600}
table{width:100%;border-collapse:collapse;background:#fff;border:1px solid var(--b);border-radius:10px;overflow:hidden}
th{padding:9px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid var(--b);font-size:11px;background:#f8fafc}
td{padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr.unchecked td{opacity:.4}
tr:hover td{background:#fafafa}
.mono{font-family:'SF Mono',Consolas,monospace;font-size:11px}
.num{text-align:right;font-variant-numeric:tabular-nums}
.center{text-align:center}
.acos-bad{color:var(--r);font-weight:700}
.acos-high{color:var(--y);font-weight:700}
.acos-inf{color:#94a3b8;font-style:italic}
.badge{font-size:10px;font-weight:700;padding:2px 8px;border-radius:20px}
.badge-zero{background:#fef2f2;color:#dc2626}
.badge-high{background:#fffbeb;color:#d97706}
.toast{position:fixed;bottom:24px;right:24px;background:var(--n);color:#fff;padding:14px 20px;border-radius:10px;font-size:13px;font-weight:600;display:none;z-index:999;max-width:360px}
.loading{text-align:center;padding:60px;color:#94a3b8;font-size:14px}
.spinner{display:inline-block;width:24px;height:24px;border:3px solid #e2e8f0;border-top-color:var(--bl);border-radius:50%;animation:spin .8s linear infinite;margin-right:10px;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}
.filter-bar{display:flex;gap:8px;flex-wrap:wrap}
.filter-btn{font-size:11px;padding:4px 12px;border-radius:20px;border:1px solid var(--b);background:#fff;cursor:pointer;font-weight:600;color:#64748b}
.filter-btn.active{background:var(--n);color:#fff;border-color:var(--n)}
.cron-box{background:#fff;border:1px solid var(--b);border-radius:10px;padding:18px 20px;margin-top:20px}
.cron-box h3{font-size:13px;font-weight:700;margin-bottom:10px}
.code{background:#0f172a;color:#e2e8f0;padding:12px 16px;border-radius:8px;font-family:monospace;font-size:12px;line-height:1.6}
</style>
</head>
<body>
<div class="page">

<div class="header">
  <div>
    <h1>🚫 Negative Keyword Review</h1>
    <div class="sub">Review flagged terms before adding as negatives · Uncheck anything to keep</div>
  </div>
  <button class="btn btn-ghost" onclick="loadData()" id="refresh-btn">🔄 Refresh Data</button>
</div>

<div class="kpi-row" id="kpis">
  <div class="kpi"><div class="big blue" id="kpi-terms">—</div><div class="lbl">Terms Flagged</div></div>
  <div class="kpi"><div class="big red"  id="kpi-waste">—</div><div class="lbl">Wasted Spend</div></div>
  <div class="kpi"><div class="big amber" id="kpi-selected">—</div><div class="lbl">Selected to Negate</div></div>
  <div class="kpi"><div class="big green" id="kpi-date">—</div><div class="lbl">Data As Of</div></div>
</div>

<div class="toolbar">
  <div class="toolbar-left">
    <div class="filter-bar">
      <button class="filter-btn active" onclick="filterTerms('all',this)">All</button>
      <button class="filter-btn" onclick="filterTerms('zero',this)">Zero Orders</button>
      <button class="filter-btn" onclick="filterTerms('high',this)">High ACoS</button>
    </div>
    <span class="sel-count" id="sel-count"></span>
  </div>
  <div style="display:flex;gap:8px">
    <button class="btn btn-ghost" onclick="toggleAll(true)">✅ Select All</button>
    <button class="btn btn-ghost" onclick="toggleAll(false)">☐ Deselect All</button>
    <button class="btn btn-danger" onclick="applyNegatives(false)" id="apply-btn" disabled>
      🚫 Apply Negatives
    </button>
  </div>
</div>

<div id="table-container">
  <div class="loading"><span class="spinner"></span>Pulling live data from Amazon Ads API...</div>
</div>

<div class="cron-box">
  <h3>⏰ Schedule Nightly (run at 2am every night)</h3>
  <div class="code">
    # Add to crontab: run <code>crontab -e</code> then paste:<br>
    0 2 * * * python3 /Users/shuqingke/Documents/amazon_ads/negative_keyword_automator.py >> /tmp/negkw.log 2>&1
  </div>
</div>

</div>

<div class="toast" id="toast"></div>

<script>
let allTerms = [];
let currentFilter = 'all';

async function loadData() {
  document.getElementById('table-container').innerHTML = '<div class="loading"><span class="spinner"></span>Pulling live data from Amazon Ads API...</div>';
  document.getElementById('refresh-btn').disabled = true;
  document.getElementById('apply-btn').disabled = true;

  try {
    const r = await fetch('/api/fetch');
    const d = await r.json();
    allTerms = d.terms.map(t => ({...t, checked: true}));

    document.getElementById('kpi-terms').textContent = d.count;
    document.getElementById('kpi-waste').textContent = '₹' + d.total_waste.toLocaleString('en-IN', {maximumFractionDigits:0});
    document.getElementById('kpi-date').textContent = d.date;
    renderTable();
  } catch(e) {
    document.getElementById('table-container').innerHTML = '<div class="loading" style="color:red">Error loading data — check server is running</div>';
  }
  document.getElementById('refresh-btn').disabled = false;
}

function renderTable() {
  const filtered = currentFilter === 'zero' ? allTerms.filter(t => t.orders === 0)
                 : currentFilter === 'high'  ? allTerms.filter(t => t.orders > 0)
                 : allTerms;

  const selected = allTerms.filter(t => t.checked).length;
  const selWaste = allTerms.filter(t => t.checked).reduce((s,t) => s+t.spend, 0);
  document.getElementById('kpi-selected').textContent = selected;
  document.getElementById('sel-count').textContent = `${selected} selected · ₹${selWaste.toLocaleString('en-IN',{maximumFractionDigits:0})} to remove`;
  document.getElementById('apply-btn').disabled = selected === 0;

  if (!filtered.length) {
    document.getElementById('table-container').innerHTML = '<div class="loading" style="color:#16a34a">✅ No flagged terms — account looks clean!</div>';
    return;
  }

  let rows = filtered.map((t,i) => {
    const idx = allTerms.indexOf(t);
    const acos_str = t.acos === null ? '<span class="acos-inf">∞ (0 orders)</span>'
                   : t.acos >= 50    ? `<span class="acos-bad">${t.acos}%</span>`
                   :                   `<span class="acos-high">${t.acos}%</span>`;
    const badge = t.orders === 0 ? '<span class="badge badge-zero">0 orders</span>'
                                 : '<span class="badge badge-high">High ACoS</span>';
    return `<tr class="${t.checked ? '' : 'unchecked'}" id="row-${idx}">
      <td class="center"><input type="checkbox" ${t.checked?'checked':''} onchange="toggle(${idx})" style="width:16px;height:16px;cursor:pointer"></td>
      <td class="mono">${t.searchTerm}</td>
      <td style="font-size:11px;color:#64748b;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${t.campaignName}</td>
      <td style="font-size:11px;color:#94a3b8">${t.adGroupName}</td>
      <td class="num red" style="font-weight:700">₹${t.spend.toLocaleString('en-IN')}</td>
      <td class="num">${t.orders}</td>
      <td class="center">${acos_str}</td>
      <td class="center">${badge}</td>
      <td class="center" style="font-size:10px;color:#94a3b8">${t.accountLabel}</td>
    </tr>`;
  }).join('');

  document.getElementById('table-container').innerHTML = `
    <table>
      <thead><tr>
        <th class="center" style="width:40px">✓</th>
        <th>Search Term</th>
        <th>Campaign</th>
        <th>Ad Group</th>
        <th style="text-align:right">Spend</th>
        <th style="text-align:right">Orders</th>
        <th class="center">ACoS</th>
        <th class="center">Reason</th>
        <th class="center">Account</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function toggle(idx) {
  allTerms[idx].checked = !allTerms[idx].checked;
  document.getElementById(`row-${idx}`).className = allTerms[idx].checked ? '' : 'unchecked';
  renderTable();
}

function toggleAll(val) {
  allTerms.forEach(t => t.checked = val);
  renderTable();
}

function filterTerms(type, btn) {
  currentFilter = type;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderTable();
}

async function applyNegatives(dryRun=false) {
  const selected = allTerms.filter(t => t.checked);
  if (!selected.length) return;

  const confirmed = confirm(`Add ${selected.length} terms as NEGATIVE EXACT match?\\n\\nThis cannot be undone. Continue?`);
  if (!confirmed) return;

  document.getElementById('apply-btn').disabled = true;
  document.getElementById('apply-btn').textContent = '⏳ Applying...';

  const r = await fetch('/api/apply', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({terms: selected, dry_run: false})
  });
  const d = await r.json();

  showToast(d.status === 'applied'
    ? `✅ ${d.added} negatives added successfully!`
    : `🔍 Dry run: would add ${d.would_add} terms`);

  document.getElementById('apply-btn').textContent = '🚫 Apply Negatives';
  document.getElementById('apply-btn').disabled = false;
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 4000);
}

loadData();
</script>
</body>
</html>"""

if __name__ == "__main__":
    print("\n🚀 Negative Keyword Review App")
    print("   Open: http://localhost:5050\n")
    app.run(port=5050, debug=False)
