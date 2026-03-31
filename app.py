"""
Menhood Ads — Negative Review + Winners Scale + Self-Target App
Three tabs:
  1. 🚫 Negative Review    — flag wasteful terms, apply negatives
  2. 🚀 Scale Winners      — find converting terms, check keyword coverage, create exact keywords
  3. 🎯 Self-Target        — create one-ASIN self-targeting campaigns to defend product pages
"""

from datetime import datetime, timedelta
import gzip
import json
import time
from typing import Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st

EU_API       = "https://advertising-api-eu.amazon.com"
MIN_SPEND    = 50       # ₹ — minimum spend to surface a term
CACHE_TTL    = 60 * 60 * 6   # 6 hours


# ─── Date range ──────────────────────────────────────────────────────────────

def date_range_30d() -> Tuple:
    today      = datetime.today().date()
    end_date   = today - timedelta(days=3)
    start_date = end_date - timedelta(days=29)
    return start_date, end_date


# ─── Credentials / profiles ──────────────────────────────────────────────────

def get_profile_map() -> Dict[str, str]:
    if "MENHOOD_PROFILES" in st.secrets:
        return {str(k): str(v) for k, v in st.secrets["MENHOOD_PROFILES"].items()}
    p1 = str(st.secrets.get("MENHOOD_PROFILE_1", "")).strip()
    p2 = str(st.secrets.get("MENHOOD_PROFILE_2", "")).strip()
    out: Dict[str, str] = {}
    if p1: out[p1] = "Account A (Postpaid)"
    if p2: out[p2] = "Account B (Prepaid)"
    return out


def _get_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    r = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={"grant_type": "refresh_token", "refresh_token": refresh_token,
              "client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Token failed ({r.status_code}): {r.text[:200]}")
    return r.json()["access_token"]


# ─── Fetch: search term report ────────────────────────────────────────────────

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def fetch_report_data_cached(
    client_id: str, client_secret: str, refresh_token: str,
    profile_id: str, profile_label: str,
    _bust: int = 0,
) -> Tuple[List[Dict], int, str]:
    token = _get_token(client_id, client_secret, refresh_token)
    start_date, end_date = date_range_30d()

    headers = {
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
        "Content-Type":                    "application/json",
    }
    body = {
        "name": f"NegKW {profile_label} {end_date}",
        "startDate": str(start_date),
        "endDate":   str(end_date),
        "configuration": {
            "adProduct":    "SPONSORED_PRODUCTS",
            "groupBy":      ["searchTerm"],
            "columns": [
                "campaignId", "campaignName",
                "adGroupId",  "adGroupName",
                "searchTerm", "impressions", "clicks",
                "cost", "purchases30d", "sales30d",
            ],
            "reportTypeId": "spSearchTerm",
            "timeUnit":     "SUMMARY",
            "format":       "GZIP_JSON",
        },
    }

    rr = requests.post(f"{EU_API}/reporting/reports", headers=headers, json=body, timeout=30)
    if rr.status_code >= 400:
        return [], 0, f"Report submit failed {rr.status_code}: {rr.text[:300]}"

    report_id = rr.json().get("reportId")
    if not report_id:
        return [], 0, f"No reportId: {rr.text[:200]}"

    for _ in range(30):
        time.sleep(10)
        rs = requests.get(f"{EU_API}/reporting/reports/{report_id}", headers=headers, timeout=30)
        if rs.status_code >= 400:
            return [], 0, f"Poll failed {rs.status_code}: {rs.text[:200]}"
        status = rs.json()
        state  = status.get("status", "UNKNOWN")

        if state == "COMPLETED":
            url = status.get("url") or status.get("location")
            if not url:
                return [], 0, f"COMPLETED but no URL: {status}"
            raw  = requests.get(url, timeout=60)
            raw.raise_for_status()
            data = json.loads(gzip.decompress(raw.content))
            total = len(data)

            rows = []
            for row in data:
                spend  = float(row.get("cost", 0) or 0)
                if spend < MIN_SPEND:
                    continue
                sales  = float(row.get("sales30d",    0) or 0)
                orders = int(row.get("purchases30d", 0) or 0)
                clicks = int(row.get("clicks",       0) or 0)
                acos   = spend / sales if sales > 0 else None
                cpc    = round(spend / clicks, 2) if clicks > 0 else None
                rows.append({
                    "id":           f"{row.get('campaignId')}_{row.get('adGroupId')}_{row.get('searchTerm','')}",
                    "searchTerm":   row.get("searchTerm", ""),
                    "campaignId":   str(row.get("campaignId", "")),
                    "campaignName": row.get("campaignName", ""),
                    "adGroupId":    str(row.get("adGroupId", "")),
                    "adGroupName":  row.get("adGroupName", ""),
                    "clicks":  clicks,
                    "cpc":     cpc,
                    "spend":   round(spend, 2),
                    "sales":   round(sales, 2),
                    "orders":  orders,
                    "acosPct": round(acos * 100, 2) if acos is not None else None,
                    "profile":      profile_id,
                    "accountLabel": profile_label,
                })
            rows.sort(key=lambda x: x["spend"], reverse=True)
            return rows, total, ""

        if state == "FAILED":
            return [], 0, f"Report FAILED ({profile_label}): {status.get('failureReason','')}"

    return [], 0, f"Timed out ({profile_id})"


# ─── Fetch: existing SP keywords ─────────────────────────────────────────────

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def fetch_keywords_cached(
    client_id: str, client_secret: str, refresh_token: str,
    profile_id: str,
    _bust: int = 0,
) -> Tuple[List[Dict], str]:
    try:
        token = _get_token(client_id, client_secret, refresh_token)
    except Exception as e:
        return [], str(e)

    headers = {
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
        "Content-Type": "application/vnd.spKeyword.v3+json",
        "Accept":       "application/vnd.spKeyword.v3+json",
    }

    keywords: List[Dict] = []
    next_token = None
    while True:
        body: Dict = {
            "maxResults": 1000,
            "stateFilter": {"include": ["ENABLED", "PAUSED"]},
        }
        if next_token:
            body["nextToken"] = next_token
        r = requests.post(
            f"{EU_API}/sp/keywords/list",
            headers=headers, json=body, timeout=30,
        )
        if r.status_code >= 400:
            return keywords, f"Keywords fetch {r.status_code}: {r.text[:200]}"
        data       = r.json()
        batch      = data.get("keywords", [])
        keywords.extend(batch)
        next_token = data.get("nextToken")
        if not next_token:
            break

    return keywords, ""


# ─── Fetch: active product ads (for ASIN list) ────────────────────────────────

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def fetch_product_ads_cached(
    client_id: str, client_secret: str, refresh_token: str,
    profile_id: str, profile_label: str,
    _bust: int = 0,
) -> Tuple[List[Dict], str]:
    """Returns (product_ads_list, error). Used to discover active ASINs."""
    try:
        token = _get_token(client_id, client_secret, refresh_token)
    except Exception as e:
        return [], str(e)

    headers = {
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
        "Content-Type": "application/vnd.spProductAd.v3+json",
        "Accept":       "application/vnd.spProductAd.v3+json",
    }

    ads: List[Dict] = []
    next_token = None
    while True:
        body: Dict = {
            "maxResults": 100,
            "stateFilter": {"include": ["ENABLED"]},
        }
        if next_token:
            body["nextToken"] = next_token
        r = requests.post(
            f"{EU_API}/sp/productAds/list",
            headers=headers, json=body, timeout=30,
        )
        if r.status_code >= 400:
            return ads, f"Product ads fetch {r.status_code}: {r.text[:200]}"
        data = r.json()
        batch = data.get("productAds", [])
        for ad in batch:
            ad["profileLabel"] = profile_label
            ad["profile"]      = profile_id
        ads.extend(batch)
        next_token = data.get("nextToken")
        if not next_token:
            break

    return ads, ""


# ─── Load everything ──────────────────────────────────────────────────────────

def load_all_data(force: bool = False):
    """Returns (terms_df, kw_by_profile, pad_by_profile, debug_lines)."""
    profile_map = get_profile_map()
    if not profile_map:
        raise RuntimeError("No profile IDs in secrets.")

    cid = st.secrets["AMAZON_CLIENT_ID"]
    sec = st.secrets["AMAZON_CLIENT_SECRET"]
    tok = st.secrets["AMAZON_REFRESH_TOKEN"]

    if force:
        fetch_report_data_cached.clear()
        fetch_keywords_cached.clear()
        fetch_product_ads_cached.clear()

    bust = int(force)
    all_terms:     List[Dict]            = []
    kw_by_profile: Dict[str, List[Dict]] = {}
    pad_by_profile: Dict[str, List[Dict]] = {}
    debug_lines:   List[str]             = []
    n = len(profile_map)
    prog = st.progress(0, text="Starting…")

    for i, (pid, label) in enumerate(profile_map.items()):
        prog.progress(i / n, text=f"Search terms: {label}…")
        terms, raw, err = fetch_report_data_cached(cid, sec, tok, pid, label, bust)
        debug_lines.append(
            f"⚠️ {label}: {err}" if err
            else f"✅ {label}: {raw} rows → {len(terms)} above ₹{MIN_SPEND}"
        )
        all_terms.extend(terms)

        kws, kw_err = fetch_keywords_cached(cid, sec, tok, pid, bust)
        debug_lines.append(
            f"⚠️ {label} keywords: {kw_err}" if kw_err
            else f"✅ {label}: {len(kws)} SP keywords fetched"
        )
        kw_by_profile[pid] = kws

        pads, pad_err = fetch_product_ads_cached(cid, sec, tok, pid, label, bust)
        debug_lines.append(
            f"⚠️ {label} product ads: {pad_err}" if pad_err
            else f"✅ {label}: {len(pads)} active product ads"
        )
        pad_by_profile[pid] = pads

    prog.progress(1.0, text="Done!")
    time.sleep(0.3)
    prog.empty()

    df = pd.DataFrame(all_terms) if all_terms else pd.DataFrame()
    return df, kw_by_profile, pad_by_profile, debug_lines


# ─── Keyword coverage helpers ─────────────────────────────────────────────────

def build_kw_sets(kw_by_profile: Dict[str, List[Dict]]) -> Dict[str, Dict[str, set]]:
    out = {}
    for pid, kws in kw_by_profile.items():
        sets: Dict[str, set] = {"EXACT": set(), "PHRASE": set(), "BROAD": set()}
        for kw in kws:
            mt = kw.get("matchType", "").upper()
            kt = kw.get("keywordText", "").lower().strip()
            if mt in sets and kt:
                sets[mt].add(kt)
        out[pid] = sets
    return out


def check_coverage(term: str, profile_id: str, kw_sets: Dict) -> Dict[str, bool]:
    t    = term.lower().strip()
    sets = kw_sets.get(profile_id, {"EXACT": set(), "PHRASE": set(), "BROAD": set()})
    exact  = t in sets["EXACT"]
    phrase = any(kw in t for kw in sets["PHRASE"] if kw)
    t_words = set(t.split())
    broad  = any(any(w in t_words for w in kw.split()) for kw in sets["BROAD"] if kw)
    return {"EXACT": exact, "PHRASE": phrase, "BROAD": broad}


# ─── Amazon API write operations ──────────────────────────────────────────────

def apply_negatives(token: str, terms: List[Dict]) -> Dict:
    cid = st.secrets["AMAZON_CLIENT_ID"]
    results: Dict[str, List] = {"success": [], "errors": []}
    by_profile: Dict[str, List[Dict]] = {}
    for t in terms:
        by_profile.setdefault(t["profile"], []).append(t)

    for pid, pts in by_profile.items():
        headers = {
            "Amazon-Advertising-API-ClientId": cid,
            "Amazon-Advertising-API-Scope":    pid,
            "Authorization":                   f"Bearer {token}",
            "Content-Type": "application/vnd.spNegativeKeyword.v3+json",
            "Accept":       "application/vnd.spNegativeKeyword.v3+json",
        }
        payload = [
            {"campaignId": t["campaignId"], "adGroupId": t["adGroupId"],
             "keywordText": t["searchTerm"], "matchType": "NEGATIVE_EXACT", "state": "ENABLED"}
            for t in pts
        ]
        for i in range(0, len(payload), 1000):
            r = requests.post(
                f"{EU_API}/sp/negativeKeywords",
                headers=headers,
                json={"negativeKeywords": payload[i:i+1000]},
                timeout=30,
            )
            r.raise_for_status()
            rd = r.json()
            results["success"].extend(rd.get("negativeKeywords", {}).get("success", []))
            results["errors"].extend(rd.get("negativeKeywords", {}).get("error",   []))
    return results


def add_exact_keywords(token: str, terms: List[Dict], bid: float) -> Dict:
    cid = st.secrets["AMAZON_CLIENT_ID"]
    results: Dict[str, List] = {"success": [], "errors": []}
    by_profile: Dict[str, List[Dict]] = {}
    for t in terms:
        by_profile.setdefault(t["profile"], []).append(t)

    for pid, pts in by_profile.items():
        headers = {
            "Amazon-Advertising-API-ClientId": cid,
            "Amazon-Advertising-API-Scope":    pid,
            "Authorization":                   f"Bearer {token}",
            "Content-Type": "application/vnd.spKeyword.v3+json",
            "Accept":       "application/vnd.spKeyword.v3+json",
        }
        payload = [
            {"campaignId": t["campaignId"], "adGroupId": t["adGroupId"],
             "keywordText": t["searchTerm"], "matchType": "EXACT",
             "state": "ENABLED", "bid": bid}
            for t in pts
        ]
        for i in range(0, len(payload), 1000):
            r = requests.post(
                f"{EU_API}/sp/keywords",
                headers=headers,
                json={"keywords": payload[i:i+1000]},
                timeout=30,
            )
            if r.status_code >= 400:
                results["errors"].append({"msg": f"{r.status_code}: {r.text[:200]}"})
                continue
            rd = r.json()
            results["success"].extend(rd.get("keywords", {}).get("success", []))
            results["errors"].extend(rd.get("keywords", {}).get("error",   []))
    return results


def create_self_target_campaigns(
    token: str,
    profile_id: str,
    asins: List[str],
    bid: float,
    daily_budget: float,
) -> Dict:
    """
    For each ASIN: creates SP campaign → ad group → product ad → asinSameAs target.
    Structure: 1 campaign | 1 ad group | 1 product | 1 target (same ASIN).
    """
    cid       = st.secrets["AMAZON_CLIENT_ID"]
    today_str = datetime.today().strftime("%Y%m%d")
    results: Dict[str, List] = {"success": [], "errors": []}

    base_headers = {
        "Amazon-Advertising-API-ClientId": cid,
        "Amazon-Advertising-API-Scope":    profile_id,
        "Authorization":                   f"Bearer {token}",
    }

    for asin in asins:
        # ── Step 1: Create campaign ───────────────────────────────────────────
        r = requests.post(
            f"{EU_API}/sp/campaigns",
            headers={**base_headers,
                     "Content-Type": "application/vnd.spCampaign.v3+json",
                     "Accept":       "application/vnd.spCampaign.v3+json"},
            json={"campaigns": [{
                "name":          f"SP|Self-Target|{asin}",
                "targetingType": "MANUAL",
                "state":         "ENABLED",
                "budget":        {"budgetType": "DAILY", "budget": daily_budget},
                "startDate":     today_str,
            }]},
            timeout=30,
        )
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "campaign", "msg": r.text[:200]})
            continue
        camp_resp    = r.json().get("campaigns", {})
        camp_success = camp_resp.get("success", [])
        if not camp_success:
            results["errors"].append({"asin": asin, "step": "campaign",
                                       "msg": str(camp_resp.get("error", "unknown"))})
            continue
        campaign_id = camp_success[0]["campaignId"]

        # ── Step 2: Create ad group ───────────────────────────────────────────
        r = requests.post(
            f"{EU_API}/sp/adGroups",
            headers={**base_headers,
                     "Content-Type": "application/vnd.spAdGroup.v3+json",
                     "Accept":       "application/vnd.spAdGroup.v3+json"},
            json={"adGroups": [{
                "name":       f"Self|{asin}",
                "campaignId": campaign_id,
                "defaultBid": bid,
                "state":      "ENABLED",
            }]},
            timeout=30,
        )
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "adGroup", "msg": r.text[:200]})
            continue
        ag_resp    = r.json().get("adGroups", {})
        ag_success = ag_resp.get("success", [])
        if not ag_success:
            results["errors"].append({"asin": asin, "step": "adGroup",
                                       "msg": str(ag_resp.get("error", "unknown"))})
            continue
        ad_group_id = ag_success[0]["adGroupId"]

        # ── Step 3: Create product ad ─────────────────────────────────────────
        r = requests.post(
            f"{EU_API}/sp/productAds",
            headers={**base_headers,
                     "Content-Type": "application/vnd.spProductAd.v3+json",
                     "Accept":       "application/vnd.spProductAd.v3+json"},
            json={"productAds": [{
                "campaignId": campaign_id,
                "adGroupId":  ad_group_id,
                "asin":       asin,
                "state":      "ENABLED",
            }]},
            timeout=30,
        )
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "productAd", "msg": r.text[:200]})
            continue

        # ── Step 4: Create asinSameAs target ──────────────────────────────────
        r = requests.post(
            f"{EU_API}/sp/targets",
            headers={**base_headers,
                     "Content-Type": "application/vnd.spTargetingClause.v3+json",
                     "Accept":       "application/vnd.spTargetingClause.v3+json"},
            json={"targetingClauses": [{
                "campaignId":     campaign_id,
                "adGroupId":      ad_group_id,
                "state":          "ENABLED",
                "bid":            bid,
                "expression":     [{"type": "asinSameAs", "value": asin}],
                "expressionType": "MANUAL",
            }]},
            timeout=30,
        )
        if r.status_code >= 400:
            results["errors"].append({"asin": asin, "step": "target", "msg": r.text[:200]})
            continue
        tgt_resp   = r.json().get("targetingClauses", {})
        tgt_errors = tgt_resp.get("error", [])
        if tgt_errors:
            results["errors"].append({"asin": asin, "step": "target", "msg": str(tgt_errors)})
        else:
            results["success"].append({
                "asin": asin, "campaignId": campaign_id, "adGroupId": ad_group_id,
            })

    return results


# ─── Main UI ──────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="Menhood Ads Review", layout="wide", page_icon="🎯")
    st.title("🎯 Menhood Ads — Keyword Review")

    start, end = date_range_30d()

    # ── Top controls ─────────────────────────────────────────────────────────
    c1, c2 = st.columns([1.2, 1.5])
    with c1:
        acos_pct = st.number_input(
            "ACoS Threshold %", min_value=1, max_value=500, value=30, step=5,
            help="Negatives: terms above this % flagged as waste. Winners: terms below this %.",
        )
    with c2:
        st.write("")
        col_r, col_d = st.columns(2)
        with col_r:
            refresh = st.button("🔄 Refresh", help="Force re-fetch from Amazon API")
        with col_d:
            st.caption(f"📅 `{start}` → `{end}`")

    # ── Load data ────────────────────────────────────────────────────────────
    if "terms_df" not in st.session_state or refresh:
        try:
            df, kw_by_profile, pad_by_profile, debug_lines = load_all_data(force=refresh)
            st.session_state.update({
                "terms_df":       df,
                "kw_by_profile":  kw_by_profile,
                "pad_by_profile": pad_by_profile,
                "debug_lines":    debug_lines,
            })
        except Exception as e:
            st.error(f"Error loading data: {e}")
            st.stop()

    df:             pd.DataFrame       = st.session_state["terms_df"].copy()
    kw_by_profile:  Dict[str, List]    = st.session_state.get("kw_by_profile", {})
    pad_by_profile: Dict[str, List]    = st.session_state.get("pad_by_profile", {})

    if st.session_state.get("debug_lines"):
        with st.expander("API Debug Info", expanded=False):
            for line in st.session_state["debug_lines"]:
                st.write(line)

    if df.empty:
        st.warning("No data. Check credentials or click Refresh.")
        st.stop()

    # ── Single-threshold logic ────────────────────────────────────────────────
    df_f = df.copy()
    neg_mask = (df_f["orders"] == 0) | (
        df_f["acosPct"].notna() & (df_f["acosPct"] > acos_pct)
    )
    win_mask = (
        (df_f["orders"] > 0) &
        (df_f["clicks"] >= 3) &
        (df_f["acosPct"].notna()) &
        (df_f["acosPct"] <= acos_pct)
    )
    neg_df = df_f[neg_mask].copy().reset_index(drop=True)
    win_df = df_f[win_mask].copy().reset_index(drop=True)

    kw_sets = build_kw_sets(kw_by_profile)

    # ── Summary metrics ───────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🚫 Negatives",       len(neg_df))
    m2.metric("💸 Wasted Spend",    f"₹{neg_df['spend'].sum():,.0f}")
    m3.metric("🚀 Winners",         len(win_df))
    m4.metric("💰 Winners Revenue", f"₹{win_df['sales'].sum():,.0f}" if not win_df.empty else "₹0")

    # ═══════════════════════════════════════════════════════════════════════════
    # TABS
    # ═══════════════════════════════════════════════════════════════════════════
    tab1, tab2, tab3 = st.tabs([
        f"🚫 Negative Review ({len(neg_df)})",
        f"🚀 Scale Winners ({len(win_df)})",
        "🎯 Self-Target Campaigns",
    ])

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 1 — NEGATIVE REVIEW
    # ═══════════════════════════════════════════════════════════════════════════
    with tab1:
        k1, k2, k3 = st.columns(3)
        k1.metric("Terms Flagged",  len(neg_df))
        k2.metric("Wasted Spend",   f"₹{neg_df['spend'].sum():,.0f}")
        k3.metric("ACoS Threshold", f"{acos_pct}%")

        view = neg_df.copy()
        if "neg_selected" not in st.session_state:
            st.session_state["neg_selected"] = {row["id"]: True for _, row in neg_df.iterrows()}
        view["selected"] = view["id"].map(
            lambda x: st.session_state["neg_selected"].get(x, True)
        )

        st.markdown(f"**{len(view)} terms flagged by threshold**")

        if not view.empty:
            cols = ["selected", "searchTerm", "campaignName", "adGroupName",
                    "clicks", "cpc", "spend", "orders", "acosPct", "accountLabel"]
            edited = st.data_editor(
                view[cols].reset_index(drop=True),
                hide_index=True,
                use_container_width=True,
                disabled=["searchTerm", "campaignName", "adGroupName",
                          "clicks", "cpc", "spend", "orders", "acosPct", "accountLabel"],
                column_config={
                    "selected":     st.column_config.CheckboxColumn("✓ Negate?"),
                    "clicks":       st.column_config.NumberColumn("Clicks",   format="%d"),
                    "cpc":          st.column_config.NumberColumn("CPC ₹",    format="%.2f"),
                    "spend":        st.column_config.NumberColumn("Spend ₹",  format="%.0f"),
                    "acosPct":      st.column_config.NumberColumn("ACoS %",   format="%.0f%%"),
                    "accountLabel": st.column_config.TextColumn("Account"),
                },
                key="neg_editor",
            )
            if not edited.empty:
                for idx, row in edited.iterrows():
                    st.session_state["neg_selected"][view.iloc[idx]["id"]] = row["selected"]

        sel_ids = [k for k, v in st.session_state.get("neg_selected", {}).items() if v]
        sel_df  = neg_df[neg_df["id"].isin(sel_ids)]
        st.info(f"**{len(sel_df)}** terms selected · Wasted spend: **₹{sel_df['spend'].sum():,.0f}**")

        if st.button("🚫 Apply Negatives", type="primary", disabled=sel_df.empty, key="apply_neg"):
            with st.spinner("Applying negatives…"):
                token   = _get_token(st.secrets["AMAZON_CLIENT_ID"],
                                     st.secrets["AMAZON_CLIENT_SECRET"],
                                     st.secrets["AMAZON_REFRESH_TOKEN"])
                results = apply_negatives(token, sel_df.to_dict("records"))
            st.success(f"✅ {len(results['success'])} added, {len(results['errors'])} errors.")
            if results["errors"]:
                st.error("Errors:")
                st.json(results["errors"][:20])

        st.divider()
        st.caption("📅 Data cached 6 hours. Click 🔄 Refresh to force new pull.")

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 2 — SCALE WINNERS
    # ═══════════════════════════════════════════════════════════════════════════
    with tab2:
        if win_df.empty:
            st.info(f"No winners yet. Try lowering ACoS threshold (now {acos_pct}%).")
            st.stop()

        cov_rows = []
        for _, row in win_df.iterrows():
            cov = check_coverage(row["searchTerm"], row["profile"], kw_sets)
            cov_rows.append({
                **row.to_dict(),
                "has_exact":     cov["EXACT"],
                "has_phrase":    cov["PHRASE"],
                "has_broad":     cov["BROAD"],
                "missing_exact": not cov["EXACT"],
            })
        cov_df = pd.DataFrame(cov_rows)

        missing_count = int(cov_df["missing_exact"].sum())

        w1, w2, w3, w4 = st.columns(4)
        w1.metric("Winning Terms",   len(cov_df))
        w2.metric("Total Revenue",   f"₹{cov_df['sales'].sum():,.0f}")
        w3.metric("Avg ACoS",        f"{cov_df['acosPct'].mean():.1f}%")
        w4.metric("Missing [Exact]", missing_count,
                  delta=f"{missing_count} to create" if missing_count else "All covered",
                  delta_color="inverse" if missing_count else "off")

        if missing_count > 0:
            st.warning(
                f"⚡ **{missing_count} converting search terms not targeted as [Exact]** — "
                "promote to exact match to scale them."
            )

        st.markdown("### Winners — Keyword Coverage")
        st.caption("✅ = already targeted  ·  ❌ = gap (opportunity)")

        disp = cov_df[[
            "searchTerm", "campaignName", "adGroupName",
            "clicks", "cpc", "orders", "spend", "sales", "acosPct",
            "accountLabel", "has_exact", "has_phrase", "has_broad",
        ]].copy()
        disp["has_exact"]  = disp["has_exact"].map({True: "✅", False: "❌ Missing"})
        disp["has_phrase"] = disp["has_phrase"].map({True: "✅", False: "❌ Missing"})
        disp["has_broad"]  = disp["has_broad"].map({True: "✅", False: "❌ Missing"})

        st.dataframe(
            disp.sort_values("spend", ascending=False).reset_index(drop=True),
            use_container_width=True,
            column_config={
                "searchTerm":   st.column_config.TextColumn("Search Term",  width="medium"),
                "campaignName": st.column_config.TextColumn("Campaign",     width="medium"),
                "adGroupName":  st.column_config.TextColumn("Ad Group",     width="small"),
                "clicks":       st.column_config.NumberColumn("Clicks",     format="%d"),
                "cpc":          st.column_config.NumberColumn("CPC ₹",      format="%.2f"),
                "orders":       st.column_config.NumberColumn("Orders",     format="%d"),
                "spend":        st.column_config.NumberColumn("Spend ₹",    format="%.0f"),
                "sales":        st.column_config.NumberColumn("Revenue ₹",  format="%.0f"),
                "acosPct":      st.column_config.NumberColumn("ACoS %",     format="%.1f%%"),
                "accountLabel": st.column_config.TextColumn("Account"),
                "has_exact":    st.column_config.TextColumn("[Exact]"),
                "has_phrase":   st.column_config.TextColumn('"Phrase"'),
                "has_broad":    st.column_config.TextColumn("Broad"),
            },
            hide_index=True,
        )

        st.divider()

        miss_df = cov_df[cov_df["missing_exact"]].copy().reset_index(drop=True)

        if not miss_df.empty:
            st.markdown(f"### ➕ Create {len(miss_df)} Missing [Exact] Keywords")
            st.caption(
                "Converting terms not yet targeted as exact match. "
                "Adding them locks in the signal and gives full bid control."
            )

            ba, bb, bc = st.columns([1, 1, 2])
            with ba:
                bid = st.number_input("Bid per keyword ₹", min_value=1.0,
                                      max_value=500.0, value=10.0, step=0.5, key="exact_bid")
            with bb:
                st.write("")
                create_btn = st.button(
                    f"➕ Add {len(miss_df)} as [Exact]", type="primary", key="create_exact"
                )
            with bc:
                st.download_button(
                    "📥 Export Missing Exact as CSV",
                    miss_df.to_csv(index=False).encode(),
                    f"menhood_missing_exact_{end}.csv",
                    "text/csv",
                    key="dl_missing",
                )

            st.dataframe(
                miss_df[[
                    "searchTerm", "campaignName", "adGroupName",
                    "clicks", "orders", "acosPct", "accountLabel",
                ]].reset_index(drop=True),
                use_container_width=True,
                column_config={
                    "acosPct": st.column_config.NumberColumn("ACoS %", format="%.1f%%"),
                    "clicks":  st.column_config.NumberColumn("Clicks",  format="%d"),
                    "orders":  st.column_config.NumberColumn("Orders",  format="%d"),
                },
                hide_index=True,
            )

            if create_btn:
                with st.spinner(f"Creating {len(miss_df)} exact keywords at ₹{bid:.0f} bid…"):
                    token   = _get_token(st.secrets["AMAZON_CLIENT_ID"],
                                         st.secrets["AMAZON_CLIENT_SECRET"],
                                         st.secrets["AMAZON_REFRESH_TOKEN"])
                    results = add_exact_keywords(token, miss_df.to_dict("records"), bid)
                if results["errors"]:
                    st.warning(f"⚠️ {len(results['success'])} created · {len(results['errors'])} errors")
                    st.json(results["errors"][:10])
                else:
                    st.success(f"✅ {len(results['success'])} exact keywords created!")

        st.divider()
        st.download_button(
            "📥 Export All Winners to CSV",
            cov_df.to_csv(index=False).encode(),
            f"menhood_winners_{end}.csv",
            "text/csv",
            key="dl_all",
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 3 — SELF-TARGET CAMPAIGNS
    # ═══════════════════════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### 🎯 Self-Targeting Campaigns")
        st.caption(
            "Creates one SP campaign per ASIN that targets the same ASIN. "
            "Defends your product pages from competitors showing ads on your listings. "
            "Structure: **1 campaign → 1 ad group → 1 product ad → 1 asinSameAs target.**"
        )

        # ── Collect all unique ASINs from product ads across all profiles ────
        all_pads: List[Dict] = []
        for pid, pads in pad_by_profile.items():
            all_pads.extend(pads)

        # Deduplicate ASINs, keep profile info
        asin_map: Dict[str, Dict] = {}
        for ad in all_pads:
            asin = ad.get("asin", "").strip().upper()
            if asin and asin not in asin_map:
                asin_map[asin] = {
                    "asin":    asin,
                    "profile": ad.get("profile", ""),
                    "account": ad.get("profileLabel", ""),
                }

        if not asin_map:
            st.warning("No active product ads found. Click 🔄 Refresh or check API Debug Info.")
            st.stop()

        # ── Campaign settings ────────────────────────────────────────────────
        st.markdown("#### Campaign Settings")
        s1, s2, s3 = st.columns(3)
        with s1:
            self_bid = st.number_input(
                "Bid per target ₹", min_value=1.0, max_value=200.0,
                value=5.0, step=0.5, key="self_bid",
                help="CPC bid for the asinSameAs target. Start low (₹3–8) — self-targeting is cheap.",
            )
        with s2:
            self_budget = st.number_input(
                "Daily budget per campaign ₹", min_value=10.0, max_value=10000.0,
                value=100.0, step=10.0, key="self_budget",
            )
        with s3:
            st.write("")
            st.info(f"**{len(asin_map)} ASINs** found across {len(pad_by_profile)} accounts")

        # ── ASIN selection table ─────────────────────────────────────────────
        st.markdown("#### Select ASINs to create self-targeting campaigns for")

        asin_rows = [
            {"create": False, "asin": v["asin"], "account": v["account"], "profile": v["profile"]}
            for v in sorted(asin_map.values(), key=lambda x: x["asin"])
        ]
        asin_df = pd.DataFrame(asin_rows)

        edited_asins = st.data_editor(
            asin_df[["create", "asin", "account"]],
            hide_index=True,
            use_container_width=True,
            disabled=["asin", "account"],
            column_config={
                "create":  st.column_config.CheckboxColumn("✓ Create?", default=False),
                "asin":    st.column_config.TextColumn("ASIN"),
                "account": st.column_config.TextColumn("Account"),
            },
            key="asin_editor",
        )

        # Also allow manual ASIN entry
        with st.expander("➕ Add ASINs manually (comma-separated)"):
            manual_input = st.text_input(
                "ASINs", placeholder="B09GSC45FS, B094JPRYYV, B0F2DRJNZK",
                key="manual_asins",
            )
            manual_profile = st.selectbox(
                "Account to create in",
                options=list(get_profile_map().keys()),
                format_func=lambda x: get_profile_map().get(x, x),
                key="manual_profile",
            )

        # Merge selected from table
        selected_from_table = []
        for idx, row in edited_asins.iterrows():
            if row["create"]:
                profile = asin_df.iloc[idx]["profile"]
                selected_from_table.append({"asin": row["asin"], "profile": profile})

        # Merge manual entries
        selected_manual = []
        if manual_input.strip():
            for a in manual_input.split(","):
                a = a.strip().upper()
                if a:
                    selected_manual.append({"asin": a, "profile": manual_profile})

        all_selected = selected_from_table + selected_manual

        # ── Preview & create ─────────────────────────────────────────────────
        st.divider()

        if not all_selected:
            st.info("Tick the checkboxes above to select ASINs, then click Create.")
        else:
            st.markdown(f"**{len(all_selected)} ASINs selected** · "
                        f"Will create {len(all_selected)} campaigns at ₹{self_budget:.0f}/day each")

            preview_df = pd.DataFrame(all_selected)
            preview_df["campaign_name"]  = "SP|Self-Target|" + preview_df["asin"]
            preview_df["adgroup_name"]   = "Self|" + preview_df["asin"]
            preview_df["target"]         = "asinSameAs: " + preview_df["asin"]
            preview_df["bid"]            = f"₹{self_bid}"
            preview_df["daily_budget"]   = f"₹{self_budget:.0f}"
            preview_df["account_label"]  = preview_df["profile"].map(get_profile_map())

            st.dataframe(
                preview_df[["asin", "campaign_name", "adgroup_name",
                             "target", "bid", "daily_budget", "account_label"]],
                use_container_width=True,
                hide_index=True,
            )

            if st.button(
                f"🎯 Create {len(all_selected)} Self-Targeting Campaign(s)",
                type="primary",
                key="create_self",
            ):
                token = _get_token(
                    st.secrets["AMAZON_CLIENT_ID"],
                    st.secrets["AMAZON_CLIENT_SECRET"],
                    st.secrets["AMAZON_REFRESH_TOKEN"],
                )

                # Group by profile
                by_profile: Dict[str, List[str]] = {}
                for item in all_selected:
                    by_profile.setdefault(item["profile"], []).append(item["asin"])

                total_success, total_errors = [], []
                for pid, asins in by_profile.items():
                    label = get_profile_map().get(pid, pid)
                    with st.spinner(f"Creating campaigns for {label} ({len(asins)} ASINs)…"):
                        res = create_self_target_campaigns(
                            token, pid, asins, self_bid, self_budget
                        )
                    total_success.extend(res["success"])
                    total_errors.extend(res["errors"])

                if total_errors:
                    st.warning(
                        f"⚠️ {len(total_success)} campaigns created · "
                        f"{len(total_errors)} errors"
                    )
                    st.json(total_errors[:20])
                else:
                    st.success(
                        f"✅ {len(total_success)} self-targeting campaign(s) created! "
                        "They will be live within a few minutes in Amazon Ads."
                    )
                    st.info(
                        "💡 Tip: check the campaigns in Seller Central after ~5 mins. "
                        "Adjust bids up if impression share is low."
                    )


if __name__ == "__main__":
    main()
