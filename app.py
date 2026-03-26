"""
Streamlit Negative Keyword Review App

Run locally:
    streamlit run app.py
"""

from datetime import datetime, timedelta
import gzip
import json
import time
from typing import Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st

EU_API = "https://advertising-api-eu.amazon.com"
MIN_SPEND = 50           # ₹ — lower for weekly window (30-day spend ÷ ~4)
MAX_ACOS = 0.30
CACHE_TTL = 60 * 60 * 24 * 7   # 7 days


def last_week_range():
    """Returns (start, end) for last Mon–Sun, always fully processed."""
    today = datetime.today().date()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def get_profile_map() -> Dict[str, str]:
    if "MENHOOD_PROFILES" in st.secrets:
        profiles = st.secrets["MENHOOD_PROFILES"]
        return {str(k): str(v) for k, v in profiles.items()}
    p1 = str(st.secrets.get("MENHOOD_PROFILE_1", "")).strip()
    p2 = str(st.secrets.get("MENHOOD_PROFILE_2", "")).strip()
    profile_map: Dict[str, str] = {}
    if p1:
        profile_map[p1] = "Menhood - Postpaid"
    if p2:
        profile_map[p2] = "Menhood - Prepaid"
    return profile_map


def _get_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    r = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = {"raw": r.text}
        raise RuntimeError(
            f"Token request failed ({r.status_code}). Amazon: {err}"
        )
    return r.json()["access_token"]


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def fetch_flagged_terms_cached(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    profile_id: str,
    profile_label: str,
    min_spend: int,
    max_acos: float,
    _unused: int = 0,
) -> Tuple[List[Dict], int, str]:
    """
    Returns (flagged_terms, total_raw_rows, error_message).
    Cached for CACHE_TTL so repeat visits are instant.
    """
    token = _get_token(client_id, client_secret, refresh_token)

    start_date, end_date = last_week_range()

    headers = {
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope": profile_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    body = {
        "name": f"NegKW {profile_label} {end_date.strftime('%Y-%m-%d')}",
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["searchTerm"],
            "columns": [
                "campaignId", "campaignName",
                "adGroupId", "adGroupName",
                "searchTerm",
                "impressions", "clicks",
                "cost", "purchases7d", "sales7d",
            ],
            "reportTypeId": "spSearchTerm",
            "timeUnit": "SUMMARY",
            "format": "GZIP_JSON",
        },
    }

    rr = requests.post(f"{EU_API}/reporting/reports", headers=headers, json=body, timeout=30)
    if rr.status_code >= 400:
        return [], 0, f"Report submit failed {rr.status_code}: {rr.text[:300]}"

    report_id = rr.json().get("reportId")
    if not report_id:
        return [], 0, f"No reportId in response: {rr.text[:300]}"

    # Poll up to 30× with 10s gap = 5 min max
    for attempt in range(30):
        time.sleep(10)
        rs = requests.get(f"{EU_API}/reporting/reports/{report_id}", headers=headers, timeout=30)
        if rs.status_code >= 400:
            return [], 0, f"Poll failed {rs.status_code}: {rs.text[:200]}"

        status = rs.json()
        state = status.get("status", "UNKNOWN")

        if state == "COMPLETED":
            url = status.get("url") or status.get("location")
            if not url:
                return [], 0, f"COMPLETED but no download URL. Full response: {status}"
            raw = requests.get(url, timeout=60)
            raw.raise_for_status()
            data = json.loads(gzip.decompress(raw.content))
            total_rows = len(data)

            flagged = []
            for row in data:
                spend = float(row.get("cost", 0) or 0)
                sales = float(row.get("sales7d", 0) or 0)
                orders = int(row.get("purchases7d", 0) or 0)
                acos = spend / sales if sales > 0 else None

                if spend >= min_spend and (
                    orders == 0 or (acos is not None and acos > max_acos)
                ):
                    flagged.append({
                        "selected": True,
                        "id": f"{row.get('campaignId')}_{row.get('adGroupId')}_{row.get('searchTerm', '')}",
                        "searchTerm": row.get("searchTerm", ""),
                        "campaignId": str(row.get("campaignId", "")),
                        "campaignName": row.get("campaignName", ""),
                        "adGroupId": str(row.get("adGroupId", "")),
                        "adGroupName": row.get("adGroupName", ""),
                        "clicks": int(row.get("clicks", 0) or 0),
                        "spend": round(spend, 2),
                        "sales": round(sales, 2),
                        "orders": orders,
                        "acosPct": round(acos * 100, 2) if acos is not None else None,
                        "reason": "Zero Orders" if orders == 0 else "High ACoS",
                        "profile": profile_id,
                        "accountLabel": profile_label,
                    })

            flagged.sort(key=lambda x: x["spend"], reverse=True)
            return flagged, total_rows, ""

        if state == "FAILED":
            failure_reason = status.get("failureReason", "no reason given")
            return [], 0, f"Report FAILED for {profile_label}: {failure_reason}"

    return [], 0, f"Report timed out after 5 min for {profile_label} (reportId={report_id})"


def load_all_terms(force: bool = False) -> pd.DataFrame:
    profile_map = get_profile_map()
    if not profile_map:
        raise RuntimeError("No profile IDs found in secrets.")

    client_id     = st.secrets["AMAZON_CLIENT_ID"]
    client_secret = st.secrets["AMAZON_CLIENT_SECRET"]
    refresh_token = st.secrets["AMAZON_REFRESH_TOKEN"]

    if force:
        fetch_flagged_terms_cached.clear()

    all_terms: List[Dict] = []
    debug_lines: List[str] = []

    progress = st.progress(0, text="Starting report generation...")
    total = len(profile_map)

    for i, (pid, label) in enumerate(profile_map.items()):
        progress.progress((i / total), text=f"Fetching {label} — this takes ~2 min...")
        flagged, raw_rows, err = fetch_flagged_terms_cached(
            client_id, client_secret, refresh_token,
            pid, label, MIN_SPEND, MAX_ACOS, 0,
        )
        if err:
            debug_lines.append(f"⚠️ {label}: {err}")
        else:
            debug_lines.append(f"✅ {label}: {raw_rows} total rows → {len(flagged)} flagged (≥₹{MIN_SPEND} spend, bad performance)")
        all_terms.extend(flagged)

    progress.progress(1.0, text="Done!")
    time.sleep(0.5)
    progress.empty()

    st.session_state["debug_lines"] = debug_lines

    if not all_terms:
        return pd.DataFrame(columns=[
            "selected", "searchTerm", "campaignName", "adGroupName",
            "spend", "orders", "acosPct", "reason", "accountLabel",
            "campaignId", "adGroupId", "profile", "sales", "id",
        ])

    return pd.DataFrame(all_terms)


def apply_negatives(token: str, selected_terms: List[Dict]) -> Dict:
    client_id = st.secrets["AMAZON_CLIENT_ID"]
    results = {"success": [], "errors": []}

    by_profile: Dict[str, List[Dict]] = {}
    for t in selected_terms:
        by_profile.setdefault(t["profile"], []).append(t)

    for profile_id, terms in by_profile.items():
        headers = {
            "Amazon-Advertising-API-ClientId": client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.spNegativeKeyword.v3+json",
            "Accept": "application/vnd.spNegativeKeyword.v3+json",
        }
        payload = [
            {
                "campaignId": t["campaignId"],
                "adGroupId": t["adGroupId"],
                "keywordText": t["searchTerm"],
                "matchType": "NEGATIVE_EXACT",
                "state": "ENABLED",
            }
            for t in terms
        ]
        for i in range(0, len(payload), 1000):
            r = requests.post(
                f"{EU_API}/sp/negativeKeywords",
                headers=headers,
                json={"negativeKeywords": payload[i: i + 1000]},
                timeout=30,
            )
            r.raise_for_status()
            rd = r.json()
            results["success"].extend(rd.get("negativeKeywords", {}).get("success", []))
            results["errors"].extend(rd.get("negativeKeywords", {}).get("error", []))

    return results


def main() -> None:
    st.set_page_config(page_title="Negative Keyword Review", layout="wide")
    st.title("Negative Keyword Review")
    st.caption("Rajesh can deselect terms he wants to keep, then click Apply.")

    col_a, col_b = st.columns([1, 3])
    with col_a:
        refresh_clicked = st.button("🔄 Refresh Data", help="Force re-fetch from Amazon API (clears weekly cache)")
    with col_b:
        start, end = last_week_range()
        st.markdown(f"**Date Range:** `{start.strftime('%Y-%m-%d')}` to `{end.strftime('%Y-%m-%d')}`")

    if "terms_df" not in st.session_state or refresh_clicked:
        try:
            st.session_state["terms_df"] = load_all_terms(force=refresh_clicked)
            st.session_state["active_filter"] = "All"
        except Exception as e:
            st.error(f"Error loading data: {e}")
            st.stop()

    # Debug panel
    if st.session_state.get("debug_lines"):
        with st.expander("API Debug Info", expanded=False):
            for line in st.session_state["debug_lines"]:
                st.write(line)

    df = st.session_state["terms_df"].copy()
    total_waste = float(df["spend"].sum()) if not df.empty else 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric("Terms Flagged", int(len(df)))
    k2.metric("Total Wasted Spend", f"₹{total_waste:,.0f}")
    k3.metric("Threshold", f"≥₹{MIN_SPEND} spend + 0 orders or ACoS>{int(MAX_ACOS*100)}%")

    st.markdown("### Filters")
    f1, f2, f3 = st.columns(3)
    if f1.button("All", width="stretch"):
        st.session_state["active_filter"] = "All"
    if f2.button("Zero Orders", width="stretch"):
        st.session_state["active_filter"] = "Zero Orders"
    if f3.button("High ACoS", width="stretch"):
        st.session_state["active_filter"] = "High ACoS"

    active_filter = st.session_state.get("active_filter", "All")
    if active_filter == "Zero Orders":
        view_df = df[df["orders"] == 0].copy()
    elif active_filter == "High ACoS":
        view_df = df[(df["orders"] > 0) & (df["acosPct"] > 30)].copy()
    else:
        view_df = df.copy()

    st.markdown(f"### Flagged Terms ({active_filter}) — {len(view_df)} shown")

    if view_df.empty:
        st.info("No flagged terms match current filter. Try 'Refresh Data' or check the API Debug Info above.")
    else:
        editable_cols = [
            "selected", "searchTerm", "campaignName", "adGroupName",
            "clicks", "spend", "orders", "acosPct", "reason", "accountLabel",
        ]
        edited = st.data_editor(
            view_df[editable_cols],
            hide_index=True,
            width="stretch",
            disabled=["searchTerm", "campaignName", "adGroupName", "clicks", "spend", "orders", "acosPct", "reason", "accountLabel"],
            column_config={
                "selected": st.column_config.CheckboxColumn("✓ Negate?"),
                "clicks": st.column_config.NumberColumn("Clicks", format="%d"),
                "spend": st.column_config.NumberColumn("Spend (₹)", format="%.0f"),
                "acosPct": st.column_config.NumberColumn("ACoS %", format="%.0f%%"),
            },
            key="editor",
        )

        if not edited.empty:
            selection_map = dict(zip(view_df["id"], edited["selected"]))
            st.session_state["terms_df"]["selected"] = st.session_state["terms_df"].apply(
                lambda r: selection_map.get(r["id"], r["selected"]), axis=1
            )

    selected_df = st.session_state["terms_df"][st.session_state["terms_df"]["selected"] == True].copy()
    st.info(f"Selected terms to negate: **{len(selected_df)}**  |  Wasted spend covered: **₹{selected_df['spend'].sum():,.0f}**")

    if st.button("🚫 Apply Negatives", type="primary", disabled=selected_df.empty):
        with st.spinner("Applying negatives to Amazon Ads..."):
            token = _get_token(
                st.secrets["AMAZON_CLIENT_ID"],
                st.secrets["AMAZON_CLIENT_SECRET"],
                st.secrets["AMAZON_REFRESH_TOKEN"],
            )
            results = apply_negatives(token, selected_df.to_dict("records"))
        st.success(f"Done! {len(results['success'])} added, {len(results['errors'])} errors.")
        if results["errors"]:
            st.error("Some failed:")
            st.json(results["errors"][:20])

    st.divider()
    st.caption("📅 Data is cached for 7 days. Use 🔄 Refresh Data to force a new pull.")


if __name__ == "__main__":
    main()
