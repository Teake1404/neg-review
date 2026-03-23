"""
Streamlit Negative Keyword Review App

Run locally:
    streamlit run app.py
"""

from datetime import datetime, timedelta
import gzip
import json
import time
from typing import Dict, List

import pandas as pd
import requests
import streamlit as st

EU_API = "https://advertising-api-eu.amazon.com"
MIN_SPEND = 500
MAX_ACOS = 0.30
LOOKBACK_DAYS = 30


def get_profile_map() -> Dict[str, str]:
    """
    Returns Menhood India profile id -> label map from st.secrets.
    Supports either:
      1) MENHOOD_PROFILES = {"id1": "Label 1", "id2": "Label 2"}
      2) MENHOOD_PROFILE_1 / MENHOOD_PROFILE_2
    """
    if "MENHOOD_PROFILES" in st.secrets:
        profiles = st.secrets["MENHOOD_PROFILES"]
        return {str(k): str(v) for k, v in profiles.items()}

    p1 = str(st.secrets.get("MENHOOD_PROFILE_1", "")).strip()
    p2 = str(st.secrets.get("MENHOOD_PROFILE_2", "")).strip()
    profile_map: Dict[str, str] = {}
    if p1:
        profile_map[p1] = "Menhood India - Profile 1"
    if p2:
        profile_map[p2] = "Menhood India - Profile 2"
    return profile_map


def get_token() -> str:
    client_id = st.secrets["AMAZON_CLIENT_ID"]
    client_secret = st.secrets["AMAZON_CLIENT_SECRET"]
    refresh_token = st.secrets["AMAZON_REFRESH_TOKEN"]

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
    r.raise_for_status()
    return r.json()["access_token"]


def fetch_flagged_terms(token: str, profile_id: str) -> List[Dict]:
    client_id = st.secrets["AMAZON_CLIENT_ID"]
    end_date = datetime.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    headers = {
        "Amazon-Advertising-API-ClientId": client_id,
        "Amazon-Advertising-API-Scope": profile_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    body = {
        "name": f"NegKW Review {end_date.strftime('%Y-%m-%d')}",
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["searchTerm"],
            "columns": [
                "campaignId",
                "campaignName",
                "adGroupId",
                "adGroupName",
                "searchTerm",
                "impressions",
                "clicks",
                "cost",
                "purchases7d",
                "sales7d",
            ],
            "reportTypeId": "spSearchTerm",
            "timeUnit": "SUMMARY",
            "format": "GZIP_JSON",
        },
    }

    rr = requests.post(f"{EU_API}/reporting/reports", headers=headers, json=body, timeout=30)
    rr.raise_for_status()
    report_id = rr.json().get("reportId")
    if not report_id:
        return []

    for _ in range(30):
        time.sleep(10)
        rs = requests.get(f"{EU_API}/reporting/reports/{report_id}", headers=headers, timeout=30)
        rs.raise_for_status()
        status = rs.json()
        if status.get("status") == "COMPLETED":
            raw = requests.get(status["url"], timeout=60)
            raw.raise_for_status()
            data = json.loads(gzip.decompress(raw.content))
            flagged = []

            for row in data:
                spend = float(row.get("cost", 0) or 0)
                sales = float(row.get("sales7d", 0) or 0)
                orders = int(row.get("purchases7d", 0) or 0)
                acos = spend / sales if sales > 0 else None

                # flagged logic: spend >= 500 and (zero orders OR acos > 30%)
                if spend >= MIN_SPEND and (orders == 0 or (acos is not None and acos > MAX_ACOS)):
                    flagged.append(
                        {
                            "selected": True,
                            "id": f"{row.get('campaignId')}_{row.get('adGroupId')}_{row.get('searchTerm', '')}",
                            "searchTerm": row.get("searchTerm", ""),
                            "campaignId": str(row.get("campaignId", "")),
                            "campaignName": row.get("campaignName", ""),
                            "adGroupId": str(row.get("adGroupId", "")),
                            "adGroupName": row.get("adGroupName", ""),
                            "spend": round(spend, 2),
                            "sales": round(sales, 2),
                            "orders": orders,
                            "acosPct": round((acos or 0) * 100, 2) if acos is not None else None,
                            "reason": "Zero Orders" if orders == 0 else "High ACoS",
                            "profile": profile_id,
                        }
                    )

            return sorted(flagged, key=lambda x: x["spend"], reverse=True)
        if status.get("status") == "FAILED":
            return []

    return []


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
                json={"negativeKeywords": payload[i : i + 1000]},
                timeout=30,
            )
            r.raise_for_status()
            rd = r.json()
            results["success"].extend(rd.get("negativeKeywords", {}).get("success", []))
            results["errors"].extend(rd.get("negativeKeywords", {}).get("error", []))

    return results


def load_all_terms() -> pd.DataFrame:
    profile_map = get_profile_map()
    if len(profile_map) < 2:
        raise RuntimeError("Please set both Menhood profile IDs in st.secrets.")

    token = get_token()
    all_terms: List[Dict] = []

    for pid, label in profile_map.items():
        terms = fetch_flagged_terms(token, pid)
        for t in terms:
            t["accountLabel"] = label
            all_terms.append(t)

    if not all_terms:
        return pd.DataFrame(
            columns=[
                "selected",
                "searchTerm",
                "campaignName",
                "adGroupName",
                "spend",
                "orders",
                "acosPct",
                "reason",
                "accountLabel",
                "campaignId",
                "adGroupId",
                "profile",
                "sales",
                "id",
            ]
        )

    return pd.DataFrame(all_terms)


def main() -> None:
    st.set_page_config(page_title="Negative Keyword Review", layout="wide")
    st.title("Negative Keyword Review")
    st.caption("Rajesh can deselect terms to keep, then apply selected negatives.")

    # Initial load on first render
    if "terms_df" not in st.session_state:
        with st.spinner("Pulling flagged terms from Amazon Ads API..."):
            st.session_state["terms_df"] = load_all_terms()
        st.session_state["date_range"] = (
            (datetime.today() - timedelta(days=LOOKBACK_DAYS + 1)).strftime("%Y-%m-%d"),
            (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        )
        st.session_state["active_filter"] = "All"

    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        if st.button("Refresh Data"):
            with st.spinner("Refreshing from Amazon Ads API..."):
                st.session_state["terms_df"] = load_all_terms()

    with col_b:
        st.write("")

    with col_c:
        st.markdown(
            f"**Date Range:** `{st.session_state['date_range'][0]}` to `{st.session_state['date_range'][1]}`"
        )

    df = st.session_state["terms_df"].copy()
    total_waste = float(df["spend"].sum()) if not df.empty else 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric("Terms Flagged", int(len(df)))
    k2.metric("Total Wasted Spend", f"₹{total_waste:,.0f}")
    k3.metric("Date Range", f"{st.session_state['date_range'][0]} → {st.session_state['date_range'][1]}")

    st.markdown("### Filters")
    f1, f2, f3 = st.columns(3)
    if f1.button("All", use_container_width=True):
        st.session_state["active_filter"] = "All"
    if f2.button("Zero Orders", use_container_width=True):
        st.session_state["active_filter"] = "Zero Orders"
    if f3.button("High ACoS", use_container_width=True):
        st.session_state["active_filter"] = "High ACoS"

    active_filter = st.session_state.get("active_filter", "All")
    if active_filter == "Zero Orders":
        view_df = df[df["orders"] == 0].copy()
    elif active_filter == "High ACoS":
        view_df = df[(df["orders"] > 0) & (df["acosPct"] > 30)].copy()
    else:
        view_df = df.copy()

    st.markdown(f"### Flagged Terms ({active_filter})")
    editable_cols = [
        "selected",
        "searchTerm",
        "campaignName",
        "adGroupName",
        "spend",
        "orders",
        "acosPct",
        "reason",
        "accountLabel",
    ]
    edited = st.data_editor(
        view_df[editable_cols],
        hide_index=True,
        use_container_width=True,
        disabled=["searchTerm", "campaignName", "adGroupName", "spend", "orders", "acosPct", "reason", "accountLabel"],
        column_config={
            "selected": st.column_config.CheckboxColumn("Keep selected for negative"),
            "spend": st.column_config.NumberColumn("Spend (₹)", format="%.2f"),
            "acosPct": st.column_config.NumberColumn("ACoS %", format="%.2f"),
        },
        key="editor",
    )

    # Merge selected checkboxes back into main dataframe
    if not edited.empty:
        selection_map = dict(zip(view_df["id"], edited["selected"]))
        st.session_state["terms_df"]["selected"] = st.session_state["terms_df"].apply(
            lambda r: selection_map.get(r["id"], r["selected"]),
            axis=1,
        )

    selected_df = st.session_state["terms_df"][st.session_state["terms_df"]["selected"] == True].copy()
    st.info(f"Selected terms to negate: {len(selected_df)}")

    if st.button("Apply Negatives", type="primary", disabled=selected_df.empty):
        with st.spinner("Applying negatives..."):
            token = get_token()
            results = apply_negatives(token, selected_df.to_dict("records"))
        st.success(
            f"Applied negatives: {len(results['success'])} success, {len(results['errors'])} errors."
        )
        if results["errors"]:
            st.error("Some rows failed. See details below.")
            st.json(results["errors"][:20])


if __name__ == "__main__":
    main()

