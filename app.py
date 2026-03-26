import os
import sys
import subprocess
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

# Load .env for local dev (no-op if not present or python-dotenv not installed)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CSV_PATH   = "Scored_Audit_Leads.csv"
LOG_PATH   = "scan_log.txt"
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "dj-audit-786543")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DJ's Audit Hunt",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("IT Audit Contract Leads")
st.caption(
    f"Remote · EAD (J2) authorized · All levels · "
    f"ntfy topic: `{NTFY_TOPIC}` · Auto-scan: every 12h via cloud scheduler"
)

# ---------------------------------------------------------------------------
# Action bar
# ---------------------------------------------------------------------------
col_a, col_b = st.columns([1, 3])

with col_a:
    if st.button("Run Scan Now", width="stretch", type="primary"):
        with st.spinner("Scanning & scoring — this takes a few minutes..."):
            result = subprocess.run(
                [sys.executable, "master_hunter.py"],
                capture_output=True, text=True
            )
        if result.returncode != 0:
            st.error(f"Scan failed:\n{result.stderr[:500]}")
        else:
            st.success("Scan complete — refreshing results.")
            st.rerun()

with col_b:
    # Last scan time from log
    last_scan = "—"
    if os.path.exists(LOG_PATH):
        try:
            with open(LOG_PATH) as f:
                for line in reversed(f.readlines()):
                    if "Scan finished" in line or "Scan started" in line:
                        last_scan = line.strip().lstrip("[").split("]")[0]
                        break
        except Exception:
            pass
    st.info(f"Auto-scan: every 12 h via cloud scheduler  ·  Last scan: {last_scan}", icon="🕐")

st.divider()

# ---------------------------------------------------------------------------
# LA city list (shared between metrics and filter)
# ---------------------------------------------------------------------------
LA_CITIES = [
    "los angeles", "irvine", "long beach", "burbank", "glendale",
    "pasadena", "santa monica", "anaheim", "orange county",
    "el segundo", "torrance", "culver city", "beverly hills",
    "west hollywood", "riverside", "ontario", "corona",
    "rancho cucamonga", "woodland hills", "encino", "sherman oaks",
    "van nuys", "thousand oaks", "ventura", "oxnard",
    "santa clarita", "hawthorne", "inglewood", "manhattan beach",
    "hermosa beach", "redondo beach", "costa mesa", "newport beach",
    "socal", "southern california", "greater los angeles",
]

def is_la(loc: str) -> bool:
    loc = str(loc).lower()
    return any(city in loc for city in LA_CITIES)

# ---------------------------------------------------------------------------
# Leads table
# ---------------------------------------------------------------------------
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)

    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df = df.dropna(subset=["Score", "Link"])
    df["Score"] = df["Score"].astype(int)
    df["PostedDate"] = pd.to_datetime(df["Posted"], errors="coerce")

    # --- Summary metrics ---
    week_ago  = datetime.now() - timedelta(days=7)
    new_count = int(df[df["PostedDate"] >= week_ago]["Score"].count()) if df["PostedDate"].notna().any() else 0
    la_count  = int(df["Location"].apply(is_la).sum())

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Leads",      len(df))
    m2.metric("Strong (80+)",     len(df[df["Score"] >= 80]))
    m3.metric("High (70–79)",     len(df[(df["Score"] >= 70) & (df["Score"] < 80)]))
    m4.metric("Fair (50–69)",     len(df[(df["Score"] >= 50) & (df["Score"] < 70)]))
    m5.metric("Posted This Week", new_count)
    m6.metric("LA-Area Leads",    la_count)

    st.write("")

    # --- Filters ---
    fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 2])
    with fc1:
        min_score = st.slider("Min score", 0, 100, 50, step=5)
    with fc2:
        recency_opts = {"All time": 0, "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30}
        recency_sel  = st.selectbox("Posted within", list(recency_opts.keys()))
        recency_days = recency_opts[recency_sel]
    with fc3:
        loc_filter = st.selectbox("Location type", ["All", "Remote", "LA Area"])
    with fc4:
        search_text = st.text_input("Search title / company", placeholder="e.g. sox, deloitte, grc")

    filtered = df[df["Score"] >= min_score].copy()

    if recency_days > 0:
        cutoff   = datetime.now() - timedelta(days=recency_days)
        has_date = filtered["PostedDate"].notna()
        filtered = filtered[~has_date | (filtered["PostedDate"] >= cutoff)]

    if loc_filter == "Remote":
        filtered = filtered[
            filtered["Location"].str.lower().str.contains("remote", na=False) |
            filtered["Location"].str.lower().str.contains("united states", na=False)
        ]
    elif loc_filter == "LA Area":
        filtered = filtered[filtered["Location"].apply(is_la)]

    if search_text:
        q = search_text.lower()
        filtered = filtered[
            filtered["Title"].str.lower().str.contains(q, na=False) |
            filtered["Company"].str.lower().str.contains(q, na=False)
        ]

    filtered = filtered.sort_values("Score", ascending=False)

    # --- Score band column ---
    def score_band(s):
        if s >= 80: return "Strong"
        if s >= 70: return "High"
        if s >= 50: return "Fair"
        return "Weak"

    display = filtered.copy()
    display.insert(1, "Band", display["Score"].apply(score_band))

    show_cols = ["Score", "Band", "Title", "Company", "Location", "Type", "Posted", "ScoredBy", "Link"]
    if "Source" in display.columns:
        show_cols.insert(-1, "Source")

    st.dataframe(
        display[[c for c in show_cols if c in display.columns]],
        width="stretch",
        column_config={
            "Score":    st.column_config.ProgressColumn("Match %", format="%d%%", min_value=0, max_value=100),
            "Band":     st.column_config.TextColumn("Band", width="small"),
            "Link":     st.column_config.LinkColumn("Apply", display_text="Apply"),
            "Posted":   st.column_config.TextColumn("Posted"),
            "ScoredBy": st.column_config.TextColumn("Scored By", width="small"),
            "Source":   st.column_config.TextColumn("Search Pass", width="medium"),
        },
        hide_index=True,
        height=540,
    )
    st.caption(f"Showing {len(filtered)} of {len(df)} total leads  ·  Min score: {min_score}")

    st.download_button(
        "Export to CSV",
        filtered.drop(columns=["PostedDate", "Band"], errors="ignore").to_csv(index=False),
        file_name=f"audit_leads_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        width="content",
    )

    if os.path.exists(LOG_PATH):
        with st.expander("Scan log"):
            with open(LOG_PATH) as f:
                st.code(f.read()[-4000:], language=None)

else:
    st.warning("No leads yet — click **Run Scan Now** above to start.")
    st.info(
        "**Mobile push notifications:** Install the free **ntfy** app "
        f"(iOS / Android) and subscribe to topic `{NTFY_TOPIC}` "
        "to receive instant alerts when a high-match job is found.",
        icon="🔔",
    )
