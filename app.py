import os
import sys
import subprocess
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

CSV_PATH   = "Scored_Audit_Leads.csv"
LOG_PATH   = "scan_log.txt"
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "dj-audit-786543")
SCAN_HOURS = 12   # auto-scan interval

# ---------------------------------------------------------------------------
# Scheduler — runs master_hunter.py every 12 hours
# ---------------------------------------------------------------------------
def run_scan():
    with open(LOG_PATH, "a") as log:
        log.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Scheduled scan started\n")
    subprocess.run([sys.executable, "master_hunter.py"], capture_output=False)
    with open(LOG_PATH, "a") as log:
        log.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Scan finished\n")

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scan, "interval",
        hours=SCAN_HOURS,
        id="auto_scan",
        next_run_time=None,   # don't run immediately on startup
    )
    scheduler.start()
    return scheduler

if "scheduler" not in st.session_state:
    st.session_state.scheduler = start_scheduler()

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
    f"ntfy topic: `{NTFY_TOPIC}` · Auto-scan: every {SCAN_HOURS}h"
)

# ---------------------------------------------------------------------------
# Action bar
# ---------------------------------------------------------------------------
col_a, col_b, col_c = st.columns([1, 1, 2])

with col_a:
    if st.button("Run Scan Now", use_container_width=True, type="primary"):
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
    scheduler = st.session_state.scheduler
    job = scheduler.get_job("auto_scan")
    auto_on = job is not None and job.next_run_time is not None

    label = f"Stop Auto-Scan" if auto_on else f"Start Auto-Scan ({SCAN_HOURS}h)"
    if st.button(label, use_container_width=True):
        if auto_on:
            scheduler.pause_job("auto_scan")
            st.toast(f"Auto-scan paused.")
        else:
            scheduler.resume_job("auto_scan")
            st.toast(f"Auto-scan active — runs every {SCAN_HOURS} hours.")
        st.rerun()

with col_c:
    if auto_on and job.next_run_time:
        nxt = job.next_run_time.strftime("%b %d %H:%M")
        st.info(f"Next auto-scan: {nxt}", icon="🕐")
    else:
        st.caption("Auto-scan is off — click above to enable.")

st.divider()

# ---------------------------------------------------------------------------
# Leads table
# ---------------------------------------------------------------------------
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)

    # Ensure Score is numeric, drop garbage rows
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df = df.dropna(subset=["Score", "Link"])
    df["Score"] = df["Score"].astype(int)

    # Parse posted date for recency filtering
    df["PostedDate"] = pd.to_datetime(df["Posted"], errors="coerce")

    # ---------------------------------------------------------------------------
    # Summary metrics
    # ---------------------------------------------------------------------------
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Leads", len(df))
    m2.metric("Strong (80+)", len(df[df["Score"] >= 80]))
    m3.metric("High (70–79)", len(df[(df["Score"] >= 70) & (df["Score"] < 80)]))
    m4.metric("Fair (50–69)", len(df[(df["Score"] >= 50) & (df["Score"] < 70)]))

    # New in last 7 days
    week_ago = datetime.now() - timedelta(days=7)
    new_count = df[df["PostedDate"] >= week_ago]["Score"].count() if df["PostedDate"].notna().any() else 0
    m5.metric("Posted This Week", int(new_count))

    st.write("")

    # ---------------------------------------------------------------------------
    # Filters
    # ---------------------------------------------------------------------------
    fc1, fc2, fc3 = st.columns([1, 1, 2])
    with fc1:
        min_score = st.slider("Min score", 0, 100, 50, step=5)
    with fc2:
        recency_opts = {"All time": 0, "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30}
        recency_sel = st.selectbox("Posted within", list(recency_opts.keys()))
        recency_days = recency_opts[recency_sel]
    with fc3:
        search_text = st.text_input("Search title / company", placeholder="e.g. sox, deloitte, manager")

    # Apply filters
    filtered = df[df["Score"] >= min_score].copy()

    if recency_days > 0:
        cutoff = datetime.now() - timedelta(days=recency_days)
        has_date = filtered["PostedDate"].notna()
        filtered = filtered[~has_date | (filtered["PostedDate"] >= cutoff)]

    if search_text:
        q = search_text.lower()
        filtered = filtered[
            filtered["Title"].str.lower().str.contains(q, na=False) |
            filtered["Company"].str.lower().str.contains(q, na=False)
        ]

    filtered = filtered.sort_values("Score", ascending=False)

    # ---------------------------------------------------------------------------
    # Score band badge column
    # ---------------------------------------------------------------------------
    def score_band(s):
        if s >= 80:   return "Strong"
        if s >= 70:   return "High"
        if s >= 50:   return "Fair"
        return "Weak"

    display = filtered.copy()
    display.insert(1, "Band", display["Score"].apply(score_band))

    st.dataframe(
        display[["Score", "Band", "Title", "Company", "Location", "Type", "Posted", "ScoredBy", "Link"]],
        use_container_width=True,
        column_config={
            "Score":    st.column_config.ProgressColumn("Match %", format="%d%%", min_value=0, max_value=100),
            "Band":     st.column_config.TextColumn("Band", width="small"),
            "Link":     st.column_config.LinkColumn("Apply", display_text="Apply"),
            "Posted":   st.column_config.TextColumn("Posted"),
            "ScoredBy": st.column_config.TextColumn("Scored By", width="small"),
        },
        hide_index=True,
        height=520,
    )
    st.caption(f"Showing {len(filtered)} of {len(df)} total leads  ·  Min score: {min_score}")

    # Export
    st.download_button(
        "Export to CSV",
        filtered.drop(columns=["PostedDate", "Band"], errors="ignore").to_csv(index=False),
        file_name=f"audit_leads_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

    # Scan log
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
