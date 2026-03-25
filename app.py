import os
import sys
import subprocess
import threading
import pandas as pd
import streamlit as st
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

CSV_PATH   = "Scored_Audit_Leads.csv"
LOG_PATH   = "scan_log.txt"
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "dj-audit-hunt")

# ---------------------------------------------------------------------------
# Auto-scheduler — runs master_hunter.py every 6 hours in the background
# ---------------------------------------------------------------------------
def run_scan():
    with open(LOG_PATH, "a") as log:
        log.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Scheduled scan started\n")
    subprocess.run([sys.executable, "master_hunter.py"], capture_output=False)
    with open(LOG_PATH, "a") as log:
        log.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Scan finished\n")

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_scan, "interval", hours=6, id="auto_scan",
                      next_run_time=None)   # don't run immediately on startup
    scheduler.start()
    return scheduler

# Start scheduler once per server process
if "scheduler" not in st.session_state:
    st.session_state.scheduler = start_scheduler()

# ---------------------------------------------------------------------------
# Page config — wide on desktop, stacks well on mobile
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DJ's Audit Hunt",
    page_icon="briefcase",
    layout="wide",
    initial_sidebar_state="collapsed",   # collapsed by default on mobile
)

st.title("IT Audit Contract Leads")
st.caption(f"Remote · EAD (J2) authorized · All levels · ntfy topic: `{NTFY_TOPIC}`")

# ---------------------------------------------------------------------------
# Top action bar — works well on mobile as stacked buttons
# ---------------------------------------------------------------------------
col_a, col_b, col_c = st.columns([1, 1, 2])

with col_a:
    if st.button("Run Scan Now", use_container_width=True, type="primary"):
        with st.spinner("Scanning & scoring..."):
            result = subprocess.run(
                [sys.executable, "master_hunter.py"],
                capture_output=True, text=True
            )
        if result.returncode != 0:
            st.error(f"Scan failed:\n{result.stderr[:400]}")
        else:
            st.success("Scan complete.")
            st.rerun()

with col_b:
    scheduler = st.session_state.scheduler
    job = scheduler.get_job("auto_scan")
    auto_on = job is not None and job.next_run_time is not None

    if st.button(
        "Stop Auto-Scan" if auto_on else "Start Auto-Scan (6h)",
        use_container_width=True
    ):
        if auto_on:
            scheduler.pause_job("auto_scan")
            st.toast("Auto-scan paused.")
        else:
            scheduler.resume_job("auto_scan")
            st.toast("Auto-scan active — runs every 6 hours.")
        st.rerun()

with col_c:
    if auto_on and job.next_run_time:
        next_run = job.next_run_time.strftime("%b %d %H:%M")
        st.info(f"Next auto-scan: {next_run}", icon="clock")
    else:
        st.caption("Auto-scan is off.")

st.divider()

# ---------------------------------------------------------------------------
# Leads table
# ---------------------------------------------------------------------------
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)

    # Summary metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Leads", len(df))
    m2.metric("Top Match (90+)", len(df[df["Score"] >= 90]))
    m3.metric("Strong (70–89)", len(df[(df["Score"] >= 70) & (df["Score"] < 90)]))
    m4.metric("Fair (50–69)", len(df[(df["Score"] >= 50) & (df["Score"] < 70)]))

    st.write("")

    # Filter controls
    fc1, fc2 = st.columns([1, 2])
    with fc1:
        min_score = st.slider("Min score", 0, 100, 60, step=5)
    with fc2:
        search_text = st.text_input("Search title / company", placeholder="e.g. manager, deloitte")

    filtered = df[df["Score"] >= min_score].copy()
    if search_text:
        q = search_text.lower()
        filtered = filtered[
            filtered["Title"].str.lower().str.contains(q, na=False) |
            filtered["Company"].str.lower().str.contains(q, na=False)
        ]
    filtered = filtered.sort_values("Score", ascending=False)

    st.dataframe(
        filtered,
        use_container_width=True,
        column_config={
            "Score":   st.column_config.ProgressColumn("Match %", format="%d%%", min_value=0, max_value=100),
            "Link":    st.column_config.LinkColumn("Apply"),
            "Posted":  "Posted",
            "Type":    "Type",
        },
        hide_index=True,
        height=480,
    )
    st.caption(f"Showing {len(filtered)} of {len(df)} total leads")

    # Export
    st.download_button(
        "Export to CSV",
        filtered.to_csv(index=False),
        file_name="audit_leads_export.csv",
        mime="text/csv",
    )

    # Scan log expander
    if os.path.exists(LOG_PATH):
        with st.expander("Scan log"):
            with open(LOG_PATH) as f:
                st.code(f.read()[-3000:], language=None)

else:
    st.warning("No leads yet — click **Run Scan Now** above.")
    st.info(
        "**Mobile push notifications:** Install the free **ntfy** app "
        f"(iOS / Android) and subscribe to topic `{NTFY_TOPIC}` "
        "to receive instant alerts when a high-match job is found.",
        icon="bell",
    )
