import os
import sys
import json
import tempfile
import subprocess
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from pwa_inject import inject_pwa

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
inject_pwa("DJ Audit Hunt")

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("💼 DJ's Audit Hunt")
st.caption("Remote · EAD (J2) · IT Audit / GRC / SOX · Auto-scan every 12h")

with st.expander("📲 Install as App on your phone", expanded=False):
    st.markdown("""
**Android (Chrome):** Open this page → tap the ⋮ menu → **"Add to Home Screen"** or **"Install app"** → tap Add
**iPhone / iPad (Safari):** Open this page in Safari → tap the **Share** button (⬆) → **"Add to Home Screen"** → Add

The app will open full-screen, no browser bar — just like a native app.
""")

# ---------------------------------------------------------------------------
# Action bar — full width for mobile tap
# ---------------------------------------------------------------------------
if st.button("🔍  Run Scan Now", use_container_width=True, type="primary"):
    with st.spinner("Scanning LinkedIn, Indeed, Glassdoor, ZipRecruiter — takes a few minutes..."):
        result = subprocess.run(
            [sys.executable, "master_hunter.py"],
            capture_output=True, text=True
        )
    if result.returncode != 0:
        st.error(f"Scan failed:\n{result.stderr[:500]}")
    else:
        st.success("Scan complete!")
        st.rerun()

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
st.caption(f"🕐 Last scan: {last_scan}")
st.divider()

# ---------------------------------------------------------------------------
# Helpers
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

NO_AGENT_DOMAINS = [
    "workday.com", "myworkdayjobs.com", "dayforcehcm.com", "dayforce.com",
    "icims.com", "taleo.net", "taleo.com", "paradox.ai", "avature.net",
    "successfactors.com", "sap.com/careers", "oraclecloud.com", "oracle.com/careers",
    "adp.com", "ultipro.com", "ukg.com", "jobvite.com", "smartrecruiters.com",
    "careers.lennar.com", "careers.walmart.com", "jobs.boeing.com", "amazon.jobs",
]
YES_AGENT_DOMAINS = [
    "linkedin.com", "greenhouse.io", "boards.greenhouse.io",
    "lever.co", "jobs.lever.co",
]

def agent_feasibility(url: str) -> str:
    if not url or not isinstance(url, str):
        return "?"
    url_lower = url.lower()
    if any(d in url_lower for d in NO_AGENT_DOMAINS):
        return "No"
    if any(d in url_lower for d in YES_AGENT_DOMAINS):
        return "Yes"
    return "?"

def score_band(s):
    if s >= 80: return "Strong"
    if s >= 70: return "High"
    if s >= 50: return "Fair"
    return "Weak"

def _classify_type(t: str) -> str:
    t = str(t).lower().strip()
    if any(k in t for k in ["contract", "temp", "freelance", "contractor"]):
        return "Contract"
    if any(k in t for k in ["part", "parttime", "part-time"]):
        return "Part-time"
    if any(k in t for k in ["full", "fulltime", "full-time", "permanent"]):
        return "Full-time"
    return "Other"

def _score_color(s: int) -> str:
    if s >= 80: return "#00c853"
    if s >= 70: return "#ff9800"
    if s >= 50: return "#2196f3"
    return "#9e9e9e"

# ---------------------------------------------------------------------------
# Card renderer — mobile-first tap-friendly view
# ---------------------------------------------------------------------------
def render_cards(jobs: pd.DataFrame, key_prefix: str):
    if jobs.empty:
        st.info("No leads match the current filters.")
        return
    for i, (_, row) in enumerate(jobs.head(50).iterrows()):
        score  = int(row.get("Score", 0))
        band   = row.get("Band", score_band(score))
        title  = row.get("Title", "")
        company= row.get("Company", "")
        loc    = row.get("Location", "")
        jtype  = row.get("Type", "")
        posted = row.get("Posted", "")
        link   = row.get("Link", "")
        color  = _score_color(score)

        with st.container(border=True):
            left, right = st.columns([1, 5])
            with left:
                st.markdown(
                    f"<div style='text-align:center;padding-top:4px'>"
                    f"<span style='font-size:1.4rem;font-weight:700;color:{color}'>{score}%</span><br>"
                    f"<span style='font-size:0.65rem;color:{color}'>{band}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with right:
                st.markdown(f"**{title}**")
                st.markdown(f"{company}  ·  {loc}")
                meta = "  ·  ".join(str(x) for x in [jtype, posted] if x and str(x) not in ("nan", "None", ""))
                st.caption(meta)
            if link:
                st.link_button("Apply →", link, use_container_width=True, type="primary")

# ---------------------------------------------------------------------------
# Table renderer
# ---------------------------------------------------------------------------
_COL_CFG = {
    "Score":    st.column_config.ProgressColumn("Match %", format="%d%%", min_value=0, max_value=100),
    "Agent?":   st.column_config.TextColumn("Agent?", width="small"),
    "Band":     st.column_config.TextColumn("Band", width="small"),
    "Link":     st.column_config.LinkColumn("Apply", display_text="Apply"),
    "Posted":   st.column_config.TextColumn("Posted"),
    "ScoredBy": st.column_config.TextColumn("Scored By", width="small"),
    "Source":   st.column_config.TextColumn("Search Pass", width="medium"),
}
SHOW_COLS = ["Score", "Agent?", "Band", "Title", "Company", "Location", "Type", "Posted", "ScoredBy", "Link"]

def render_table(jobs: pd.DataFrame):
    if jobs.empty:
        st.info("No leads match the current filters.")
        return
    cols = [c for c in SHOW_COLS if c in jobs.columns]
    if "Source" in jobs.columns:
        cols.insert(-1, "Source")
    st.dataframe(jobs[cols], column_config=_COL_CFG, hide_index=True,
                 height=480, width="stretch")

# ---------------------------------------------------------------------------
# Main — leads
# ---------------------------------------------------------------------------
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df = df.dropna(subset=["Score", "Link"])
    df["Score"] = df["Score"].astype(int)
    df["PostedDate"] = pd.to_datetime(df["Posted"], errors="coerce")

    # Identify latest scan batch (jobs scanned within 2h of the most recent ScannedAt)
    if "ScannedAt" in df.columns:
        df["ScannedAt"] = pd.to_datetime(df["ScannedAt"], errors="coerce")
        latest_scan_dt  = df["ScannedAt"].max()
        df["IsNew"]     = df["ScannedAt"] >= (latest_scan_dt - pd.Timedelta(hours=2))
    else:
        df["IsNew"]     = False
        latest_scan_dt  = None
    new_scan_count = int(df["IsNew"].sum())

    # --- Metrics — 3 per row (fits mobile) ---
    la_count  = int(df["Location"].apply(is_la).sum())

    m1, m2, m3 = st.columns(3)
    m1.metric("Total",           len(df))
    m2.metric("Strong 80+",      len(df[df["Score"] >= 80]))
    m3.metric("High 70–79",      len(df[(df["Score"] >= 70) & (df["Score"] < 80)]))
    m4, m5, m6 = st.columns(3)
    m4.metric("Fair 50–69",      len(df[(df["Score"] >= 50) & (df["Score"] < 70)]))
    m5.metric("New This Scan",   new_scan_count)
    m6.metric("LA Area",         la_count)

    st.write("")

    # --- New Opportunities section (latest scan only) ---
    _new_base = df[df["IsNew"]].copy()
    if not _new_base.empty:
        scan_label = (
            latest_scan_dt.strftime("%d %b %Y, %H:%M").lstrip("0") if latest_scan_dt and pd.notna(latest_scan_dt) else "latest"
        )
        with st.expander(f"New Opportunities — {len(_new_base)} new from scan on {scan_label}", expanded=True):
            # Filters
            nf1, nf2 = st.columns(2)
            with nf1:
                n_min_score = st.slider("Min score", 0, 100, 50, step=5, key="new_min_score")
            with nf2:
                n_loc_filter = st.selectbox("Location", ["All", "Remote", "LA Area"], key="new_loc_filter")
            nf3, nf4 = st.columns(2)
            with nf3:
                n_recency_opts = {"All time": 0, "Last 7d": 7, "Last 14d": 14, "Last 30d": 30}
                n_recency_sel  = st.selectbox("Posted within", list(n_recency_opts.keys()), key="new_recency")
                n_recency_days = n_recency_opts[n_recency_sel]
            with nf4:
                n_search = st.text_input("Search", placeholder="sox, deloitte, grc…", key="new_search")

            # Apply filters
            new_filtered = _new_base[_new_base["Score"] >= n_min_score].copy()
            if n_recency_days > 0:
                n_cutoff  = datetime.now() - timedelta(days=n_recency_days)
                n_has_date = new_filtered["PostedDate"].notna()
                new_filtered = new_filtered[~n_has_date | (new_filtered["PostedDate"] >= n_cutoff)]
            if n_loc_filter == "Remote":
                new_filtered = new_filtered[
                    new_filtered["Location"].str.lower().str.contains("remote", na=False) |
                    new_filtered["Location"].str.lower().str.contains("united states", na=False)
                ]
            elif n_loc_filter == "LA Area":
                new_filtered = new_filtered[new_filtered["Location"].apply(is_la)]
            if n_search:
                _q = n_search.lower()
                new_filtered = new_filtered[
                    new_filtered["Title"].str.lower().str.contains(_q, na=False) |
                    new_filtered["Company"].str.lower().str.contains(_q, na=False)
                ]
            new_filtered = new_filtered.sort_values("Score", ascending=False)
            new_display  = new_filtered.copy()
            new_display.insert(1, "Band", new_display["Score"].apply(score_band))
            new_display["Agent?"] = new_display["Link"].apply(agent_feasibility)

            # View toggle
            nvc, nnc = st.columns([3, 1])
            with nvc:
                n_view = st.radio("View as", ["Cards", "Table"], horizontal=True,
                                  key="new_view", label_visibility="collapsed")
            with nnc:
                st.caption(f"{len(new_filtered)} leads")

            if n_view == "Cards":
                render_cards(new_display, "new")
            else:
                render_table(new_display)
        st.divider()

    # --- Filters inside expander — 2-col grid fits any screen ---
    with st.expander("Filters", expanded=False):
        f1, f2 = st.columns(2)
        with f1:
            min_score = st.slider("Min score", 0, 100, 50, step=5)
        with f2:
            recency_opts = {"All time": 0, "Last 7d": 7, "Last 14d": 14, "Last 30d": 30}
            recency_sel  = st.selectbox("Posted within", list(recency_opts.keys()))
            recency_days = recency_opts[recency_sel]
        f3, f4 = st.columns(2)
        with f3:
            loc_filter = st.selectbox("Location", ["All", "Remote", "LA Area"])
        with f4:
            search_text = st.text_input("Search", placeholder="sox, deloitte, grc…")

    # --- Apply filters ---
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

    display = filtered.copy()
    display.insert(1, "Band", display["Score"].apply(score_band))
    display["Agent?"] = display["Link"].apply(agent_feasibility)

    # --- View toggle ---
    vcol, ncol = st.columns([3, 1])
    with vcol:
        view = st.radio("View as", ["Cards", "Table"], horizontal=True,
                        key="dj_view", label_visibility="collapsed")
    with ncol:
        st.caption(f"{len(filtered)} leads")

    if view == "Cards":
        render_cards(display, "all")
    else:
        render_table(display)

    st.download_button(
        "Export CSV",
        filtered.drop(columns=["PostedDate", "Band"], errors="ignore").to_csv(index=False),
        file_name=f"audit_leads_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    # ── Job Type Breakdown ────────────────────────────────────────────────
    st.write("")
    st.divider()
    st.subheader("📋 Job Type Breakdown")
    st.caption("Contract + Remote is your priority. Filters above apply here too.")

    typed = display.copy()
    typed["_cat"] = typed["Type"].apply(_classify_type) if "Type" in typed.columns else "Other"

    contract_df = typed[typed["_cat"] == "Contract"].copy()
    parttime_df = typed[typed["_cat"] == "Part-time"].copy()
    fulltime_df = typed[typed["_cat"] == "Full-time"].copy()
    unknown_df  = typed[typed["_cat"] == "Other"].copy()

    _is_remote  = contract_df["Location"].str.lower().str.contains("remote|united states", na=False)
    contract_df = pd.concat([contract_df[_is_remote], contract_df[~_is_remote]])
    n_remote    = int(_is_remote.sum())

    t1, t2, t3, t4 = st.tabs([
        f"🎯 Contract ({len(contract_df)})",
        f"⏱️ Part-time ({len(parttime_df)})",
        f"🏢 Full-time ({len(fulltime_df)})",
        f"❓ Other ({len(unknown_df)})",
    ])

    with t1:
        if contract_df.empty:
            st.info("No contract leads match current filters.")
        else:
            st.caption(f"Remote-first · {n_remote} remote / {len(contract_df) - n_remote} on-site or hybrid")
            if view == "Cards":
                render_cards(contract_df, "contract")
            else:
                render_table(contract_df)
    with t2:
        if view == "Cards":
            render_cards(parttime_df, "pt")
        else:
            render_table(parttime_df)
    with t3:
        if view == "Cards":
            render_cards(fulltime_df, "ft")
        else:
            render_table(fulltime_df)
    with t4:
        if not unknown_df.empty:
            st.caption("Type not specified in posting.")
        if view == "Cards":
            render_cards(unknown_df, "unk")
        else:
            render_table(unknown_df)

    # ── Scan log ──────────────────────────────────────────────────────────
    if os.path.exists(LOG_PATH):
        with st.expander("Scan log"):
            with open(LOG_PATH) as f:
                st.code(f.read()[-4000:], language=None)

    # ── Sitting Agent (desktop only) ─────────────────────────────────────
    _IS_LOCAL = os.getenv("IS_LOCAL_RUN", "false").lower() == "true"
    st.write("")
    st.divider()
    _agent_mode = st.toggle("🤖 Agent Mode — auto-fill applications (desktop only)",
                            value=False, key="dj_agent_toggle")

    if _agent_mode:
        if not _IS_LOCAL:
            st.warning(
                "**Sitting Agent requires your Windows PC.**  \n"
                "Add `IS_LOCAL_RUN=true` to your `.env` and run `streamlit run app.py` locally.",
                icon="🖥️",
            )
        else:
            st.info(
                "Agent fills all fields then **stops — never clicks Submit.**  \n"
                "You review and submit yourself.",
                icon="✋",
            )

            def _launch_dj_agent(job: dict):
                tmp = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False, prefix="agent_job_"
                )
                json.dump(job, tmp, default=lambda o: o.item() if hasattr(o, "item") else str(o))
                tmp.close()
                import platform
                popen_kwargs = dict(
                    args=[sys.executable, "sitting_agent/browser_agent.py",
                          "--job-file",     tmp.name,
                          "--profile-file", "profiles/deobrat_profile.json"],
                )
                if platform.system() == "Windows":
                    popen_kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
                subprocess.Popen(**popen_kwargs)
                st.success(f"🚀 Agent launched for **{job.get('Title', '')}** @ **{job.get('Company', '')}**")

            st.caption(f"Top {min(len(filtered), 50)} leads · tap 🚀 to launch")
            for _i, (_, _row) in enumerate(filtered.head(50).iterrows()):
                with st.container(border=True):
                    _ca, _cb = st.columns([5, 1])
                    with _ca:
                        st.markdown(f"**{_row['Score']}%** · {_row['Title']}")
                        st.caption(f"{_row.get('Company', '')}  ·  {_row.get('Location', '')}")
                    with _cb:
                        if st.button("🚀", key=f"dj_agent_{_i}",
                                     help=f"Launch: {_row.get('Title', '')}"):
                            _launch_dj_agent(_row.to_dict())

else:
    st.warning("No leads yet — tap **Run Scan Now** above to start.")
    st.info(
        "**Push notifications:** Install the free **ntfy** app (Android/iOS) "
        f"and subscribe to topic `{NTFY_TOPIC}` for instant alerts.",
        icon="🔔",
    )
