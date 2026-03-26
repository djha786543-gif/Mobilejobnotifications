"""
Pooja Choubey — Biotech/Pharma Industry Job Hunt
Ph.D. Research Scientist | Cardiovascular Biology | Preclinical | Translational Medicine
STRICTLY ISOLATED from DJ's audit job portal.
"""

import os
import sys
import subprocess
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CSV_PATH   = "Scored_Pooja_Leads.csv"
LOG_PATH   = "pooja_scan_log.txt"
NTFY_TOPIC = os.getenv("POOJA_NTFY_TOPIC", "pooja-industry-oppor")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Pooja's Industry Hunt",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("Pooja Choubey — Biotech & Pharma Industry Leads")
st.caption(
    "Ph.D. Research Scientist · Cardiovascular Biology · Preclinical · Translational Medicine  \n"
    f"Open to relocation (US & International) · J1 visa · ntfy: `{NTFY_TOPIC}`"
)

# Pooja's profile snapshot
with st.expander("Candidate profile (click to expand)", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
**Pooja Choubey, Ph.D.**
- Post-Doctoral Research Scientist — Lundquist Institute / Harbor-UCLA, Torrance CA
- Co-first author **Nature Communications 2026** (PTRH2 / peripartum cardiomyopathy)
  — 5,093 accesses · Altmetric 78
- 10+ years preclinical research · managed 200+ mouse colony (3 transgenic lines)
- Mentored 4–6 scientists · grant support: CIRM, Cohen Fellowship, PCVRD trial
""")
    with col2:
        st.markdown("""
**Core skills:**
- In vivo: Langendorff isolation · echocardiography (VEVO F2) · EKG · contractility
- Assays: FACS · Western blot · IHC/ICC · ELISA · qRT-PCR · TUNEL · Beta-gal
- Omics: RNA-seq · scRNA-seq · Xenium · Visium spatial transcriptomics · IPA · STRING
- Target: R&D Scientist / Preclinical Scientist / Translational Scientist at biotech/pharma/CRO
""")

st.divider()

# ---------------------------------------------------------------------------
# Action bar
# ---------------------------------------------------------------------------
col_a, col_b = st.columns([1, 3])

with col_a:
    if st.button("Run Scan Now", width="stretch", type="primary"):
        with st.status("Searching Global Hubs...", expanded=True) as scan_status:
            log_box = st.empty()
            lines   = []

            proc = subprocess.Popen(
                [sys.executable, "-u", "pooja_hunter.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            for line in proc.stdout:
                line = line.rstrip()
                lines.append(line)
                if "[Search]" in line:
                    st.write(line.replace("[Search]", "").strip())
                elif "[Score]" in line:
                    st.write(line.strip())
                elif "[Done]" in line:
                    st.write(f"**{line.strip()}**")
                log_box.code("\n".join(lines[-15:]), language=None)

            proc.wait()

            if proc.returncode == 0:
                scan_status.update(label="Scan Complete!", state="complete", expanded=False)
                st.rerun()
            else:
                scan_status.update(label="Scan Failed", state="error", expanded=True)
                st.error("\n".join(lines[-10:]))

with col_b:
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
    st.info(f"Click **Run Scan Now** to search all biotech/pharma hubs  ·  Last scan: {last_scan}", icon="🔬")

st.divider()

# ---------------------------------------------------------------------------
# Key biotech/pharma hubs tracked
# ---------------------------------------------------------------------------
BIOTECH_HUBS = [
    # US hubs
    "cambridge", "boston", "san diego", "san francisco", "south san francisco",
    "torrance", "los angeles", "irvine", "orange county",
    "philadelphia", "new jersey", "nj", "durham", "raleigh", "research triangle",
    "seattle", "new york", "nyc", "new haven", "connecticut",
    "gaithersburg", "rockville", "maryland", "bethesda",
    "chicago", "houston", "austin", "indianapolis",
    # Europe hubs
    "london", "oxford", "cambridge, united kingdom", "cambridge uk",
    "basel", "zurich", "geneva", "munich", "frankfurt", "berlin", "heidelberg",
    "amsterdam", "leiden", "paris", "lyon", "stockholm", "copenhagen",
    "milan", "dublin", "brussels",
    # India hubs
    "bangalore", "bengaluru", "hyderabad", "pune", "mumbai", "chennai", "delhi",
    "gurugram", "gurgaon", "noida",
]

def is_biotech_hub(loc: str) -> bool:
    loc = str(loc).lower()
    return any(hub in loc for hub in BIOTECH_HUBS)

def score_band(s: int) -> str:
    if s >= 85: return "Elite"
    if s >= 75: return "Strong"
    if s >= 65: return "High"
    if s >= 50: return "Fair"
    return "Weak"

# ---------------------------------------------------------------------------
# Leads table
# ---------------------------------------------------------------------------
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)

    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df = df.dropna(subset=["Score", "Link"])
    df["Score"] = df["Score"].astype(int)
    df["PostedDate"] = pd.to_datetime(df["Posted"], errors="coerce")
    if "Region" not in df.columns:
        df["Region"] = "US"  # backfill legacy rows

    # --- Summary metrics ---
    week_ago  = datetime.now() - timedelta(days=7)
    new_count = int(df[df["PostedDate"] >= week_ago]["Score"].count()) if df["PostedDate"].notna().any() else 0
    hub_count = int(df["Location"].apply(is_biotech_hub).sum())
    llm_count = int((df["ScoredBy"] == "llm").sum()) if "ScoredBy" in df.columns else 0

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Leads",      len(df))
    m2.metric("Elite (85+)",      len(df[df["Score"] >= 85]))
    m3.metric("Strong (75–84)",   len(df[(df["Score"] >= 75) & (df["Score"] < 85)]))
    m4.metric("High (65–74)",     len(df[(df["Score"] >= 65) & (df["Score"] < 75)]))
    m5.metric("Posted This Week", new_count)
    m6.metric("Biotech Hub Roles", hub_count)

    st.write("")

    # --- Filters ---
    fc1, fc2, fc3, fc4, fc5 = st.columns([1, 1, 1, 1, 2])
    with fc1:
        min_score = st.slider("Min score", 0, 100, 50, step=5)
    with fc2:
        recency_opts = {"All time": 0, "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30}
        recency_sel  = st.selectbox("Posted within", list(recency_opts.keys()))
        recency_days = recency_opts[recency_sel]
    with fc3:
        region_filter = st.selectbox("Region", ["All regions", "US", "Europe", "India"])
    with fc4:
        hub_filter = st.selectbox("Location filter", ["All locations", "Major biotech hubs", "LA / Torrance area"])
    with fc5:
        search_text = st.text_input("Search title / company", placeholder="e.g. cardiovascular, genentech, senior scientist")

    filtered = df[df["Score"] >= min_score].copy()

    if region_filter != "All regions":
        filtered = filtered[filtered["Region"] == region_filter]

    if recency_days > 0:
        cutoff   = datetime.now() - timedelta(days=recency_days)
        has_date = filtered["PostedDate"].notna()
        filtered = filtered[~has_date | (filtered["PostedDate"] >= cutoff)]

    if hub_filter == "Major biotech hubs":
        filtered = filtered[filtered["Location"].apply(is_biotech_hub)]
    elif hub_filter == "LA / Torrance area":
        la = ["los angeles", "torrance", "irvine", "long beach", "el segundo",
              "santa monica", "culver city", "burbank", "glendale", "pasadena",
              "orange county", "anaheim", "costa mesa", "newport beach"]
        filtered = filtered[
            filtered["Location"].str.lower().apply(lambda l: any(c in str(l) for c in la))
        ]

    if search_text:
        q = search_text.lower()
        filtered = filtered[
            filtered["Title"].str.lower().str.contains(q, na=False) |
            filtered["Company"].str.lower().str.contains(q, na=False)
        ]

    filtered = filtered.sort_values("Score", ascending=False)

    # --- Display ---
    display = filtered.copy()
    display.insert(1, "Band", display["Score"].apply(score_band))

    show_cols = ["Score", "Band", "Title", "Company", "Location", "Region", "Type", "Posted", "ScoredBy", "Link"]
    if "Source" in display.columns:
        show_cols.insert(-1, "Source")

    st.dataframe(
        display[[c for c in show_cols if c in display.columns]],
        width="stretch",
        column_config={
            "Score":    st.column_config.ProgressColumn("Match %", format="%d%%", min_value=0, max_value=100),
            "Band":     st.column_config.TextColumn("Band", width="small"),
            "Region":   st.column_config.TextColumn("Region", width="small"),
            "Link":     st.column_config.LinkColumn("Apply", display_text="Apply"),
            "Posted":   st.column_config.TextColumn("Posted"),
            "ScoredBy": st.column_config.TextColumn("Scored By", width="small"),
            "Source":   st.column_config.TextColumn("Search Pass", width="medium"),
        },
        hide_index=True,
        height=560,
    )
    st.caption(
        f"Showing {len(filtered)} of {len(df)} total leads  ·  "
        f"Min score: {min_score}  ·  "
        f"{'LLM-scored' if llm_count else 'Keyword-scored'}: {llm_count if llm_count else len(df)} jobs"
    )

    st.download_button(
        "Export to CSV",
        filtered.drop(columns=["PostedDate", "Band"], errors="ignore").to_csv(index=False),
        file_name=f"pooja_bio_leads_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        width="content",
    )

    if os.path.exists(LOG_PATH):
        with st.expander("Scan log"):
            with open(LOG_PATH) as f:
                st.code(f.read()[-4000:], language=None)

else:
    st.warning("No leads yet — click **Run Scan Now** above to start the first scan.")
    st.info(
        "**Mobile push notifications:** Install the free **ntfy** app (iOS / Android) "
        f"and subscribe to topic `{NTFY_TOPIC}` to receive instant alerts for high-match roles.",
        icon="🔔",
    )
    st.markdown("""
    **Search coverage (18 passes — US, Europe & India):**

    🇺🇸 **United States (10 passes)**
    - Cardiovascular / Cardiac Research Scientist — US nationwide
    - Preclinical / In Vivo Scientist — US nationwide
    - Translational / Biomarker Scientist — US nationwide
    - Senior / Staff / Principal Scientist — US nationwide
    - LA / Torrance area (current location)
    - Boston / Cambridge MA (world's #1 biotech cluster)
    - San Diego CA (Pfizer, Illumina, Vertex, Neurocrine)
    - San Francisco Bay Area (Genentech, BioMarin)
    - Philadelphia / NJ (J&J, GSK, Merck, AstraZeneca)
    - Research Triangle Park NC (GSK, Biogen, Novo Nordisk)

    🇪🇺 **Europe (5 passes)**
    - Cambridge UK — AstraZeneca global HQ, Wellcome Sanger, GSK
    - London UK — GSK HQ, UCB Pharma, Immunocore
    - Basel Switzerland — Novartis global HQ, Roche global HQ, Lonza
    - Munich Germany — BioNTech, Bayer AG, Helmholtz Munich
    - Paris France — Sanofi global HQ, Institut Pasteur

    🇮🇳 **India (3 passes)**
    - Bangalore — Biocon, AstraZeneca India R&D, Syngene, Novo Nordisk
    - Hyderabad — Dr. Reddy's, Aurobindo, Cipla R&D
    - Pune — Serum Institute, Lupin R&D, Piramal
    """)
