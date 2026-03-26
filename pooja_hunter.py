"""
pooja_hunter.py — Pooja Choubey's Biotech/Pharma Job Scanner
Ph.D. Research Scientist | Cardiovascular Biology | Preclinical | Translational Medicine
STRICTLY ISOLATED from DJ's audit scanner.
"""

import os
import re
import sys
import time
import requests
import pandas as pd
from datetime import datetime
from jobspy import scrape_jobs

# Load .env for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def sprint(*args, **kwargs):
    text = " ".join(str(a) for a in args)
    safe = text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
           sys.stdout.encoding or "utf-8", errors="replace")
    print(safe, **kwargs)

# --- CONFIG (completely separate from DJ) ---
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
NTFY_TOPIC     = os.getenv("POOJA_NTFY_TOPIC", "pooja-industry-oppor")
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN")
GITHUB_REPO    = os.getenv("GITHUB_REPO", "djha786543-gif/Mobilejobnotifications")
CSV_PATH       = "Scored_Bio_Leads.csv"
GROQ_MODEL     = "llama-3.3-70b-versatile"
GROQ_ENDPOINT  = "https://api.groq.com/openai/v1/chat/completions"
MIN_SAVE_SCORE = 35
MAX_ALERTS     = 10
SCORE_TOP_N    = 160

# ---------------------------------------------------------------------------
# Title whitelist — research science roles in biotech/pharma/CRO space
# ---------------------------------------------------------------------------
TITLE_WHITELIST = [
    r"\bresearch\s+scientist\b",
    r"\bscientist\s+(?:i+|1|2|3|4)\b",
    r"\bsenior\s+(?:research\s+)?scientist\b",
    r"\bstaff\s+scientist\b",
    r"\bprincipal\s+scientist\b",
    r"\bassociate\s+(?:research\s+)?scientist\b",
    r"\btranslational\s+(?:scientist|research|medicine|biology)\b",
    r"\bpreclinical\s+(?:scientist|research|researcher)\b",
    r"\bin\s+vivo\s+(?:scientist|research|researcher|biologist)\b",
    r"\bcardiovascular\s+(?:scientist|research|researcher|biologist|biology)\b",
    r"\bcardiac\s+(?:scientist|research|researcher|biologist|biology)\b",
    r"\bbiomarker\s+(?:scientist|discovery|research|researcher)\b",
    r"\bdisease\s+model(?:ing|ler)?\b",
    r"\bdrug\s+discovery\s+(?:scientist|researcher|biologist)\b",
    r"\bpharmacolog(?:ist|y\s+scientist|y\s+researcher)\b",
    r"\bflow\s+cytometry\s+(?:scientist|specialist)\b",
    r"\bimmunolog(?:ist|y\s+scientist)\b",
    r"\bmolecular\s+biolog(?:ist|y\s+scientist)\b",
    r"\bcell\s+biolog(?:ist|y\s+scientist)\b",
    r"\br&d\s+scientist\b",
    r"\bscientific\s+researcher\b",
    r"\bin\s+vitro\s+in\s+vivo\b",
    r"\bmouse\s+(?:model|colony|genetics|phenotyping)\b",
    r"\btransgenic\s+(?:mouse|model|scientist)\b",
    r"\bomics\s+scientist\b",
    r"\bspatial\s+(?:transcriptomics|genomics)\s+scientist\b",
]

# Hard blacklist — veto even if whitelist matched
# Exception: explicit science/research keyword overrides these
TITLE_BLACKLIST = [
    r"\bsoftware\s+(?:engineer|developer)\b",
    r"\bdata\s+(?:engineer|architect)\b",
    r"\bdata\s+scientist\b",        # pure ML/AI data science
    r"\bbioinformatician\b",        # pure bioinformatics (no wet lab)
    r"\bsales\b|\bmarketing\b",
    r"\bclinical\s+research\s+(?:coordinator|associate)\b",   # human trials admin
    r"\bregulatory\s+affairs\b",
    r"\bmanufacturing\s+scientist\b",
    r"\bprocess\s+development\s+scientist\b",
    r"\bquality\s+(?:assurance|control)\s+scientist\b",
    r"\bnurse\b|\bphysician\b|\bclinician\b",
    r"\bprofessor\b|\bfaculty\b|\blecturer\b",
    r"\bpostdoc(?:toral)?\b",       # she's transitioning OUT of postdoc
]

def matches_title(title: str) -> bool:
    """Two-stage filter: whitelist → blacklist veto."""
    t = title.lower()
    if not any(re.search(p, t) for p in TITLE_WHITELIST):
        return False
    # Explicit scientist/research overrides blacklist
    if re.search(r"\bscientist\b|\bresearcher\b|\bresearch\b", t):
        return True
    if any(re.search(p, t) for p in TITLE_BLACKLIST):
        return False
    return True


# ---------------------------------------------------------------------------
# GitHub API — persist CSV across Streamlit Cloud redeployments
# ---------------------------------------------------------------------------
def save_csv_to_github(csv_path: str) -> bool:
    if not GITHUB_TOKEN or not os.path.exists(csv_path):
        return False
    try:
        import base64
        with open(csv_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode()

        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{csv_path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}",
                   "Accept": "application/vnd.github+json"}

        r = requests.get(api_url, headers=headers, timeout=10)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""

        payload = {
            "message": f"chore: auto-save Pooja scan results {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
            "content": content_b64,
            "branch":  "main",
        }
        if sha:
            payload["sha"] = sha

        r2 = requests.put(api_url, json=payload, headers=headers, timeout=15)
        if r2.status_code in (200, 201):
            sprint(f"[GitHub] CSV saved to repo ({csv_path})")
            return True
        sprint(f"[GitHub] Save failed: {r2.status_code} {r2.text[:120]}")
    except Exception as e:
        sprint(f"[GitHub] Error: {e}")
    return False


# ---------------------------------------------------------------------------
# Push notification
# ---------------------------------------------------------------------------
def push_notification(title: str, message: str, priority: str = "default"):
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title":    title.encode("ascii", errors="replace").decode("ascii"),
                "Priority": priority,
                "Tags":     "microscope",
            },
            timeout=10,
        )
    except Exception as e:
        sprint(f"[Push Error] {e}")


# ---------------------------------------------------------------------------
# Keyword scorer — tuned for Pooja's cardiovascular/preclinical PhD profile
# ---------------------------------------------------------------------------
def keyword_score(title: str, desc: str, location: str = "") -> int:
    t = title.lower()
    d = (title + " " + desc).lower()
    score = 22   # base — passed title filter

    # --- Title signals ---
    title_core = {
        "cardiovascular research scientist": 18,
        "cardiovascular scientist": 16,
        "cardiac research scientist": 16,
        "preclinical research scientist": 17,
        "preclinical scientist": 15,
        "translational research scientist": 16,
        "translational scientist": 14,
        "in vivo scientist": 15,
        "staff scientist": 12,
        "principal scientist": 12,
        "senior research scientist": 13,
        "senior scientist": 11,
        "research scientist": 10,
        "biomarker scientist": 14,
        "drug discovery scientist": 13,
        "pharmacologist": 12,
    }
    title_good = {
        "cardiovascular": 8, "cardiac": 7, "cardiomyopathy": 9,
        "preclinical": 8, "in vivo": 8, "translational": 7,
        "scientist ii": 6, "scientist iii": 7, "associate scientist": 5,
        "biomarker": 7, "disease model": 8, "mouse model": 8,
        "drug discovery": 7, "pharmacology": 6,
        "flow cytometry": 6, "molecular biology": 5,
        "cell biology": 5, "immunology": 5,
    }
    title_pts  = sum(v for k, v in title_core.items() if k in t)
    title_pts += sum(v for k, v in title_good.items() if k in t)
    score += min(title_pts, 32)

    # --- Description keyword signals (Pooja's expertise) ---
    desc_kw = {
        # Core cardiovascular / cardiac
        "cardiovascular":       9, "cardiac":           8, "cardiomyopathy":   10,
        "peripartum":          10, "heart failure":      9, "cardiomyocyte":     9,
        "echocardiography":     8, "langendorff":        9, "ekg":               6,
        "cardiac phenotyp":     9, "contractility":      7,

        # Preclinical / in vivo
        "preclinical":          9, "in vivo":            8, "mouse model":       9,
        "transgenic":           8, "knockout":           7, "mouse colony":      8,
        "animal model":         7, "disease model":      8, "genotyping":        6,
        "subcutaneous":         5, "tissue harvest":     6, "organ harvest":     6,

        # Specific assays Pooja is expert in
        "facs":                 7, "flow cytometry":     7, "western blot":      6,
        "elisa":                6, "ihc":                6, "immunohistochemistry": 6,
        "icc":                  5, "immunocytochemistry": 5,
        "qrt-pcr":              6, "qpcr":               5, "tunel":             7,
        "beta-gal":             7, "beta-galactosidase": 7,
        "xtt":                  5, "mtt":                5, "hydroxyproline":    7,
        "cell culture":         4,

        # Omics / bioinformatics
        "rna-seq":              7, "rnaseq":             7, "scrna-seq":         7,
        "spatial transcriptomics": 9, "xenium":          9, "visium":            9,
        "bulk rna":             6, "transcriptomics":    7,
        "ipa":                  6, "ingenuity":          6, "string":            5,
        "graphpad":             4, "bioinformatics":     4,

        # Translational / biomarker
        "translational":        8, "biomarker":          8, "biomarker discovery": 9,
        "therapeutic target":   9, "target validation":  9, "drug discovery":    8,
        "pharmacology":         7, "pharmacokinetics":   6,

        # Molecular/genetics skills
        "molecular genetics":   7, "molecular biology":  6, "gene expression":   5,
        "protein expression":   5, "pathway analysis":   5, "ptrh2":            10,  # her key protein

        # Grant/publication signals (industry values this)
        "nature communications": 8, "peer review":       5, "publication":       4,
        "grant":                 4, "cirm":              7,

        # Industry setting signals (positive)
        "biotech":               6, "pharmaceutical":    6, "pharma":            6,
        "cro":                   5, "contract research": 5,
        "r&d":                   6, "research and development": 6,
        "drug development":      7, "therapeutics":      6,

        # PhD requirement (strong match for Pooja)
        "ph.d. required":       10, "phd required":     10, "ph.d required":    10,
        "ph.d. preferred":       8, "phd preferred":     8,
        "doctorate required":    9, "doctoral degree":   8,

        # Visa/relocation (important for Pooja's J1 situation)
        "visa sponsorship":     8, "will sponsor":       8, "sponsorship available": 8,
        "relocation assistance": 6, "relocation package": 6, "relocation provided": 6,
        "global":               4, "international":      4,
    }
    desc_pts = sum(v for k, v in desc_kw.items() if k in d)
    score += min(desc_pts, 40)

    # --- Seniority fit ---
    if re.search(r"\bsenior\b|\bstaff\b|\bprincipal\b|\bsr\.\b", t):
        score += 4   # senior levels are fine — she has 10+ years
    if re.search(r"\bscientist\s+(ii|iii|2|3)\b", t):
        score += 3
    if re.search(r"\bjunior\b|\bentry\s+level\b|\bscientist\s+i\b|\bscientist\s+1\b", t):
        score -= 8   # too junior for 10+ years PhD experience
    if re.search(r"\bpostdoc\b|\bpost-doc\b|\bpostdoctoral\b", d):
        score -= 15  # she is actively transitioning OUT of postdoc

    # Hard seniority penalties
    if re.search(r"\bvp\b|\bvice\s+president\b", t):           score -= 30
    if re.search(r"\bchief\b|\bcso\b|\bcmo\b", t):             score -= 35
    if re.search(r"\bhead\s+of\b", t):                         score -= 20
    if re.search(r"\bdirector\b", t):                          score -= 15
    if re.search(r"\bpartner\b|\bprincipal\s+investigator\b", t): score -= 10

    # Academic PI / professor role penalty (she wants industry)
    if any(k in d for k in ["tenure", "tenure track", "faculty position",
                             "academic appointment", "university professor"]):
        score -= 20

    # --- Hard disqualifiers ---
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci", "clearance required"]):
        score -= 30

    return max(0, min(score, 100))


# ---------------------------------------------------------------------------
# LLM scorer via Groq — tuned for Pooja's profile
# ---------------------------------------------------------------------------
def llm_score(desc: str, location: str = "") -> int | None:
    if not GROQ_API_KEY:
        return None

    prompt = f"""Rate 0–100 fit for this candidate:

Candidate profile — Pooja Choubey, Ph.D.:
- Ph.D. Molecular Genetics, University of Delhi (UGC-NET JRF AIR 64; ICMR JRF Rank 15)
- 10+ years preclinical research experience in cardiovascular biology, rare genetic disorders, cancer biology
- Co-first author, Nature Communications 2026: PTRH2 as therapeutic target in peripartum cardiomyopathy (5,093 accesses, Altmetric 78)
- Current role: Post-Doctoral Research Scientist, The Lundquist Institute / Harbor-UCLA, Torrance, CA
- In vivo expertise: Mouse colony (200+ mice, 3 transgenic lines); cardiac phenotyping pipeline (Langendorff isolation, VEVO F2 echocardiography, EKG, contractility assays); genotyping; tissue/organ harvest
- Assay expertise: FACS, Western blot, IHC/ICC, ELISA, qRT-PCR, TUNEL, Beta-gal, XTT/MTT, Hydroxyproline
- Omics: Bulk RNA-seq, scRNA-seq, Xenium & Visium spatial transcriptomics, IPA, STRING network analysis
- Grant support: CIRM, Cohen Fellowship, PCVRD clinical trial applications
- Open to relocate globally — any US city, UK/Europe (Cambridge, London, Basel, Munich, Paris), or India (Bangalore, Hyderabad, Pune); no location restrictions
- NOT seeking remote — open to on-site positions anywhere in the world
- Target: Industry transition — R&D Scientist, Preclinical Research Scientist, Translational Scientist, Biomarker Scientist at biotech/pharma/CRO

Job description:
{desc[:2800]}

Scoring guide:
- 90–100: Perfect fit — cardiovascular/preclinical R&D scientist role at biotech/pharma/CRO, PhD required, in vivo/mouse expertise needed, strong skill alignment
- 70–89: Strong fit — relevant preclinical/translational role, good skill overlap, industry setting
- 50–69: Decent fit — some overlap (related therapeutic area or relevant techniques), may lack key requirement
- 30–49: Partial — adjacent field, mostly bioinformatics/computational, or missing core preclinical skills
- 0–29: Poor fit — wrong field (clinical trials admin, sales, software, QA/QC, academic faculty, postdoc)

Return ONLY a single integer 0–100. No explanation."""

    for attempt in range(3):
        try:
            r = requests.post(
                GROQ_ENDPOINT,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": GROQ_MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 10,
                      "temperature": 0},
                timeout=20,
            )
            if r.status_code == 429:
                wait = 10 if attempt == 0 else 25
                sprint(f"  [Rate limit] Waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()
            m = re.search(r"\b(\d{1,3})\b", text)
            return min(int(m.group(1)), 100) if m else None
        except Exception as e:
            sprint(f"  [LLM error attempt {attempt+1}]: {e}")
            if attempt < 2:
                time.sleep(3)
    return None


def score_job(title: str, desc: str, location: str = "") -> tuple[int, str]:
    llm = llm_score(desc, location)
    if llm is not None:
        return llm, "llm"
    return keyword_score(title, desc, location), "keyword"


# ---------------------------------------------------------------------------
# Search configuration — 18 passes: US hubs + Europe + India
# Pooja is open to relocation anywhere globally; we search all major pharma/biotech markets
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:
    # Reusable broad science term for hub-level passes
    _HUB_TERM = (
        '"Research Scientist" OR "Senior Scientist" OR "Staff Scientist" '
        'OR "Principal Scientist" OR "Translational Scientist" '
        'OR "Preclinical Scientist" OR "Cardiovascular Scientist" '
        'OR "In Vivo Scientist" OR "Biomarker Scientist" '
        'OR "Drug Discovery Scientist" OR "Pharmacologist"'
    )
    return [
        # ── US: Nationwide thematic passes ──────────────────────────────────
        {
            "label":    "Cardiovascular Research Scientist (US nationwide)",
            "term":     ('"Cardiovascular Research Scientist" OR "Cardiovascular Scientist" '
                         'OR "Cardiac Research Scientist" OR "Cardiomyopathy" OR '
                         '"Heart Failure Research Scientist"'),
            "location": "United States",
            "results":  100,
            "region":   "US",
        },
        {
            "label":    "Preclinical / In Vivo Scientist (US nationwide)",
            "term":     ('"Preclinical Research Scientist" OR "Preclinical Scientist" '
                         'OR "In Vivo Scientist" OR "In Vivo Research Scientist" '
                         'OR "Disease Model Scientist" OR "Animal Model Scientist"'),
            "location": "United States",
            "results":  125,
            "region":   "US",
        },
        {
            "label":    "Translational / Biomarker Scientist (US nationwide)",
            "term":     ('"Translational Research Scientist" OR "Translational Scientist" '
                         'OR "Biomarker Scientist" OR "Biomarker Discovery" '
                         'OR "Drug Discovery Scientist" OR "Pharmacologist"'),
            "location": "United States",
            "results":  100,
            "region":   "US",
        },
        {
            "label":    "Senior / Staff / Principal Scientist (US nationwide)",
            "term":     ('"Senior Research Scientist" OR "Staff Scientist" '
                         'OR "Principal Scientist" OR "Scientist II" OR "Scientist III" '
                         'OR "Associate Scientist" cardiovascular OR preclinical'),
            "location": "United States",
            "results":  125,
            "region":   "US",
        },
        # ── US: Geographic hub passes ────────────────────────────────────────
        {
            "label":    "Research Scientist — LA / Torrance area",
            "term":     ('"Research Scientist" OR "Senior Scientist" OR "Translational Scientist" '
                         'OR "Preclinical Scientist" OR "Cardiovascular" OR "In Vivo"'),
            "location": "Torrance, CA",
            "results":  75,
            "distance": 40,
            "region":   "US",
        },
        {
            "label":    "Research Scientist — Boston / Cambridge MA",
            "term":     _HUB_TERM,
            "location": "Cambridge, MA",
            "results":  100,
            "distance": 30,
            "region":   "US",
        },
        {
            "label":    "Research Scientist — San Diego CA",
            "term":     _HUB_TERM,
            "location": "San Diego, CA",
            "results":  75,
            "distance": 30,
            "region":   "US",
        },
        {
            "label":    "Research Scientist — San Francisco Bay Area",
            "term":     _HUB_TERM,
            "location": "South San Francisco, CA",
            "results":  75,
            "distance": 40,
            "region":   "US",
        },
        {
            "label":    "Research Scientist — Philadelphia / NJ pharma corridor",
            "term":     _HUB_TERM,
            "location": "Philadelphia, PA",
            "results":  75,
            "distance": 50,
            "region":   "US",
        },
        {
            "label":    "Research Scientist — Research Triangle Park NC",
            "term":     _HUB_TERM,
            "location": "Durham, NC",
            "results":  60,
            "distance": 30,
            "region":   "US",
        },
        # ── EUROPE — LinkedIn only (globally supported; Indeed needs country-specific setup) ──
        # Cambridge UK — AstraZeneca global HQ, Wellcome Sanger, GSK research park
        {
            "label":    "Research Scientist — Cambridge UK (AstraZeneca / GSK / Wellcome Sanger)",
            "term":     _HUB_TERM,
            "location": "Cambridge, England, United Kingdom",
            "results":  75,
            "distance": 25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # London UK — GSK HQ, UCB Pharma, MedImmune, Immunocore
        {
            "label":    "Research Scientist — London UK (GSK / UCB / Immunocore)",
            "term":     _HUB_TERM,
            "location": "London, England, United Kingdom",
            "results":  75,
            "distance": 25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # Basel Switzerland — Novartis global HQ, Roche global HQ, Lonza
        {
            "label":    "Research Scientist — Basel Switzerland (Novartis / Roche / Lonza)",
            "term":     _HUB_TERM,
            "location": "Basel, Basel-Stadt, Switzerland",
            "results":  60,
            "distance": 30,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # Munich Germany — BioNTech R&D hub, Bayer AG, Helmholtz Munich, Roche Diagnostics
        {
            "label":    "Research Scientist — Munich Germany (BioNTech / Bayer / Helmholtz)",
            "term":     _HUB_TERM,
            "location": "Munich, Bavaria, Germany",
            "results":  60,
            "distance": 25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # Paris France — Sanofi global HQ, Institut Pasteur, Servier
        {
            "label":    "Research Scientist — Paris France (Sanofi / Institut Pasteur)",
            "term":     _HUB_TERM,
            "location": "Paris, Ile-de-France, France",
            "results":  50,
            "distance": 25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # ── INDIA — LinkedIn only ─────────────────────────────────────────────
        # Bengaluru — Biocon, AstraZeneca India R&D, Novo Nordisk India, Syngene, Strand
        {
            "label":    "Research Scientist — Bangalore India (Biocon / AstraZeneca / Syngene)",
            "term":     _HUB_TERM,
            "location": "Bengaluru, Karnataka, India",
            "results":  75,
            "distance": 30,
            "region":   "India",
            "sites":    ["linkedin"],
        },
        # Hyderabad — Dr. Reddy's, Aurobindo, MSN Labs, Cipla R&D centre
        {
            "label":    "Research Scientist — Hyderabad India (Dr Reddy's / Aurobindo / Cipla)",
            "term":     _HUB_TERM,
            "location": "Hyderabad, Telangana, India",
            "results":  60,
            "distance": 30,
            "region":   "India",
            "sites":    ["linkedin"],
        },
        # Pune — Serum Institute, Lupin R&D, Piramal, Indoco
        {
            "label":    "Research Scientist — Pune India (Serum Institute / Lupin / Piramal)",
            "term":     _HUB_TERM,
            "location": "Pune, Maharashtra, India",
            "results":  50,
            "distance": 25,
            "region":   "India",
            "sites":    ["linkedin"],
        },
    ]


# ---------------------------------------------------------------------------
# Main hunt
# ---------------------------------------------------------------------------
def pooja_hunt():
    sprint(f"\n{'='*60}")
    sprint(f"[Pooja Scanner] Biotech/Pharma Job Hunt — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    sprint(f"{'='*60}")
    alerts_sent = 0
    all_frames  = []

    for cfg in build_search_configs():
        sprint(f"\n[Search] {cfg['label']} ({cfg['results']} results)...")
        kwargs = dict(
            site_name=cfg.get("sites", ["linkedin", "indeed"]),
            search_term=cfg["term"],
            location=cfg["location"],
            results_wanted=cfg["results"],
        )
        # is_remote=False only works reliably for US/Indeed; skip for international passes
        if cfg.get("region", "US") == "US":
            kwargs["is_remote"] = False
        if "distance" in cfg:
            kwargs["distance"] = cfg["distance"]

        try:
            df = scrape_jobs(**kwargs)
            if not df.empty:
                df["_search_pass"] = cfg["label"]
                df["_region"]      = cfg.get("region", "US")
                all_frames.append(df)
                sprint(f"  → {len(df)} raw results")
            else:
                sprint(f"  → 0 results")
            time.sleep(4)
        except Exception as e:
            sprint(f"  [Error] {e}")
            time.sleep(5)

    if not all_frames:
        sprint("[Pooja Scanner] No results from any search pass.")
        push_notification("Pooja Bio Hunt", "Scan ran — 0 results from all passes.", "low")
        return

    # --- Combine + deduplicate ---
    raw = pd.concat(all_frames, ignore_index=True)
    sprint(f"\n[Dedup] {len(raw)} total raw → ", end="")

    if "job_url" in raw.columns:
        raw["_url_norm"] = (
            raw["job_url"].astype(str)
            .str.split("?").str[0].str.strip().str.lower()
        )
        raw = raw.drop_duplicates(subset=["_url_norm"], keep="first")

    raw["_title_co"] = (
        raw["title"].astype(str).str.lower().str.strip() + "|" +
        raw["company"].astype(str).str.lower().str.strip()
    )
    raw = raw.drop_duplicates(subset=["_title_co"], keep="first")
    sprint(f"{len(raw)} after dedup")

    # --- Title filter ---
    raw["title_lower"] = raw["title"].str.lower().fillna("")
    mask     = raw["title_lower"].apply(matches_title)
    filtered = raw[mask].copy()
    sprint(f"[Filter] {len(raw)} → {len(filtered)} passed title filter "
           f"(removed {len(raw) - len(filtered)} non-science jobs)")

    if filtered.empty:
        push_notification("Pooja Bio Hunt", "Scan complete — 0 relevant titles found.", "low")
        return

    if "date_posted" in filtered.columns:
        filtered = filtered.sort_values("date_posted", ascending=False)

    to_score = filtered.head(SCORE_TOP_N)
    sprint(f"[Score]  Scoring top {len(to_score)} jobs...\n")

    # --- Score each job ---
    scored_list = []
    for _, row in to_score.iterrows():
        title    = str(row.get("title", "Unknown"))
        desc     = str(row.get("description", ""))
        company  = str(row.get("company", "Unknown"))
        location = str(row.get("location", ""))
        url      = str(row.get("job_url", ""))
        src      = str(row.get("_search_pass", ""))
        region   = str(row.get("_region", "US"))

        try:
            score, method = score_job(title, desc, location)

            if score >= 80:   tag = "STRONG"
            elif score >= 70: tag = "HIGH  "
            elif score >= 50: tag = "fair  "
            else:             tag = "low   "

            sprint(f"  [{score:3d}][{tag}][{method}] {title[:48]:<48} @ {company[:25]}")

            if score >= 60 and alerts_sent < MAX_ALERTS:
                priority = "urgent" if score >= 82 else "high"
                push_notification(
                    title=f"{score}% — {title[:48]}",
                    message=f"{company}\n{location}\n{url}",
                    priority=priority,
                )
                alerts_sent += 1

            if score >= MIN_SAVE_SCORE:
                scored_list.append({
                    "Score":     score,
                    "Title":     title,
                    "Company":   company,
                    "Location":  location,
                    "Type":      row.get("job_type", ""),
                    "Link":      url,
                    "Posted":    str(row.get("date_posted", "")),
                    "ScoredBy":  method,
                    "Source":    src,
                    "Region":    region,
                    "ScannedAt": datetime.now().strftime("%Y-%m-%d %H:%M"),
                })

        except Exception as e:
            sprint(f"  [Error] {title[:40]}: {e}")

        time.sleep(1)

    if not scored_list:
        sprint("\n[Pooja Scanner] No jobs met the minimum score threshold.")
        push_notification("Pooja Bio Hunt", "Scan done — no jobs cleared score threshold.", "low")
        return

    new_df = pd.DataFrame(scored_list)

    if os.path.exists(CSV_PATH):
        existing = pd.read_csv(CSV_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Link"], keep="last")
    else:
        combined = new_df

    combined = combined.sort_values("Score", ascending=False)
    combined.to_csv(CSV_PATH, index=False)

    # Persist to GitHub so data survives Streamlit Cloud redeployments
    save_csv_to_github(CSV_PATH)

    strong = len(new_df[new_df["Score"] >= 80])
    high   = len(new_df[new_df["Score"] >= 70])
    sprint(f"\n{'='*60}")
    sprint(f"[Done] {len(new_df)} new leads  |  {strong} strong (80+)  |  {high} high (70+)")
    sprint(f"       {len(combined)} total in CSV  |  {alerts_sent} alerts sent")
    sprint(f"{'='*60}\n")

    push_notification(
        title="Pooja Bio Scan Complete",
        message=(
            f"{len(new_df)} new leads  |  {strong} strong (80+)  |  {high} high (70+)\n"
            f"{alerts_sent} alerts  |  {len(combined)} total leads"
        ),
        priority="low",
    )


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    pooja_hunt()
