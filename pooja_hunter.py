"""
pooja_hunter.py — Pooja Choubey's Biotech/Pharma Job Scanner
Ph.D. Research Scientist | Cardiovascular Biology | Preclinical | Translational Medicine
STRICTLY ISOLATED from DJ's audit scanner.
"""

import json
import os
import re
import sys
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    print(safe, flush=True, **kwargs)

# --- CONFIG (completely separate from DJ) ---
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
NTFY_TOPIC     = os.getenv("POOJA_NTFY_TOPIC", "pooja-industry-oppor")
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN")
GITHUB_REPO    = os.getenv("GITHUB_REPO", "djha786543-gif/Mobilejobnotifications")
CSV_PATH       = "Scored_Pooja_Leads.csv"
GROQ_MODEL     = "llama-3.3-70b-versatile"
GROQ_ENDPOINT  = "https://api.groq.com/openai/v1/chat/completions"
MIN_SAVE_SCORE = 35
MAX_ALERTS     = 10
SCORE_LLM_TOP  = 40    # top N by title relevance → LLM; rest → keyword scorer
BATCH_SIZE     = 15    # jobs per Groq call
SCRAPE_WORKERS = 4     # parallel scrape threads (stays under LinkedIn radar)


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
    if re.search(r"\bscientist\b|\bresearcher\b|\bresearch\b", t):
        return True
    if any(re.search(p, t) for p in TITLE_BLACKLIST):
        return False
    return True


# ---------------------------------------------------------------------------
# Title relevance pre-ranker — fast keyword signal, no API call
# Higher score → send to LLM; lower score → keyword scorer only
# ---------------------------------------------------------------------------
_TITLE_HIGH = [
    "cardiovascular", "preclinical", "in vivo", "translational",
    "biomarker", "drug discovery", "pharmacolog", "cardiac",
    "cardiomyopathy", "heart failure",
]
_TITLE_MED = [
    "research scientist", "senior scientist", "staff scientist",
    "principal scientist", "scientist ii", "scientist iii", "scientist 2",
    "scientist 3",
]

def title_relevance(title: str) -> int:
    t = title.lower()
    score = sum(3 for k in _TITLE_HIGH if k in t)
    score += sum(1 for k in _TITLE_MED if k in t)
    return score


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
        "protein expression":   5, "pathway analysis":   5, "ptrh2":            10,

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
        score += 4
    if re.search(r"\bscientist\s+(ii|iii|2|3)\b", t):
        score += 3
    if re.search(r"\bjunior\b|\bentry\s+level\b|\bscientist\s+i\b|\bscientist\s+1\b", t):
        score -= 8
    if re.search(r"\bpostdoc\b|\bpost-doc\b|\bpostdoctoral\b", d):
        score -= 15

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

    # Hard disqualifiers
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci", "clearance required"]):
        score -= 30

    return max(0, min(score, 100))


# ---------------------------------------------------------------------------
# Batch LLM scorer — 15 jobs per Groq call, strict JSON output
# ---------------------------------------------------------------------------
def llm_score_batch(batch: list[dict]) -> list[int | None]:
    """Score up to 15 jobs in one Groq call. Returns JSON list of ints."""
    if not GROQ_API_KEY or not batch:
        return [None] * len(batch)

    n = len(batch)
    blocks = []
    for i, j in enumerate(batch, 1):
        blocks.append(
            f"[{i}] {j['title']} @ {j['company']} | {j['location']}\n"
            f"{j['desc'][:350]}"
        )

    prompt = (
        f"You are a scientific recruiter. Score each of the {n} jobs 0-100 for fit "
        f"with this candidate.\n\n"
        "CANDIDATE — Pooja Choubey, Ph.D.:\n"
        "- Ph.D. Molecular Genetics; 10+ years preclinical cardiovascular research\n"
        "- Co-first author Nature Communications 2026 (PTRH2 / peripartum cardiomyopathy)\n"
        "- In vivo: Langendorff isolation, echocardiography (VEVO F2), mouse colony 200+ mice, 3 transgenic lines\n"
        "- Assays: FACS, Western blot, IHC/ICC, ELISA, qRT-PCR, TUNEL, Beta-gal, Hydroxyproline\n"
        "- Omics: RNA-seq, scRNA-seq, Xenium & Visium spatial transcriptomics, IPA, STRING\n"
        "- Target: R&D / Preclinical / Translational Scientist at biotech/pharma/CRO\n"
        "- Open to relocation: US, Europe, India. NOT seeking remote.\n\n"
        "STRICT SCORING RUBRIC:\n"
        "90-100: ONLY if the JD EXPLICITLY requires a Ph.D. or 'Scientist' title AND is hands-on "
        "preclinical/cardiovascular/translational R&D at a biotech, pharma, or CRO.\n"
        "70-89:  Strong preclinical/translational R&D role, PhD preferred, good skill overlap, industry setting.\n"
        "50-69:  Decent — adjacent therapeutic area or relevant wet-lab techniques, partial match.\n"
        "30-49:  Partial — mostly bioinformatics/computational, missing core in vivo/wet-lab requirement.\n"
        "< 30:   Poor — general lab tech, sales, software engineer, QA/QC, regulatory affairs, "
        "clinical coordinator, or any role NOT requiring hands-on preclinical research.\n\n"
        "JOBS:\n" +
        "\n\n".join(blocks) +
        f"\n\nReturn ONLY a raw JSON array of {n} integers, e.g. [82,45,91]. "
        "No text, no explanation, no markdown."
    )

    text = ""
    for attempt in range(3):
        try:
            r = requests.post(
                GROQ_ENDPOINT,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": GROQ_MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 100,
                      "temperature": 0},
                timeout=30,
            )
            if r.status_code == 429:
                wait = 10 if attempt == 0 else 25
                sprint(f"  [Rate limit] Waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()

            # Strict JSON parse
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list) and len(parsed) >= n:
                    return [min(int(x), 100) for x in parsed[:n]]
            except (json.JSONDecodeError, ValueError):
                pass

            # Regex fallback
            nums = re.findall(r"\b(\d{1,3})\b", text)
            if len(nums) >= n:
                return [min(int(x), 100) for x in nums[:n]]

            sprint(f"  [Batch] Unexpected response ({len(nums)} ints for {n} jobs): {text[:80]}")
            return [None] * n

        except Exception as e:
            sprint(f"  [Batch LLM error attempt {attempt+1}]: {e}")
            if attempt < 2:
                time.sleep(3)

    return [None] * n


# ---------------------------------------------------------------------------
# Parallel scrape helper — one thread per search config
# ---------------------------------------------------------------------------
def _scrape_one(cfg: dict) -> pd.DataFrame:
    """Run a single search pass. Returns a DataFrame (may be empty)."""
    kwargs = dict(
        site_name=cfg.get("sites", ["linkedin", "indeed", "glassdoor", "zip_recruiter"]),
        search_term=cfg["term"],
        location=cfg["location"],
        results_wanted=cfg["results"],
    )
    if cfg.get("region", "US") == "US":
        kwargs["is_remote"] = False
    if "distance" in cfg:
        kwargs["distance"] = cfg["distance"]

    df = scrape_jobs(**kwargs)
    if not df.empty:
        df["_search_pass"] = cfg["label"]
        df["_region"]      = cfg.get("region", "US")
    return df


# ---------------------------------------------------------------------------
# Search configuration — 18 passes: US hubs + Europe + India (LinkedIn only)
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:
    _HUB_TERM = (
        '"Research Scientist" OR "Senior Scientist" OR "Staff Scientist" '
        'OR "Principal Scientist" OR "Translational Scientist" '
        'OR "Preclinical Scientist" OR "Cardiovascular Scientist" '
        'OR "In Vivo Scientist" OR "Biomarker Scientist" '
        'OR "Drug Discovery Scientist" OR "Pharmacologist"'
    )
    return [
        # 1 — US nationwide
        {
            "label":    "Cardiovascular Research Scientist (US nationwide)",
            "term":     ('"Cardiovascular Research Scientist" OR "Cardiovascular Scientist" '
                         'OR "Cardiac Research Scientist" OR "Cardiomyopathy" OR '
                         '"Heart Failure Research Scientist"'),
            "location": "United States",
            "results":  25,
            "region":   "US",
        },
        # 2 — US nationwide
        {
            "label":    "Preclinical / In Vivo Scientist (US nationwide)",
            "term":     ('"Preclinical Research Scientist" OR "Preclinical Scientist" '
                         'OR "In Vivo Scientist" OR "In Vivo Research Scientist" '
                         'OR "Disease Model Scientist" OR "Animal Model Scientist"'),
            "location": "United States",
            "results":  25,
            "region":   "US",
        },
        # 3 — EUROPE: Cambridge UK — AstraZeneca HQ, Wellcome Sanger, GSK
        {
            "label":    "Research Scientist — Cambridge UK (AstraZeneca / GSK / Wellcome Sanger)",
            "term":     _HUB_TERM,
            "location": "Cambridge, United Kingdom",
            "results":  25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # 4 — EUROPE: London UK — GSK HQ, UCB, Immunocore
        {
            "label":    "Research Scientist — London UK (GSK / UCB / Immunocore)",
            "term":     _HUB_TERM,
            "location": "London, United Kingdom",
            "results":  25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # 5 — US nationwide
        {
            "label":    "Translational / Biomarker Scientist (US nationwide)",
            "term":     ('"Translational Research Scientist" OR "Translational Scientist" '
                         'OR "Biomarker Scientist" OR "Biomarker Discovery" '
                         'OR "Drug Discovery Scientist" OR "Pharmacologist"'),
            "location": "United States",
            "results":  25,
            "region":   "US",
        },
        # 6 — US nationwide
        {
            "label":    "Senior / Staff / Principal Scientist (US nationwide)",
            "term":     ('"Senior Research Scientist" OR "Staff Scientist" '
                         'OR "Principal Scientist" OR "Scientist II" OR "Scientist III" '
                         'OR "Associate Scientist" cardiovascular OR preclinical'),
            "location": "United States",
            "results":  25,
            "region":   "US",
        },
        # 7 — US hub: LA / Torrance
        {
            "label":    "Research Scientist — LA / Torrance area",
            "term":     ('"Research Scientist" OR "Senior Scientist" OR "Translational Scientist" '
                         'OR "Preclinical Scientist" OR "Cardiovascular" OR "In Vivo"'),
            "location": "Torrance, CA",
            "results":  25,
            "distance": 40,
            "region":   "US",
        },
        # 8 — EUROPE: Basel Switzerland — Novartis HQ, Roche HQ, Lonza
        {
            "label":    "Research Scientist — Basel Switzerland (Novartis / Roche / Lonza)",
            "term":     _HUB_TERM,
            "location": "Basel, Switzerland",
            "results":  25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # 9 — US hub: Boston / Cambridge MA
        {
            "label":    "Research Scientist — Boston / Cambridge MA",
            "term":     _HUB_TERM,
            "location": "Cambridge, MA",
            "results":  25,
            "distance": 30,
            "region":   "US",
        },
        # 10 — EUROPE: Munich Germany — BioNTech, Bayer, Helmholtz
        {
            "label":    "Research Scientist — Munich Germany (BioNTech / Bayer / Helmholtz)",
            "term":     _HUB_TERM,
            "location": "Munich, Germany",
            "results":  25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # 11 — US hub: San Diego CA
        {
            "label":    "Research Scientist — San Diego CA",
            "term":     _HUB_TERM,
            "location": "San Diego, CA",
            "results":  25,
            "distance": 30,
            "region":   "US",
        },
        # 12 — EUROPE: Paris France — Sanofi HQ, Institut Pasteur
        {
            "label":    "Research Scientist — Paris France (Sanofi / Institut Pasteur)",
            "term":     _HUB_TERM,
            "location": "Paris, France",
            "results":  25,
            "region":   "Europe",
            "sites":    ["linkedin"],
        },
        # 13 — US hub: San Francisco Bay Area
        {
            "label":    "Research Scientist — San Francisco Bay Area",
            "term":     _HUB_TERM,
            "location": "South San Francisco, CA",
            "results":  25,
            "distance": 40,
            "region":   "US",
        },
        # 14 — US hub: Philadelphia / NJ pharma corridor
        {
            "label":    "Research Scientist — Philadelphia / NJ pharma corridor",
            "term":     _HUB_TERM,
            "location": "Philadelphia, PA",
            "results":  25,
            "distance": 50,
            "region":   "US",
        },
        # 15 — INDIA: Bengaluru — Biocon, AstraZeneca India, Syngene (LinkedIn only)
        {
            "label":    "Research Scientist — Bangalore India (Biocon / AstraZeneca / Syngene)",
            "term":     _HUB_TERM,
            "location": "Bengaluru",
            "results":  25,
            "region":   "India",
            "sites":    ["linkedin"],
        },
        # 16 — INDIA: Hyderabad — Dr. Reddy's, Aurobindo, Cipla R&D (LinkedIn only)
        {
            "label":    "Research Scientist — Hyderabad India (Dr Reddy's / Aurobindo / Cipla)",
            "term":     _HUB_TERM,
            "location": "Hyderabad",
            "results":  25,
            "region":   "India",
            "sites":    ["linkedin"],
        },
        # 17 — US hub: Research Triangle Park NC
        {
            "label":    "Research Scientist — Research Triangle Park NC",
            "term":     _HUB_TERM,
            "location": "Durham, NC",
            "results":  25,
            "distance": 30,
            "region":   "US",
        },
        # 18 — INDIA: Pune — Serum Institute, Lupin, Piramal (LinkedIn only)
        {
            "label":    "Research Scientist — Pune India (Serum Institute / Lupin / Piramal)",
            "term":     _HUB_TERM,
            "location": "Pune",
            "results":  25,
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
    configs     = build_search_configs()

    # -----------------------------------------------------------------------
    # Step 1 — Parallel scrape (4 workers)
    # -----------------------------------------------------------------------
    sprint(f"\n[Scrape] Launching {len(configs)} passes with {SCRAPE_WORKERS} parallel workers...")
    all_frames = []

    with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as ex:
        futures = {ex.submit(_scrape_one, cfg): cfg for cfg in configs}
        for fut in as_completed(futures):
            cfg = futures[fut]
            sprint(f"[Search] {cfg['label']}...")
            try:
                df = fut.result()
                if not df.empty:
                    all_frames.append(df)
                    sprint(f"  → {len(df)} raw results")
                else:
                    sprint(f"  → 0 results")
            except Exception as e:
                sprint(f"  [Error] {cfg['label']}: {e}")

    if not all_frames:
        sprint("[Pooja Scanner] No results from any search pass.")
        push_notification("Pooja Bio Hunt", "Scan ran — 0 results from all passes.", "low")
        return

    # -----------------------------------------------------------------------
    # Step 2 — Combine + deduplicate
    # -----------------------------------------------------------------------
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

    # -----------------------------------------------------------------------
    # Step 3 — Title filter
    # -----------------------------------------------------------------------
    raw["title_lower"] = raw["title"].str.lower().fillna("")
    mask     = raw["title_lower"].apply(matches_title)
    filtered = raw[mask].copy()
    sprint(f"[Filter] {len(raw)} → {len(filtered)} passed title filter "
           f"(removed {len(raw) - len(filtered)} non-science jobs)")

    if filtered.empty:
        push_notification("Pooja Bio Hunt", "Scan complete — 0 relevant titles found.", "low")
        return

    # -----------------------------------------------------------------------
    # Step 4 — Pre-rank by title relevance; split into LLM pool vs keyword pool
    # -----------------------------------------------------------------------
    filtered["_title_rel"] = filtered["title"].apply(title_relevance)

    # Sort: highest title relevance first, then most recent
    if "date_posted" in filtered.columns:
        filtered = filtered.sort_values(
            ["_title_rel", "date_posted"], ascending=[False, False]
        )
    else:
        filtered = filtered.sort_values("_title_rel", ascending=False)

    llm_pool     = filtered.head(SCORE_LLM_TOP).copy()
    keyword_pool = filtered.iloc[SCORE_LLM_TOP:].copy()

    sprint(f"[Score]  {len(llm_pool)} → LLM batch  |  {len(keyword_pool)} → keyword scorer\n")

    # -----------------------------------------------------------------------
    # Step 5a — LLM batch scoring (15 jobs per Groq call)
    # -----------------------------------------------------------------------
    score_map: dict[str, tuple[int, str]] = {}

    llm_rows = list(llm_pool.iterrows())
    for b_start in range(0, len(llm_rows), BATCH_SIZE):
        batch = llm_rows[b_start:b_start + BATCH_SIZE]
        payload = [
            {
                "title":    str(r.get("title", "")),
                "desc":     str(r.get("description", "")),
                "company":  str(r.get("company", "")),
                "location": str(r.get("location", "")),
            }
            for _, r in batch
        ]
        scores = llm_score_batch(payload)

        for (_, row), llm in zip(batch, scores):
            url = str(row.get("job_url", ""))
            if llm is not None:
                score_map[url] = (llm, "llm-batch")
            else:
                score_map[url] = (
                    keyword_score(
                        str(row.get("title", "")),
                        str(row.get("description", "")),
                        str(row.get("location", "")),
                    ),
                    "keyword",
                )

        time.sleep(2)   # polite delay between batch API calls

    # -----------------------------------------------------------------------
    # Step 5b — Keyword scoring for the remainder
    # -----------------------------------------------------------------------
    for _, row in keyword_pool.iterrows():
        url = str(row.get("job_url", ""))
        score_map[url] = (
            keyword_score(
                str(row.get("title", "")),
                str(row.get("description", "")),
                str(row.get("location", "")),
            ),
            "keyword",
        )

    # -----------------------------------------------------------------------
    # Step 6 — Collect results, send alerts, build CSV
    # -----------------------------------------------------------------------
    scored_list = []
    for _, row in filtered.iterrows():
        title    = str(row.get("title", "Unknown"))
        company  = str(row.get("company", "Unknown"))
        location = str(row.get("location", ""))
        url      = str(row.get("job_url", ""))
        src      = str(row.get("_search_pass", ""))
        region   = str(row.get("_region", "US"))

        score, method = score_map.get(url, (0, "keyword"))

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

    if not scored_list:
        sprint("\n[Pooja Scanner] No jobs met the minimum score threshold.")
        push_notification("Pooja Bio Hunt", "Scan done — no jobs cleared score threshold.", "low")
        return

    # -----------------------------------------------------------------------
    # Step 7 — Merge with existing CSV (utf-8-sig for international chars)
    # -----------------------------------------------------------------------
    new_df = pd.DataFrame(scored_list)

    if os.path.exists(CSV_PATH):
        existing = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Link"], keep="last")
    else:
        combined = new_df

    combined = combined.sort_values("Score", ascending=False)
    combined.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

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
