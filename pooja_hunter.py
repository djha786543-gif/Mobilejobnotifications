"""
pooja_hunter.py — Pooja Choubey's Biotech/Pharma Job Scanner
Ph.D. Molecular Genetics | Cardiovascular Biology | Preclinical | Translational Medicine
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

# --- CONFIG ---
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
NTFY_TOPIC     = os.getenv("POOJA_NTFY_TOPIC", "pooja-industry-oppor")
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN")
GITHUB_REPO    = os.getenv("GITHUB_REPO", "djha786543-gif/Mobilejobnotifications")
CSV_PATH       = "Scored_Pooja_Leads.csv"
GROQ_MODEL     = "llama-3.3-70b-versatile"
GROQ_ENDPOINT  = "https://api.groq.com/openai/v1/chat/completions"
MIN_SAVE_SCORE = 35
MAX_ALERTS     = 10
SCORE_LLM_TOP  = 80    # top N by title relevance → LLM batch
BATCH_SIZE     = 15    # jobs per Groq call
SCRAPE_WORKERS = 4     # parallel scrape threads


# ---------------------------------------------------------------------------
# Title whitelist — research science roles in biotech/pharma/CRO space
# ---------------------------------------------------------------------------
TITLE_WHITELIST = [
    # Core scientist titles
    r"\bresearch\s+scientist\b",
    r"\bscientist\s+(?:i+|1|2|3|4|v)\b",
    r"\bsenior\s+(?:research\s+)?scientist\b",
    r"\bstaff\s+scientist\b",
    r"\bprincipal\s+scientist\b",
    r"\bassociate\s+(?:research\s+)?scientist\b",
    r"\br&d\s+scientist\b",
    r"\bscientific\s+researcher\b",
    # Investigator equivalents (biotech/pharma term for Scientist)
    r"\bresearch\s+investigator\b",
    r"\bsenior\s+(?:research\s+)?investigator\b",
    r"\bprincipal\s+(?:research\s+)?investigator\b(?!\s+\(pi\))",  # exclude academic PI
    r"\bassociate\s+(?:research\s+)?investigator\b",
    r"\binvestigator\s+(?:i+|1|2|3)\b",
    # Specialty scientist
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
    r"\bin\s+vitro\s+in\s+vivo\b",
    r"\bmouse\s+(?:model|colony|genetics|phenotyping)\b",
    r"\btransgenic\s+(?:mouse|model|scientist)\b",
    r"\bomics\s+scientist\b",
    r"\bspatial\s+(?:transcriptomics|genomics)\s*(?:scientist|specialist)?\b",
    r"\bcardiology\s+(?:research|scientist)\b",
    r"\bheart\s+(?:failure|disease)\s+(?:research|scientist)\b",
    r"\bfibrosis\s+(?:research|scientist)\b",
    r"\bgenomics\s+scientist\b",
    r"\btranscriptomics\s+scientist\b",
    r"\bbiologist\b.*\b(?:cardiovascular|cardiac|preclinical|in\s+vivo|translational)\b",
    r"\b(?:cardiovascular|cardiac|preclinical|in\s+vivo|translational)\b.*\bbiologist\b",
]

# Hard blacklist — veto even if whitelist matched
TITLE_BLACKLIST = [
    r"\bsoftware\s+(?:engineer|developer)\b",
    r"\bdata\s+(?:engineer|architect)\b",
    r"\bdata\s+scientist\b",           # pure ML/AI data science
    r"\bbioinformatician\b",           # pure bioinformatics (no wet lab)
    r"\bsales\b|\bmarketing\b",
    r"\bclinical\s+research\s+(?:coordinator|associate)\b",
    r"\bregulatory\s+affairs\b",
    r"\bmanufacturing\s+scientist\b",
    r"\bprocess\s+development\s+scientist\b",
    r"\bquality\s+(?:assurance|control)\s+scientist\b",
    r"\bnurse\b|\bphysician\b|\bclinician\b",
    r"\bprofessor\b|\bfaculty\b|\blecturer\b|\btenure\b",
    r"\bpostdoc(?:toral)?\b",          # transitioning OUT of postdoc
]

def matches_title(title: str) -> bool:
    t = title.lower()
    if not any(re.search(p, t) for p in TITLE_WHITELIST):
        return False
    # Scientist/researcher/investigator keyword overrides blacklist
    if re.search(r"\bscientist\b|\bresearcher\b|\bresearch\b|\binvestigator\b", t):
        if not any(re.search(p, t) for p in TITLE_BLACKLIST):
            return True
    if any(re.search(p, t) for p in TITLE_BLACKLIST):
        return False
    return True


# ---------------------------------------------------------------------------
# Title relevance pre-ranker — fast signal, no API call
# ---------------------------------------------------------------------------
_TITLE_HIGH = [
    "cardiovascular", "preclinical", "in vivo", "translational",
    "biomarker", "drug discovery", "pharmacolog", "cardiac",
    "cardiomyopathy", "heart failure", "fibrosis", "disease model",
    "mouse model", "transgenic",
]
_TITLE_MED = [
    "research scientist", "senior scientist", "staff scientist",
    "principal scientist", "scientist ii", "scientist iii", "scientist 2",
    "scientist 3", "research investigator", "senior investigator",
]

def title_relevance(title: str) -> int:
    t = title.lower()
    score = sum(3 for k in _TITLE_HIGH if k in t)
    score += sum(1 for k in _TITLE_MED if k in t)
    return score


# ---------------------------------------------------------------------------
# GitHub API
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
        "cardiovascular scientist":          16,
        "cardiac research scientist":        16,
        "preclinical research scientist":    17,
        "preclinical scientist":             15,
        "translational research scientist":  16,
        "translational scientist":           14,
        "in vivo scientist":                 15,
        "in vivo research scientist":        16,
        "staff scientist":                   12,
        "principal scientist":               12,
        "senior research scientist":         13,
        "senior scientist":                  11,
        "research scientist":                10,
        "research investigator":             11,
        "senior investigator":               12,
        "principal investigator":            10,
        "biomarker scientist":               14,
        "drug discovery scientist":          13,
        "pharmacologist":                    12,
        "heart failure scientist":           16,
        "fibrosis scientist":                13,
        "disease model scientist":           14,
    }
    title_good = {
        "cardiovascular": 8, "cardiac": 7, "cardiomyopathy": 9,
        "preclinical": 8, "in vivo": 8, "translational": 7,
        "scientist ii": 6, "scientist iii": 7, "scientist 2": 6,
        "associate scientist": 5, "biomarker": 7, "disease model": 8,
        "mouse model": 8, "drug discovery": 7, "pharmacology": 6,
        "flow cytometry": 6, "molecular biology": 5,
        "cell biology": 5, "immunology": 5, "heart failure": 9,
        "fibrosis": 7, "myocardial": 8, "cardiomyocyte": 8,
    }
    title_pts  = sum(v for k, v in title_core.items() if k in t)
    title_pts += sum(v for k, v in title_good.items() if k in t)
    score += min(title_pts, 32)

    # --- Description keyword signals ---
    desc_kw = {
        # Core cardiovascular / cardiac
        "cardiovascular":           9, "cardiac":               8,
        "cardiomyopathy":          10, "peripartum":           10,
        "heart failure":            9, "cardiomyocyte":         9,
        "myocardial":               8, "myocyte":               7,
        "echocardiography":         8, "echo":                  4,
        "langendorff":              9, "ekg":                   6,
        "cardiac phenotyp":         9, "contractility":         7,
        "ejection fraction":        7, "fractional shortening": 7,
        "cardiac fibrosis":         9, "cardiac hypertrophy":   8,
        "pressure overload":        8, "tac model":             9,
        "myocardial infarction":    8, "mi model":              7,

        # Preclinical / in vivo
        "preclinical":              9, "in vivo":               8,
        "mouse model":              9, "transgenic":            8,
        "knockout":                 7, "mouse colony":          8,
        "animal model":             7, "disease model":         8,
        "genotyping":               6, "phenotyping":           7,
        "subcutaneous":             5, "tissue harvest":        6,
        "organ harvest":            6, "survival surgery":      8,
        "osmotic pump":             7, "isoproterenol":         8,
        "aortic banding":           9, "pressure overload":     8,
        "echocardiographic":        8,

        # Assays Pooja is expert in
        "facs":                     7, "flow cytometry":        7,
        "western blot":             6, "western blotting":      6,
        "elisa":                    6, "ihc":                   6,
        "immunohistochemistry":     6, "icc":                   5,
        "immunocytochemistry":      5, "qrt-pcr":               6,
        "qpcr":                     5, "tunel":                 7,
        "hydroxyproline":           7, "cell culture":          4,
        "confocal":                 5, "immunofluorescence":    6,
        "masson trichrome":         7, "sirius red":            7,

        # Omics / transcriptomics
        "rna-seq":                  7, "rnaseq":                7,
        "scrna-seq":                7, "single cell rna":       8,
        "spatial transcriptomics":  9, "xenium":                9,
        "visium":                   9, "seurat":                6,
        "bulk rna":                 6, "transcriptomics":       7,
        "ipa":                      6, "ingenuity":             6,
        "bioinformatics":           4, "pathway analysis":      5,
        "gene ontology":            5, "gsea":                  5,
        "string":                   4, "graphpad":              4,

        # Translational / biomarker
        "translational":            8, "biomarker":             8,
        "biomarker discovery":      9, "therapeutic target":    9,
        "target validation":        9, "target identification": 8,
        "drug discovery":           8, "pharmacology":          7,
        "pharmacokinetics":         6, "mechanism of action":   7,
        "proof of concept":         6, "efficacy model":        8,

        # Molecular / genetics
        "molecular genetics":       7, "molecular biology":     6,
        "gene expression":          5, "protein expression":    5,
        "signaling pathway":        6, "western":               5,
        "ptrh2":                   10, "ptrh":                  8,

        # PhD requirement (strong match)
        "ph.d. required":          10, "phd required":         10,
        "ph.d required":           10, "ph.d. preferred":       8,
        "phd preferred":            8, "doctorate required":    9,
        "doctoral degree":          8, "advanced degree":       5,

        # Industry context (positive)
        "biotech":                  6, "pharmaceutical":        6,
        "pharma":                   6, "cro":                   5,
        "contract research":        5, "r&d":                   6,
        "research and development": 6, "drug development":      7,
        "therapeutics":             6, "life sciences":         4,
        "biopharma":                6,

        # Visa / relocation (important — she needs sponsorship or already approved)
        "visa sponsorship":         8, "will sponsor":          8,
        "sponsorship available":    8, "h1b":                   6,
        "relocation assistance":    6, "relocation package":    6,
        "relocation provided":      6,

        # Company name signals (top targets)
        "astrazeneca":              6, "novartis":              6,
        "roche":                    6, "genentech":             7,
        "pfizer":                   5, "bristol-myers":         5,
        "bms":                      5, "merck":                 5,
        "abbvie":                   5, "amgen":                 6,
        "regeneron":                6, "biogen":                5,
        "vertex":                   5, "myokardia":             8,
        "cytokinetics":             8, "tenax":                 7,
        "cardiol":                  7,
    }
    desc_pts = sum(v for k, v in desc_kw.items() if k in d)
    score += min(desc_pts, 42)

    # --- Seniority fit ---
    if re.search(r"\bsenior\b|\bstaff\b|\bsr\.\b", t):
        score += 4
    if re.search(r"\bscientist\s+(ii|iii|iv|2|3|4)\b", t):
        score += 3
    if re.search(r"\bjunior\b|\bentry\s+level\b|\bscientist\s+i\b|\bscientist\s+1\b", t):
        score -= 8
    if re.search(r"\bpostdoc\b|\bpost-doc\b|\bpostdoctoral\b", d[:200]):
        score -= 15

    # Hard seniority penalties
    if re.search(r"\bvp\b|\bvice\s+president\b", t):            score -= 30
    if re.search(r"\bchief\b|\bcso\b|\bcmo\b", t):              score -= 35
    if re.search(r"\bhead\s+of\b", t):                          score -= 20
    if re.search(r"\bdirector\b", t):                           score -= 15

    # Academic role penalty
    if any(k in d for k in ["tenure track", "faculty position",
                             "academic appointment", "university professor"]):
        score -= 20

    # Hard disqualifiers
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci", "clearance required"]):
        score -= 30

    return max(0, min(score, 100))


# ---------------------------------------------------------------------------
# Batch LLM scorer — 15 jobs per Groq call
# ---------------------------------------------------------------------------
def llm_score_batch(batch: list[dict]) -> list[int | None]:
    if not GROQ_API_KEY or not batch:
        return [None] * len(batch)

    n = len(batch)
    blocks = []
    for i, j in enumerate(batch, 1):
        blocks.append(
            f"[{i}] {j['title']} @ {j['company']} | {j['location']}\n"
            f"{j['desc'][:400]}"
        )

    prompt = (
        f"You are a scientific recruiter evaluating {n} biotech/pharma job postings.\n"
        "Score each 0–100 for fit with this specific candidate:\n\n"

        "CANDIDATE — Dr. Pooja Choubey, Ph.D. Molecular Genetics:\n"
        "CURRENT: Post-Doctoral Fellow, Lundquist Institute / Harbor-UCLA Medical Center, Torrance CA\n"
        "EDUCATION: Ph.D. Molecular Genetics | B.Sc. Biochemistry\n"
        "PUBLICATIONS: Co-first author, Nature Communications 2026 — PTRH2 role in peripartum "
        "cardiomyopathy (Altmetric 78); additional publications in cardiovascular biology\n\n"

        "IN VIVO EXPERTISE (10+ years):\n"
        "- Mouse colony management: 200+ mice, 3 transgenic lines (cardiac-specific overexpression)\n"
        "- Cardiac phenotyping: Langendorff heart isolation, echocardiography (VEVO F2), "
        "EKG recording, in vivo contractility, pressure-volume loops\n"
        "- Disease models: TAC (aortic banding), isoproterenol, MI, angiotensin II infusion, "
        "osmotic pumps, survival surgeries, tissue harvest\n\n"

        "CELLULAR & MOLECULAR ASSAYS:\n"
        "- FACS / flow cytometry, Western blot, IHC, ICC, ELISA, qRT-PCR, TUNEL\n"
        "- Beta-gal staining, hydroxyproline assay, Masson trichrome, Sirius Red\n"
        "- Cell culture (primary cardiomyocytes, cardiac fibroblasts)\n\n"

        "OMICS:\n"
        "- RNA-seq, scRNA-seq (Seurat, cell annotation), Xenium & Visium spatial transcriptomics\n"
        "- IPA (Ingenuity Pathway Analysis), STRING, GSEA, GO enrichment\n\n"

        "TARGET ROLES: R&D Scientist / Preclinical Scientist / Translational Scientist "
        "at biotech, pharma, or CRO — hands-on bench science.\n"
        "PREFERRED: Senior Scientist / Scientist II–IV / Staff Scientist / Principal Scientist\n"
        "NOT SUITABLE FOR: Director, VP, Head of, Chief, Managing Director, SVP — "
        "candidate is a postdoc transitioning to industry scientist, NOT a department head.\n"
        "OPEN TO: US nationwide, Europe (UK, Switzerland, Germany, France, Netherlands, "
        "Sweden, Denmark, Belgium), India (Bangalore, Hyderabad, Pune). NOT seeking purely remote.\n\n"

        "SCORING RUBRIC (BE STRICT):\n"
        "90–100: Role is EXPLICITLY hands-on preclinical/cardiovascular/translational R&D at "
        "biotech/pharma/CRO, PhD required/preferred, strong skill overlap with in vivo or "
        "cardiac phenotyping or spatial transcriptomics\n"
        "70–89: Strong R&D/preclinical role at biotech/pharma, PhD preferred, good wet-lab skill "
        "overlap, adjacent therapeutic area (e.g. fibrosis, metabolic disease, oncology with "
        "mouse models)\n"
        "50–69: Decent — relevant wet-lab techniques but missing cardiovascular/preclinical focus, "
        "OR good role but missing PhD requirement, OR CRO lab role\n"
        "30–49: Some overlap — mostly computational, OR missing PhD, OR wrong therapeutic area "
        "with minimal in vivo\n"
        "< 30: Poor — QA/QC, manufacturing, regulatory, clinical coordinator, software engineer, "
        "sales, marketing, pure bioinformatics with no wet lab, OR role clearly not needing PhD\n\n"
        "CRITICAL: If the title contains Director, VP, Vice President, Head of, Chief, SVP, or "
        "Managing Director — score MAX 30. This candidate needs a bench scientist role, NOT management.\n\n"

        "JOBS:\n" +
        "\n\n".join(blocks) +
        f"\n\nReturn ONLY a raw JSON array of {n} integers, e.g. [82,45,91,67]. "
        "No text, no explanation, no markdown."
    )

    for attempt in range(3):
        try:
            r = requests.post(
                GROQ_ENDPOINT,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": GROQ_MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 150,
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

            try:
                parsed = json.loads(text)
                if isinstance(parsed, list) and len(parsed) >= n:
                    return [min(int(x), 100) for x in parsed[:n]]
            except (json.JSONDecodeError, ValueError):
                pass

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
# Parallel scrape helper
# ---------------------------------------------------------------------------
def _scrape_one(cfg: dict) -> pd.DataFrame:
    kwargs = dict(
        site_name=cfg.get("sites", ["indeed"]),   # LinkedIn/Glassdoor/ZipRecruiter 403 on Render
        search_term=cfg["term"],
        location=cfg["location"],
        results_wanted=cfg["results"],
    )
    if "distance" in cfg:
        kwargs["distance"] = cfg["distance"]

    df = scrape_jobs(**kwargs)
    if not df.empty:
        df["_search_pass"] = cfg["label"]
        df["_region"]      = cfg.get("region", "US")
    return df


# ---------------------------------------------------------------------------
# Search configuration — 22 passes: US hubs + Europe + India
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:
    # Core scientist terms used across many passes
    _CORE = (
        '"Research Scientist" OR "Senior Scientist" OR "Staff Scientist" '
        'OR "Principal Scientist" OR "Scientist II" OR "Scientist III" '
        'OR "Translational Scientist" OR "Preclinical Scientist" '
        'OR "Cardiovascular Scientist" OR "In Vivo Scientist" '
        'OR "Biomarker Scientist" OR "Drug Discovery Scientist" '
        'OR "Research Investigator" OR "Senior Investigator"'
    )
    # Cardiovascular-specific terms for focused passes
    _CV = (
        '"Cardiovascular Research Scientist" OR "Cardiovascular Scientist" '
        'OR "Cardiac Research Scientist" OR "Heart Failure Scientist" '
        'OR "Cardiomyopathy" OR "Preclinical Cardiovascular" '
        'OR "In Vivo Cardiovascular" OR "Translational Cardiology"'
    )
    return [
        # ── US NATIONWIDE ──────────────────────────────────────────────────
        {
            "label":    "Cardiovascular / Cardiac Scientist (US nationwide)",
            "term":     _CV,
            "location": "United States",
            "results":  50,
            "region":   "US",
        },
        {
            "label":    "Preclinical / In Vivo Scientist (US nationwide)",
            "term":     ('"Preclinical Research Scientist" OR "Preclinical Scientist" '
                         'OR "In Vivo Scientist" OR "In Vivo Research Scientist" '
                         'OR "Disease Model Scientist" OR "Animal Model Scientist" '
                         'OR "Translational Research Scientist" OR "Translational Scientist"'),
            "location": "United States",
            "results":  50,
            "region":   "US",
        },
        {
            "label":    "Biomarker / Drug Discovery Scientist (US nationwide)",
            "term":     ('"Biomarker Scientist" OR "Biomarker Discovery" '
                         'OR "Drug Discovery Scientist" OR "Pharmacologist" '
                         'OR "Target Identification Scientist" '
                         'OR "Target Validation Scientist"'),
            "location": "United States",
            "results":  50,
            "region":   "US",
        },
        {
            "label":    "Senior / Staff / Principal Scientist — Cardiovascular (US)",
            "term":     ('"Senior Research Scientist" OR "Staff Scientist" '
                         'OR "Principal Scientist" OR "Research Investigator" '
                         'OR "Senior Investigator" cardiovascular OR cardiac OR '
                         '"heart failure" OR preclinical OR "in vivo"'),
            "location": "United States",
            "results":  50,
            "region":   "US",
        },
        {
            "label":    "Spatial Transcriptomics / Omics Scientist (US)",
            "term":     ('"Spatial Transcriptomics" OR "Xenium" OR "Visium" '
                         'OR "Single Cell RNA" OR "scRNA-seq" OR "Transcriptomics Scientist" '
                         'OR "Genomics Scientist"'),
            "location": "United States",
            "results":  30,
            "region":   "US",
        },
        # ── US HUBS ────────────────────────────────────────────────────────
        {
            "label":    "Scientist — LA / Torrance / Irvine area",
            "term":     ('"Research Scientist" OR "Senior Scientist" '
                         'OR "Translational Scientist" OR "Preclinical Scientist" '
                         'OR "Cardiovascular" OR "In Vivo" OR "Drug Discovery"'),
            "location": "Torrance, CA",
            "results":  50,
            "distance": 40,
            "region":   "US",
        },
        {
            "label":    "Scientist — Boston / Cambridge MA (Broad / MIT / Biogen / Vertex)",
            "term":     _CORE,
            "location": "Cambridge, MA",
            "results":  50,
            "distance": 30,
            "region":   "US",
        },
        {
            "label":    "Scientist — San Francisco / South SF (Genentech / Amgen / Gilead)",
            "term":     _CORE,
            "location": "South San Francisco, CA",
            "results":  50,
            "distance": 40,
            "region":   "US",
        },
        {
            "label":    "Scientist — San Diego CA (Pfizer / AstraZeneca / Janssen SD)",
            "term":     _CORE,
            "location": "San Diego, CA",
            "results":  50,
            "distance": 30,
            "region":   "US",
        },
        {
            "label":    "Scientist — Philadelphia / NJ pharma corridor (Merck / J&J / GSK)",
            "term":     _CORE,
            "location": "Philadelphia, PA",
            "results":  50,
            "distance": 50,
            "region":   "US",
        },
        {
            "label":    "Scientist — Research Triangle Park NC (GSK / Bayer / Syneos)",
            "term":     _CORE,
            "location": "Durham, NC",
            "results":  50,
            "distance": 30,
            "region":   "US",
        },
        {
            "label":    "Scientist — Seattle / Bothell WA (Seagen / Novo Nordisk / IQVIA)",
            "term":     _CORE,
            "location": "Seattle, WA",
            "results":  30,
            "distance": 30,
            "region":   "US",
        },
        # ── EUROPE ─────────────────────────────────────────────────────────
        {
            "label":    "Scientist — Cambridge UK (AstraZeneca / Wellcome Sanger / GSK)",
            "term":     _CORE,
            "location": "Cambridge, United Kingdom",
            "results":  50,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — London UK (GSK / UCB / MSD / Immunocore)",
            "term":     _CORE,
            "location": "London, United Kingdom",
            "results":  50,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Basel Switzerland (Novartis / Roche / Lonza)",
            "term":     _CORE,
            "location": "Basel, Switzerland",
            "results":  50,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Zurich / Schlieren Switzerland (Novartis Institutes)",
            "term":     _CORE,
            "location": "Zurich, Switzerland",
            "results":  30,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Munich Germany (BioNTech / Bayer / Helmholtz)",
            "term":     _CORE,
            "location": "Munich, Germany",
            "results":  50,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Paris France (Sanofi / Institut Pasteur / Servier)",
            "term":     _CORE,
            "location": "Paris, France",
            "results":  30,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Amsterdam / Leiden NL (Janssen / LUMC / BioLegend)",
            "term":     _CORE,
            "location": "Amsterdam, Netherlands",
            "results":  30,
            "region":   "Europe",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        # ── INDIA ──────────────────────────────────────────────────────────
        {
            "label":    "Scientist — Bangalore India (Biocon / AstraZeneca / Syngene)",
            "term":     _CORE,
            "location": "Bengaluru",
            "results":  50,
            "region":   "India",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Hyderabad India (Dr Reddy's / Aurobindo / Cipla R&D)",
            "term":     _CORE,
            "location": "Hyderabad",
            "results":  50,
            "region":   "India",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
        },
        {
            "label":    "Scientist — Pune India (Serum Institute / Lupin / Piramal)",
            "term":     _CORE,
            "location": "Pune",
            "results":  30,
            "region":   "India",
            "sites":    ["indeed"],   # LinkedIn 403 on Render cloud IPs
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

    # ── Step 1: Parallel scrape ────────────────────────────────────────────
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

    # ── Step 2: Combine + deduplicate ─────────────────────────────────────
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

    # ── Step 3: Title filter ───────────────────────────────────────────────
    raw["title_lower"] = raw["title"].str.lower().fillna("")
    mask     = raw["title_lower"].apply(matches_title)
    filtered = raw[mask].copy()
    sprint(f"[Filter] {len(raw)} → {len(filtered)} passed title filter "
           f"(removed {len(raw) - len(filtered)} non-science jobs)")

    if filtered.empty:
        push_notification("Pooja Bio Hunt", "Scan complete — 0 relevant titles found.", "low")
        return

    # ── Step 4: Pre-rank by title relevance; split LLM vs keyword pool ────
    filtered["_title_rel"] = filtered["title"].apply(title_relevance)
    if "date_posted" in filtered.columns:
        filtered = filtered.sort_values(
            ["_title_rel", "date_posted"], ascending=[False, False]
        )
    else:
        filtered = filtered.sort_values("_title_rel", ascending=False)

    llm_pool     = filtered.head(SCORE_LLM_TOP).copy()
    keyword_pool = filtered.iloc[SCORE_LLM_TOP:].copy()
    sprint(f"[Score]  {len(llm_pool)} → LLM batch  |  {len(keyword_pool)} → keyword scorer\n")

    # ── Step 5a: LLM batch scoring ────────────────────────────────────────
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
        time.sleep(2)

    # ── Step 5b: Keyword scoring for remainder ────────────────────────────
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

    # ── Step 6: Collect results, alerts, CSV ──────────────────────────────
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

    # ── Step 7: Merge + save CSV ──────────────────────────────────────────
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


if __name__ == "__main__":
    pooja_hunt()
