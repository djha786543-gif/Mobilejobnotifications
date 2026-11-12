"""
pooja_hunter.py — Pooja Choubey's Academic Job Scanner (Universities & Research Institutes)
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
HOURS_OLD      = 360   # 15 days (broader window — fewer postings than IT audit)
MIN_SAVE_SCORE = 35
MAX_ALERTS     = 10
SCORE_LLM_TOP  = 60    # top N by title relevance → LLM batch (increased from 45)
BATCH_SIZE     = 15    # jobs per Groq call
SCRAPE_WORKERS = 8     # parallel scrape threads
MAX_RESULTS    = 100   # cap per search pass — raised to match per-city config volumes
MAX_RETRIES    = 1     # retries per search pass on 0 results
RETRY_DELAY    = 10    # seconds between retries


# ---------------------------------------------------------------------------
# Title whitelist — research science and academic roles at universities / research institutes
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
    # Postdoctoral positions (academic/research institutions)
    r"\bpostdoctoral\s+(?:fellow|scientist|researcher|associate|position)\b",
    r"\bpostdoc(?:toral)?\s+(?:fellow|researcher|associate)\b",
    r"\bpostdoc\b",
    # Academic/faculty positions
    r"\bassistant\s+professor\b",
    r"\bassociate\s+professor\b",
    r"\bfull\s+professor\b",
    r"\blecturer\b",
    r"\bjunior\s+(?:research\s+)?group\s+leader\b",
    r"\bgroup\s+leader\b",
    r"\bteam\s+leader\b.*\b(?:research|science|biology)\b",
    r"\bfaculty\s+(?:member|position|role)\b",
    r"\btenure.track\b",
    r"\btenure\s+track\b",
    r"\bPI\s+position\b",
    r"\bindependent\s+(?:researcher|investigator|fellow)\b",
    r"\bEMBO\s+fellow\b",
    r"\bHumboldt\s+fellow\b",
    r"\bWellcome\s+(?:fellow|trust)\b",
    r"\bMarie\s+Curie\b",
    r"\bMSCA\b",
    r"\bHFSP\s+fellow\b",
    r"\bvisiting\s+(?:scholar|researcher|scientist|fellow)\b",
    r"\bresearch\s+instructor\b",
    r"\binstructor\b.*\b(?:research|biology|science)\b",
]

# Hard blacklist — veto even if whitelist matched
TITLE_BLACKLIST = [
    r"\bsoftware\s+(?:engineer|developer)\b",
    r"\bdata\s+(?:engineer|architect)\b",
    r"\bdata\s+scientist\b",                    # pure ML/AI data science
    r"\bbioinformatician\b",                    # hardcore bioinformatics, no wet lab
    r"\bbioinformatics\s+(?:scientist|analyst|engineer|developer|specialist)\b",
    r"\bcomputational\s+biolog(?:ist|y)\b",     # pure computational role
    r"\bcomputational\s+scientist\b",
    r"\bsales\b|\bmarketing\b",
    r"\bclinical\s+research\s+(?:coordinator|associate)\b",
    r"\bregulatory\s+affairs\b",
    r"\bmanufacturing\s+scientist\b",
    r"\bprocess\s+development\s+scientist\b",
    r"\bquality\s+(?:assurance|control)\s+scientist\b",
    r"\bnurse\b|\bphysician\b|\bclinician\b",
    r"\bdrug\s+discovery\s+(?:director|manager|head)\b",
    r"\bvp\s+(?:of\s+)?(?:research|science|biology)\b",
    r"\bhead\s+of\s+(?:research|biology|science|pharmacology)\b",
    r"\bcommercial\b.*\bscientist\b",
    r"\bmedical\s+science\s+liaison\b",
    r"\bfield\s+application\s+scientist\b",  # sales-focused
    r"\bclinical\s+development\s+scientist\b",
    r"\bquality\s+control\s+scientist\b",
    r"\bmanufacturing\s+process\s+scientist\b",
    # Non-biology sciences — prevent generic "Research Scientist" / "Senior Scientist"
    # matching materials, physics, environmental, or other non-life-sciences domains.
    # Note: word-boundary \b ensures "biochemist" is NOT matched by r"\bchemist\b",
    # and "biophysicist" is NOT matched by r"\bphysicist\b".
    r"\bmaterials?\s+(?:scientist|science|engineer|engineering)\b",
    r"\bphysicist\b",
    r"\bphysics\s+(?:research|researcher|scientist)\b",
    r"\benvironmental\s+scientist\b",
    r"\bgeolog(?:y|ist)\b",
    r"\becolog(?:y|ist)\b",
    r"\bstatistician\b",
    r"\bchemist\b",                             # \b: "biochemist" is NOT matched
    r"\bchemical\s+engineer",
    r"\bepidemiolog(?:y|ist)\b",
    r"\bagricult(?:ure|ural)\s+scientist\b",
    r"\bfood\s+scientist\b",
    r"\bveterinar(?:y\s+scientist|ian)\b",
    # NOTE: postdoc intentionally NOT blacklisted — Pooja needs academic US roles (J1)
]

# ---------------------------------------------------------------------------
# J1-eligible US employer filter
# Pooja is on J1 — corporate pharma/biotech cannot sponsor H1B immediately.
# Only keep US positions at academic, government, or non-profit research orgs.
# ---------------------------------------------------------------------------
_US_J1_ELIGIBLE_RE = re.compile(
    r"universit|college\b|medical\s+center|medical\s+school|school\s+of\s+medicine"
    r"|health\s+(system|network|sciences|center)|hospital\b|clinic\b"
    r"|\binstitute\b|\bfoundation\b|research\s+(center|institute|consortium)"
    r"|national\s+(institute|cancer|heart|lab|laboratory|institute)"
    r"|\bnih\b|\bnci\b|\bnhlbi\b|\bniddk\b|\bnimh\b|\bcdc\b|\bfda\b|\busda\b"
    r"|veterans\s+affairs|\bvamc\b|\bva\s+medical"
    r"|children'?s\b|pediatric\b|academic\s+medical"
    r"|\bhutchinson\b|\bhutch\b|dana.farber|\bscripps\b|\bsalk\b"
    r"|broad\s+institute|\bwistar\b|mayo\s+clinic|cleveland\s+clinic"
    r"|johns\s+hopkins|moffitt|jackson\s+lab|jax\b|chop\b"
    r"|\blundquist\b|harbor.ucla|cedars.sinai|mount\s+sinai|cornell\b"
    r"|stanford\s+(?:university|hospital|medicine)|ucsf\b|ucsd\b|ucla\b|usc\b"
    r"|mit\s*\b|caltech\b|carnegie\s+mellon|emory|vanderbilt|duke\s+university",
    re.IGNORECASE,
)

def is_us_j1_eligible(company: str) -> bool:
    """Return True if the company is an academic / government / non-profit
    research institution suitable for Pooja's J1 visa status."""
    return bool(_US_J1_ELIGIBLE_RE.search(company))


_ACADEMIC_INST_RE = re.compile(
    r"universit|college\b|medical\s+cent(?:er|re)|medical\s+school|school\s+of\s+medicine"
    r"|health\s+(system|network|sciences|cent(?:er|re))|hospital\b"
    r"|\binstitut(?:e|o|um)\b|\binstituts?\b"
    r"|\bumc\b|\bamc\b|\buclh\b|\bnhs\b"               # European medical center abbrevs
    r"|research\s+cent(?:er|re)|research\s+institute|research\s+foundation"
    r"|national\s+(institute|laboratory|lab\b)"
    r"|\bnih\b|\bnci\b|\bnhlbi\b|\bniddk\b|\bnimh\b|\bcdc\b|\bfda\b|\busda\b"
    r"|karolinska|helmholtz|max.planck|max-planck|leibniz"
    r"|\bcnrs\b|\binserm\b|pasteur\b|\bcnio\b|\bcnic\b"
    r"|weizmann|\bembl\b|\bdkfz\b|\bvib\b|\bhubrecht\b"
    r"|charité|charite|\bdzhk\b|\bbih\b|\bmdc\b"        # Berlin academic centres
    r"|\bimba\b|\bimp\s+vienna\b|\bczb\b"               # Vienna
    r"|\bfmi\b|\bepfl\b|\beth\s+zurich\b|\bsnf\b"       # Swiss
    r"|scilife|\bki\b.*karolinska"                       # Karolinska / SciLifeLab
    r"|\bnnf\b|\bdtu\b|novo\s+nordisk\s+foundation"     # Denmark
    r"|veterans\s+affairs|\bvamc\b"
    r"|children'?s\b|pediatric\b|academic\s+medical"
    r"|\bhutchinson\b|\bhutch\b|dana.farber|\bscripps\b|\bsalk\b"
    r"|broad\s+institute|\bwistar\b|mayo\s+clinic|cleveland\s+clinic"
    r"|johns\s+hopkins|moffitt|jackson\s+lab|jax\b|chop\b"
    r"|\blundquist\b|harbor.ucla|cedars.sinai|mount\s+sinai"
    r"|stanford|ucsf\b|ucsd\b|ucla\b"
    r"|mit\b|caltech\b|carnegie\s+mellon|emory|vanderbilt|duke\s+university"
    r"|gladstone|garvan|wehi\b|monash|baker\s+(heart|institute)"
    r"|wellcome\s+trust|crick\b|babraham|sanger\b|\bmrc\b",
    re.IGNORECASE,
)

def is_academic_institution(company: str) -> bool:
    return bool(_ACADEMIC_INST_RE.search(company))


def matches_title(title: str) -> bool:
    t = title.lower()
    if not any(re.search(p, t) for p in TITLE_WHITELIST):
        return False
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
    score = 20   # base — passed title filter

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

        # Academic institution signals (strong positive for academic search)
        "university":               8, "universität":              8,
        "research institute":       9, "research center":          9,
        "research centre":          9, "medical school":           8,
        "medical center":           8, "medical centre":           8,
        "research hospital":        7, "academic medical":         8,
        "postdoctoral fellowship":  9, "postdoc fellowship":       9,
        "fellowship position":      8, "research fellowship":      8,
        "group leader":             9, "junior group leader":      9,
        "independent group":        8, "starting group":           8,
        "tenure track":             9, "tenure-track":             9,
        "assistant professor":      9, "associate professor":      8,
        "lecturer position":        8, "research lecturer":        7,
        "k99":                      8, "r00":                      7,
        "k award":                  7, "nih grant":                6,
        "grant funding":            6, "extramural":               6,
        "phd students":             7, "graduate students":        7,
        "mentoring students":       6, "supervise students":       6,
        "embo":                     8, "hfsp":                     8,
        "marie curie":              8, "msca":                     8,
        "humboldt":                 7, "wellcome trust":           8,
        "bhf":                      7, "mrc funded":               7,
        # Visa/work permit for international candidates
        "work permit":              6, "visa sponsorship":         7,
        "relocation":               5,

        # Senescence / aging (Pooja's MASH/PTRH2 program extends into senescence)
        "senescence":               8, "cellular senescence":   9,
        "sa-β-gal":                 7, "sa-bgal":               7,
        "p16":                      5, "p21":                   5,
        "sasp":                     7, "senolytic":             7,
        "senolytics":               7, "aging":                 4,
        "age-related":              5, "healthspan":            5,

        # MASH / NASH / hepatic / metabolic disease (Catalyst Grant focus)
        "mash":                     9, "nash":                  9,
        "steatohepatitis":          9, "steatosis":             7,
        "fatty liver":              7, "nafld":                 8,
        "hepatic":                  7, "liver disease":         7,
        "metabolic disease":        7, "metabolic dysfunction": 7,
        "metabolic syndrome":       6, "obesity model":         7,
        "diet-induced":             7, "high-fat diet":         7,
        "hfd":                      6, "insulin resistance":    6,

        # Omics tools (from CV — more specific than before)
        "deseq2":                   7, "seurat":                6,
        "scanpy":                   6, "ggplot2":               4,
        "loupe browser":            6, "xenium explorer":       7,
        "clc genomics":             5, "bulk rna":              6,
        "metabolomics":             7, "lipidomics":            5,

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
    # Postdoc at academic institution — actively desired for J1 US eligibility
    if re.search(r"\bpostdoc\b|\bpost-doc\b|\bpostdoctoral\b", t):
        score += 10   # academic postdoc is a target role

    # Hard bioinformatics-only penalty — Pooja is a wet-lab scientist, not a pure analyst
    _WET_LAB_KW = ["mouse", "in vivo", "animal", "cell culture", "western blot",
                   "flow cytometry", "facs", "ihc", "elisa", "phenotyp", "echocard",
                   "langendorff", "qpcr", "qrt-pcr", "confocal", "immunofluorescence"]
    _BIOINF_HEAVY = ["bioinformatics", "pipeline", "workflow", "cloud", "aws", "hpc",
                     "kubernetes", "docker", "nextflow", "snakemake", "linux", "bash scripting"]
    has_wet = any(k in d for k in _WET_LAB_KW)
    has_bioinf = sum(1 for k in _BIOINF_HEAVY if k in d)
    if has_bioinf >= 3 and not has_wet:
        score -= 25   # hardcore bioinformatics/computational, no wet-lab component

    # Hard seniority penalties
    if re.search(r"\bvp\b|\bvice\s+president\b", t):            score -= 30
    if re.search(r"\bchief\b|\bcso\b|\bcmo\b", t):              score -= 35
    if re.search(r"\bhead\s+of\b", t):                          score -= 20
    if re.search(r"\bdirector\b", t):                           score -= 15

    # Hard disqualifiers
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci", "clearance required"]):
        score -= 30

    # Industry company / commercial penalty
    _INDUSTRY_SIGNALS = [
        "drug candidate", "ind filing", "clinical pipeline", "commercial pipeline",
        "gmp facility", "cmc development", "manufacturing process",
        "business development", "market access", "commercial strategy",
        "quarterly targets", "sales territory", "key account",
    ]
    _PHARMA_COMPANIES = [
        "novartis", "astrazeneca", "roche", "genentech", "pfizer",
        "merck kga", "merck & co", "sanofi", "abbvie", "amgen", "biogen",
        "vertex pharmaceut", "bristol-myers", "bms\b", "lilly\b",
        "boehringer ingelheim", "bayer ag", "ucb pharma",
        "charles river", "covance", "labcorp drug", "icon plc", "iqvia",
        "evotec", "crown bioscience", "cyagen",
    ]
    has_industry_signal = sum(1 for k in _INDUSTRY_SIGNALS if k in d)
    has_pharma_company  = sum(1 for k in _PHARMA_COMPANIES if k in d)
    if has_industry_signal >= 2:
        score -= 20
    if has_pharma_company >= 1:
        score -= 15

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
        f"You are an academic recruiter evaluating {n} university/research-institute job postings.\n"
        "Score each 0–100 for fit with this specific candidate:\n\n"

        "CANDIDATE — Dr. Pooja Choubey, Ph.D. Molecular Genetics:\n"
        "CURRENT: Postdoctoral Scientist, Lundquist Institute / Harbor-UCLA Medical Center, "
        "Torrance CA (Matter Lab) — Sep 2025–Present\n"
        "EDUCATION: Ph.D. Molecular Genetics, University of Delhi (2023) | M.Sc. Zoology | "
        "B.Sc. (Hons.) Zoology — first position\n"
        "PUBLICATIONS: 10 peer-reviewed articles + 1 book chapter; "
        "co-first author Nature Communications 2026 (PTRH2/peripartum cardiomyopathy); "
        "first-author JACC: Advances 2026 (PERCVD genetic screening trial); "
        "first-author Cells 2026 (cardiac transcriptomics)\n"
        "GRANT: PI on competitive intramural Postdoctoral Catalyst Grant (Oct 2026–Sep 2027) "
        "on PTRH2-driven hepatic senescence in MASH — building towards K99/R00\n\n"

        "IN VIVO EXPERTISE (10+ years):\n"
        "- Transgenic mouse colony: 200+ mice, 3 lines (Hmox1, Ptrh2)\n"
        "- Cardiac phenotyping: Langendorff perfusion, echocardiography (Vevo F2), ECG, "
        "adult cardiomyocyte isolation, contractility assays\n"
        "- Disease models: peripartum cardiomyopathy, heart failure, MASH/NASH, obesity, "
        "diet-induced liver disease, timed matings, embryo collection (E12.5–E18.5)\n"
        "- Senescence: SA-β-gal, TUNEL, flow cytometry senescence panels\n\n"

        "CELLULAR & MOLECULAR ASSAYS:\n"
        "- FACS/flow cytometry, Western blot, IHC, ICC, immunofluorescence, ELISA, qRT-PCR\n"
        "- Confocal and Keyence imaging; histology (Masson trichrome, Sirius Red)\n"
        "- Primary cell culture (cardiomyocytes, fibroblasts, myoblasts, patient-derived)\n\n"

        "OMICS & DATA ANALYSIS:\n"
        "- Bulk RNA-seq, scRNA-seq (Seurat, Scanpy)\n"
        "- 10x Visium & Xenium spatial transcriptomics; Loupe Browser; Xenium Explorer\n"
        "- R (DESeq2, Seurat, ggplot2); Python (Scanpy, pandas); QIAGEN IPA; STRING\n\n"

        "TARGET ROLES (academic/research institutions ONLY):\n"
        "1. Postdoctoral Fellow/Researcher at a university, medical school, research institute "
        "or government lab (NIH) — WET-LAB focused; cardiovascular, cardiometabolic, "
        "MASH/hepatic, senescence/aging, reproductive/placental, or spatial transcriptomics biology\n"
        "2. Research Scientist / Staff Scientist / Research Fellow at a university or "
        "non-profit research institute\n"
        "3. Junior Group Leader / Assistant Professor / Lecturer — if at appropriate stage\n"
        "NOT SUITABLE FOR: Pure computational/bioinformatics, clinical coordinator, "
        "regulatory affairs, any industry role (pharma/biotech/CRO), VP/Director/Head of roles\n"
        "CRITICAL WET-LAB REQUIREMENT: Role must require or strongly prefer wet-lab work "
        "(mouse models, cardiac/metabolic phenotyping, cell biology, histology). "
        "Score LOW for roles that are primarily computational with no bench component.\n\n"

        "SCORING RUBRIC (BE STRICT — ACADEMIC FOCUS):\n"
        "90–100: Explicitly hands-on wet-lab cardiovascular/cardiometabolic/MASH/senescence "
        "research at a university or research institute; PhD required; strong overlap with "
        "in vivo, cardiac phenotyping, spatial transcriptomics, or mouse models\n"
        "70–89: Strong wet-lab academic role, PhD preferred, good overlap, adjacent area "
        "(liver/metabolic, aging, reproductive biology, developmental biology)\n"
        "50–69: Academic postdoc or research scientist, relevant techniques but missing "
        "cardiovascular/preclinical focus, OR general molecular biology/cell biology\n"
        "30–49: Academic role but mostly computational, OR wrong field, OR missing PhD\n"
        "< 30: Industry role, QA/QC, manufacturing, clinical, software, pure bioinformatics\n\n"
        "CRITICAL: Industry/pharma/biotech/CRO postings — score MAX 20.\n"
        "CRITICAL: Pure bioinformatics/computational roles with no bench work — score MAX 30.\n"
        "CRITICAL: Director, VP, Head of — score MAX 20.\n\n"

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
# Robust scrape with retry + fallback sites (mirrors DJ's scrape_with_retry)
# ---------------------------------------------------------------------------
def scrape_with_retry(cfg: dict) -> pd.DataFrame:
    """
    Try Indeed first. If 0 results, retry up to MAX_RETRIES times.
    If still 0, fall back to cfg['fallback_sites'] (one attempt each).
    For US passes, passes country_indeed="USA" to prevent geo-blocking.
    Returns labelled DataFrame or empty DataFrame.
    """
    is_us = cfg.get("region", "US") == "US"
    base_kwargs = dict(
        search_term=cfg["term"],
        location=cfg["location"],
        results_wanted=min(cfg["results"], MAX_RESULTS),
        hours_old=HOURS_OLD,
    )
    # Set country_indeed so jobspy hits the correct national Indeed site.
    # Without this, all European passes default to indeed.com (US) → 0 results.
    if "country_indeed" in cfg:
        base_kwargs["country_indeed"] = cfg["country_indeed"]
    elif is_us:
        base_kwargs["country_indeed"] = "USA"
    if "distance" in cfg:
        base_kwargs["distance"] = cfg["distance"]
    if cfg.get("remote"):
        base_kwargs["is_remote"] = True

    sites = cfg.get("sites", ["indeed"])

    # --- Primary: Indeed (with retry) ---
    for attempt in range(1 + MAX_RETRIES):
        try:
            df = scrape_jobs(site_name=sites, **base_kwargs)
            if not df.empty:
                df["_search_pass"] = cfg["label"]
                df["_region"]      = cfg.get("region", "US")
                sprint(f"  → {len(df)} raw results (indeed, attempt {attempt+1})")
                return df
            if attempt < MAX_RETRIES:
                sprint(f"  → 0 results (attempt {attempt+1}/{1+MAX_RETRIES}) — retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
        except Exception as e:
            sprint(f"  [Scrape error attempt {attempt+1}]: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    # --- Fallback: try other sites ---
    fallback_sites = cfg.get("fallback_sites", [])
    for site in fallback_sites:
        try:
            sprint(f"  → Trying fallback: {site}...")
            # Strip country_indeed / hours_old for non-Indeed sites
            fb_kwargs = {k: v for k, v in base_kwargs.items()
                         if k not in ("country_indeed", "hours_old")}
            df = scrape_jobs(site_name=[site], **fb_kwargs)
            if not df.empty:
                df["_search_pass"] = cfg["label"]
                df["_region"]      = cfg.get("region", "US")
                sprint(f"  → {len(df)} raw results ({site} fallback)")
                return df
            sprint(f"  → 0 results ({site} fallback)")
        except Exception as e:
            sprint(f"  [{site} fallback error]: {e}")
        time.sleep(8)

    sprint(f"  → 0 results from all sources (likely IP block or no new postings)")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Search configuration — domain-specific passes across Europe + US academic hubs
#
# Root problem with generic title-only searches: Indeed returns any "Research
# Scientist" in any field (AI/ML, materials, physics…).  By embedding Pooja's
# domain keywords directly in the search term, Indeed's own relevance engine
# pre-filters to biology-relevant postings before our title/institution filters
# even run.
#
# Three domain search strings:
#   _CARDIO  — cardiovascular / cardiac / preclinical / in vivo / mouse models
#   _METAB   — metabolic liver disease / MASH / NASH / senescence / aging
#   _SPATIAL — spatial transcriptomics / single-cell / molecular & cell biology
#
# City strategy: 13 Europe + 10 US = 23 cities.
# _CARDIO runs everywhere (Pooja's core 10+ years expertise).
# _METAB + _SPATIAL run in top 10 high-volume hubs.
# Total: ~36 passes.
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:

    # Boolean OR groups: (role title) AND (domain keyword)
    # Space-separated words without OR = AND in Indeed → returns 0 results.
    # Parenthesised OR groups ensure Indeed matches job title OR domain term.

    # Cardiovascular / preclinical / in vivo (Pooja's primary expertise)
    _CARDIO = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist" OR "Staff Scientist" OR '
        '"In Vivo Scientist" OR "Cardiovascular Scientist" OR '
        '"Assistant Professor" OR "Group Leader" OR "Junior Group Leader") '
        '(cardiovascular OR cardiac OR "heart failure" OR cardiomyopathy OR '
        '"in vivo" OR preclinical OR "mouse model" OR transgenic OR '
        '"disease model" OR "cardiac phenotyping" OR Langendorff OR '
        'echocardiography OR cardiomyocyte)'
    )

    # Metabolic liver disease / MASH / NASH / senescence (Pooja's current grant)
    _METAB = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist" OR "Staff Scientist") '
        '(MASH OR NASH OR steatohepatitis OR "fatty liver" OR NAFLD OR '
        '"metabolic disease" OR senescence OR "cellular senescence" OR '
        '"liver disease" OR "hepatic steatosis" OR aging OR "metabolic dysfunction")'
    )

    # Spatial transcriptomics / single-cell / molecular biology wet-lab
    _SPATIAL = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist") '
        '("spatial transcriptomics" OR "single cell RNA" OR "scRNA-seq" OR '
        'Xenium OR Visium OR "10x Genomics" OR "single-cell sequencing" OR '
        '"RNA sequencing" OR "flow cytometry" OR "FACS" OR '
        '"molecular biology" OR "cell biology")'
    )

    def _eu(label, city, term, n=75, country="UK"):
        return {"label": label, "term": term, "location": city,
                "results": n, "region": "Europe", "sites": ["indeed"],
                "country_indeed": country}

    def _us(label, city, term, n=75):
        return {"label": label, "term": term, "location": city,
                "results": n, "region": "US", "sites": ["indeed"]}

    return [
        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — UK
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Cambridge UK (Crick/Sanger/Babraham/MRC)",     "Cambridge, United Kingdom",  _CARDIO, 100, "UK"),
        _eu("Metab/Spatial  — Cambridge UK",                                  "Cambridge, United Kingdom",  _METAB,   75, "UK"),
        _eu("Spatial/Omics  — Cambridge UK",                                  "Cambridge, United Kingdom",  _SPATIAL, 75, "UK"),

        _eu("Cardio/Preclin — London UK (UCL/King's/Imperial/Wellcome)",      "London, United Kingdom",     _CARDIO, 100, "UK"),
        _eu("Metab/Spatial  — London UK",                                     "London, United Kingdom",     _METAB,   75, "UK"),
        _eu("Spatial/Omics  — London UK",                                     "London, United Kingdom",     _SPATIAL, 75, "UK"),

        _eu("Cardio/Preclin — Oxford UK (RDM / Cardiovascular Dept)",         "Oxford, United Kingdom",     _CARDIO,  75, "UK"),
        _eu("Cardio/Preclin — Edinburgh UK (BHF Centre / MRC HGU)",           "Edinburgh, United Kingdom",  _CARDIO,  75, "UK"),
        _eu("Cardio/Preclin — Manchester UK",                                  "Manchester, United Kingdom", _CARDIO,  50, "UK"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Germany
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Berlin (MDC / Charité / BIH / DZHK)",          "Berlin, Germany",            _CARDIO, 100, "Germany"),
        _eu("Metab/Spatial  — Berlin",                                         "Berlin, Germany",            _METAB,   75, "Germany"),

        _eu("Cardio/Preclin — Munich (Helmholtz / TUM / LMU / MPI Ageing)",  "Munich, Germany",            _CARDIO, 100, "Germany"),
        _eu("Metab/Spatial  — Munich",                                         "Munich, Germany",            _METAB,   75, "Germany"),

        _eu("Cardio/Preclin — Heidelberg (EMBL / DKFZ / Heidelberg Uni)",    "Heidelberg, Germany",        _CARDIO,  75, "Germany"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Switzerland / Scandinavia / Netherlands / France / Ireland
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Basel/Zurich (ETH / Uni Zurich / FMI / EPFL)", "Basel, Switzerland",         _CARDIO, 100, "Switzerland"),
        _eu("Metab/Spatial  — Basel/Zurich",                                   "Basel, Switzerland",         _METAB,   75, "Switzerland"),

        _eu("Cardio/Preclin — Stockholm (Karolinska / Uppsala / SciLifeLab)", "Stockholm, Sweden",          _CARDIO, 100, "Sweden"),
        _eu("Metab/Spatial  — Stockholm",                                      "Stockholm, Sweden",          _METAB,   75, "Sweden"),

        _eu("Cardio/Preclin — Copenhagen (KU / NNF CBMR / reNEW)",           "Copenhagen, Denmark",        _CARDIO,  75, "Denmark"),
        _eu("Cardio/Preclin — Amsterdam NL (Amsterdam UMC / Hubrecht)",       "Amsterdam, Netherlands",     _CARDIO,  75, "Netherlands"),
        _eu("Cardio/Preclin — Paris (Pasteur / INSERM / Institut Cochin)",    "Paris, France",              _CARDIO,  75, "France"),
        _eu("Cardio/Preclin — Dublin IE (Trinity / RCSI / UCD)",              "Dublin, Ireland",            _CARDIO,  50, "Ireland"),

        # ═══════════════════════════════════════════════════════════════════
        # US — Northeast
        # ═══════════════════════════════════════════════════════════════════
        _us("Cardio/Preclin — Boston MA (Harvard/MGH/BWH/Tufts)",             "Boston, MA",                 _CARDIO, 100),
        _us("Metab/Spatial  — Boston MA",                                      "Boston, MA",                 _METAB,   75),
        _us("Spatial/Omics  — Boston MA",                                      "Boston, MA",                 _SPATIAL, 75),

        _us("Cardio/Preclin — New York NY (Columbia/Rockefeller/Sinai/NYU)",  "New York, NY",               _CARDIO, 100),
        _us("Metab/Spatial  — New York NY",                                    "New York, NY",               _METAB,   75),

        _us("Cardio/Preclin — Baltimore MD (Johns Hopkins / NIH / NHLBI)",    "Baltimore, MD",              _CARDIO, 100),
        _us("Metab/Spatial  — Baltimore MD",                                   "Baltimore, MD",              _METAB,   75),

        _us("Cardio/Preclin — Philadelphia PA (Penn / Wistar / Temple)",      "Philadelphia, PA",           _CARDIO,  75),

        # ═══════════════════════════════════════════════════════════════════
        # US — West Coast
        # ═══════════════════════════════════════════════════════════════════
        _us("Cardio/Preclin — San Francisco CA (UCSF / Gladstone / Stanford)","San Francisco, CA",          _CARDIO, 100),
        _us("Metab/Spatial  — San Francisco CA",                               "San Francisco, CA",          _METAB,   75),

        _us("Cardio/Preclin — San Diego CA (Salk / Scripps / UCSD)",          "San Diego, CA",              _CARDIO, 100),
        _us("Cardio/Preclin — Los Angeles CA (Cedars-Sinai / UCLA / USC)",    "Los Angeles, CA",            _CARDIO,  75),
        _us("Cardio/Preclin — Seattle WA (UW / Fred Hutch / Allen Institute)","Seattle, WA",                _CARDIO,  75),

        # ═══════════════════════════════════════════════════════════════════
        # US — South / Midwest
        # ═══════════════════════════════════════════════════════════════════
        _us("Cardio/Preclin — Houston TX (Baylor / Texas Heart / UTHealth)",  "Houston, TX",                _CARDIO,  75),
        _us("Cardio/Preclin — Chicago IL (Northwestern / UChicago / Rush)",   "Chicago, IL",                _CARDIO,  75),
    ]


# ---------------------------------------------------------------------------
# Main hunt
# ---------------------------------------------------------------------------
def pooja_hunt():
    sprint(f"\n{'='*60}")
    sprint(f"[Pooja Scanner] Academic Job Hunt (Universities & Research Institutes) — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    sprint(f"{'='*60}")
    alerts_sent = 0
    configs     = build_search_configs()

    # ── Step 1: Parallel scrape ────────────────────────────────────────────
    sprint(f"\n[Scrape] Launching {len(configs)} passes with {SCRAPE_WORKERS} parallel workers...")
    all_frames   = []
    blocked_passes = 0

    with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as ex:
        futures = {ex.submit(scrape_with_retry, cfg): cfg for cfg in configs}
        for fut in as_completed(futures):
            cfg = futures[fut]
            sprint(f"[Search] {cfg['label']}...")
            try:
                df = fut.result()
                if not df.empty:
                    all_frames.append(df)
                    sprint(f"  → {len(df)} results")
                else:
                    blocked_passes += 1
                    sprint(f"  → 0 results")
            except Exception as e:
                sprint(f"  [Error] {cfg['label']}: {e}")
                blocked_passes += 1

    if blocked_passes > 0:
        sprint(f"\n[Warning] {blocked_passes}/{len(configs)} passes returned 0 results.")
        if blocked_passes == len(configs):
            sprint("[Pooja Scanner] No results from any search pass. Likely IP block by Indeed.")
            push_notification("Pooja Academic Scan", "Scan ran — 0 results from all passes (possible IP block).", "low")
            return

    # ── Step 2: Combine + deduplicate ─────────────────────────────────────
    raw = pd.concat(all_frames, ignore_index=True)
    sprint(f"\n[Dedup] {len(raw)} total raw → ", end="")

    if "job_url" in raw.columns:
        def _norm_url(u: str) -> str:
            u = str(u).strip().lower()
            # Indeed job IDs live in the query string (?jk=...) — keep them.
            # For all other sites the ID is in the path, so stripping is safe.
            if "indeed.com" in u:
                return u
            return u.split("?")[0]

        raw["_url_norm"] = raw["job_url"].apply(_norm_url)
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

    # ── Step 3b: Academic institution filter (ALL regions) ───────────────
    # We want ONLY academic/research/government institutions worldwide.
    all_academic  = filtered["company"].fillna("").apply(is_academic_institution)
    # For unrecognised companies, check description for institutional-posting signals.
    # IMPORTANT: use specific phrases that only appear in institutional job ads,
    # NOT generic phrases like "university" or "research institute" that appear
    # in candidate requirements ("PhD from a university") in industry postings.
    _acad_desc_re = re.compile(
        r"postdoctoral\s+(?:fellow|researcher|associate|position|opening|opportunity)"
        r"|postdoc\s+(?:position|opening|fellow|fellowship)"
        r"|we\s+(?:are\s+)?(?:a\s+)?(?:university|research\s+institute|medical\s+school|research\s+center|research\s+centre)"
        r"|our\s+(?:university|research\s+institute|medical\s+school|research\s+group|lab|laboratory)"
        r"|joining\s+(?:our\s+)?(?:university|research\s+institute|department\s+of)"
        r"|tenure.track\s+(?:position|faculty|appointment)"
        r"|faculty\s+(?:position|opening|appointment|search)"
        r"|junior\s+group\s+leader\s+position"
        r"|independent\s+research\s+group"
        r"|nih\s+funded|nhlbi\s+funded|wellcome\s+trust\s+funded|mrc\s+funded"
        r"|bhf\s+(?:funded|fellowship|grant)|embo\s+(?:fellow|postdoc)"
        r"|marie\s+curie\s+(?:fellow|fellowship|postdoc)"
        r"|k99|r00|f32\s+fellowship",
        re.IGNORECASE,
    )
    desc_academic = filtered["description"].fillna("").apply(lambda d: bool(_acad_desc_re.search(d)))

    academic_mask = all_academic | desc_academic
    non_academic  = filtered[~academic_mask]
    filtered      = filtered[academic_mask].copy()

    sprint(f"[Acad Filter] {len(academic_mask)} → {len(filtered)} kept "
           f"({len(non_academic)} non-academic/industry removed)")

    if filtered.empty:
        push_notification("Pooja Academic Hunt", "Scan done — 0 academic roles found.", "low")
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
        push_notification("Pooja Academic Scan", "Scan done — no jobs cleared score threshold.", "low")
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
        title="Pooja Academic Scan Complete",
        message=(
            f"{len(new_df)} new leads  |  {strong} strong (80+)  |  {high} high (70+)\n"
            f"{alerts_sent} alerts  |  {len(combined)} total leads"
        ),
        priority="low",
    )


if __name__ == "__main__":
    pooja_hunt()
