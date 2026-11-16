"""
pooja_hunter.py — Pooja Choubey's Academic Job Scanner (Universities & Research Institutes)
Ph.D. Molecular Genetics | Cardiovascular Biology | Liver/MASH | Organoids | Cell Culture | Spatial Transcriptomics
STRICTLY ISOLATED from DJ's audit scanner.
"""

import json
import os
import re
import sys
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
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
HOURS_OLD      = 360   # 15 days
MIN_SAVE_SCORE = 35
MAX_ALERTS     = 15    # raised for wider coverage
SCORE_LLM_TOP  = 80    # top N by title relevance → LLM batch
BATCH_SIZE     = 15
SCRAPE_WORKERS = 8
MAX_RESULTS    = 100
MAX_RETRIES    = 1
RETRY_DELAY    = 10

# --- MUST-APPLY CONFIG ---
MUST_APPLY_SCORE = 78   # score threshold → auto-flag as must-apply

_ELITE_INSTITUTIONS = [
    "karolinska", "embl", "max planck", "max-planck", "helmholtz",
    "nih", "nhlbi", "niddk", "nih.gov", "nci",
    "harvard", "mit", "stanford", "caltech", "yale", "columbia",
    "johns hopkins", "ucsf", "ucsd", "ucla", "penn", "duke",
    "broad institute", "salk", "scripps",
    "crick", "babraham", "mrc", "wellcome", "bhf",
    "dkfz", "charité", "charite", "eth zurich", "epfl",
    "pasteur", "inserm", "cnrs", "weizmann",
    "gladstone", "dana-farber", "mayo clinic", "vanderbilt",
    "hubrecht", "lumc", "ku leuven", "vib", "dtu",
    "scilife", "novo nordisk foundation",
]

# Perfect-niche keywords → must-apply even at score 70+
_MUST_APPLY_CORE_KW = [
    "peripartum cardiomyopathy", "cardiomyopathy", "heart failure",
    "mash", "nash", "steatohepatitis", "nafld",
    "spatial transcriptomics", "xenium", "visium",
    "ptrh2", "hepatic senescence", "cellular senescence",
    "langendorff", "cardiac organoid", "liver organoid",
    "in vivo cardiovascular", "cardiovascular mouse",
    "cardiac fibrosis", "myocardial infarction model",
]

# Fellowship/grant types → flag regardless of score (high career value)
_FELLOWSHIP_KW = [
    "k99", "r00", "f32", "embo fellow", "marie curie", "msca",
    "wellcome trust fellow", "bhf fellow", "hfsp fellow",
    "humboldt fellow", "junior group leader", "independent fellow",
    "group leader position", "tenure track", "tenure-track",
    "starting group", "independent research group",
]


# ---------------------------------------------------------------------------
# Title whitelist — research science and academic roles
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
    # Investigator equivalents
    r"\bresearch\s+investigator\b",
    r"\bsenior\s+(?:research\s+)?investigator\b",
    r"\bprincipal\s+(?:research\s+)?investigator\b(?!\s+\(pi\))",
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
    r"\bbiologist\b.*\b(?:cardiovascular|cardiac|preclinical|in\s+vivo|translational|hepatic|liver)\b",
    r"\b(?:cardiovascular|cardiac|preclinical|in\s+vivo|translational|hepatic|liver)\b.*\bbiologist\b",
    # Liver / hepatic / metabolic
    r"\bliver\s+(?:biologist|research|researcher|scientist)\b",
    r"\bhepatolog(?:ist|y\s+researcher)\b",
    r"\bhepatic\s+(?:biologist|research|researcher)\b",
    r"\bmetabolic\s+(?:disease\s+)?(?:biologist|researcher|scientist)\b",
    r"\bmash\s+(?:research|researcher|scientist)\b",
    r"\bnash\s+(?:research|researcher|scientist)\b",
    r"\bsteatohepatitis\s+(?:research|researcher)\b",
    # Organoid / iPSC / 3D models
    r"\borganoid\s+(?:scientist|researcher|biologist)\b",
    r"\bipsc\s+(?:scientist|researcher|biologist)\b",
    r"\binduced\s+pluripotent\s+(?:stem\s+cell)?\s*(?:scientist|researcher)?\b",
    r"\bstem\s+cell\s+(?:biologist|researcher|scientist)\b",
    r"\borgans?\s+on\s+chip\b",
    r"\bmicrophysiological\s+(?:systems?|model)\b",
    r"\b3d\s+(?:cell\s+)?(?:culture|model|biology)\s+(?:scientist|researcher)?\b",
    r"\bcardiac\s+organoid\b",
    r"\bliver\s+organoid\b",
    r"\bcardiomyocyte\s+(?:differentiation|biology|researcher)\b",
    r"\bipsc-derived\b",
    # Postdoctoral positions
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
    r"\bdata\s+scientist\b",
    r"\bbioinformatician\b",
    r"\bbioinformatics\s+(?:scientist|analyst|engineer|developer|specialist)\b",
    r"\bcomputational\s+biolog(?:ist|y)\b",
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
    r"\bfield\s+application\s+scientist\b",
    r"\bclinical\s+development\s+scientist\b",
    r"\bquality\s+control\s+scientist\b",
    r"\bmanufacturing\s+process\s+scientist\b",
    r"\bmaterials?\s+(?:scientist|science|engineer|engineering)\b",
    r"\bphysicist\b",
    r"\bphysics\s+(?:research|researcher|scientist)\b",
    r"\benvironmental\s+scientist\b",
    r"\bgeolog(?:y|ist)\b",
    r"\becolog(?:y|ist)\b",
    r"\bstatistician\b",
    r"\bchemist\b",
    r"\bchemical\s+engineer",
    r"\bepidemiolog(?:y|ist)\b",
    r"\bagricult(?:ure|ural)\s+scientist\b",
    r"\bfood\s+scientist\b",
    r"\bveterinar(?:y\s+scientist|ian)\b",
    # NOTE: postdoc intentionally NOT blacklisted
]

# ---------------------------------------------------------------------------
# J1-eligible US employer filter
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
    return bool(_US_J1_ELIGIBLE_RE.search(company))


_ACADEMIC_INST_RE = re.compile(
    r"universit|college\b|medical\s+cent(?:er|re)|medical\s+school|school\s+of\s+medicine"
    r"|health\s+(system|network|sciences|cent(?:er|re))|hospital\b"
    r"|\binstitut(?:e|o|um)\b|\binstituts?\b"
    r"|\bumc\b|\bamc\b|\buclh\b|\bnhs\b"
    r"|research\s+cent(?:er|re)|research\s+institute|research\s+foundation"
    r"|national\s+(institute|laboratory|lab\b)"
    r"|\bnih\b|\bnci\b|\bnhlbi\b|\bniddk\b|\bnimh\b|\bcdc\b|\bfda\b|\busda\b"
    r"|karolinska|helmholtz|max.planck|max-planck|leibniz"
    r"|\bcnrs\b|\binserm\b|pasteur\b|\bcnio\b|\bcnic\b"
    r"|weizmann|\bembl\b|\bdkfz\b|\bvib\b|\bhubrecht\b"
    r"|charité|charite|\bdzhk\b|\bbih\b|\bmdc\b"
    r"|\bimba\b|\bimp\s+vienna\b|\bczb\b"
    r"|\bfmi\b|\bepfl\b|\beth\s+zurich\b|\bsnf\b"
    r"|scilife|\bki\b.*karolinska"
    r"|\bnnf\b|\bdtu\b|novo\s+nordisk\s+foundation"
    r"|veterans\s+affairs|\bvamc\b"
    r"|children'?s\b|pediatric\b|academic\s+medical"
    r"|\bhutchinson\b|\bhutch\b|dana.farber|\bscripps\b|\bsalk\b"
    r"|broad\s+institute|\bwistar\b|mayo\s+clinic|cleveland\s+clinic"
    r"|johns\s+hopkins|moffitt|jackson\s+lab|jax\b|chop\b"
    r"|\blundquist\b|harbor.ucla|cedars.sinai|mount\s+sinai"
    r"|stanford|ucsf\b|ucsd\b|ucla\b"
    r"|mit\b|caltech\b|carnegie\s+mellon|emory|vanderbilt|duke\s+university"
    r"|gladstone|garvan|wehi\b|monash|baker\s+(heart|institute)"
    r"|wellcome\s+trust|crick\b|babraham|sanger\b|\bmrc\b"
    r"|hubrecht|\blumc\b|\bumcu\b|\bradboud\b|\bgroninge\b",
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
# Title relevance pre-ranker
# ---------------------------------------------------------------------------
_TITLE_HIGH = [
    "cardiovascular", "preclinical", "in vivo", "translational",
    "biomarker", "drug discovery", "pharmacolog", "cardiac",
    "cardiomyopathy", "heart failure", "fibrosis", "disease model",
    "mouse model", "transgenic",
    # New high-relevance terms
    "mash", "nash", "steatohepatitis", "liver disease", "hepatic",
    "organoid", "cardiac organoid", "liver organoid", "ipsc",
    "cardiomyocyte", "langendorff", "senescence",
]
_TITLE_MED = [
    "research scientist", "senior scientist", "staff scientist",
    "principal scientist", "scientist ii", "scientist iii", "scientist 2",
    "scientist 3", "research investigator", "senior investigator",
    # New medium terms
    "cell biology", "molecular biology", "spatial transcriptomics",
    "hepatologist", "liver biologist", "stem cell",
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
# Keyword scorer — tuned for Pooja's full profile
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
        # New title cores
        "liver scientist":                   15,
        "liver biologist":                   15,
        "hepatic scientist":                 14,
        "hepatologist":                      13,
        "mash scientist":                    17,
        "nash scientist":                    17,
        "metabolic disease scientist":       15,
        "organoid scientist":                14,
        "organoid biologist":                14,
        "ipsc scientist":                    13,
        "stem cell biologist":               12,
        "cardiomyocyte biologist":           16,
        "cardiac organoid":                  17,
        "liver organoid":                    17,
        "senescence scientist":              14,
        "cell biology scientist":            11,
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
        # New good terms
        "liver": 7, "hepatic": 7, "mash": 9, "nash": 9,
        "steatohepatitis": 9, "organoid": 8, "ipsc": 7,
        "stem cell": 6, "senescence": 7, "hepatocyte": 8,
        "metabolic": 6, "obesity": 5,
    }
    title_pts  = sum(v for k, v in title_core.items() if k in t)
    title_pts += sum(v for k, v in title_good.items() if k in t)
    score += min(title_pts, 32)

    # --- Description keyword signals ---
    desc_kw = {
        # === CARDIOVASCULAR / CARDIAC ===
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
        "arrhythmia":               7, "atrial fibrillation":   7,
        "ventricular":              6, "aortic stenosis":       7,
        "dilated cardiomyopathy":  10, "hypertrophic cardiomyopathy": 10,
        "right ventricular":        7, "pulmonary hypertension": 7,

        # === PRECLINICAL / IN VIVO ===
        "preclinical":              9, "in vivo":               8,
        "mouse model":              9, "transgenic":            8,
        "knockout":                 7, "mouse colony":          8,
        "animal model":             7, "disease model":         8,
        "genotyping":               6, "phenotyping":           7,
        "subcutaneous":             5, "tissue harvest":        6,
        "organ harvest":            6, "survival surgery":      8,
        "osmotic pump":             7, "isoproterenol":         8,
        "aortic banding":           9, "echocardiographic":     8,

        # === LIVER / HEPATIC / MASH / NASH ===
        "mash":                     10, "nash":                 10,
        "steatohepatitis":           10, "steatosis":            8,
        "fatty liver":               8, "nafld":                 9,
        "hepatic":                   8, "liver disease":         8,
        "liver fibrosis":            9, "liver biology":         8,
        "hepatocyte":                9, "primary hepatocyte":   10,
        "hepatocyte isolation":     10, "kupffer cell":          8,
        "hepatic stellate":          9, "cholangiocyte":         7,
        "biliary":                   6, "bile duct":             6,
        "hepatic senescence":       10, "liver organoid":       10,
        "hepatic organoid":         10, "liver on chip":         9,
        "asgr":                      7, "hcc":                   7,
        "liver cancer":              6, "hepatocellular":        7,
        "metabolic dysfunction":     8, "metabolic disease":     8,
        "metabolic syndrome":        7, "obesity model":         8,
        "diet-induced":              8, "high-fat diet":         8,
        "hfd":                       7, "insulin resistance":    7,
        "glucose tolerance":         7, "hyperglycemia":         6,
        "adipose":                   6, "adipogenesis":          6,

        # === ORGANOIDS / iPSC / 3D MODELS ===
        "organoid":                 10, "cardiac organoid":     11,
        "heart organoid":           11, "liver organoid":       11,
        "gut organoid":              8, "intestinal organoid":   8,
        "ipsc":                     10, "ipsc-cm":              11,
        "ipsc-derived":             10, "induced pluripotent":  10,
        "cardiomyocyte differentiation": 10, "ipsc-cardiomyocyte": 11,
        "stem cell differentiation": 9, "pluripotent stem cell": 9,
        "organ-on-chip":            10, "microphysiological":   10,
        "engineered heart tissue":   9, "3d cardiac":            9,
        "3d liver":                  9, "3d cell model":         8,
        "gastruloid":                7, "blastoid":              7,
        "assembloid":                8,

        # === CELL CULTURE / PRIMARY CELLS ===
        "primary cell culture":      8, "primary cardiomyocyte": 9,
        "adult cardiomyocyte":       9, "neonatal cardiomyocyte": 8,
        "cardiac fibroblast":        8, "myofibroblast":          7,
        "fibroblast":                6, "myoblast":               6,
        "patient-derived cell":      8, "patient-derived":        7,
        "cell culture":              5, "in vitro model":         7,
        "cell line":                 4, "cell-based assay":       6,
        "phenotypic screen":         7,

        # === ASSAYS (Pooja's expertise) ===
        "facs":                      7, "flow cytometry":        7,
        "western blot":              6, "western blotting":      6,
        "elisa":                     6, "ihc":                   6,
        "immunohistochemistry":      6, "icc":                   5,
        "immunocytochemistry":       5, "qrt-pcr":               6,
        "qpcr":                      5, "tunel":                 7,
        "hydroxyproline":            7, "cell culture":          4,
        "confocal":                  5, "immunofluorescence":    6,
        "masson trichrome":          7, "sirius red":            7,
        "sa-β-gal":                  8, "sa-bgal":               8,
        "senescence assay":          8, "senolytic":             8,
        "p16":                       6, "p21":                   6,
        "sasp":                      8,

        # === OMICS / TRANSCRIPTOMICS ===
        "rna-seq":                   7, "rnaseq":                7,
        "scrna-seq":                 7, "single cell rna":       8,
        "spatial transcriptomics":   9, "xenium":                9,
        "visium":                    9, "seurat":                6,
        "bulk rna":                  6, "transcriptomics":       7,
        "ipa":                       6, "ingenuity":             6,
        "bioinformatics":            4, "pathway analysis":      5,
        "gene ontology":             5, "gsea":                  5,
        "string":                    4, "graphpad":              4,
        "deseq2":                    7, "scanpy":                6,
        "ggplot2":                   4, "loupe browser":         6,
        "xenium explorer":           7, "clc genomics":          5,
        "metabolomics":              7, "lipidomics":            6,
        "proteomics":                6,

        # === TRANSLATIONAL / BIOMARKER ===
        "translational":             8, "biomarker":             8,
        "biomarker discovery":       9, "therapeutic target":    9,
        "target validation":         9, "target identification": 8,
        "drug discovery":            8, "pharmacology":          7,
        "pharmacokinetics":          6, "mechanism of action":   7,
        "proof of concept":          6, "efficacy model":        8,

        # === MOLECULAR / GENETICS ===
        "molecular genetics":        7, "molecular biology":     6,
        "gene expression":           5, "protein expression":    5,
        "signaling pathway":         6, "western":               5,
        "ptrh2":                    10, "ptrh":                   8,
        "heme oxygenase":            7, "hmox1":                  8,

        # === SENESCENCE / AGING ===
        "senescence":                8, "cellular senescence":   9,
        "p16":                       5, "p21":                   5,
        "sasp":                      7, "senolytic":             7,
        "senolytics":                7, "aging":                 5,
        "age-related":               5, "healthspan":            5,
        "lifespan":                  4, "geroscience":           7,

        # === ACADEMIC INSTITUTION SIGNALS ===
        "university":                8, "universität":               8,
        "research institute":        9, "research center":           9,
        "research centre":           9, "medical school":            8,
        "medical center":            8, "medical centre":            8,
        "research hospital":         7, "academic medical":          8,
        "postdoctoral fellowship":   9, "postdoc fellowship":        9,
        "fellowship position":       8, "research fellowship":       8,
        "group leader":              9, "junior group leader":       9,
        "independent group":         8, "starting group":            8,
        "tenure track":              9, "tenure-track":              9,
        "assistant professor":       9, "associate professor":       8,
        "lecturer position":         8, "research lecturer":         7,
        "k99":                       8, "r00":                       7,
        "k award":                   7, "nih grant":                 6,
        "grant funding":             6, "extramural":                6,
        "phd students":              7, "graduate students":         7,
        "mentoring students":        6, "supervise students":        6,
        "embo":                      8, "hfsp":                      8,
        "marie curie":               8, "msca":                      8,
        "humboldt":                  7, "wellcome trust":            8,
        "bhf":                       7, "mrc funded":                7,

        # === PhD REQUIREMENT ===
        "ph.d. required":           10, "phd required":             10,
        "ph.d required":            10, "ph.d. preferred":           8,
        "phd preferred":             8, "doctorate required":        9,
        "doctoral degree":           8, "advanced degree":           5,

        # === VISA / RELOCATION ===
        "work permit":               6, "visa sponsorship":          7,
        "relocation":                5, "relocation package":        6,
    }
    desc_pts = sum(v for k, v in desc_kw.items() if k in d)
    score += min(desc_pts, 46)   # raised cap (more domains now)

    # --- Seniority fit ---
    if re.search(r"\bsenior\b|\bstaff\b|\bsr\.\b", t):
        score += 4
    if re.search(r"\bscientist\s+(ii|iii|iv|2|3|4)\b", t):
        score += 3
    if re.search(r"\bjunior\b|\bentry\s+level\b|\bscientist\s+i\b|\bscientist\s+1\b", t):
        score -= 8
    if re.search(r"\bpostdoc\b|\bpost-doc\b|\bpostdoctoral\b", t):
        score += 10

    # Hard bioinformatics-only penalty
    _WET_LAB_KW = ["mouse", "in vivo", "animal", "cell culture", "western blot",
                   "flow cytometry", "facs", "ihc", "elisa", "phenotyp", "echocard",
                   "langendorff", "qpcr", "qrt-pcr", "confocal", "immunofluorescence",
                   "hepatocyte", "organoid", "cardiomyocyte", "primary cell"]
    _BIOINF_HEAVY = ["bioinformatics", "pipeline", "workflow", "cloud", "aws", "hpc",
                     "kubernetes", "docker", "nextflow", "snakemake", "linux", "bash scripting"]
    has_wet = any(k in d for k in _WET_LAB_KW)
    has_bioinf = sum(1 for k in _BIOINF_HEAVY if k in d)
    if has_bioinf >= 3 and not has_wet:
        score -= 25

    # Hard seniority penalties
    if re.search(r"\bvp\b|\bvice\s+president\b", t):            score -= 30
    if re.search(r"\bchief\b|\bcso\b|\bcmo\b", t):              score -= 35
    if re.search(r"\bhead\s+of\b", t):                          score -= 20
    if re.search(r"\bdirector\b", t):                           score -= 15

    # Hard disqualifiers
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci", "clearance required"]):
        score -= 30

    # Industry / commercial penalty
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
# Must-Apply flag — no exceptions, Pooja applies to these
# ---------------------------------------------------------------------------
def is_must_apply(score: int, title: str, company: str, desc: str) -> bool:
    """Return True if Pooja must apply — zero exceptions."""
    if score >= MUST_APPLY_SCORE:
        return True

    t = title.lower()
    d = (title + " " + desc).lower()
    c = company.lower()

    # At elite institution + decent score → must apply
    at_elite = any(k in c for k in _ELITE_INSTITUTIONS)
    if score >= 65 and at_elite:
        return True

    # Perfect niche match → must apply even at score 65+
    core_match = any(k in d for k in _MUST_APPLY_CORE_KW)
    if score >= 65 and core_match:
        return True

    # Fellowship/grant positions → always must apply (high career value)
    fellowship_match = any(k in d for k in _FELLOWSHIP_KW)
    if score >= 50 and fellowship_match:
        return True

    return False


# ---------------------------------------------------------------------------
# Batch LLM scorer
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
        "EDUCATION: Ph.D. Molecular Genetics, University of Delhi (2023) | M.Sc. Zoology\n"
        "PUBLICATIONS: 10 peer-reviewed articles; co-first author Nature Communications 2026 "
        "(PTRH2/peripartum cardiomyopathy); first-author JACC: Advances 2026; "
        "first-author Cells 2026 (cardiac transcriptomics)\n"
        "GRANT: PI on competitive Postdoctoral Catalyst Grant (2026–27) on PTRH2-driven "
        "hepatic senescence in MASH — building towards K99/R00\n\n"

        "IN VIVO (10+ years): transgenic mouse colony 200+ mice (Hmox1, Ptrh2); "
        "cardiac phenotyping (Langendorff perfusion, echocardiography Vevo F2, ECG, "
        "adult cardiomyocyte isolation, contractility); disease models: peripartum cardiomyopathy, "
        "heart failure, MASH/NASH, obesity/diet-induced liver disease, embryo collection E12.5-18.5; "
        "senescence panels (SA-β-gal, TUNEL, flow cytometry)\n\n"

        "CELL CULTURE & ORGANOIDS: primary cardiomyocytes, cardiac fibroblasts, myoblasts, "
        "hepatocytes, patient-derived cells (IMNEPD); experience with 3D cell models; "
        "familiar with iPSC-derived cardiomyocyte and liver organoid approaches\n\n"

        "LIVER / MASH EXPERTISE: diet-induced MASH/NAFLD mouse models, hepatic senescence, "
        "liver histology (Sirius Red, Oil Red O), hepatocyte isolation, metabolic phenotyping\n\n"

        "ASSAYS: FACS/flow cytometry, Western blot, IHC/ICC, immunofluorescence, ELISA, "
        "qRT-PCR, confocal/Keyence imaging, histology (Masson trichrome, Sirius Red), "
        "SA-β-gal, TUNEL\n\n"

        "OMICS: bulk RNA-seq, scRNA-seq (Seurat, Scanpy), 10x Visium & Xenium spatial "
        "transcriptomics, R (DESeq2, ggplot2), Python (Scanpy, pandas), QIAGEN IPA, STRING\n\n"

        "TARGET ROLES (academic/research institutions ONLY):\n"
        "1. Postdoctoral Fellow/Researcher — wet-lab focused; cardiovascular, MASH/hepatic, "
        "liver biology, organoid/iPSC models, senescence/aging, spatial transcriptomics\n"
        "2. Research Scientist / Staff Scientist at university or non-profit research institute\n"
        "3. Junior Group Leader / Assistant Professor at appropriate career stage\n"
        "NOT SUITABLE FOR: pure computational/bioinformatics, clinical coordinator, "
        "regulatory, industry (pharma/biotech/CRO), VP/Director/Head roles\n"
        "CRITICAL WET-LAB REQUIREMENT: role must involve wet-lab bench work.\n\n"

        "SCORING RUBRIC (BE STRICT):\n"
        "90–100: Hands-on wet-lab cardiovascular/MASH/liver/organoid/senescence research at "
        "university or research institute; PhD required; strong overlap\n"
        "75–89: Strong wet-lab academic role, good domain overlap, adjacent areas "
        "(liver/metabolic, aging, reproductive biology, cell biology, iPSC)\n"
        "55–74: Academic postdoc/scientist, relevant techniques but partial domain match\n"
        "30–54: Academic but mostly computational, OR wrong field, OR missing PhD\n"
        "< 30: Industry, QA/QC, manufacturing, clinical, software, pure bioinformatics\n\n"
        "CRITICAL: Industry/pharma/biotech/CRO → score MAX 20.\n"
        "CRITICAL: Pure bioinformatics/computational, no bench work → score MAX 30.\n"
        "CRITICAL: Director, VP, Head of → score MAX 20.\n\n"

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
                      "max_tokens": 200,
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
# Robust scrape with retry + fallback
# ---------------------------------------------------------------------------
def scrape_with_retry(cfg: dict) -> pd.DataFrame:
    is_us = cfg.get("region", "US") == "US"
    base_kwargs = dict(
        search_term=cfg["term"],
        location=cfg["location"],
        results_wanted=min(cfg["results"], MAX_RESULTS),
        hours_old=HOURS_OLD,
    )
    if "country_indeed" in cfg:
        base_kwargs["country_indeed"] = cfg["country_indeed"]
    elif is_us:
        base_kwargs["country_indeed"] = "USA"
    if "distance" in cfg:
        base_kwargs["distance"] = cfg["distance"]
    if cfg.get("remote"):
        base_kwargs["is_remote"] = True

    sites = cfg.get("sites", ["indeed"])

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

    fallback_sites = cfg.get("fallback_sites", [])
    for site in fallback_sites:
        try:
            sprint(f"  → Trying fallback: {site}...")
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

    sprint(f"  → 0 results from all sources")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Custom scraper — jobs.ac.uk  (UK academic job board, RSS feed)
# ---------------------------------------------------------------------------
_JOBS_AC_UK_QUERIES = [
    # Cardiovascular / cardiac
    "postdoctoral cardiovascular cardiac",
    "postdoctoral in vivo mouse model preclinical",
    "postdoctoral spatial transcriptomics single cell",
    "postdoctoral MASH liver metabolic senescence",
    "research scientist cardiovascular cardiac in vivo",
    "research fellow cardiovascular cardiac molecular biology",
    "group leader cardiovascular junior",
    "assistant professor cardiovascular molecular biology",
    # Liver / hepatic / MASH
    "postdoctoral liver hepatic biology metabolic",
    "postdoctoral MASH NASH steatohepatitis liver",
    "postdoctoral hepatocyte liver disease metabolic",
    "research scientist liver hepatic disease",
    "postdoctoral metabolic disease obesity liver mouse",
    # Organoids / iPSC / cell culture
    "postdoctoral organoid cardiac heart iPSC",
    "postdoctoral organoid liver hepatic iPSC",
    "postdoctoral iPSC induced pluripotent stem cell cardiomyocyte",
    "postdoctoral cell culture primary cell cardiomyocyte",
    "research scientist organoid iPSC cardiac",
    # Senescence / aging
    "postdoctoral senescence aging cellular cardiovascular",
    "postdoctoral cellular senescence SASP aging biology",
    # Spatial transcriptomics
    "postdoctoral spatial transcriptomics Xenium Visium cardiovascular",
    "postdoctoral single cell RNA sequencing cardiovascular",
]

def scrape_jobs_ac_uk() -> pd.DataFrame:
    sprint("\n[jobs.ac.uk] Scraping UK academic job board...")
    rows = []
    seen_urls: set[str] = set()
    base = "https://www.jobs.ac.uk/search/?keywords={q}&sort=pd&orderby=d&format=rss"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JobScanner/1.0)"}

    for q in _JOBS_AC_UK_QUERIES:
        url = base.format(q=quote_plus(q))
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                sprint(f"  [jobs.ac.uk] HTTP {r.status_code} for '{q}'")
                continue
            root = ET.fromstring(r.content)
            jac_ns = "http://www.jobs.ac.uk/"
            count_before = len(rows)
            for item in root.findall(".//item"):
                link = (item.findtext("link") or "").strip()
                if not link or link in seen_urls:
                    continue
                seen_urls.add(link)
                raw_desc = item.findtext("description") or ""
                desc = re.sub(r"<[^>]+>", " ", raw_desc).strip()
                rows.append({
                    "title":        (item.findtext("title") or "").strip(),
                    "company":      (item.findtext(f"{{{jac_ns}}}institution") or "").strip(),
                    "location":     (item.findtext(f"{{{jac_ns}}}location") or "").strip(),
                    "job_url":      link,
                    "description":  desc,
                    "date_posted":  (item.findtext("pubDate") or "").strip(),
                    "job_type":     "fulltime",
                    "_search_pass": f"jobs.ac.uk — {q}",
                    "_region":      "Europe",
                })
            sprint(f"  [jobs.ac.uk] '{q}' → {len(rows) - count_before} new")
            time.sleep(2)
        except ET.ParseError as e:
            sprint(f"  [jobs.ac.uk] XML parse error for '{q}': {e}")
        except Exception as e:
            sprint(f"  [jobs.ac.uk] Error for '{q}': {e}")

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    sprint(f"  [jobs.ac.uk] Total: {len(df)} unique UK academic postings")
    return df


# ---------------------------------------------------------------------------
# Custom scraper — Nature Careers
# ---------------------------------------------------------------------------
_NATURE_QUERIES = [
    "postdoctoral cardiovascular cardiac",
    "postdoctoral in vivo mouse model preclinical cardiovascular",
    "postdoctoral spatial transcriptomics cardiovascular",
    "research scientist cardiovascular heart failure mouse",
    "postdoctoral MASH liver metabolic steatohepatitis",
    "postdoctoral senescence aging cardiovascular liver",
    "group leader cardiovascular molecular biology",
    "assistant professor cardiovascular biology",
    "postdoctoral organoid cardiac liver iPSC",
    "postdoctoral hepatocyte liver disease metabolic",
    "postdoctoral cell biology cardiovascular flow cytometry",
    "postdoctoral transcriptomics single cell cardiovascular",
]

def scrape_nature_careers() -> pd.DataFrame:
    sprint("\n[Nature Careers] Scraping nature.com/naturecareers...")
    rows = []
    seen_urls: set[str] = set()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; JobScanner/1.0)",
        "Accept": "application/json, text/html",
    }

    for q in _NATURE_QUERIES:
        url = (
            "https://www.nature.com/naturecareers/jobs/search.json"
            f"?q={quote_plus(q)}&sort=date&order=desc&per_page=25"
        )
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                rss_url = (
                    "https://www.nature.com/naturecareers/jobs/search/rss"
                    f"?q={quote_plus(q)}"
                )
                r = requests.get(rss_url, headers=headers, timeout=20)
                if r.status_code != 200:
                    sprint(f"  [Nature] HTTP {r.status_code} for '{q}'")
                    continue
                try:
                    root = ET.fromstring(r.content)
                    for item in root.findall(".//item"):
                        link = (item.findtext("link") or "").strip()
                        if not link or link in seen_urls:
                            continue
                        seen_urls.add(link)
                        desc = re.sub(r"<[^>]+>", " ", item.findtext("description") or "").strip()
                        rows.append({
                            "title":        (item.findtext("title") or "").strip(),
                            "company":      "",
                            "location":     "",
                            "job_url":      link,
                            "description":  desc,
                            "date_posted":  (item.findtext("pubDate") or "").strip(),
                            "job_type":     "fulltime",
                            "_search_pass": f"Nature Careers — {q}",
                            "_region":      "Europe",
                        })
                except ET.ParseError:
                    pass
                time.sleep(2)
                continue

            data = r.json()
            jobs = data if isinstance(data, list) else data.get("jobs", data.get("results", []))
            count_before = len(rows)
            for job in jobs:
                link = job.get("url") or job.get("link") or job.get("apply_url", "")
                if not link or link in seen_urls:
                    continue
                seen_urls.add(link)
                rows.append({
                    "title":        job.get("title", "").strip(),
                    "company":      job.get("institution", job.get("company", "")).strip(),
                    "location":     job.get("location", "").strip(),
                    "job_url":      link,
                    "description":  job.get("description", job.get("body", "")).strip(),
                    "date_posted":  job.get("posted_date", job.get("date", "")).strip(),
                    "job_type":     "fulltime",
                    "_search_pass": f"Nature Careers — {q}",
                    "_region":      "Europe",
                })
            sprint(f"  [Nature] '{q}' → {len(rows) - count_before} new")

        except Exception as e:
            sprint(f"  [Nature Careers] Error for '{q}': {e}")
        time.sleep(2)

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    sprint(f"  [Nature Careers] Total: {len(df)} unique postings")
    return df


# ---------------------------------------------------------------------------
# Custom scraper — EurAxess  (European academic job portal, RSS)
# Covers ALL EU countries: Germany, France, Netherlands, Sweden, Switzerland,
# Belgium, Austria, Italy, Spain, Denmark, Norway, Finland, etc.
# ---------------------------------------------------------------------------
_EURAXESS_QUERIES = [
    "postdoctoral cardiovascular cardiac molecular biology",
    "postdoctoral in vivo mouse model preclinical cardiovascular",
    "postdoctoral spatial transcriptomics cardiovascular cardiac",
    "postdoctoral MASH liver metabolic steatohepatitis",
    "postdoctoral senescence aging cardiovascular heart",
    "postdoctoral organoid cardiac liver iPSC induced pluripotent",
    "postdoctoral hepatocyte liver disease metabolic",
    "postdoctoral cell biology cardiovascular flow cytometry",
    "research scientist cardiovascular heart failure mouse",
    "junior group leader cardiovascular molecular biology",
    "group leader cardiovascular molecular biology independent",
    "postdoctoral transcriptomics single cell cardiovascular",
    "postdoctoral cardiomyopathy heart failure molecular",
    "postdoctoral cardiac fibrosis senescence liver",
    "researcher cardiovascular biology in vivo mouse",
    "postdoctoral NASH NAFLD liver fibrosis hepatic",
]

def scrape_euraxess() -> pd.DataFrame:
    """Scrape EurAxess RSS for EU academic research positions."""
    sprint("\n[EurAxess] Scraping euraxess.ec.europa.eu...")
    rows = []
    seen_urls: set[str] = set()
    base = "https://euraxess.ec.europa.eu/jobs/search/rss?keywords={q}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JobScanner/1.0)",
               "Accept": "application/rss+xml, application/xml, text/xml"}

    for q in _EURAXESS_QUERIES:
        url = base.format(q=quote_plus(q))
        try:
            r = requests.get(url, headers=headers, timeout=25)
            if r.status_code != 200:
                sprint(f"  [EurAxess] HTTP {r.status_code} for '{q}'")
                time.sleep(2)
                continue
            # EurAxess sometimes returns HTML on error — check
            if b"<rss" not in r.content[:200] and b"<?xml" not in r.content[:200]:
                sprint(f"  [EurAxess] Non-XML response for '{q}' (possibly CAPTCHA)")
                time.sleep(5)
                continue
            root = ET.fromstring(r.content)
            dc_ns = "http://purl.org/dc/elements/1.1/"
            count_before = len(rows)
            for item in root.findall(".//item"):
                link = (item.findtext("link") or "").strip()
                if not link or link in seen_urls:
                    continue
                seen_urls.add(link)
                raw_desc = item.findtext("description") or ""
                desc = re.sub(r"<[^>]+>", " ", raw_desc).strip()
                institution = item.findtext(f"{{{dc_ns}}}publisher") or \
                              item.findtext(f"{{{dc_ns}}}creator") or ""
                location    = item.findtext(f"{{{dc_ns}}}coverage") or \
                              item.findtext(f"{{{dc_ns}}}spatial") or ""
                rows.append({
                    "title":        (item.findtext("title") or "").strip(),
                    "company":      institution.strip(),
                    "location":     location.strip(),
                    "job_url":      link,
                    "description":  desc,
                    "date_posted":  (item.findtext("pubDate") or "").strip(),
                    "job_type":     "fulltime",
                    "_search_pass": f"EurAxess — {q}",
                    "_region":      "Europe",
                })
            sprint(f"  [EurAxess] '{q}' → {len(rows) - count_before} new")
            time.sleep(2)
        except ET.ParseError as e:
            sprint(f"  [EurAxess] XML parse error for '{q}': {e}")
        except Exception as e:
            sprint(f"  [EurAxess] Error for '{q}': {e}")

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    sprint(f"  [EurAxess] Total: {len(df)} unique EU academic postings")
    return df


# ---------------------------------------------------------------------------
# Custom scraper — HigherEdJobs RSS  (US academic, life sciences)
# Covers US universities, medical schools, research institutes.
# ---------------------------------------------------------------------------
_HIGHERED_RSS_URLS = [
    # Category 145 = Biology/Life Sciences
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=cardiovascular+research",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=cardiac+biology+mouse",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=MASH+liver+metabolic",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=spatial+transcriptomics",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=in+vivo+cardiovascular+mouse",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=organoid+cardiac+iPSC",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=liver+organoid+hepatocyte",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=senescence+aging+cardiovascular",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=145&Keyword=cardiomyocyte+flow+cytometry",
    # Category 148 = Medical/Health Sciences
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=148&Keyword=cardiovascular+research+postdoc",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=148&Keyword=liver+metabolic+disease+research",
    "https://www.higheredjobs.com/rss/jobfeed.cfm?JobCat=148&Keyword=cardiac+mouse+model+preclinical",
]

def scrape_higheredjobs_rss() -> pd.DataFrame:
    """Scrape HigherEdJobs RSS for US academic life science positions."""
    sprint("\n[HigherEdJobs] Scraping higheredjobs.com RSS...")
    rows = []
    seen_urls: set[str] = set()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JobScanner/1.0)"}

    for url in _HIGHERED_RSS_URLS:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                sprint(f"  [HigherEd] HTTP {r.status_code} for {url[-60:]}")
                time.sleep(2)
                continue
            root = ET.fromstring(r.content)
            count_before = len(rows)
            for item in root.findall(".//item"):
                link = (item.findtext("link") or "").strip()
                if not link or link in seen_urls:
                    continue
                seen_urls.add(link)
                raw_desc = item.findtext("description") or ""
                desc = re.sub(r"<[^>]+>", " ", raw_desc).strip()
                # HigherEdJobs encodes institution in title sometimes; extract if possible
                raw_title = (item.findtext("title") or "").strip()
                rows.append({
                    "title":        raw_title,
                    "company":      "",   # not in RSS, will be pulled from desc later
                    "location":     "",
                    "job_url":      link,
                    "description":  desc,
                    "date_posted":  (item.findtext("pubDate") or "").strip(),
                    "job_type":     "fulltime",
                    "_search_pass": f"HigherEdJobs — {url.split('Keyword=')[-1][:40]}",
                    "_region":      "US",
                })
            sprint(f"  [HigherEdJobs] {url.split('Keyword=')[-1][:40]} → {len(rows) - count_before} new")
            time.sleep(1)
        except ET.ParseError as e:
            sprint(f"  [HigherEdJobs] XML parse error: {e}")
        except Exception as e:
            sprint(f"  [HigherEdJobs] Error: {e}")

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    sprint(f"  [HigherEdJobs] Total: {len(df)} unique US academic postings")
    return df


# ---------------------------------------------------------------------------
# Search configuration — domain-specific passes across Europe + US
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:

    # === DOMAIN 1: Cardiovascular / Cardiac / Preclinical / In Vivo ===
    _CARDIO = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist" OR "Staff Scientist" OR '
        '"In Vivo Scientist" OR "Cardiovascular Scientist" OR '
        '"Assistant Professor" OR "Group Leader" OR "Junior Group Leader") '
        '(cardiovascular OR cardiac OR "heart failure" OR cardiomyopathy OR '
        '"in vivo" OR preclinical OR "mouse model" OR transgenic OR '
        '"disease model" OR "cardiac phenotyping" OR Langendorff OR '
        'echocardiography OR cardiomyocyte OR "cardiac fibrosis")'
    )

    # === DOMAIN 2: Liver / Hepatic / MASH / NASH ===
    _LIVER = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist" OR "Staff Scientist") '
        '(MASH OR NASH OR steatohepatitis OR "fatty liver" OR NAFLD OR '
        '"liver disease" OR "hepatic steatosis" OR hepatocyte OR '
        '"liver fibrosis" OR "liver biology" OR "hepatic biology" OR '
        '"liver organoid" OR "hepatic organoid" OR "primary hepatocyte" OR '
        '"metabolic liver" OR "liver cancer" OR HCC OR '
        '"metabolic dysfunction" OR "diet-induced" OR "high-fat diet")'
    )

    # === DOMAIN 3: Metabolic Disease / Senescence / Aging ===
    _METAB = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist" OR "Staff Scientist") '
        '(MASH OR NASH OR steatohepatitis OR "fatty liver" OR NAFLD OR '
        '"metabolic disease" OR senescence OR "cellular senescence" OR '
        '"liver disease" OR "hepatic steatosis" OR aging OR "metabolic dysfunction" OR '
        '"metabolic syndrome" OR obesity OR "adipose tissue" OR diabetes)'
    )

    # === DOMAIN 4: Organoids / iPSC / 3D Cell Models ===
    _ORGANOID = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist" OR "Staff Scientist") '
        '(organoid OR "cardiac organoid" OR "heart organoid" OR "liver organoid" OR '
        '"hepatic organoid" OR iPSC OR "induced pluripotent" OR '
        '"cardiomyocyte differentiation" OR "iPSC-derived cardiomyocyte" OR '
        '"organ-on-chip" OR "microphysiological" OR "3D cell model" OR '
        '"stem cell cardiomyocyte" OR "engineered heart tissue")'
    )

    # === DOMAIN 5: Cell Culture / Primary Cells / Cardiomyocytes ===
    _CELL_CULT = (
        '("Postdoctoral Fellow" OR "Postdoctoral Researcher" OR "Postdoc" OR '
        '"Research Fellow" OR "Research Scientist") '
        '(cardiomyocyte OR "primary cardiomyocyte" OR "cardiac fibroblast" OR '
        '"primary hepatocyte" OR "hepatocyte isolation" OR "cell culture" OR '
        '"patient-derived" OR "primary cell" OR "cell biology" OR '
        '"iPSC-CM" OR "neonatal cardiomyocyte")'
    )

    # === DOMAIN 6: Spatial Transcriptomics / scRNA-seq / Omics ===
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
        _eu("Cardio/Preclin — Cambridge UK",         "Cambridge, United Kingdom",  _CARDIO, 100, "UK"),
        _eu("Liver/MASH    — Cambridge UK",           "Cambridge, United Kingdom",  _LIVER,   75, "UK"),
        _eu("Metab/Senesc  — Cambridge UK",           "Cambridge, United Kingdom",  _METAB,   75, "UK"),
        _eu("Organoid/iPSC — Cambridge UK",           "Cambridge, United Kingdom",  _ORGANOID,75, "UK"),
        _eu("Spatial/Omics — Cambridge UK",           "Cambridge, United Kingdom",  _SPATIAL, 75, "UK"),

        _eu("Cardio/Preclin — London UK",             "London, United Kingdom",     _CARDIO, 100, "UK"),
        _eu("Liver/MASH    — London UK",              "London, United Kingdom",     _LIVER,   75, "UK"),
        _eu("Metab/Senesc  — London UK",              "London, United Kingdom",     _METAB,   75, "UK"),
        _eu("Organoid/iPSC — London UK",              "London, United Kingdom",     _ORGANOID,75, "UK"),
        _eu("Spatial/Omics — London UK",              "London, United Kingdom",     _SPATIAL, 75, "UK"),

        _eu("Cardio/Preclin — Oxford UK",             "Oxford, United Kingdom",     _CARDIO,  75, "UK"),
        _eu("Liver/MASH    — Oxford UK",              "Oxford, United Kingdom",     _LIVER,   50, "UK"),
        _eu("Cardio/Preclin — Edinburgh UK",          "Edinburgh, United Kingdom",  _CARDIO,  75, "UK"),
        _eu("Liver/MASH    — Edinburgh UK",           "Edinburgh, United Kingdom",  _LIVER,   50, "UK"),
        _eu("Cardio/Preclin — Manchester UK",         "Manchester, United Kingdom", _CARDIO,  75, "UK"),
        _eu("Cardio/Preclin — Glasgow UK",            "Glasgow, United Kingdom",    _CARDIO,  50, "UK"),
        _eu("Cardio/Preclin — Bristol UK",            "Bristol, United Kingdom",    _CARDIO,  50, "UK"),
        _eu("Cardio/Preclin — Birmingham UK",         "Birmingham, United Kingdom", _CARDIO,  50, "UK"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Germany
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Berlin",                "Berlin, Germany",            _CARDIO, 100, "Germany"),
        _eu("Liver/MASH    — Berlin",                 "Berlin, Germany",            _LIVER,   75, "Germany"),
        _eu("Metab/Senesc  — Berlin",                 "Berlin, Germany",            _METAB,   75, "Germany"),
        _eu("Organoid/iPSC — Berlin",                 "Berlin, Germany",            _ORGANOID,75, "Germany"),

        _eu("Cardio/Preclin — Munich",                "Munich, Germany",            _CARDIO, 100, "Germany"),
        _eu("Liver/MASH    — Munich",                 "Munich, Germany",            _LIVER,   75, "Germany"),
        _eu("Metab/Senesc  — Munich",                 "Munich, Germany",            _METAB,   75, "Germany"),
        _eu("Organoid/iPSC — Munich",                 "Munich, Germany",            _ORGANOID,75, "Germany"),

        _eu("Cardio/Preclin — Heidelberg",            "Heidelberg, Germany",        _CARDIO,  75, "Germany"),
        _eu("Liver/MASH    — Heidelberg",             "Heidelberg, Germany",        _LIVER,   50, "Germany"),
        _eu("Cardio/Preclin — Hamburg",               "Hamburg, Germany",           _CARDIO,  75, "Germany"),
        _eu("Liver/MASH    — Hamburg",                "Hamburg, Germany",           _LIVER,   50, "Germany"),
        _eu("Cardio/Preclin — Frankfurt",             "Frankfurt, Germany",         _CARDIO,  50, "Germany"),
        _eu("Cardio/Preclin — Cologne/Dusseldorf",    "Cologne, Germany",           _CARDIO,  50, "Germany"),
        _eu("Cardio/Preclin — Goettingen",            "Gottingen, Germany",         _CARDIO,  50, "Germany"),
        _eu("Liver/MASH    — Goettingen",             "Gottingen, Germany",         _LIVER,   50, "Germany"),
        _eu("Cardio/Preclin — Freiburg",              "Freiburg, Germany",          _CARDIO,  50, "Germany"),
        _eu("Cardio/Preclin — Dresden/Leipzig",       "Dresden, Germany",           _CARDIO,  50, "Germany"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Switzerland
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Basel/Zurich",          "Basel, Switzerland",         _CARDIO, 100, "Switzerland"),
        _eu("Liver/MASH    — Basel/Zurich",           "Basel, Switzerland",         _LIVER,   75, "Switzerland"),
        _eu("Metab/Senesc  — Basel/Zurich",           "Basel, Switzerland",         _METAB,   75, "Switzerland"),
        _eu("Organoid/iPSC — Basel/Zurich",           "Basel, Switzerland",         _ORGANOID,75, "Switzerland"),

        _eu("Cardio/Preclin — Geneva/Lausanne",       "Geneva, Switzerland",        _CARDIO,  75, "Switzerland"),
        _eu("Liver/MASH    — Geneva/Lausanne",        "Geneva, Switzerland",        _LIVER,   50, "Switzerland"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Scandinavia
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Stockholm",             "Stockholm, Sweden",          _CARDIO, 100, "Sweden"),
        _eu("Liver/MASH    — Stockholm",              "Stockholm, Sweden",          _LIVER,   75, "Sweden"),
        _eu("Metab/Senesc  — Stockholm",              "Stockholm, Sweden",          _METAB,   75, "Sweden"),
        _eu("Organoid/iPSC — Stockholm",              "Stockholm, Sweden",          _ORGANOID,50, "Sweden"),
        _eu("Cardio/Preclin — Gothenburg",            "Gothenburg, Sweden",         _CARDIO,  50, "Sweden"),

        _eu("Cardio/Preclin — Copenhagen",            "Copenhagen, Denmark",        _CARDIO,  75, "Denmark"),
        _eu("Liver/MASH    — Copenhagen",             "Copenhagen, Denmark",        _LIVER,   75, "Denmark"),
        _eu("Metab/Senesc  — Copenhagen",             "Copenhagen, Denmark",        _METAB,   75, "Denmark"),
        _eu("Cardio/Preclin — Aarhus",                "Aarhus, Denmark",            _CARDIO,  50, "Denmark"),
        _eu("Liver/MASH    — Aarhus",                 "Aarhus, Denmark",            _LIVER,   50, "Denmark"),

        _eu("Cardio/Preclin — Oslo",                  "Oslo, Norway",               _CARDIO,  75, "Norway"),
        _eu("Liver/MASH    — Oslo",                   "Oslo, Norway",               _LIVER,   50, "Norway"),
        _eu("Cardio/Preclin — Helsinki",              "Helsinki, Finland",          _CARDIO,  50, "Finland"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Netherlands / Belgium
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Amsterdam NL",          "Amsterdam, Netherlands",     _CARDIO,  75, "Netherlands"),
        _eu("Liver/MASH    — Amsterdam NL",           "Amsterdam, Netherlands",     _LIVER,   75, "Netherlands"),
        _eu("Organoid/iPSC — Amsterdam NL",           "Amsterdam, Netherlands",     _ORGANOID,75, "Netherlands"),
        _eu("Cardio/Preclin — Utrecht NL",            "Utrecht, Netherlands",       _CARDIO,  75, "Netherlands"),
        _eu("Liver/MASH    — Utrecht NL",             "Utrecht, Netherlands",       _LIVER,   50, "Netherlands"),
        _eu("Organoid/iPSC — Utrecht NL",             "Utrecht, Netherlands",       _ORGANOID,75, "Netherlands"),
        _eu("Cardio/Preclin — Leiden NL",             "Leiden, Netherlands",        _CARDIO,  50, "Netherlands"),
        _eu("Cardio/Preclin — Groningen NL",          "Groningen, Netherlands",     _CARDIO,  50, "Netherlands"),
        _eu("Liver/MASH    — Groningen NL",           "Groningen, Netherlands",     _LIVER,   50, "Netherlands"),
        _eu("Cardio/Preclin — Nijmegen NL",           "Nijmegen, Netherlands",      _CARDIO,  50, "Netherlands"),

        _eu("Cardio/Preclin — Leuven/Brussels",       "Leuven, Belgium",            _CARDIO,  75, "Belgium"),
        _eu("Liver/MASH    — Leuven/Brussels",        "Leuven, Belgium",            _LIVER,   75, "Belgium"),
        _eu("Organoid/iPSC — Leuven/Brussels",        "Leuven, Belgium",            _ORGANOID,50, "Belgium"),
        _eu("Cardio/Preclin — Ghent",                 "Ghent, Belgium",             _CARDIO,  50, "Belgium"),
        _eu("Liver/MASH    — Ghent",                  "Ghent, Belgium",             _LIVER,   50, "Belgium"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Austria
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Vienna",                "Vienna, Austria",            _CARDIO,  75, "Austria"),
        _eu("Liver/MASH    — Vienna",                 "Vienna, Austria",            _LIVER,   75, "Austria"),
        _eu("Metab/Senesc  — Vienna",                 "Vienna, Austria",            _METAB,   50, "Austria"),
        _eu("Organoid/iPSC — Vienna",                 "Vienna, Austria",            _ORGANOID,75, "Austria"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — France / Spain / Italy / Portugal / Ireland
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Paris",                 "Paris, France",              _CARDIO,  75, "France"),
        _eu("Liver/MASH    — Paris",                  "Paris, France",              _LIVER,   75, "France"),
        _eu("Cardio/Preclin — Lyon",                  "Lyon, France",               _CARDIO,  50, "France"),
        _eu("Liver/MASH    — Lyon",                   "Lyon, France",               _LIVER,   50, "France"),

        _eu("Cardio/Preclin — Madrid/Barcelona",      "Madrid, Spain",              _CARDIO,  75, "Spain"),
        _eu("Liver/MASH    — Madrid/Barcelona",       "Madrid, Spain",              _LIVER,   75, "Spain"),
        _eu("Cardio/Preclin — Barcelona",             "Barcelona, Spain",           _CARDIO,  50, "Spain"),
        _eu("Liver/MASH    — Barcelona",              "Barcelona, Spain",           _LIVER,   50, "Spain"),

        _eu("Cardio/Preclin — Milan",                 "Milan, Italy",               _CARDIO,  75, "Italy"),
        _eu("Liver/MASH    — Milan",                  "Milan, Italy",               _LIVER,   75, "Italy"),
        _eu("Cardio/Preclin — Rome",                  "Rome, Italy",                _CARDIO,  50, "Italy"),
        _eu("Cardio/Preclin — Bologna",               "Bologna, Italy",             _CARDIO,  50, "Italy"),

        _eu("Cardio/Preclin — Lisbon/Porto",          "Lisbon, Portugal",           _CARDIO,  50, "Portugal"),
        _eu("Liver/MASH    — Lisbon/Porto",           "Lisbon, Portugal",           _LIVER,   50, "Portugal"),

        _eu("Cardio/Preclin — Dublin",                "Dublin, Ireland",            _CARDIO,  50, "Ireland"),
        _eu("Cardio/Preclin — Luxembourg",            "Luxembourg City, Luxembourg", _CARDIO, 50, "Luxembourg"),
        _eu("Liver/MASH    — Luxembourg",             "Luxembourg City, Luxembourg", _LIVER,  50, "Luxembourg"),

        # ═══════════════════════════════════════════════════════════════════
        # EUROPE — Israel
        # ═══════════════════════════════════════════════════════════════════
        _eu("Cardio/Preclin — Israel",                "Tel Aviv, Israel",           _CARDIO,  50, "Israel"),
        _eu("Liver/MASH    — Israel",                 "Tel Aviv, Israel",           _LIVER,   50, "Israel"),

        # ═══════════════════════════════════════════════════════════════════
        # US — Northeast
        # ═══════════════════════════════════════════════════════════════════
        _us("Cardio/Preclin — Boston MA",             "Boston, MA",                 _CARDIO, 100),
        _us("Liver/MASH    — Boston MA",              "Boston, MA",                 _LIVER,   75),
        _us("Metab/Senesc  — Boston MA",              "Boston, MA",                 _METAB,   75),
        _us("Organoid/iPSC — Boston MA",              "Boston, MA",                 _ORGANOID,75),
        _us("Spatial/Omics — Boston MA",              "Boston, MA",                 _SPATIAL, 75),

        _us("Cardio/Preclin — New York NY",           "New York, NY",               _CARDIO, 100),
        _us("Liver/MASH    — New York NY",            "New York, NY",               _LIVER,   75),
        _us("Metab/Senesc  — New York NY",            "New York, NY",               _METAB,   75),
        _us("Organoid/iPSC — New York NY",            "New York, NY",               _ORGANOID,75),

        _us("Cardio/Preclin — Baltimore/NIH",         "Baltimore, MD",              _CARDIO, 100),
        _us("Liver/MASH    — Baltimore/NIH",          "Baltimore, MD",              _LIVER,   75),
        _us("Metab/Senesc  — Baltimore/NIH",          "Baltimore, MD",              _METAB,   75),
        _us("Cardio/Preclin — Bethesda/NIH",          "Bethesda, MD",               _CARDIO, 100),
        _us("Liver/MASH    — Bethesda/NIH",           "Bethesda, MD",               _LIVER,   75),
        _us("Organoid/iPSC — Bethesda/NIH",           "Bethesda, MD",               _ORGANOID,75),

        _us("Cardio/Preclin — Philadelphia PA",       "Philadelphia, PA",           _CARDIO,  75),
        _us("Liver/MASH    — Philadelphia PA",        "Philadelphia, PA",           _LIVER,   75),
        _us("Organoid/iPSC — Philadelphia PA",        "Philadelphia, PA",           _ORGANOID,50),

        _us("Cardio/Preclin — New Haven CT",          "New Haven, CT",              _CARDIO,  75),
        _us("Liver/MASH    — New Haven CT",           "New Haven, CT",              _LIVER,   50),
        _us("Cardio/Preclin — Pittsburgh PA",         "Pittsburgh, PA",             _CARDIO,  75),
        _us("Liver/MASH    — Pittsburgh PA",          "Pittsburgh, PA",             _LIVER,   50),

        # ═══════════════════════════════════════════════════════════════════
        # US — West Coast
        # ═══════════════════════════════════════════════════════════════════
        _us("Cardio/Preclin — San Francisco CA",      "San Francisco, CA",          _CARDIO, 100),
        _us("Liver/MASH    — San Francisco CA",       "San Francisco, CA",          _LIVER,   75),
        _us("Metab/Senesc  — San Francisco CA",       "San Francisco, CA",          _METAB,   75),
        _us("Organoid/iPSC — San Francisco CA",       "San Francisco, CA",          _ORGANOID,75),

        _us("Cardio/Preclin — San Diego CA",          "San Diego, CA",              _CARDIO, 100),
        _us("Liver/MASH    — San Diego CA",           "San Diego, CA",              _LIVER,   75),
        _us("Organoid/iPSC — San Diego CA",           "San Diego, CA",              _ORGANOID,75),

        _us("Cardio/Preclin — Los Angeles CA",        "Los Angeles, CA",            _CARDIO,  75),
        _us("Liver/MASH    — Los Angeles CA",         "Los Angeles, CA",            _LIVER,   75),
        _us("Organoid/iPSC — Los Angeles CA",         "Los Angeles, CA",            _ORGANOID,75),

        _us("Cardio/Preclin — Seattle WA",            "Seattle, WA",                _CARDIO,  75),
        _us("Liver/MASH    — Seattle WA",             "Seattle, WA",                _LIVER,   50),
        _us("Organoid/iPSC — Seattle WA",             "Seattle, WA",                _ORGANOID,50),

        # ═══════════════════════════════════════════════════════════════════
        # US — South / Midwest
        # ═══════════════════════════════════════════════════════════════════
        _us("Cardio/Preclin — Houston TX",            "Houston, TX",                _CARDIO,  75),
        _us("Liver/MASH    — Houston TX",             "Houston, TX",                _LIVER,   75),
        _us("Cardio/Preclin — Chicago IL",            "Chicago, IL",                _CARDIO,  75),
        _us("Liver/MASH    — Chicago IL",             "Chicago, IL",                _LIVER,   75),
        _us("Cardio/Preclin — Nashville TN",          "Nashville, TN",              _CARDIO,  75),
        _us("Liver/MASH    — Nashville TN",           "Nashville, TN",              _LIVER,   75),
        _us("Cardio/Preclin — St. Louis MO",          "St. Louis, MO",              _CARDIO,  75),
        _us("Liver/MASH    — St. Louis MO",           "St. Louis, MO",              _LIVER,   50),
        _us("Cardio/Preclin — Ann Arbor MI",          "Ann Arbor, MI",              _CARDIO,  75),
        _us("Liver/MASH    — Ann Arbor MI",           "Ann Arbor, MI",              _LIVER,   50),
        _us("Cardio/Preclin — Durham/Chapel Hill NC", "Durham, NC",                 _CARDIO,  75),
        _us("Liver/MASH    — Durham NC",              "Durham, NC",                 _LIVER,   50),
        _us("Cardio/Preclin — Cleveland OH",          "Cleveland, OH",              _CARDIO,  75),
        _us("Liver/MASH    — Cleveland OH",           "Cleveland, OH",              _LIVER,   50),
        _us("Cardio/Preclin — Atlanta GA",            "Atlanta, GA",                _CARDIO,  75),
        _us("Cardio/Preclin — Denver CO",             "Denver, CO",                 _CARDIO,  50),

        # ═══════════════════════════════════════════════════════════════════
        # LINKEDIN — global academic postings (cardio + liver + organoid)
        # ═══════════════════════════════════════════════════════════════════
        {"label": "LinkedIn — London UK (cardiovascular)",     "term": _CARDIO,   "location": "London, United Kingdom",   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — London UK (liver/MASH)",         "term": _LIVER,    "location": "London, United Kingdom",   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — London UK (organoid/iPSC)",      "term": _ORGANOID, "location": "London, United Kingdom",   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Cambridge UK (cardiovascular)",  "term": _CARDIO,   "location": "Cambridge, United Kingdom","results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Cambridge UK (liver/MASH)",      "term": _LIVER,    "location": "Cambridge, United Kingdom","results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Germany (cardiovascular)",       "term": _CARDIO,   "location": "Germany",                  "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Germany (liver/MASH)",           "term": _LIVER,    "location": "Germany",                  "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Germany (organoid/iPSC)",        "term": _ORGANOID, "location": "Germany",                  "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Switzerland (cardiovascular)",   "term": _CARDIO,   "location": "Switzerland",              "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Switzerland (liver/MASH)",       "term": _LIVER,    "location": "Switzerland",              "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Netherlands/Belgium (cardio)",   "term": _CARDIO,   "location": "Netherlands",              "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Netherlands/Belgium (liver)",    "term": _LIVER,    "location": "Netherlands",              "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Netherlands (organoid)",         "term": _ORGANOID, "location": "Netherlands",              "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Scandinavia (cardiovascular)",   "term": _CARDIO,   "location": "Sweden",                   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Scandinavia (liver/MASH)",       "term": _LIVER,    "location": "Sweden",                   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — France/Spain (cardiovascular)",  "term": _CARDIO,   "location": "France",                   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — France/Spain (liver)",           "term": _LIVER,    "location": "France",                   "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Austria (cardiovascular)",       "term": _CARDIO,   "location": "Austria",                  "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Austria (organoid/iPSC)",        "term": _ORGANOID, "location": "Austria",                  "results": 50, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Boston MA (cardiovascular)",     "term": _CARDIO,   "location": "Boston, MA",               "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Boston MA (liver/MASH)",         "term": _LIVER,    "location": "Boston, MA",               "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Boston MA (organoid/iPSC)",      "term": _ORGANOID, "location": "Boston, MA",               "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — New York NY (cardiovascular)",   "term": _CARDIO,   "location": "New York, NY",             "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — New York NY (liver/MASH)",       "term": _LIVER,    "location": "New York, NY",             "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Baltimore/NIH (cardio)",         "term": _CARDIO,   "location": "Baltimore, MD",            "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Bethesda/NIH (liver/MASH)",      "term": _LIVER,    "location": "Bethesda, MD",             "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — San Francisco CA (cardio)",      "term": _CARDIO,   "location": "San Francisco, CA",        "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — San Francisco CA (liver/MASH)",  "term": _LIVER,    "location": "San Francisco, CA",        "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — San Diego CA (cardiovascular)",  "term": _CARDIO,   "location": "San Diego, CA",            "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — San Diego CA (organoid/iPSC)",   "term": _ORGANOID, "location": "San Diego, CA",            "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — MASH/Metab/Senescence (global)", "term": _METAB,    "location": "United States",            "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Liver/MASH (global US)",         "term": _LIVER,    "location": "United States",            "results": 75, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Organoid/iPSC (global US)",      "term": _ORGANOID, "location": "United States",            "results": 75, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Cell Culture/Cardiomyo (global)","term": _CELL_CULT,"location": "United States",            "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Spatial Transcriptomics (global)","term": _SPATIAL, "location": "United States",            "results": 50, "region": "US",     "sites": ["linkedin"]},
        {"label": "LinkedIn — Liver/MASH Europe (global)",     "term": _LIVER,    "location": "Europe",                   "results": 75, "region": "Europe", "sites": ["linkedin"]},
        {"label": "LinkedIn — Organoid/iPSC Europe (global)",  "term": _ORGANOID, "location": "Europe",                   "results": 75, "region": "Europe", "sites": ["linkedin"]},
    ]


# ---------------------------------------------------------------------------
# Main hunt
# ---------------------------------------------------------------------------
def pooja_hunt():
    sprint(f"\n{'='*70}")
    sprint(f"[Pooja Scanner] Academic Job Hunt — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    sprint(f"Domains: Cardiology · Liver/MASH · Organoids · Cell Culture · Spatial Transcriptomics")
    sprint(f"Regions: Europe (25+ countries) + US (20+ academic hubs)")
    sprint(f"{'='*70}")
    alerts_sent  = 0
    must_apply_sent = 0
    configs      = build_search_configs()

    # ── Step 1: Parallel scrape (Indeed + LinkedIn via jobspy) ──────────────
    sprint(f"\n[Scrape] Launching {len(configs)} jobspy passes with {SCRAPE_WORKERS} workers...")
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
        sprint(f"\n[Warning] {blocked_passes}/{len(configs)} jobspy passes returned 0 results.")
        if blocked_passes == len(configs):
            sprint("[Pooja Scanner] All jobspy passes blocked — continuing with custom scrapers.")

    # ── Step 1b: Custom scrapers ─────────────────────────────────────────
    sprint("\n[Custom Scrapers] Running jobs.ac.uk · Nature Careers · EurAxess · HigherEdJobs...")
    for scraper_fn, name in [
        (scrape_jobs_ac_uk,       "jobs.ac.uk"),
        (scrape_nature_careers,   "Nature Careers"),
        (scrape_euraxess,         "EurAxess"),
        (scrape_higheredjobs_rss, "HigherEdJobs"),
    ]:
        try:
            df_custom = scraper_fn()
            if not df_custom.empty:
                all_frames.append(df_custom)
                sprint(f"  [{name}] Added {len(df_custom)} results")
        except Exception as e:
            sprint(f"  [{name}] Scraper failed: {e}")

    if not all_frames:
        sprint("[Pooja Scanner] No results from any source. Likely IP block.")
        push_notification("Pooja Academic Scan", "Scan ran — 0 results from all sources.", "low")
        return

    # ── Step 2: Combine + deduplicate ────────────────────────────────────
    raw = pd.concat(all_frames, ignore_index=True)
    sprint(f"\n[Dedup] {len(raw)} total raw → ", end="")

    if "job_url" in raw.columns:
        def _norm_url(u: str) -> str:
            u = str(u).strip().lower()
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

    # ── Step 3: Title filter ─────────────────────────────────────────────
    raw["title_lower"] = raw["title"].str.lower().fillna("")
    mask     = raw["title_lower"].apply(matches_title)
    filtered = raw[mask].copy()
    sprint(f"[Filter] {len(raw)} → {len(filtered)} passed title filter "
           f"(removed {len(raw) - len(filtered)} non-science jobs)")

    # ── Step 3b: Academic institution filter ────────────────────────────
    all_academic  = filtered["company"].fillna("").apply(is_academic_institution)
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

    sprint(f"[Acad Filter] → {len(filtered)} kept "
           f"({len(non_academic)} non-academic/industry removed)")

    if filtered.empty:
        push_notification("Pooja Academic Hunt", "Scan done — 0 academic roles found.", "low")
        return

    # ── Step 4: Pre-rank by title relevance ─────────────────────────────
    filtered["_title_rel"] = filtered["title"].apply(title_relevance)
    if "date_posted" in filtered.columns:
        filtered = filtered.sort_values(["_title_rel", "date_posted"], ascending=[False, False])
    else:
        filtered = filtered.sort_values("_title_rel", ascending=False)

    llm_pool     = filtered.head(SCORE_LLM_TOP).copy()
    keyword_pool = filtered.iloc[SCORE_LLM_TOP:].copy()
    sprint(f"[Score]  {len(llm_pool)} → LLM batch  |  {len(keyword_pool)} → keyword scorer\n")

    # ── Step 5a: LLM batch scoring ───────────────────────────────────────
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

    # ── Step 5b: Keyword scoring for remainder ──────────────────────────
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

    # ── Step 6: Collect results, must-apply flags, alerts, CSV ──────────
    scored_list     = []
    must_apply_jobs = []

    for _, row in filtered.iterrows():
        title    = str(row.get("title", "Unknown"))
        company  = str(row.get("company", "Unknown"))
        location = str(row.get("location", ""))
        url      = str(row.get("job_url", ""))
        src      = str(row.get("_search_pass", ""))
        region   = str(row.get("_region", "US"))
        desc     = str(row.get("description", ""))

        score, method = score_map.get(url, (0, "keyword"))

        must_apply = is_must_apply(score, title, company, desc)

        if score >= 85:   tag = "ELITE "
        elif score >= 80: tag = "STRONG"
        elif score >= 70: tag = "HIGH  "
        elif score >= 50: tag = "fair  "
        else:             tag = "low   "

        ma_marker = " ★MUST★" if must_apply else ""
        sprint(f"  [{score:3d}][{tag}][{method}] {title[:46]:<46} @ {company[:22]}{ma_marker}")

        # Must-apply urgent push (separate from regular alerts)
        if must_apply and must_apply_sent < 10:
            push_notification(
                title=f"★MUST APPLY★ {score}% — {title[:45]}",
                message=f"{company}\n{location}\n{url}",
                priority="urgent",
            )
            must_apply_jobs.append(f"{score}% {title[:40]} @ {company[:20]}")
            must_apply_sent += 1

        # Regular alerts for high-scoring non-must-apply jobs
        elif score >= 65 and not must_apply and alerts_sent < MAX_ALERTS:
            priority = "high" if score >= 75 else "default"
            push_notification(
                title=f"{score}% — {title[:48]}",
                message=f"{company}\n{location}\n{url}",
                priority=priority,
            )
            alerts_sent += 1

        if score >= MIN_SAVE_SCORE:
            scored_list.append({
                "Score":      score,
                "MustApply":  "YES" if must_apply else "",
                "Title":      title,
                "Company":    company,
                "Location":   location,
                "Type":       row.get("job_type", ""),
                "Link":       url,
                "Posted":     str(row.get("date_posted", "")),
                "ScoredBy":   method,
                "Source":     src,
                "Region":     region,
                "ScannedAt":  datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

    if not scored_list:
        sprint("\n[Pooja Scanner] No jobs met the minimum score threshold.")
        push_notification("Pooja Academic Scan", "Scan done — no jobs cleared score threshold.", "low")
        return

    # ── Step 7: Merge + save CSV ─────────────────────────────────────────
    new_df = pd.DataFrame(scored_list)

    if os.path.exists(CSV_PATH):
        existing = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
        # Ensure MustApply column exists in legacy CSV
        if "MustApply" not in existing.columns:
            existing["MustApply"] = ""
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Link"], keep="last")
    else:
        combined = new_df

    # Sort: must-apply first, then by score
    combined["_ma_sort"] = (combined["MustApply"] == "YES").astype(int)
    combined = combined.sort_values(["_ma_sort", "Score"], ascending=[False, False])
    combined = combined.drop(columns=["_ma_sort"])
    combined.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    save_csv_to_github(CSV_PATH)

    strong      = len(new_df[new_df["Score"] >= 80])
    high        = len(new_df[new_df["Score"] >= 70])
    must_count  = len(new_df[new_df["MustApply"] == "YES"])

    sprint(f"\n{'='*70}")
    sprint(f"[Done] {len(new_df)} new leads  |  {must_count} MUST-APPLY  |  {strong} strong (80+)  |  {high} high (70+)")
    sprint(f"       {len(combined)} total in CSV  |  {must_apply_sent} must-apply alerts  |  {alerts_sent} regular alerts")
    sprint(f"{'='*70}\n")

    if must_apply_jobs:
        sprint("[MUST APPLY JOBS THIS SCAN:]")
        for m in must_apply_jobs:
            sprint(f"  ★ {m}")

    push_notification(
        title="Pooja Academic Scan Complete",
        message=(
            f"★ {must_count} MUST-APPLY ★\n"
            f"{len(new_df)} new leads  |  {strong} strong (80+)  |  {high} high (70+)\n"
            f"{alerts_sent} regular alerts  |  {len(combined)} total leads"
        ),
        priority="high" if must_count > 0 else "low",
    )


if __name__ == "__main__":
    pooja_hunt()
