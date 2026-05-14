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

# Safe print for Windows terminals
def sprint(*args, **kwargs):
    text = " ".join(str(a) for a in args)
    safe = text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
           sys.stdout.encoding or "utf-8", errors="replace")
    print(safe, **kwargs)

# --- CONFIG ---
GROQ_API_KEY    = os.getenv("GROQ_API_KEY")
NTFY_TOPIC      = os.getenv("NTFY_TOPIC", "dj-audit-786543")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN")
GITHUB_REPO     = os.getenv("GITHUB_REPO", "djha786543-gif/Mobilejobnotifications")
CSV_PATH        = "Scored_Audit_Leads.csv"
GROQ_MODEL      = "llama-3.3-70b-versatile"
GROQ_ENDPOINT   = "https://api.groq.com/openai/v1/chat/completions"
MIN_SAVE_SCORE  = 35
MAX_ALERTS      = 10
SCORE_TOP_N     = 180       # score top N after title filter
HOURS_OLD       = 168       # only pull jobs posted in last 7 days
MAX_RETRIES     = 2         # retries per search pass on 0 results
RETRY_DELAY     = 20        # seconds between retries


# ---------------------------------------------------------------------------
# LA-area city patterns
# ---------------------------------------------------------------------------
LA_AREA_PATTERNS = [
    r"los angeles", r"\bla\b.*\bca\b", r"irvine", r"long beach",
    r"el segundo", r"torrance", r"burbank", r"glendale", r"pasadena",
    r"santa monica", r"culver city", r"manhattan beach", r"anaheim",
    r"costa mesa", r"newport beach", r"santa ana", r"orange county",
    r"riverside", r"\bontario\b.*\bca\b", r"rancho cucamonga", r"pomona",
    r"woodland hills", r"encino", r"sherman oaks", r"van nuys",
    r"northridge", r"calabasas", r"thousand oaks", r"ventura",
    r"oxnard", r"santa clarita", r"hawthorne", r"inglewood",
    r"gardena", r"carson", r"lakewood", r"downey", r"whittier",
    r"corona.*ca", r"beverly hills", r"west hollywood", r"venice.*ca",
    r"marina del rey", r"playa vista", r"hermosa beach",
    r"redondo beach", r"palos verdes", r"century city",
    r"socal", r"southern california", r"greater los angeles",
    r"los angeles metro", r"l\.a\.", r"west l\.?a\.",
]

def is_la_area(location: str) -> bool:
    loc = location.lower()
    return any(re.search(p, loc) for p in LA_AREA_PATTERNS)


# ---------------------------------------------------------------------------
# Title whitelist — must match at least one pattern
# ---------------------------------------------------------------------------
TITLE_WHITELIST = [
    r"\bit\s+audit",
    r"\bit\s+auditor",
    r"\bitgc\b",
    r"\bsox\b",
    r"\bgrc\b",
    r"\bit\s+compliance",
    r"\bit\s+risk\b",
    r"\bit\s+governance",
    r"\bcyber\s+audit",
    r"\bsecurity\s+audit",
    r"\bcloud\s+audit",
    r"\btechnology\s+audit",
    r"\btechnology\s+risk",
    r"\btech\s+risk",
    r"\binformation\s+(?:technology\s+)?audit",
    r"\binformation\s+systems?\s+audit",
    r"\bis\s+auditor",
    r"\bit\s+assurance",
    r"\bit\s+controls",
    r"\bcontrols?\s+(?:analyst|testing|assessment|review)",
    r"\bsox\s+(?:analyst|auditor|compliance|controls|testing|itgc)",
    r"\bgrc\s+(?:analyst|consultant|specialist|manager|lead)",
    r"\bit\s+risk\s+(?:analyst|manager|consultant)",
    r"\baudit\s+(?:analyst|manager|lead|consultant|specialist|senior|associate)",
    r"\binternal\s+audit\b",
    r"\bcompliance\s+(?:analyst|manager|lead|specialist|consultant)",
    r"\bcyber\s+(?:risk|compliance|governance)",
    r"\bgovernance\s+risk\s*(?:and\s+)?compliance",
    r"\brisk\s+(?:and\s+)?compliance\s+(?:analyst|manager|specialist|consultant)",
    r"\bsecurity\s+(?:compliance|governance|controls)",
    r"\bapplication\s+controls",
    r"\berp\s+audit",
    r"\bsoc\s*[12]\b.*\baudit",
    r"\baudit\s+manager\b",
    r"\baudit\s+senior\b",
    r"\bsenior\s+auditor\b",
    r"\bstaff\s+auditor\b",
    r"\baccounting\s+(?:and\s+)?it\s+audit",
    r"\bai\s+(?:audit|governance|risk|compliance)",
    r"\bai\/ml\s+(?:governance|risk|compliance|audit)",
    r"\bnist\s+ai",
    r"\bsoc\s*[12]\b",          # SOC 1 / SOC 2 standalone
    r"\bservice\s+auditor\b",
    r"\bffiec\b",               # banking IT audit
    r"\bmodel\s+risk\b",        # model risk governance (AI-adjacent)
    r"\bthird.party\s+risk\b",
    r"\btprm\b",
]

# Hard title blacklist — veto even if whitelist matched
# Exception: explicit 'audit', 'itgc', 'sox' in title overrides blacklist
TITLE_BLACKLIST = [
    r"\bsoftware\s+(?:engineer|developer|architect)",
    r"\bdata\s+(?:engineer|scientist|architect)",
    r"\bwarehouse\b",
    r"\bmedical\b",
    r"\bsales\b",
    r"\bmarketing\b",
    r"\bprogram\s+manager\b",
    r"\bproject\s+manager\b",
    r"\bproduct\s+manager\b",
    r"\bservice\s+delivery\b",
    r"\benterprise\s+architect",
    r"\bsolutions?\s+architect",
    r"\baccountant\b",
    r"\bfinancial\s+analyst\b",
    r"\bhr\b|\bhuman\s+resources\b",
    r"\bphysician\b|\bnurse\b|\bclinical\b",
    r"\bdevops\b",
    r"\bnetwork\s+engineer\b",
    r"\bsystem\s+(?:engineer|administrator)\b",
    r"\boperations\s+(?:manager|analyst|specialist)(?!\s+(?:risk|compliance|audit))",
    r"\bpayroll\b",
    r"\baccounts\s+(?:payable|receivable)\b",
    r"\bcost\s+accountant\b",
    r"\btax\s+(?:analyst|manager|consultant)\b",
    r"\brecruiter\b|\btalent\s+acquisition\b",
    r"\bsupply\s+chain\b",
    r"\bprocurement\b",
]

def matches_title(title: str) -> bool:
    """Two-stage title filter: whitelist → blacklist veto."""
    t = title.lower()
    if not any(re.search(p, t) for p in TITLE_WHITELIST):
        return False
    # Explicit audit/ITGC/SOX always overrides blacklist
    if re.search(r"\baudit\b|\bitgc\b|\bsox\b|\bgrc\b", t):
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
            "message": f"chore: auto-save DJ scan results {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}",
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
                "Tags":     "briefcase",
            },
            timeout=10,
        )
    except Exception as e:
        sprint(f"[Push Error] {e}")


# ---------------------------------------------------------------------------
# Keyword scorer — tuned to Deobrat Jha's actual CV
#
# Profile summary:
#   - 8+ yrs IT Audit: EY Manager (Big 4), Public Storage, Investar Bank
#   - Certs: CISA (Jan 2026), AAIA (May 2026), AWS CP (Feb 2026), Six Sigma YB
#   - Skills: SOX 404, ITGC, ITAC, SOC 1/2, GRC, AI/ML governance, NIST AI RMF
#   - ERPs: SAP S/4HANA, Oracle, NetSuite, Workday, Salesforce, ServiceNow, AD
#   - Cloud: AWS (IAM, S3, EC2, CloudTrail), Azure
#   - Industries: Financial services, healthcare, public company (REIT), banking
#   - Auth: US EAD, no sponsorship needed
#   - Location: Torrance CA — remote preferred, LA/OC commutable
# ---------------------------------------------------------------------------
def keyword_score(title: str, desc: str, location: str = "") -> int:
    t = title.lower()
    d = (title + " " + desc).lower()
    score = 22   # base — passed title filter

    # --- Title signals ---
    title_core = {
        "it audit": 15, "it auditor": 15, "itgc": 14, "sox": 12,
        "it compliance": 13, "it risk": 12, "it governance": 11,
        "it controls": 10, "it assurance": 12, "is auditor": 13,
        "information systems audit": 13, "information technology audit": 14,
    }
    title_good = {
        "grc": 9, "internal audit": 9, "audit manager": 8, "audit lead": 8,
        "audit analyst": 8, "audit consultant": 9, "audit specialist": 8,
        "audit senior": 8, "senior auditor": 9, "staff auditor": 7,
        "compliance analyst": 7, "controls analyst": 8, "controls testing": 9,
        "application controls": 9, "erp audit": 10,
        "cyber audit": 10, "cloud audit": 10, "security audit": 9,
        "sox analyst": 11, "sox auditor": 11, "sox compliance": 10,
        "grc analyst": 11, "grc consultant": 11, "grc specialist": 10,
        "grc manager": 9, "technology audit": 10, "technology risk": 9,
        "tech risk": 8, "cyber risk": 8, "cyber compliance": 8,
        "information security": 7,
        # AI governance — DJ's AAIA is rare and high-value
        "ai audit": 14, "ai governance": 13, "ai risk": 12,
        "ai compliance": 11, "ai/ml governance": 12,
        # SOC-specific
        "soc 1": 9, "soc 2": 9, "soc1": 9, "soc2": 9,
        "service auditor": 9,
        # Banking/FFIEC
        "ffiec": 8,
        # Third-party / vendor risk
        "third party risk": 8, "tprm": 9, "vendor risk": 7,
        "model risk": 8,
    }
    title_pts  = sum(v for k, v in title_core.items() if k in t)
    title_pts += sum(v for k, v in title_good.items() if k in t)
    score += min(title_pts, 36)

    # --- Description signals ---
    desc_kw = {
        # CISA / AAIA — DJ holds both; very strong signal
        "cisa required": 14, "cisa is required": 14,
        "cisa preferred": 11, "cisa certified": 11,
        "cisa certification": 11, "cisa or equivalent": 11,
        "cisa, cism": 10, "cisa/cism": 10, "cisa": 7,
        # AAIA is a new cert — any mention = very relevant
        "aaia": 12, "ai audit professional": 12,
        "ai audit": 10, "ai governance": 10, "ai risk": 8,
        "nist ai rmf": 10, "ai/ml governance": 9, "ai/ml risk": 8,
        "responsible ai": 7, "ai ethics": 6, "model risk": 8,
        "aaia certified": 12,
        # Core IT audit
        "itgc": 9, "sox": 8, "sox 404": 10, "it audit": 8,
        "internal controls": 7, "control testing": 7,
        "controls testing": 7, "audit program": 6,
        "itac": 9, "it application controls": 9,
        "segregation of duties": 9, "sod review": 8, "sod analysis": 8,
        "access controls": 7, "privileged access": 8, "iam": 5,
        "logical access": 7,
        "change management controls": 7, "application controls": 8,
        "change management": 5, "batch scheduling": 6,
        "erp audit": 8, "soc 1": 7, "soc 2": 7, "soc1": 7, "soc2": 7,
        "soc type ii": 8, "soc type 2": 8,
        "trust services": 6, "trust principles": 6,
        # ERPs DJ knows deeply
        "s/4hana": 10, "sap s/4hana": 10, "sap": 5,
        "oracle": 5, "netsuite": 6, "workday": 5,
        "salesforce": 4, "servicenow": 4,
        "active directory": 5,
        # Cloud — AWS CP certified
        "aws": 5, "aws iam": 7, "aws cloudtrail": 7,
        "azure": 4, "gcp": 3, "cloud audit": 6,
        "cloud controls": 6, "cloud security": 5,
        # Analytics tools DJ has
        "power bi": 4, "excel vba": 5, "acl": 5, "python": 3, "sql": 3,
        # Credentials / Big 4 background
        "cissp": 5, "cism": 5, "big 4": 7, "big four": 7,
        "ey ": 5, "ernst & young": 5, "ernst and young": 5,
        "deloitte": 4, "pwc": 4, "kpmg": 4,
        "public accounting": 5, "public company": 4,
        # Frameworks
        "grc": 6, "nist": 5, "iso 27001": 6, "cobit": 7, "coso": 6,
        "nist csf": 5, "nist sp": 4,
        # Compliance areas
        "hipaa": 6, "pci": 6, "gdpr": 4, "ccpa": 4,
        "ffiec": 8, "bsa/aml": 5, "bank secrecy": 5,
        "sox compliance": 8, "sox testing": 9,
        "risk assessment": 5, "risk management": 4, "governance": 4,
        # Business cycles DJ tested
        "p2p": 5, "o2c": 5, "r2r": 5, "procure to pay": 5,
        "order to cash": 5, "record to report": 5,
        "bcp": 4, "disaster recovery": 4, "dr testing": 4,
        # Third-party / vendor risk
        "third party risk": 7, "tprm": 7, "vendor risk": 6,
        # Industries DJ has worked in
        "financial services": 4, "banking": 4, "reit": 4,
        "healthcare": 4, "insurance": 3,
        # Contract / work type — DJ prefers contract
        "c2c": 7, "corp-to-corp": 7, "corp to corp": 7,
        "1099": 6, "w2 contract": 7, "contract role": 5,
        "contract position": 5, "consulting role": 5,
        # EAD-friendly signals
        "ead": 6, "no sponsorship required": 6,
        "authorized to work": 5, "work authorization": 4,
        "gc holders": 5, "green card": 4,
        "employment authorization": 5,
    }
    desc_pts = sum(v for k, v in desc_kw.items() if k in d)
    score += min(desc_pts, 42)

    # --- Remote & work type ---
    is_remote_job = any(k in d for k in [
        "fully remote", "100% remote", "remote position", "work from home",
        "work remotely", "remote work", "remote opportunity",
        "remote role", "remote-first", "wfh", " remote",
    ])
    is_contract = any(k in d for k in [
        "contract ", "contractor", " w2 ", "w2 contract", "c2c",
        "corp to corp", "corp-to-corp", "1099", "contract role",
        "contract position", "consulting role",
    ])
    is_ead_ok = any(k in d for k in [
        "ead", "no sponsorship required", "authorized to work",
        "work authorization", "gc holders", "green card",
        "must be authorized", "employment authorization",
        "us citizen or", "authorized to work in the us",
    ])

    la_job = is_la_area(location) or is_la_area(desc[:300])

    if is_remote_job:   score += 10
    if is_contract:     score += 10
    if is_ead_ok:       score += 5
    if la_job and not is_remote_job:
        score += 6
    elif not is_remote_job and not la_job:
        score -= 8

    # --- Seniority (DJ: analyst → manager, open to senior) ---
    if re.search(r"\bsenior\b|\bsr\.\b|\blead\b|\bmanager\b", t):  score += 3
    if re.search(r"\bstaff\b|\bjunior\b", t):                       score -= 5
    if re.search(r"\bassociate\b", t) and "audit" in t:             score -= 3

    # Hard seniority penalties (too senior for DJ's target level)
    if re.search(r"\bvp\b|\bvice\s+president\b", t):          score -= 30
    if re.search(r"\bchief\b|\bcto\b|\bciso\b|\bcfo\b", t):   score -= 35
    if re.search(r"\bhead\s+of\b", t):                         score -= 25
    if re.search(r"\bdirector\b", t):                          score -= 20
    if re.search(r"\bpartner\b", t) and "audit" in t:          score -= 15
    if re.search(r"\bprincipal\b", t):                         score -= 10
    if re.search(r"\bmanaging\s+director\b", t):               score -= 25

    # --- Hard disqualifiers ---
    if re.search(r"requir.{0,30}(?:visa\s+)?sponsor", d) and "no sponsorship" not in d:
        score -= 25
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci",
                             "polygraph", "poly clearance", "active clearance",
                             "security clearance required", "clearance required"]):
        score -= 30
    if any(k in d for k in ["must be on-site", "onsite only", "must be local",
                             "no remote", "not eligible for remote",
                             "in-office required", "on site only",
                             "must report to office"]):
        if not la_job:
            score -= 20

    return max(0, min(score, 100))


# ---------------------------------------------------------------------------
# LLM scorer via Groq — llama-3.3-70b, fast + free tier
# ---------------------------------------------------------------------------
def llm_score(desc: str, location: str = "") -> int | None:
    if not GROQ_API_KEY:
        return None

    la_context = (
        "LA/Orange County on-site or hybrid is also acceptable."
        if is_la_area(location) else
        "Targeting remote positions; on-site outside LA is not preferred."
    )

    prompt = f"""Rate 0–100 fit for this candidate:

Candidate profile:
- US EAD — no sponsorship needed, authorized for W2 & contract (C2C, 1099)
- 8+ years IT Audit — Big 4 EY (Manager – Technology Risk, 2016–2024) + Public Storage (IT Auditor) + Investar Bank (Senior Internal Auditor)
- Certifications: CISA (Jan 2026), AAIA – AI Audit Professional (May 2026), AWS Cloud Practitioner (Feb 2026), Six Sigma Yellow Belt
- Core skills: SOX 404, ITGC & ITAC testing, SOC 1 / SOC 2 Type II audits, internal controls, GRC, AI/ML governance, NIST AI RMF, NIST, ISO 27001, COBIT, COSO
- ERP expertise: SAP S/4HANA, Oracle, NetSuite, Workday, Salesforce, ServiceNow, Active Directory
- Cloud: AWS (IAM, S3, EC2, CloudTrail), Azure — AWS Certified
- Tools: Excel VBA, Power BI, SQL, Python, ACL, Jira
- Business cycles: P2P, O2C, R2R, IAM, change management, BCP/DR
- Industries: Financial services, banking (FFIEC), REIT/real estate, healthcare
- Building agentic AI tool for ITGC/SOX automation (personal project)
- Location: Torrance CA — remote preferred (anywhere US) OR LA/Orange County commutable
- {la_context}
- Open to: Analyst, Senior Auditor, Lead, Consultant, Manager levels
- NOT suitable for: VP, Director, Head of, Chief, Partner, C-suite roles

Job description:
{desc[:2800]}

Scoring guide:
- 90–100: Perfect IT Audit/ITGC/SOX/GRC/AI-Governance role, remote or LA/OC area, EAD-ok, analyst-to-manager level, contract or perm
- 70–89: Strong IT audit relevance, good skill overlap, remote or LA hybrid, appropriate seniority
- 50–69: Decent audit/compliance role, partial criteria match
- 30–49: Some overlap but missing key criteria (wrong location, partial relevance, seniority stretch)
- 0–29: Poor fit — wrong field, clearance required, sponsorship required, on-site outside LA, OR executive-level role

IMPORTANT: VP, Vice President, Director, Head of, Chief, Managing Director, Partner → score MAX 30.
IMPORTANT: AI Governance, AI Audit, NIST AI RMF, AAIA roles → bonus — candidate holds the AAIA certification (rare).
IMPORTANT: SOC 1/SOC 2, FFIEC, SAP S/4HANA audit → strong match.

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
                wait = 15 if attempt == 0 else 30
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
# Search configuration — 12 passes
#
# Strategy:
#   - country_indeed="USA" on every pass (bypasses geo-detection blocking)
#   - hours_old=168 (7-day freshness)
#   - Search terms split into focused groups — fewer ORs = more reliable results
#   - fallback_sites: if Indeed returns 0, retry with these sites instead
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:
    return [
        # --- Nationwide: Core IT Audit titles ---
        {
            "label": "Core IT Audit (US)",
            "term":  '"IT Audit" OR "IT Auditor" OR "ITGC" OR "SOX ITGC" OR "IT Controls" OR "IT Assurance"',
            "location": "United States",
            "results": 150,
            "fallback_sites": ["ziprecruiter", "glassdoor"],
        },
        # --- Nationwide: SOX + ITGC specialists ---
        {
            "label": "SOX / ITGC (US)",
            "term":  '"SOX Analyst" OR "SOX Auditor" OR "SOX Compliance" OR "SOX 404" OR "ITGC Analyst" OR "IT Governance" OR "IT Assurance"',
            "location": "United States",
            "results": 125,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Nationwide: GRC specializations ---
        {
            "label": "GRC Roles (US)",
            "term":  '"GRC Analyst" OR "GRC Consultant" OR "GRC Specialist" OR "GRC Manager" OR "Governance Risk Compliance"',
            "location": "United States",
            "results": 125,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Nationwide: Technology risk & compliance ---
        {
            "label": "Tech Risk / Compliance (US)",
            "term":  '"Technology Audit" OR "Technology Risk" OR "IT Risk" OR "IT Compliance" OR "IS Auditor" OR "Cyber Audit" OR "Cloud Audit"',
            "location": "United States",
            "results": 125,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Nationwide: Internal / controls audit ---
        {
            "label": "Internal Audit / Controls (US)",
            "term":  '"Controls Analyst" OR "Audit Analyst" OR "Audit Manager" OR "Internal Audit" OR "Audit Consultant" OR "Application Controls" OR "ERP Audit"',
            "location": "United States",
            "results": 125,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Nationwide: CISA-required/preferred ---
        {
            "label": "CISA Required/Preferred (US)",
            "term":  '"CISA required" OR "CISA preferred" OR "CISA certified" OR "CISA certification" OR "CISA or equivalent" OR "CISA, CISM" OR "CISA/CISM"',
            "location": "United States",
            "results": 100,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Nationwide: AI Governance / AI Audit (DJ holds AAIA — rare cert) ---
        {
            "label": "AI Governance / AI Audit (US)",
            "term":  '"AI Governance" OR "AI Audit" OR "AI Risk" OR "AI Compliance" OR "NIST AI" OR "AI/ML Governance" OR "Model Risk" OR "Responsible AI"',
            "location": "United States",
            "results": 100,
            "fallback_sites": ["ziprecruiter", "glassdoor"],
        },
        # --- Nationwide: SOC 1 / SOC 2 (DJ has deep SOC experience from EY) ---
        {
            "label": "SOC 1 / SOC 2 Audit (US)",
            "term":  '"SOC 1" OR "SOC 2" OR "SOC Type II" OR "Service Auditor" OR "Trust Services" OR "SSAE 18" OR "SSAE18"',
            "location": "United States",
            "results": 100,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Nationwide: FFIEC / Banking IT Audit (Investar Bank experience) ---
        {
            "label": "FFIEC / Banking IT Audit (US)",
            "term":  '"FFIEC" OR "Banking IT Audit" OR "Financial Services Audit" OR "BSA AML Audit" OR "Core Banking Audit" OR "IT Audit Bank"',
            "location": "United States",
            "results": 75,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- LA area: broad IT audit sweep ---
        {
            "label": "IT Audit — Los Angeles area",
            "term":  '"IT Audit" OR "ITGC" OR "SOX" OR "GRC" OR "IT Compliance" OR "IT Risk" OR "CISA" OR "Compliance Analyst"',
            "location": "Los Angeles, CA",
            "results": 100,
            "distance": 50,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Orange County (Irvine hub) ---
        {
            "label": "IT Audit / GRC — Orange County",
            "term":  '"IT Audit" OR "SOX" OR "GRC" OR "ITGC" OR "IT Compliance" OR "IT Risk" OR "CISA" OR "Audit Analyst"',
            "location": "Irvine, CA",
            "results": 75,
            "distance": 40,
            "fallback_sites": ["ziprecruiter"],
        },
        # --- Remote only: broad sweep to catch remote postings missed above ---
        {
            "label": "Remote IT Audit / GRC",
            "term":  '"IT Audit" OR "ITGC" OR "SOX" OR "GRC" OR "IT Risk" OR "IT Compliance" OR "Audit Consultant" OR "CISA"',
            "location": "remote",
            "results": 100,
            "fallback_sites": ["ziprecruiter", "glassdoor"],
        },
    ]


# ---------------------------------------------------------------------------
# Robust scrape with retry + fallback sites
# ---------------------------------------------------------------------------
def scrape_with_retry(cfg: dict) -> pd.DataFrame:
    """
    Try Indeed first. If 0 results, retry up to MAX_RETRIES times.
    If still 0, fall back to cfg['fallback_sites'] (one attempt each).
    Returns combined non-empty DataFrame or empty DataFrame.
    """
    base_kwargs = dict(
        search_term=cfg["term"],
        location=cfg["location"],
        results_wanted=cfg["results"],
        country_indeed="USA",           # critical: prevents geo-blocking
        hours_old=HOURS_OLD,
        is_remote=cfg.get("remote", False),
    )
    if "distance" in cfg:
        base_kwargs["distance"] = cfg["distance"]

    # --- Primary: Indeed ---
    for attempt in range(1 + MAX_RETRIES):
        try:
            df = scrape_jobs(site_name=["indeed"], **base_kwargs)
            if not df.empty:
                sprint(f"  → {len(df)} raw results (indeed, attempt {attempt+1})")
                return df
            if attempt < MAX_RETRIES:
                sprint(f"  → 0 results (indeed attempt {attempt+1}/{1+MAX_RETRIES}) — retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
        except Exception as e:
            sprint(f"  [Indeed error attempt {attempt+1}]: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    # --- Fallback: try other sites ---
    fallback_sites = cfg.get("fallback_sites", [])
    for site in fallback_sites:
        try:
            sprint(f"  → Trying fallback: {site}...")
            # Some sites don't support hours_old or distance — strip them if needed
            fb_kwargs = {k: v for k, v in base_kwargs.items()
                         if k not in ("country_indeed", "hours_old")}
            df = scrape_jobs(site_name=[site], **fb_kwargs)
            if not df.empty:
                sprint(f"  → {len(df)} raw results ({site} fallback)")
                return df
            sprint(f"  → 0 results ({site} fallback)")
        except Exception as e:
            sprint(f"  [{site} fallback error]: {e}")
        time.sleep(8)

    sprint(f"  → 0 results from all sources (likely IP block or no new postings)")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Main hunt
# ---------------------------------------------------------------------------
def master_hunt():
    sprint(f"\n{'='*60}")
    sprint(f"[Scanner] IT Audit Hunt — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    sprint(f"{'='*60}")
    alerts_sent = 0
    all_frames  = []
    blocked_passes = 0

    for cfg in build_search_configs():
        sprint(f"\n[Search] {cfg['label']} ({cfg['results']} results)...")
        df = scrape_with_retry(cfg)
        if not df.empty:
            df["_search_pass"] = cfg["label"]
            all_frames.append(df)
        else:
            blocked_passes += 1
        time.sleep(5)   # polite delay between passes

    if blocked_passes > 0:
        sprint(f"\n[Warning] {blocked_passes}/{len(build_search_configs())} passes returned 0 results.")
        if blocked_passes == len(build_search_configs()):
            sprint("[Scanner] No results from any search pass. Likely IP block by Indeed.")
            push_notification("Audit Hunt", "Scan ran — 0 results from all passes (possible IP block).", "low")
            return

    # -----------------------------------------------------------------------
    # Combine + deduplicate
    # -----------------------------------------------------------------------
    raw = pd.concat(all_frames, ignore_index=True)
    sprint(f"\n[Dedup] {len(raw)} total raw → ", end="")

    if "job_url" in raw.columns:
        def _norm_url(u: str) -> str:
            u = str(u).strip().lower()
            # Indeed job IDs live IN the query string (?jk=...) — don't strip them.
            # For all other sites the ID is in the path, so stripping query params is safe.
            if "indeed.com" in u:
                return u   # keep full URL including ?jk=...
            return u.split("?")[0]

        raw["_url_norm"] = raw["job_url"].apply(_norm_url)
        raw = raw.drop_duplicates(subset=["_url_norm"], keep="first")

    raw["_title_co"] = (
        raw["title"].astype(str).str.lower().str.strip() + "|" +
        raw["company"].astype(str).str.lower().str.strip()
    )
    raw = raw.drop_duplicates(subset=["_title_co"], keep="first")
    sprint(f"{len(raw)} after dedup")

    # -----------------------------------------------------------------------
    # Two-stage title filter
    # -----------------------------------------------------------------------
    raw["title_lower"] = raw["title"].str.lower().fillna("")
    mask = raw["title_lower"].apply(matches_title)
    filtered = raw[mask].copy()
    sprint(f"[Filter] {len(raw)} → {len(filtered)} passed title filter "
           f"(removed {len(raw) - len(filtered)} non-audit jobs)")

    if filtered.empty:
        push_notification("Audit Hunt", "Scan complete — 0 audit titles found.", "low")
        return

    # Sort by recency before scoring
    if "date_posted" in filtered.columns:
        filtered = filtered.sort_values("date_posted", ascending=False)

    to_score = filtered.head(SCORE_TOP_N)
    sprint(f"[Score]  Scoring top {len(to_score)} jobs...\n")

    # -----------------------------------------------------------------------
    # Score each job
    # -----------------------------------------------------------------------
    scored_list = []
    for _, row in to_score.iterrows():
        title    = str(row.get("title", "Unknown"))
        desc     = str(row.get("description", ""))
        company  = str(row.get("company", "Unknown"))
        location = str(row.get("location", ""))
        url      = str(row.get("job_url", ""))
        src      = str(row.get("_search_pass", ""))

        try:
            score, method = score_job(title, desc, location)

            if score >= 80:   tag = "STRONG"
            elif score >= 70: tag = "HIGH  "
            elif score >= 50: tag = "fair  "
            else:             tag = "low   "

            la_tag = " [LA]" if is_la_area(location) else ""
            sprint(f"  [{score:3d}][{tag}][{method}] {title[:48]:<48} @ {company[:25]}{la_tag}")

            if score >= 60 and alerts_sent < MAX_ALERTS:
                priority = "urgent" if score >= 82 else "high"
                loc_note = f" | {location}" if location else ""
                push_notification(
                    title=f"{score}% — {title[:48]}",
                    message=f"{company}{loc_note}\n{url}",
                    priority=priority,
                )
                alerts_sent += 1

            if score >= MIN_SAVE_SCORE:
                scored_list.append({
                    "Score":     score,
                    "Title":     title,
                    "Company":   company,
                    "Location":  location or "Remote",
                    "Type":      row.get("job_type", ""),
                    "Link":      url,
                    "Posted":    str(row.get("date_posted", "")),
                    "ScoredBy":  method,
                    "Source":    src,
                    "ScannedAt": datetime.now().strftime("%Y-%m-%d %H:%M"),
                })

        except Exception as e:
            sprint(f"  [Error] {title[:40]}: {e}")

        time.sleep(1)

    # -----------------------------------------------------------------------
    # Merge with existing CSV
    # -----------------------------------------------------------------------
    if not scored_list:
        sprint("\n[Scanner] No jobs met the minimum score threshold.")
        push_notification("Audit Hunt", "Scan done — no jobs cleared the score threshold.", "low")
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

    save_csv_to_github(CSV_PATH)

    strong = len(new_df[new_df["Score"] >= 80])
    high   = len(new_df[new_df["Score"] >= 70])
    la_new = len(new_df[new_df["Location"].str.lower().apply(is_la_area)])

    sprint(f"\n{'='*60}")
    sprint(f"[Done] {len(new_df)} new leads saved  |  "
           f"{strong} strong (80+)  |  {high} high (70+)  |  {la_new} LA-area")
    sprint(f"       {len(combined)} total in CSV  |  {alerts_sent} alerts sent")
    sprint(f"{'='*60}\n")

    push_notification(
        title="Audit Scan Complete",
        message=(
            f"{len(new_df)} new leads  |  {strong} strong (80+)  |  {high} high (70+)\n"
            f"{la_new} LA-area  |  {alerts_sent} alerts  |  {len(combined)} total"
        ),
        priority="low",
    )


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    master_hunt()
