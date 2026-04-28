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
GROQ_MODEL      = "llama-3.3-70b-versatile"   # fast, capable, free tier
GROQ_ENDPOINT   = "https://api.groq.com/openai/v1/chat/completions"
MIN_SAVE_SCORE  = 35    # jobs below this are NOT saved
MAX_ALERTS      = 10    # max push alerts per scan run
SCORE_TOP_N     = 120   # score top N after title filter (increased)

# ---------------------------------------------------------------------------
# LA-area city patterns — used for location-aware scoring
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
# Title whitelist — regex, word-boundary safe
# Must match at least one pattern to pass
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
]

def matches_title(title: str) -> bool:
    """Two-stage title filter: whitelist → blacklist veto."""
    t = title.lower()
    if not any(re.search(p, t) for p in TITLE_WHITELIST):
        return False
    # Explicit audit/ITGC/SOX overrides blacklist
    if re.search(r"\baudit\b|\bitgc\b|\bsox\b", t):
        return True
    if any(re.search(p, t) for p in TITLE_BLACKLIST):
        return False
    return True


# ---------------------------------------------------------------------------
# GitHub API — persist CSV across Streamlit Cloud redeployments
# ---------------------------------------------------------------------------
def save_csv_to_github(csv_path: str) -> bool:
    """Push the CSV to GitHub so it survives Streamlit Cloud redeploys."""
    if not GITHUB_TOKEN or not os.path.exists(csv_path):
        return False
    try:
        import base64
        with open(csv_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode()

        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{csv_path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}",
                   "Accept": "application/vnd.github+json"}

        # Get current SHA (required for updates)
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
# Keyword scorer — location-aware, tuned for DJ's profile
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
    }
    title_pts  = sum(v for k, v in title_core.items() if k in t)
    title_pts += sum(v for k, v in title_good.items() if k in t)
    score += min(title_pts, 32)

    # --- Description signals ---
    desc_kw = {
        # CISA / AAIA — very strong signal
        "cisa required": 14, "cisa is required": 14,
        "cisa preferred": 11, "cisa certified": 11,
        "cisa certification": 11, "cisa or equivalent": 11,
        "cisa, cism": 10, "cisa/cism": 10, "cisa": 7,
        "aaia": 8, "ai audit": 8, "ai governance": 8, "ai risk": 7,
        "nist ai rmf": 7, "ai/ml governance": 7,
        # Core audit signals
        "itgc": 9, "sox": 8, "it audit": 8, "internal controls": 7,
        "control testing": 7, "controls testing": 7, "audit program": 6,
        "segregation of duties": 9, "sod review": 8, "sod analysis": 8,
        "access controls": 7, "privileged access": 7, "iam": 5,
        "change management controls": 7, "application controls": 8,
        "erp audit": 8, "soc 1": 7, "soc 2": 7, "soc1": 7, "soc2": 7,
        # Technologies
        "s/4hana": 8, "sap": 5, "oracle": 4, "workday": 4,
        "aws": 4, "azure": 4, "gcp": 3, "cloud audit": 6,
        # Credentials / background
        "cissp": 5, "cism": 5, "big 4": 6, "ey ": 5,
        "deloitte": 4, "pwc": 4, "kpmg": 4, "public accounting": 5,
        # Frameworks
        "grc": 6, "nist": 5, "iso 27001": 6, "cobit": 6,
        "hipaa": 5, "pci": 6, "gdpr": 4, "ccpa": 4,
        "risk assessment": 5, "risk management": 4, "governance": 4,
        # Third-party / vendor risk
        "third party risk": 6, "tprm": 6, "vendor risk": 5,
        # AI governance (DJ holds AAIA certification)
        "ai governance": 8, "ai audit": 8, "ai risk": 7,
        "nist ai rmf": 8, "ai/ml governance": 7, "aaia": 9,
        # Contract / work type
        "c2c": 6, "corp-to-corp": 6, "corp to corp": 6,
        "1099": 5, "w2 contract": 6, "contract role": 5,
    }
    desc_pts = sum(v for k, v in desc_kw.items() if k in d)
    score += min(desc_pts, 38)

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
    ])

    la_job = is_la_area(location) or is_la_area(desc[:300])

    if is_remote_job:   score += 10
    if is_contract:     score += 10
    if is_ead_ok:       score += 5
    if la_job and not is_remote_job:
        score += 6     # local LA role — DJ can commute, still valuable
    elif not is_remote_job and not la_job:
        score -= 8     # non-remote, non-LA — penalize but don't eliminate

    # --- Seniority ---
    if re.search(r"\bsenior\b|\bsr\.\b|\blead\b", t):  score += 3
    if re.search(r"\bstaff\b|\bjunior\b", t):           score -= 5
    if re.search(r"\bassociate\b", t) and "audit" in t: score -= 3

    # Hard seniority penalties
    if re.search(r"\bvp\b|\bvice\s+president\b", t):          score -= 30
    if re.search(r"\bchief\b|\bcto\b|\bciso\b|\bcfo\b", t):   score -= 35
    if re.search(r"\bhead\s+of\b", t):                         score -= 25
    if re.search(r"\bdirector\b", t):                          score -= 20
    if re.search(r"\bpartner\b", t) and "audit" in t:          score -= 15
    if re.search(r"\bprincipal\b", t):                         score -= 10

    # --- Hard disqualifiers ---
    if re.search(r"requir.{0,30}(?:visa\s+)?sponsor", d) and "no sponsorship" not in d:
        score -= 25
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci",
                             "polygraph", "poly clearance", "active clearance",
                             "security clearance required"]):
        score -= 30
    if any(k in d for k in ["must be on-site", "onsite only", "must be local",
                             "no remote", "not eligible for remote",
                             "in-office required", "on site only",
                             "must report to office"]):
        if not la_job:      # strict on-site outside LA = skip
            score -= 20

    return max(0, min(score, 100))


# ---------------------------------------------------------------------------
# LLM scorer via Groq — very fast, free tier, llama-3.3-70b
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
- EAD-authorized (US EAD), no sponsorship needed, authorized for W2 & contract work
- 8+ years IT Audit — Big 4 / EY (Manager – Technology Risk, 2016–2024) + Fortune 500
- Certifications: CISA (Jan 2026), AAIA – AI Audit Professional (Apr 2026), AWS Cloud Practitioner (Feb 2026), Six Sigma Yellow Belt
- Core skills: SOX 404, ITGC & ITAC testing, SOC 1 / SOC 2 audits, internal controls, GRC, AI/ML governance, NIST AI RMF, NIST, ISO 27001, COBIT, COSO
- ERP & platforms: SAP S/4HANA, Oracle, NetSuite, Workday, Salesforce, ServiceNow, Active Directory
- Cloud: AWS (IAM, S3, EC2, CloudTrail), Azure, cloud audit
- Tools: Excel VBA, Power BI, SQL, Python, ACL, Jira
- Business cycles: P2P, O2C, R2R, IAM, change management, BCP/DR
- Location preference: Remote (anywhere US) OR in/around Los Angeles CA area (Torrance-based)
- {la_context}
- Open to all levels (analyst → manager), prefers contract/W2

Job description:
{desc[:2800]}

Scoring guide:
- 90–100: Perfect IT Audit/ITGC/SOX/GRC contract or direct hire, fully remote or LA area, EAD-ok
- 70–89: Strong IT audit relevance, good skill overlap, remote or LA hybrid
- 50–69: Decent audit/compliance role, partial criteria match
- 30–49: Some overlap but missing key criteria (wrong location, seniority, partial relevance)
- 0–29: Poor fit (wrong field, clearance required, sponsorship required, fully on-site outside LA)

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
# Search configuration — 8 passes, NO is_remote filter (unreliable tag)
# We rely on scoring to distinguish remote vs on-site, LA vs other
# ---------------------------------------------------------------------------
def build_search_configs() -> list[dict]:
    return [
        # --- Nationwide: Core IT Audit titles ---
        {
            "label": "Core IT Audit (US)",
            "term":  '"IT Audit" OR "IT Auditor" OR "ITGC" OR "SOX ITGC" OR "IT Controls" OR "IT Governance" OR "IT Assurance"',
            "location": "United States",
            "results": 150,
            "remote": False,
        },
        # --- Nationwide: SOX + GRC specializations ---
        {
            "label": "SOX / GRC (US)",
            "term":  '"SOX Analyst" OR "SOX Auditor" OR "SOX Compliance" OR "GRC Analyst" OR "GRC Consultant" OR "GRC Specialist" OR "GRC Manager"',
            "location": "United States",
            "results": 150,
            "remote": False,
        },
        # --- Nationwide: Technology risk & compliance titles ---
        {
            "label": "Tech Risk / Compliance Analyst (US)",
            "term":  '"Technology Audit" OR "Technology Risk" OR "IT Risk" OR "IT Compliance" OR "IS Auditor" OR "Cyber Audit" OR "Cloud Audit" OR "Compliance Analyst"',
            "location": "United States",
            "results": 125,
            "remote": False,
        },
        # --- Nationwide: Controls & internal audit ---
        {
            "label": "Controls / Internal Audit (US)",
            "term":  '"Controls Analyst" OR "Audit Analyst" OR "Audit Manager" OR "Internal Audit" OR "Audit Consultant" OR "Application Controls" OR "ERP Audit"',
            "location": "United States",
            "results": 125,
            "remote": False,
        },
        # --- Nationwide: CISA-required/preferred ---
        # CISA in the search = employer explicitly mentions CISA → near-guaranteed audit role
        {
            "label": "CISA required/preferred (US)",
            "term":  '"CISA required" OR "CISA preferred" OR "CISA certified" OR "CISA certification" OR "CISA or equivalent" OR "CISA, CISM" OR "CISA/CISM"',
            "location": "United States",
            "results": 100,
            "remote": False,
        },
        # --- LA area: broad IT audit sweep (50-mile radius) ---
        {
            "label": "IT Audit — Los Angeles area",
            "term":  '"IT Audit" OR "ITGC" OR "SOX" OR "GRC" OR "IT Compliance" OR "IT Risk" OR "IT Controls" OR "IT Governance" OR "CISA"',
            "location": "Los Angeles, CA",
            "results": 100,
            "remote": False,
            "distance": 50,
        },
        # --- LA area: GRC, compliance, controls titles ---
        {
            "label": "GRC / Compliance / Controls — LA area",
            "term":  '"GRC Analyst" OR "GRC Consultant" OR "Compliance Analyst" OR "Controls Analyst" OR "Audit Analyst" OR "SOX Analyst" OR "Audit Manager" OR "IS Auditor"',
            "location": "Los Angeles, CA",
            "results": 75,
            "remote": False,
            "distance": 50,
        },
        # --- Orange County (Irvine hub — major financial/tech market) ---
        {
            "label": "IT Audit / GRC — Orange County",
            "term":  '"IT Audit" OR "SOX" OR "GRC" OR "ITGC" OR "IT Compliance" OR "IT Risk" OR "CISA" OR "Compliance Analyst" OR "Audit Analyst"',
            "location": "Irvine, CA",
            "results": 75,
            "remote": False,
            "distance": 40,
        },
    ]


# ---------------------------------------------------------------------------
# Main hunt
# ---------------------------------------------------------------------------
def master_hunt():
    sprint(f"\n{'='*60}")
    sprint(f"[Scanner] IT Audit Hunt — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    sprint(f"{'='*60}")
    alerts_sent = 0
    all_frames  = []

    for cfg in build_search_configs():
        sprint(f"\n[Search] {cfg['label']} ({cfg['results']} results)...")
        kwargs = dict(
            site_name=["linkedin", "indeed", "glassdoor", "zip_recruiter"],
            search_term=cfg["term"],
            location=cfg["location"],
            results_wanted=cfg["results"],
            is_remote=cfg.get("remote", False),
        )
        if "distance" in cfg:
            kwargs["distance"] = cfg["distance"]

        try:
            df = scrape_jobs(**kwargs)
            if not df.empty:
                df["_search_pass"] = cfg["label"]
                all_frames.append(df)
                sprint(f"  → {len(df)} raw results")
            else:
                sprint(f"  → 0 results")
            time.sleep(4)   # polite delay between passes
        except Exception as e:
            sprint(f"  [Error] {e}")
            time.sleep(5)

    if not all_frames:
        sprint("[Scanner] No results from any search pass.")
        push_notification("Audit Hunt", "Scan ran — 0 results from all passes.", "low")
        return

    # -----------------------------------------------------------------------
    # Combine + deduplicate
    # -----------------------------------------------------------------------
    raw = pd.concat(all_frames, ignore_index=True)
    sprint(f"\n[Dedup] {len(raw)} total raw → ", end="")

    # Normalize URLs for better dedup (strip tracking params)
    if "job_url" in raw.columns:
        raw["_url_norm"] = (
            raw["job_url"]
            .astype(str)
            .str.split("?").str[0]
            .str.strip()
            .str.lower()
        )
        raw = raw.drop_duplicates(subset=["_url_norm"], keep="first")

    # Also dedup by title+company in case same job appears under different URLs
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

            # Tag label for console
            if score >= 80:   tag = "STRONG"
            elif score >= 70: tag = "HIGH  "
            elif score >= 50: tag = "fair  "
            else:             tag = "low   "

            la_tag = " [LA]" if is_la_area(location) else ""
            sprint(f"  [{score:3d}][{tag}][{method}] {title[:48]:<48} @ {company[:25]}{la_tag}")

            # Push alerts for strong matches
            if score >= 60 and alerts_sent < MAX_ALERTS:
                priority = "urgent" if score >= 82 else "high"
                loc_note = f" | {location}" if location else ""
                push_notification(
                    title=f"{score}% — {title[:48]}",
                    message=f"{company}{loc_note}\n{url}",
                    priority=priority,
                )
                alerts_sent += 1

            # Save only above threshold
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
        # Dedup on Link — keep most recent score
        combined = combined.drop_duplicates(subset=["Link"], keep="last")
    else:
        combined = new_df

    combined = combined.sort_values("Score", ascending=False)
    combined.to_csv(CSV_PATH, index=False)

    # Persist to GitHub so data survives Streamlit Cloud redeployments
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
