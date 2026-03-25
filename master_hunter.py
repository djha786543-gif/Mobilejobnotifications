import os
import re
import sys
import time
import requests
import pandas as pd
from datetime import datetime
from jobspy import scrape_jobs

# Safe print for Windows terminals that can't handle all Unicode
def sprint(*args, **kwargs):
    text = " ".join(str(a) for a in args)
    safe = text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
           sys.stdout.encoding or "utf-8", errors="replace")
    print(safe, **kwargs)

# --- CONFIG ---
OPENROUTER_KEY  = os.getenv("OPENROUTER_API_KEY")
NTFY_TOPIC      = os.getenv("NTFY_TOPIC", "dj-audit-786543")
CSV_PATH        = "Scored_Audit_Leads.csv"
MODEL           = "google/gemma-3-12b-it:free"
MIN_SAVE_SCORE  = 35   # jobs below this threshold are NOT saved to CSV
MAX_ALERTS      = 8    # max push alerts per scan run
SCORE_TOP_N     = 60   # score top N after filtering

# ---------------------------------------------------------------------------
# Title must match at least one of these patterns (regex, word-boundary safe)
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
    r"\btech(?:nology)?\s+risk",
    r"\binformation\s+(?:technology\s+)?audit",
    r"\bis\s+auditor",
    r"\bit\s+assurance",
    r"\bit\s+controls",
    r"\bcontrols?\s+(?:analyst|testing|assessment|review)",
    r"\bsox\s+(?:analyst|auditor|compliance|controls|testing)",
    r"\bsox\s+itgc",
    r"\bgrc\s+(?:analyst|consultant|specialist|manager|lead)",
    r"\bit\s+risk\s+(?:analyst|manager|consultant)",
    r"\baudit\s+(?:analyst|manager|lead|consultant|specialist|senior|associate)",
    r"\binternal\s+audit\b",
    r"\bcompliance\s+(?:analyst|manager|lead|specialist|consultant)",
    r"\bcyber\s+(?:risk|compliance|governance)",
    r"\bgovernance\s+risk\s+(?:and\s+)?compliance",
    r"\brisk\s+(?:and\s+)?compliance\s+(?:analyst|manager|specialist|consultant)",
    r"\bsecurity\s+(?:compliance|governance|controls)",
]

# Hard title blacklist — even if a whitelist word matches, reject these roles
# Exception: if 'audit', 'itgc', or 'sox' explicitly in title we trust it
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
    r"\bfinancial\s+analyst\b",          # not IT-focused
    r"\bhr\b|\bhuman\s+resources\b",
    r"\bphysician\b|\bnurse\b|\bclinical\b",
    r"\boperations\s+(?:manager|analyst|specialist)(?!\s+(?:risk|compliance))",
    r"\bdevops\b",
    r"\bnetwork\s+engineer\b",
    r"\bsystem\s+(?:engineer|administrator)\b",
]

def matches_title(title: str) -> bool:
    """Two-stage title filter: whitelist match required, blacklist veto."""
    t = title.lower()

    # Stage 1: must hit at least one whitelist pattern
    if not any(re.search(p, t) for p in TITLE_WHITELIST):
        return False

    # Stage 2: explicit audit/ITGC/SOX in title overrides blacklist
    if re.search(r"\baudit\b|\bitgc\b|\bsox\b", t):
        return True

    # Stage 3: blacklist veto for everything else
    if any(re.search(p, t) for p in TITLE_BLACKLIST):
        return False

    return True


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
# Keyword scorer — tuned for DJ's IT Audit / EAD / remote profile
# ---------------------------------------------------------------------------
def keyword_score(title: str, desc: str) -> int:
    t = title.lower()
    d = (title + " " + desc).lower()
    score = 22  # base — passed title filter

    # --- Title signals (high weight, capped) ---
    title_core = {
        "it audit": 15, "it auditor": 15, "itgc": 14, "sox": 12,
        "it compliance": 13, "it risk": 11, "it governance": 11,
        "it controls": 10, "it assurance": 11,
    }
    title_good = {
        "grc": 9, "internal audit": 8, "audit manager": 8, "audit lead": 8,
        "audit analyst": 8, "audit consultant": 9, "audit specialist": 8,
        "compliance analyst": 7, "controls analyst": 8, "controls testing": 9,
        "cyber audit": 9, "cloud audit": 9, "security audit": 9,
        "sox analyst": 10, "sox auditor": 10, "sox compliance": 9,
        "grc analyst": 10, "grc consultant": 10, "grc specialist": 9,
        "grc manager": 8, "technology audit": 9, "tech risk": 8,
        "information security": 7, "cyber risk": 8, "cyber compliance": 8,
    }
    title_pts = sum(v for k, v in title_core.items() if k in t)
    title_pts += sum(v for k, v in title_good.items() if k in t)
    score += min(title_pts, 30)   # cap title contribution

    # --- Description keyword signals ---
    desc_kw = {
        "itgc": 9, "sox": 8,
        "cisa required": 12, "cisa preferred": 10, "cisa certified": 10,
        "cisa certification": 10, "cisa or equivalent": 10, "cisa": 7,
        "cissp": 5, "cism": 5,
        "s/4hana": 7, "sap": 5, "grc": 6, "internal controls": 6,
        "control testing": 7, "controls testing": 7, "audit program": 6,
        "risk assessment": 5, "it audit": 7, "cloud audit": 6,
        "aws": 3, "azure": 3, "gcp": 3,
        "governance": 4, "compliance testing": 6,
        "segregation of duties": 8, "sod": 6,
        "access controls": 6, "privileged access": 6, "iam": 5,
        "change management": 4, "incident management": 4,
        "big 4": 5, "ey ": 5, "deloitte": 4, "pwc": 4, "kpmg": 4,
        "ai governance": 6, "ai audit": 6, "nist": 4, "iso 27001": 5,
        "hipaa": 4, "pci": 5, "gdpr": 4, "ccpa": 4,
        "third party risk": 5, "tprm": 5, "vendor risk": 4,
        "contract": 5, "c2c": 5, "corp-to-corp": 5, "1099": 4,
    }
    desc_pts = sum(v for k, v in desc_kw.items() if k in d)
    score += min(desc_pts, 35)

    # --- Work type & authorization bonuses ---
    if any(k in d for k in ["contract ", "contractor", " w2 ", "w2 contract", "c2c",
                             "corp to corp", "corp-to-corp", "1099"]):
        score += 10
    if any(k in d for k in ["remote", "work from home", "wfh", "fully remote"]):
        score += 8
    if any(k in d for k in ["ead", "no sponsorship required", "authorized to work",
                             "work authorization", "gc holders", "green card"]):
        score += 5

    # --- Seniority adjustments ---
    # Senior / lead / manager are fine for DJ's profile
    if re.search(r"\bsenior\b|\bsr\.\b|\blead\b", t):
        score += 3  # slight boost — competitive pay
    if re.search(r"\bmanager\b|\bmanagement\b", t) and "audit" in t:
        score += 0  # neutral — still relevant
    if re.search(r"\bstaff\b|\bjunior\b|\bassociate\b", t):
        score -= 5  # too junior for 8-yr experience

    # Hard seniority penalties — these roles are out of scope
    if re.search(r"\bvp\b|\bvice\s+president\b", t):         score -= 30
    if re.search(r"\bchief\b|\bcto\b|\bciso\b|\bcfo\b", t):  score -= 35
    if re.search(r"\bhead\s+of\b", t):                        score -= 25
    if re.search(r"\bdirector\b", t):                         score -= 20
    if re.search(r"\bpartner\b", t) and "audit" in t:         score -= 15
    if re.search(r"\bprincipal\b", t):                        score -= 10

    # --- Hard disqualifiers ---
    if re.search(r"requir.{0,20}sponsor", d) and "no sponsorship" not in d:
        score -= 25
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci",
                             "polygraph", "poly clearance", "active clearance"]):
        score -= 30
    if any(k in d for k in ["must be on-site", "onsite only", "must be local",
                             "no remote", "not eligible for remote",
                             "in-office required", "on site only"]):
        score -= 20

    return max(0, min(score, 100))


# ---------------------------------------------------------------------------
# LLM scorer (OpenRouter)
# ---------------------------------------------------------------------------
def llm_score(desc: str) -> int | None:
    if not OPENROUTER_KEY:
        return None

    prompt = f"""Rate 0–100 fit for this candidate:
Candidate profile:
- EAD-authorized (J2 visa), no sponsorship needed, authorized for W2 employment
- 8 years IT Audit experience — Big 4 / EY background
- CISA certified, AWS Cloud Practitioner
- Core skills: SOX ITGC, IT Audit, GRC, internal controls, SAP S/4HANA, cloud audit, AI governance
- Targeting: remote W2 contract or direct hire in the United States
- Open to all seniority levels (analyst → manager), prefer contract/consulting

Rate this job description:
{desc[:2800]}

Scoring guide:
- 90–100: Perfect IT Audit/ITGC/SOX/GRC contract or direct hire, fully remote, EAD-ok
- 70–89: Strong IT audit relevance, mostly remote, good skill overlap
- 50–69: Decent audit/compliance role but missing some key criteria
- 30–49: Some overlap but off-target (wrong seniority, partial relevance, on-site)
- 0–29: Poor fit (wrong field, requires clearance, requires sponsorship, on-site)

Return ONLY a single integer 0–100. No explanation."""

    for attempt in range(3):
        try:
            r = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENROUTER_KEY}",
                         "Content-Type": "application/json"},
                json={"model": MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 10},
                timeout=25,
            )
            if r.status_code == 429:
                wait = 20 if attempt == 0 else 40
                sprint(f"  [Rate limit] Waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()
            match = re.search(r"\b(\d{1,3})\b", text)
            if match:
                return min(int(match.group(1)), 100)
            return None
        except Exception as e:
            sprint(f"  [LLM error attempt {attempt+1}]: {e}")
            if attempt < 2:
                time.sleep(5)
    return None


def score_job(title: str, desc: str) -> tuple[int, str]:
    llm = llm_score(desc)
    if llm is not None:
        return llm, "llm"
    kw = keyword_score(title, desc)
    return kw, "keyword"


# ---------------------------------------------------------------------------
# Main hunt
# ---------------------------------------------------------------------------
def master_hunt():
    sprint(f"[Scanner] Starting IT Audit scan — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    alerts_sent = 0

    # Three targeted searches for maximum coverage
    search_configs = [
        {
            "term": (
                '"IT Audit" OR "ITGC" OR "SOX ITGC" OR "IT Compliance" '
                'OR "IT Risk" OR "IT Controls" OR "IT Governance"'
            ),
            "remote": True,
        },
        {
            "term": (
                '"GRC Analyst" OR "GRC Consultant" OR "GRC Specialist" '
                'OR "SOX Analyst" OR "SOX Auditor" OR "Audit Analyst" '
                'OR "Technology Audit" OR "Cyber Audit" OR "Cloud Audit"'
            ),
            "remote": True,
        },
        {
            # CISA-required roles are almost always IT audit/GRC — very targeted
            "term": (
                '"CISA required" OR "CISA preferred" OR "CISA certified" '
                'OR "CISA certification" OR "CISA or equivalent"'
            ),
            "remote": True,
        },
    ]

    all_frames = []
    for cfg in search_configs:
        try:
            df = scrape_jobs(
                site_name=["linkedin", "indeed"],
                search_term=cfg["term"],
                location="United States",
                results_wanted=75,
                is_remote=cfg["remote"],
            )
            if not df.empty:
                all_frames.append(df)
                sprint(f"  [Search] '{cfg['term'][:60]}...' → {len(df)} raw results")
            time.sleep(3)
        except Exception as e:
            sprint(f"  [Search error] {e}")

    if not all_frames:
        sprint("[Scanner] No results returned from any job board.")
        push_notification("Audit Hunt", "Scan ran — 0 results from job boards.", "low")
        return

    raw = pd.concat(all_frames, ignore_index=True)

    # Deduplicate by URL before any processing
    if "job_url" in raw.columns:
        raw = raw.drop_duplicates(subset=["job_url"], keep="first")

    raw["title_lower"] = raw["title"].str.lower().fillna("")
    raw["desc_lower"]  = raw["description"].str.lower().fillna("")

    # Strict two-stage title filter
    mask = raw["title_lower"].apply(matches_title)
    filtered = raw[mask].copy()
    sprint(f"[Scanner] {len(raw)} raw → {len(filtered)} passed title filter "
           f"(dropped {len(raw) - len(filtered)})")

    if filtered.empty:
        push_notification("Audit Hunt", "Scan ran — 0 relevant titles found.", "low")
        return

    # Sort by recency, score the most recent SCORE_TOP_N
    if "date_posted" in filtered.columns:
        filtered = filtered.sort_values("date_posted", ascending=False)

    to_score = filtered.head(SCORE_TOP_N)
    sprint(f"[Scanner] Scoring {len(to_score)} jobs...")

    scored_list = []
    for _, row in to_score.iterrows():
        title   = str(row.get("title", "Unknown"))
        desc    = str(row.get("description", ""))
        company = str(row.get("company", "Unknown"))
        url     = str(row.get("job_url", ""))

        try:
            score, method = score_job(title, desc)
            label = "HIGH" if score >= 70 else ("OK" if score >= 50 else "low")
            sprint(f"  [{score:3d}][{method}][{label}] {title[:55]} @ {company[:30]}")

            # Push alert for high-match jobs
            if score >= 60 and alerts_sent < MAX_ALERTS:
                priority = "urgent" if score >= 80 else "high"
                push_notification(
                    title=f"{score}% Match — {title[:45]}",
                    message=f"{company}\n{url}",
                    priority=priority,
                )
                alerts_sent += 1

            # Only save jobs above min threshold
            if score >= MIN_SAVE_SCORE:
                scored_list.append({
                    "Score":    score,
                    "Title":    title,
                    "Company":  company,
                    "Location": row.get("location", "Remote"),
                    "Type":     row.get("job_type", ""),
                    "Link":     url,
                    "Posted":   str(row.get("date_posted", "")),
                    "ScoredBy": method,
                    "ScannedAt": datetime.now().strftime("%Y-%m-%d %H:%M"),
                })

        except Exception as e:
            sprint(f"  [Error scoring] {title}: {e}")

        time.sleep(1.5)

    if not scored_list:
        sprint("[Scanner] No jobs met the minimum score threshold.")
        push_notification("Audit Hunt", "Scan done — no jobs met the score threshold.", "low")
        return

    new_df = pd.DataFrame(scored_list)

    # Merge with existing CSV, deduplicate on Link
    if os.path.exists(CSV_PATH):
        existing = pd.read_csv(CSV_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Link"], keep="last")
    else:
        combined = new_df

    combined = combined.sort_values("Score", ascending=False)
    combined.to_csv(CSV_PATH, index=False)

    high   = len(new_df[new_df["Score"] >= 70])
    strong = len(new_df[new_df["Score"] >= 80])
    sprint(f"\n[Scanner] Done. {len(new_df)} new leads saved "
           f"({strong} strong 80+, {high} high 70+). "
           f"{len(combined)} total in CSV. {alerts_sent} alerts sent.")

    push_notification(
        title="Scan Complete",
        message=(
            f"{len(new_df)} new leads | {strong} strong (80+) | {high} high (70+)\n"
            f"{alerts_sent} alerts sent | {len(combined)} total leads"
        ),
        priority="low",
    )


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    master_hunt()
