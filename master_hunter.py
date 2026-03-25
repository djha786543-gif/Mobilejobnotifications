import os
import re
import sys
import time
import requests
import pandas as pd
from jobspy import scrape_jobs

# Safe print for Windows terminals that can't handle all Unicode
def sprint(*args, **kwargs):
    text = " ".join(str(a) for a in args)
    safe = text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
           sys.stdout.encoding or "utf-8", errors="replace")
    __builtins__["print"](safe, **kwargs) if isinstance(__builtins__, dict) else print(safe, **kwargs)

# --- CONFIG ---
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")
NTFY_TOPIC     = os.getenv("NTFY_TOPIC", "dj-audit-786543")
CSV_PATH       = "Scored_Audit_Leads.csv"
MODEL          = "google/gemma-3-12b-it:free"

# Jobs MUST have at least one of these in the TITLE to be considered
TITLE_KEYWORDS = [
    "it audit", "it auditor", "itgc", "sox", "grc", "internal audit",
    "information technology audit", "it compliance", "it risk",
    "cyber audit", "security audit", "cloud audit", "it governance",
    "compliance analyst", "risk analyst", "controls analyst",
    "audit manager", "audit lead", "audit consultant", "audit specialist",
    "audit analyst", "sox analyst", "sox auditor", "it controls",
    "information security audit", "is auditor", "it assurance",
    "technology audit", "technology risk", "technology compliance",
    "governance risk", "risk compliance", "grc analyst", "grc consultant",
    "grc specialist", "grc manager", "compliance manager", "compliance lead",
    "it risk analyst", "it risk manager", "cyber risk", "cyber compliance",
    "sox compliance", "sox controls", "controls testing", "controls analyst",
]

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
def keyword_score(title: str, desc: str) -> int:
    """Keyword-based scorer — no API needed, tuned for DJ's IT Audit profile."""
    t = title.lower()
    d = (title + " " + desc).lower()
    score = 20  # base — passed title filter, so it's at least relevant

    # --- Title signals (high weight) ---
    title_core = ["it audit", "itgc", "sox", "it compliance", "it risk", "it governance"]
    title_good = ["grc", "internal audit", "audit manager", "audit lead",
                  "compliance analyst", "controls analyst", "audit consultant",
                  "cyber audit", "cloud audit", "security audit"]
    score += sum(12 for k in title_core if k in t)
    score += sum(8  for k in title_good if k in t)
    score = min(score, 60)  # cap title contribution

    # --- Description keyword signals ---
    desc_keywords = {
        "itgc": 8, "sox": 7, "cisa": 6, "s/4hana": 5, "sap": 4,
        "grc": 5, "internal controls": 5, "control testing": 6,
        "audit program": 5, "risk assessment": 4, "it audit": 6,
        "cloud audit": 5, "aws": 3, "azure": 3,
        "governance": 3, "compliance testing": 5, "segregation of duties": 6,
        "access controls": 5, "change management": 3, "ey ": 4, "big 4": 4,
    }
    desc_score = sum(v for k, v in desc_keywords.items() if k in d)
    score += min(desc_score, 35)

    # --- Work type & remote bonuses ---
    if any(k in d for k in ["contract ", "contractor", " w2 ", "w2 contract", "c2c"]): score += 10
    if any(k in d for k in ["remote", "work from home", "wfh"]): score += 8
    if "ead" in d or "no sponsorship" in d or "authorized to work" in d: score += 5

    # --- Seniority penalties ---
    if any(k in t for k in ["vp ", "vice president", "chief ", "cto ", "ciso ", "cfo "]): score -= 20
    if "head of" in t: score -= 15
    if "director" in t: score -= 10
    if any(k in t for k in ["sr. director", "senior director"]): score -= 5  # extra

    # --- Hard disqualifiers ---
    if "require" in d and "sponsor" in d and "no sponsorship" not in d: score -= 20
    if any(k in d for k in ["secret clearance", "top secret", "ts/sci", "poly"]): score -= 25
    if any(k in d for k in ["must be on-site", "onsite only", "must be local",
                             "no remote", "not remote"]): score -= 20

    return max(0, min(score, 100))


def llm_score(desc: str) -> int | None:
    """Returns score from LLM, or None if unavailable."""
    if not OPENROUTER_KEY:
        return None

    prompt = f"""Rate 0-100 fit for this candidate:
- EAD-authorized (J2 visa), no sponsorship needed, authorized for W2 employment
- 8 years IT Audit experience (Big 4 / EY background), open to ALL seniority levels
- CISA certified, SAP S/4HANA, SOX ITGC, cloud audit, AWS Cloud Practitioner
- Targeting: remote W2 contract or direct hire in the United States
- Skills: IT Audit, ITGC, SOX, GRC, internal controls, cloud security, AI governance

Job description:
{desc[:2500]}

Return ONLY a single integer 0-100. No explanation."""

    for attempt in range(2):
        try:
            r = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENROUTER_KEY}",
                         "Content-Type": "application/json"},
                json={"model": MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 10},
                timeout=20,
            )
            if r.status_code == 429:
                if attempt == 0:
                    sprint("  [Rate limit] Waiting 15s...")
                    time.sleep(15)
                    continue
                return None
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()
            match = re.search(r"\b(\d{1,3})\b", text)
            return min(int(match.group(1)), 100) if match else None
        except Exception:
            return None
    return None


def score_job(title: str, desc: str) -> tuple[int, str]:
    """Returns (score, method) where method is 'llm' or 'keyword'."""
    llm = llm_score(desc)
    if llm is not None:
        return llm, "llm"
    return keyword_score(title, desc), "keyword"


# ---------------------------------------------------------------------------
def master_hunt():
    sprint("[Scanner] Starting IT Audit contract scan...")
    alerts_sent = 0

    try:
        df = scrape_jobs(
            site_name=["linkedin", "indeed"],
            search_term=(
                '"IT Audit" OR "ITGC" OR "SOX" OR "GRC" OR "IT Compliance" '
                'OR "Internal Audit" OR "IT Risk" OR "Technology Audit" '
                'OR "IT Controls" OR "Cyber Audit" OR "IT Governance"'
            ),
            location="United States",
            results_wanted=100,
            is_remote=True,
        )
        if df.empty:
            sprint("[Scanner] No results returned from job boards.")
            push_notification("Audit Hunt", "Scan ran — 0 results from job boards.", "low")
            return

        df["title_lower"] = df["title"].str.lower().fillna("")
        df["desc_lower"]  = df["description"].str.lower().fillna("")

        # Strict title-based filter — must be an actual audit/compliance role
        mask = df["title_lower"].apply(
            lambda t: any(k in t for k in TITLE_KEYWORDS)
        )
        filtered = df[mask].copy()
        sprint(f"[Scanner] {len(df)} scraped → {len(filtered)} passed title filter.")

        if filtered.empty:
            push_notification("Audit Hunt", "Scan ran — 0 relevant titles found.", "low")
            return

        if "date_posted" in filtered.columns:
            filtered = filtered.sort_values("date_posted", ascending=False)

        sprint(f"[Scanner] Scoring top {min(50, len(filtered))} jobs...")

        scored_list = []
        for _, row in filtered.head(50).iterrows():
            title   = str(row.get("title", "Unknown"))
            desc    = str(row.get("description", ""))
            company = str(row.get("company", ""))
            try:
                score, method = score_job(title, desc)
                sprint(f"  [{score:3d}][{method}] {title} @ {company}")

                if score >= 50 and alerts_sent < 5:
                    push_notification(
                        title=f"Match {score}% - {title[:50]}",
                        message=f"{company}\n{row.get('job_url', '')}",
                        priority="high",
                    )
                    alerts_sent += 1

                scored_list.append({
                    "Score":    score,
                    "Title":    title,
                    "Company":  company,
                    "Location": row.get("location", "Remote"),
                    "Type":     row.get("job_type", ""),
                    "Link":     row.get("job_url", ""),
                    "Posted":   str(row.get("date_posted", "")),
                    "ScoredBy": method,
                })
            except Exception as e:
                sprint(f"  [Error] {title}: {e}")
            time.sleep(2)

        if not scored_list:
            sprint("[Scanner] No jobs scored.")
            return

        new_df = pd.DataFrame(scored_list)

        if os.path.exists(CSV_PATH):
            existing = pd.read_csv(CSV_PATH)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["Link"], keep="last")
        else:
            combined = new_df

        combined.sort_values("Score", ascending=False).to_csv(CSV_PATH, index=False)
        high = len(new_df[new_df["Score"] >= 70])
        sprint(f"[Scanner] Done. {len(new_df)} new leads ({high} high-match). {len(combined)} total.")

        push_notification(
            title="Scan Complete",
            message=f"{len(new_df)} new leads. {high} high-match (70+). {alerts_sent} alerts sent.",
            priority="low",
        )

    except Exception as e:
        sprint(f"[Fatal] {e}")
        push_notification("Scan Error", str(e)[:200], priority="urgent")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    master_hunt()
