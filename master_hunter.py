import os
import re
import time
import requests
import pandas as pd
from jobspy import scrape_jobs
from google import genai

# --- CONFIG ---
GEMINI_KEY   = os.getenv("GEMINI_API_KEY")
NTFY_TOPIC   = os.getenv("NTFY_TOPIC", "dj-audit-hunt")   # set your own secret topic name
CSV_PATH     = "Scored_Audit_Leads.csv"

client = genai.Client(api_key=GEMINI_KEY)

MANDATORY = [
    "audit", "sox", "itgc", "compliance", "cisa",
    "controls", "grc", "risk", "nist", "governance"
]

# ---------------------------------------------------------------------------
def push_notification(title: str, message: str, priority: str = "default"):
    """Send a push notification to phone via ntfy.sh (free, no account needed).
    Install the free 'ntfy' app on iOS/Android and subscribe to your NTFY_TOPIC."""
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title":    title,
                "Priority": priority,   # urgent | high | default | low | min
                "Tags":     "briefcase",
            },
            timeout=10,
        )
    except Exception as e:
        print(f"[Push Error] {e}")


# ---------------------------------------------------------------------------
def score_job(desc: str) -> int:
    prompt = f"""Rate 0-100 fit for this candidate:
- EAD-authorized (J2 visa), no sponsorship needed, authorized for W2 employment
- 8 years IT Audit experience (EY background), open to ALL seniority levels
- CISA certified, SAP S/4HANA, SOX ITGC, cloud audit, AWS Cloud Practitioner
- Targeting: remote W2 contract or direct hire positions in the United States
- Skills: IT Audit, ITGC, SOX, GRC, internal controls, cloud security, AI governance

Job description:
{desc[:2500]}

Return ONLY a single integer 0-100. No explanation, no punctuation."""
    res = client.models.generate_content(model="gemini-2.0-flash-lite", contents=prompt)
    match = re.search(r"\b(\d{1,3})\b", res.text)
    return min(int(match.group(1)), 100) if match else 0


# ---------------------------------------------------------------------------
def master_hunt():
    print("[Scanner] Starting IT audit contract scan...")
    alerts_sent = 0

    try:
        df = scrape_jobs(
            site_name=["linkedin", "indeed", "zip_recruiter"],
            search_term=(
                '"IT Audit" OR "ITGC" OR "SOX Auditor" OR "GRC Auditor" '
                'OR "IT Compliance" OR "Internal Audit" contract remote'
            ),
            location="United States",
            results_wanted=60,
            is_remote=True,
        )
        if df.empty:
            print("[Scanner] No results returned.")
            push_notification("Audit Hunt", "Scan ran — 0 results from job boards.", "low")
            return

        df["title_lower"] = df["title"].str.lower().fillna("")
        df["desc_lower"]  = df["description"].str.lower().fillna("")

        mask = (
            df["title_lower"].apply(lambda x: any(k in x for k in MANDATORY)) |
            df["desc_lower"].apply(lambda x: any(k in x for k in MANDATORY))
        )
        filtered = df[mask].copy()

        if "date_posted" in filtered.columns:
            filtered = filtered.sort_values("date_posted", ascending=False)

        print(f"[Scanner] {len(filtered)} jobs passed keyword filter. Scoring top 25...")

        scored_list = []
        for _, row in filtered.head(25).iterrows():
            desc = str(row.get("description", ""))
            try:
                score = score_job(desc)
                title   = row.get("title", "Unknown")
                company = row.get("company", "")
                print(f"  [{score:3d}] {title} @ {company}")

                if score >= 50 and alerts_sent < 3:
                    push_notification(
                        title=f"High Match {score}% — {title}",
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
                })
            except (ValueError, AttributeError) as e:
                print(f"  [Skip] {row.get('title', '?')}: {e}")
            time.sleep(1.2)   # respect Gemini rate limits

        if not scored_list:
            print("[Scanner] No jobs scored.")
            return

        new_df = pd.DataFrame(scored_list)

        # Append + deduplicate
        if os.path.exists(CSV_PATH):
            existing = pd.read_csv(CSV_PATH)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["Link"], keep="last")
        else:
            combined = new_df

        combined.sort_values("Score", ascending=False).to_csv(CSV_PATH, index=False)
        print(f"[Scanner] Done. {len(new_df)} new, {len(combined)} total leads.")

        push_notification(
            title="Scan Complete",
            message=f"{len(new_df)} new leads scored. {alerts_sent} high-match alerts sent.",
            priority="low",
        )

    except Exception as e:
        print(f"[Fatal] {e}")
        push_notification("Scan Error", str(e), priority="urgent")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    master_hunt()
