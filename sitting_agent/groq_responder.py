"""
Custom question answering via Groq.
Reuses the existing GROQ_API_KEY — no additional setup required.
"""

import os
import requests

GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL    = "llama-3.3-70b-versatile"
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"


def ask_groq(question: str, profile: dict, job: dict = None) -> str:
    """
    Generate a concise, professional answer to a custom application question.
    Includes job context so answers are role-specific.
    Falls back to a clear placeholder if the API is unavailable.
    """
    if not GROQ_API_KEY:
        return f"[GROQ_API_KEY not set — answer manually: {question[:80]}]"

    job_context = ""
    if job:
        title   = job.get("Title", "")
        company = job.get("Company", "")
        if title or company:
            job_context = f"Applying for: {title} at {company}.\n\n"

    prompt = (
        f"You are filling a job application on behalf of {profile['name']}.\n\n"
        f"{job_context}"
        f"Candidate background:\n{profile.get('custom_question_context', '')}\n\n"
        f"Answer the following application question concisely and professionally "
        f"(2–4 sentences maximum). Write in first person as the candidate. "
        f"Be specific — mention relevant experience, certifications, or skills.\n\n"
        f'Question: "{question}"\n\n'
        f"Return ONLY the answer text. No preamble, no labels, no explanation."
    )

    try:
        r = requests.post(
            GROQ_ENDPOINT,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       GROQ_MODEL,
                "messages":    [{"role": "user", "content": prompt}],
                "max_tokens":  300,
                "temperature": 0.3,
            },
            timeout=25,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"[Groq unavailable — answer manually: {question[:60]}]"
