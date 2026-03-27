"""
Custom question answering via Groq.
Reuses the existing GROQ_API_KEY — no additional setup required.
"""

import os
import requests

GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL    = "llama-3.3-70b-versatile"
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"


def ask_groq(question: str, profile: dict) -> str:
    """
    Generate a concise, professional answer to a custom application question.
    Uses the same GROQ_API_KEY already used for job scoring.
    Falls back to a clear placeholder if the API is unavailable.
    """
    if not GROQ_API_KEY:
        return f"[GROQ_API_KEY not configured — please answer manually: {question[:60]}]"

    prompt = (
        f"You are filling a job application on behalf of {profile['name']}.\n\n"
        f"Candidate background:\n{profile.get('custom_question_context', '')}\n\n"
        f"Answer this application question concisely and professionally (2–3 sentences maximum). "
        f"Write in first person as the candidate:\n"
        f'"{question}"\n\n'
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
                "max_tokens":  250,
                "temperature": 0.3,
            },
            timeout=20,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"[Groq unavailable ({e}) — please answer manually]"
