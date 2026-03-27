#!/usr/bin/env python3
"""
Sitting Agent — main entry point (CLI script).

Launched by the Streamlit UI via subprocess.Popen.
Opens a headed (visible) Chrome browser, fills all form fields,
then STOPS on the review page — the human reviews and clicks Submit.

Usage:
    python sitting_agent/browser_agent.py \
        --job-file  /tmp/agent_job_xxx.json \
        --profile-file profiles/deobrat_profile.json

Environment variables (from .env):
    GROQ_API_KEY         — already set; used for custom question answering
    CHROME_PROFILE_PATH  — path to your local Chrome profile directory
                           macOS:   ~/Library/Application Support/Google/Chrome/Default
                           Windows: C:/Users/YOU/AppData/Local/Google/Chrome/User Data/Default
                           Leave blank to launch a clean browser (you may need to log in manually)
"""

import argparse
import json
import os
import sys

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("\n" + "=" * 60)
    print("  ERROR: playwright is not installed.")
    print("  Run these two commands once:")
    print("    pip install playwright")
    print("    playwright install chromium")
    print("=" * 60 + "\n")
    sys.exit(1)

from sitting_agent.form_filler import fill_application, stop_on_review


def main():
    parser = argparse.ArgumentParser(description="Sitting Agent — fills job applications")
    parser.add_argument("--job-file",     required=True, help="Path to temp JSON file with job data")
    parser.add_argument("--profile-file", required=True, help="Path to candidate profile JSON")
    args = parser.parse_args()

    # Load job and profile data
    with open(args.job_file) as f:
        job = json.load(f)
    with open(args.profile_file) as f:
        profile = json.load(f)

    job_url   = str(job.get("Link", ""))
    job_title = str(job.get("Title", "Unknown"))
    job_co    = str(job.get("Company", "Unknown"))

    chrome_profile = os.path.expanduser(
        os.getenv("CHROME_PROFILE_PATH", "")
    )

    print("\n" + "=" * 65)
    print(f"  🤖  SITTING AGENT — {profile['name']}")
    print(f"  Job  : {job_title} @ {job_co}")
    print(f"  URL  : {job_url}")
    print(f"  Mode : {'Using your Chrome profile (cookies preserved)' if chrome_profile and os.path.exists(chrome_profile) else 'Fresh browser (no saved cookies)'}")
    print("=" * 65 + "\n")

    if not job_url:
        print("[Agent] ERROR: Job has no URL. Cannot continue.")
        sys.exit(1)

    with sync_playwright() as p:
        # ── Browser launch ──────────────────────────────────────────────
        if chrome_profile and os.path.exists(chrome_profile):
            # Persistent context = your real Chrome profile with saved logins/cookies
            # This is the lowest-risk launch mode — you're already logged in to LinkedIn
            print(f"[Agent] Launching Chrome with your profile...")
            browser_ctx = p.chromium.launch_persistent_context(
                user_data_dir=chrome_profile,
                headless=False,
                slow_mo=1000,            # 1 second between every action (human-like)
                args=[
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",  # reduce bot signals
                ],
            )
            page = browser_ctx.new_page()
        else:
            print("[Agent] ⚠️  No Chrome profile set — launching fresh browser.")
            print("[Agent]    You may need to log in to LinkedIn manually.")
            print("[Agent]    Set CHROME_PROFILE_PATH in your .env to avoid this.")
            browser = p.chromium.launch(
                headless=False,
                slow_mo=1000,
                args=[
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            browser_ctx = browser.new_context(
                viewport={"width": 1400, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = browser_ctx.new_page()

        # ── Navigate and fill ───────────────────────────────────────────
        try:
            print(f"[Agent] Navigating to job page...")
            page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            fill_application(page, profile, job_url)

        except KeyboardInterrupt:
            print("\n[Agent] Stopped by user (Ctrl+C).")
            stop_on_review(page)

        except Exception as e:
            print(f"\n[Agent] Unexpected error: {e}")
            print("[Agent] Browser left open for manual review.")
            stop_on_review(page)

        finally:
            try:
                browser_ctx.close()
            except Exception:
                pass

    print("\n[Agent] Browser closed. Session complete.")


if __name__ == "__main__":
    main()
