#!/usr/bin/env python3
"""
Sitting Agent -- main entry point.

Strategy: uses a DEDICATED Chrome profile stored at  DJmobile/agent_chrome_data/
  - Never conflicts with your regular Chrome (separate user-data-dir)
  - No file-locking issues -- the agent owns its own profile directory
  - First launch: Chrome opens, you log in to LinkedIn once, session is saved
  - Every subsequent launch: already logged in, goes straight to the job

Usage:
    python sitting_agent/browser_agent.py \
        --job-file  /tmp/agent_job_xxx.json \
        --profile-file profiles/deobrat_profile.json
"""

import argparse
import csv
import datetime
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from playwright.sync_api import sync_playwright, Error as PlaywrightError
except ImportError:
    print("\n[Agent] ERROR: playwright is not installed.")
    print("  Run: pip install playwright && playwright install chrome")
    sys.exit(1)

try:
    from playwright_stealth import Stealth
    _STEALTH = True
except ImportError:
    _STEALTH = False

from sitting_agent.form_filler import fill_application, stop_on_review

# Dedicated profile stored inside the project -- never touches your main Chrome
_PROJECT_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT_PROFILE   = os.path.join(_PROJECT_ROOT, "agent_chrome_data")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-file",     required=True)
    parser.add_argument("--profile-file", required=True)
    args = parser.parse_args()

    with open(args.job_file) as f:
        job = json.load(f)
    with open(args.profile_file) as f:
        profile = json.load(f)

    job_url   = str(job.get("Link", ""))
    job_title = str(job.get("Title", "Unknown"))
    job_co    = str(job.get("Company", "Unknown"))

    if not job_url:
        print("[Agent] ERROR: Job has no URL.")
        sys.exit(1)

    os.makedirs(AGENT_PROFILE, exist_ok=True)
    first_run = not os.path.exists(os.path.join(AGENT_PROFILE, "Default", "Preferences"))

    print("\n" + "=" * 65)
    print(f"  SITTING AGENT -- {profile['name']}")
    print(f"  Job   : {job_title} @ {job_co}")
    print(f"  URL   : {job_url}")
    print(f"  Setup : {'First run -- you will log in to LinkedIn once' if first_run else 'Session restored -- already logged in'}")
    print("=" * 65 + "\n")

    if first_run:
        print("[Agent] FIRST-TIME SETUP:")
        print("[Agent]   A new Chrome window will open.")
        print("[Agent]   Log in to LinkedIn in that window.")
        print("[Agent]   Then come back here and press Enter to continue.")
        print()

    # ---- stealth config -------------------------------------------------------
    stealth = None
    if _STEALTH:
        stealth = Stealth(
            navigator_webdriver=True,
            navigator_platform_override="Win32",
            navigator_languages_override=("en-US", "en"),
            navigator_vendor_override="Google Inc.",
            chrome_runtime=True,
        )

    try:
        with sync_playwright() as p:
            browser_ctx = p.chromium.launch_persistent_context(
                user_data_dir=AGENT_PROFILE,
                channel="chrome",
                headless=False,
                slow_mo=800,
                args=[
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-infobars",
                ],
                ignore_default_args=["--enable-automation"],
            )

            # Apply stealth to every page in this context
            if stealth:
                try:
                    stealth.apply_stealth_sync(browser_ctx)
                except Exception as e:
                    print(f"[Agent] Stealth warning: {e}")

            page = browser_ctx.pages[0] if browser_ctx.pages else browser_ctx.new_page()

            # First-run: land on LinkedIn login, wait for human to log in
            if first_run:
                page.goto("https://www.linkedin.com/login",
                          wait_until="domcontentloaded", timeout=60000)
                try:
                    input("[Agent] >>> Log in to LinkedIn in the browser, then press Enter here... ")
                except (EOFError, KeyboardInterrupt):
                    print("[Agent] Cancelled.")
                    browser_ctx.close()
                    return
                print("[Agent] Login acknowledged. Navigating to job page...")

            # Navigate to the job
            try:
                print("[Agent] Navigating to job page...")
                start_time = time.time()
                page.goto(job_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)

                fill_application(page, profile, job, start_time=start_time)

                elapsed = time.time() - start_time
                if elapsed > 300:
                    print(f"[Agent] ⚠️  Session ran {elapsed:.0f}s (> 5 min) — "
                          "review the browser for any missed fields.")

                _log_run(profile, job, page.url)

            except KeyboardInterrupt:
                print("\n[Agent] Stopped by Ctrl+C.")
                _try_stop(page)

            except PlaywrightError as e:
                msg = str(e).lower()
                if "closed" in msg or "target" in msg:
                    print("\n[Agent] Browser was closed. Session ended.")
                else:
                    print(f"\n[Agent] Playwright error: {e}")
                    _try_stop(page)

            except Exception as e:
                print(f"\n[Agent] Unexpected error: {e}")
                _try_stop(page)

            finally:
                try:
                    browser_ctx.close()
                except Exception:
                    pass

    except Exception as e:
        print(f"\n[Agent] Could not launch Chrome: {e}")
        print("[Agent] Make sure Google Chrome is installed.")

    print("\n[Agent] Session complete.")


_ATS_MAP_BROWSER = {
    "linkedin.com":             "LinkedIn",
    "indeed.com":               "Indeed",
    "greenhouse.io":            "Greenhouse",
    "lever.co":                 "Lever",
    "myworkdayjobs.com":        "Workday",
    "workday.com":              "Workday",
    "dayforcehcm.com":          "Dayforce HCM",
    "icims.com":                "iCIMS",
    "taleo.net":                "Taleo (Oracle)",
    "smartrecruiters.com":      "SmartRecruiters",
    "jobvite.com":              "Jobvite",
    "bamboohr.com":             "BambooHR",
    "successfactors.com":       "SAP SuccessFactors",
    "adp.com":                  "ADP",
    "oraclecloud.com":          "Oracle HCM",
    "jazz.co":                  "JazzHR",
    "applytojob.com":           "ApplyToJob",
    "recruitingbypaycor.com":   "Paycor",
    "ultipro.com":              "UKG / UltiPro",
    "breezy.hr":                "Breezy HR",
}

_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "agent_run_log.csv")
_LOG_HEADER = ["timestamp", "candidate", "job_title", "company", "url",
               "platform", "outcome", "fields_filled", "notes"]


def _log_run(profile: dict, job: dict, final_url: str,
             outcome: str = "filled+handed_off",
             fields_filled: int = 0,
             notes: str = ""):
    """Append one row to agent_run_log.csv (create with header if missing)."""
    try:
        url_lower = final_url.lower()
        platform = "Unknown"
        for domain, name in _ATS_MAP_BROWSER.items():
            if domain in url_lower:
                platform = name
                break

        write_header = not os.path.exists(_LOG_PATH)
        with open(_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_LOG_HEADER)
            if write_header:
                writer.writeheader()
            writer.writerow({
                "timestamp":    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "candidate":    profile.get("name", ""),
                "job_title":    job.get("Title", ""),
                "company":      job.get("Company", ""),
                "url":          job.get("Link", "")[:120],
                "platform":     platform,
                "outcome":      outcome,
                "fields_filled": fields_filled,
                "notes":        notes,
            })
        print("[Agent] Run logged → agent_run_log.csv")
    except Exception as e:
        print(f"[Agent] Log write failed (non-critical): {e}")


def _try_stop(page):
    try:
        stop_on_review(page)
    except Exception:
        print("[Agent] Could not show review screen -- browser may have been closed.")


if __name__ == "__main__":
    main()
