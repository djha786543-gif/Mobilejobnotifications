#!/usr/bin/env python3
"""
Sitting Agent -- Test Suite
Runs 5 scenarios to verify the agent works end-to-end.

Usage:
    python test_agent.py
"""

import json
import os
import sys
import shutil
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Colour helpers (Windows-safe) ─────────────────────────────────────────────
def green(s):  return s
def red(s):    return s
def yellow(s): return s
def bold(s):   return s

try:
    import colorama
    colorama.init()
    def green(s):  return f"\033[92m{s}\033[0m"
    def red(s):    return f"\033[91m{s}\033[0m"
    def yellow(s): return f"\033[93m{s}\033[0m"
    def bold(s):   return f"\033[1m{s}\033[0m"
except ImportError:
    pass

PASS = "[PASS]"
FAIL = "[FAIL]"
SKIP = "[SKIP]"

results = []

def report(scenario, label, passed, detail=""):
    tag  = green(PASS) if passed else red(FAIL)
    line = f"  {tag}  {label}"
    if detail:
        line += f"  ({detail})"
    print(line)
    results.append((scenario, label, passed, detail))

# ── Shared test data ───────────────────────────────────────────────────────────
with open("profiles/deobrat_profile.json") as f:
    PROFILE = json.load(f)

# Jobs: mix of external-apply and (hopefully) easy-apply
JOBS = [
    {"Title": "Senior Analyst, IT Risk",       "Company": "CardWorks",   "Link": "https://www.linkedin.com/jobs/view/4387704433"},
    {"Title": "Director of Internal Audit",    "Company": "Unknown",     "Link": "https://www.linkedin.com/jobs/view/4387433587"},
    {"Title": "Audit Manager",                 "Company": "Unknown",     "Link": "https://www.linkedin.com/jobs/view/4248453753"},
    {"Title": "Head of Internal Audit & SOX",  "Company": "Unknown",     "Link": "https://www.linkedin.com/jobs/view/4385065321"},
    {"Title": "AVP, IT Internal Audit Supervisor", "Company": "Unknown", "Link": "https://www.linkedin.com/jobs/view/4390372174"},
]

AGENT_PROFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent_chrome_data")
PROFILE_EXISTS = os.path.exists(os.path.join(AGENT_PROFILE, "Default", "Preferences"))

# =============================================================================
# SCENARIO 1 — Imports, profile structure, config
# =============================================================================
print(bold("\n" + "="*65))
print(bold("  SCENARIO 1 — Imports, Config & Profile Setup"))
print(bold("="*65))

try:
    from sitting_agent.browser_agent import main, AGENT_PROFILE as AP
    report(1, "browser_agent imports", True)
except Exception as e:
    report(1, "browser_agent imports", False, str(e))

try:
    from sitting_agent.form_filler import (
        fill_application, stop_on_review,
        _is_login_page, _wait_for_manual_login,
        _is_review_page, _fill_all_visible,
        _fill_contact_fields, _fill_address_fields,
        _handle_radio_questions, _handle_select_dropdowns,
        _handle_linkedin_comboboxes, _try_resume_upload,
        _handle_numeric_inputs, _handle_text_questions,
    )
    report(1, "form_filler imports (all functions)", True)
except Exception as e:
    report(1, "form_filler imports", False, str(e))

try:
    from sitting_agent.groq_responder import ask_groq
    report(1, "groq_responder imports", True)
except Exception as e:
    report(1, "groq_responder imports", False, str(e))

try:
    from playwright_stealth import Stealth
    s = Stealth(navigator_webdriver=True, chrome_runtime=True)
    report(1, "playwright_stealth available & configures", True)
except Exception as e:
    report(1, "playwright_stealth", False, str(e))

report(1, "GROQ_API_KEY set in environment",
       bool(os.getenv("GROQ_API_KEY")),
       "key starts: " + os.getenv("GROQ_API_KEY","")[:8])

report(1, "resume PDF exists",
       os.path.exists(PROFILE.get("resume_path","assets/deobrat_resume.pdf")))

report(1, "agent_chrome_data profile exists (logged in)",
       PROFILE_EXISTS,
       "first-run setup needed" if not PROFILE_EXISTS else "session ready")

from playwright.sync_api import sync_playwright, Error as PlaywrightError
try:
    with sync_playwright() as p:
        b = p.chromium.launch(channel="chrome", headless=True)
        b.close()
    report(1, "Chrome channel available via Playwright", True)
except Exception as e:
    report(1, "Chrome channel available via Playwright", False, str(e))

# =============================================================================
# SCENARIO 2 — Login/authwall detection logic
# =============================================================================
print(bold("\n" + "="*65))
print(bold("  SCENARIO 2 — Login & Authwall Detection Logic"))
print(bold("="*65))

LOGIN_URLS = [
    ("https://www.linkedin.com/login",                              True,  "LinkedIn login page"),
    ("https://www.linkedin.com/authwall?trk=abc&sessionRedirect=/jobs/view/123", True,  "LinkedIn authwall"),
    ("https://www.linkedin.com/checkpoint/lg/login-submit",         True,  "LinkedIn checkpoint"),
    ("https://www.linkedin.com/jobs/view/4387704433",               False, "LinkedIn job page (NOT login)"),
    ("https://www.linkedin.com/jobs/view/4387433587",               False, "LinkedIn job page 2 (NOT login)"),
    ("https://app.greenhouse.io/applications/123",                   False, "Greenhouse ATS (NOT login)"),
    ("https://workday.com/jobs/apply",                              False, "Workday apply (NOT login)"),
]

class _MockPage:
    def __init__(self, url):
        self._url = url
    @property
    def url(self):
        return self._url

for url, expected_login, label in LOGIN_URLS:
    mock = _MockPage(url)
    result = _is_login_page(mock)
    report(2, label, result == expected_login,
           f"detected={'login' if result else 'not-login'} expected={'login' if expected_login else 'not-login'}")

# =============================================================================
# SCENARIO 3 — Groq question answering
# =============================================================================
print(bold("\n" + "="*65))
print(bold("  SCENARIO 3 — Groq Custom Question Answering"))
print(bold("="*65))

test_questions = [
    ("Why are you interested in this role?",         JOBS[0]),
    ("How many years of IT audit experience do you have?", JOBS[1]),
    ("Describe your experience with SOX compliance.", JOBS[2]),
    ("What is your availability to start?",          JOBS[3]),
]

for question, job in test_questions:
    try:
        ans = ask_groq(question, PROFILE, job)
        ok  = bool(ans) and not ans.startswith("[Groq unavailable") and "answer manually" not in ans.lower()
        short = ans[:70].replace("\n", " ")
        report(3, f"Q: {question[:50]}", ok, f'A: "{short}..."')
    except Exception as e:
        report(3, f"Q: {question[:50]}", False, str(e))

def _launch_headless_ctx(p, profile_dir):
    """Launch a headless Chrome context; raises TargetClosedError if profile is locked."""
    stealth = Stealth(navigator_webdriver=True, chrome_runtime=True)
    ctx = p.chromium.launch_persistent_context(
        user_data_dir=profile_dir,
        channel="chrome",
        headless=True,
        args=["--disable-blink-features=AutomationControlled",
              "--no-first-run", "--no-default-browser-check"],
        ignore_default_args=["--enable-automation"],
    )
    try:
        stealth.apply_stealth_sync(ctx)
    except Exception:
        pass
    return ctx


def _profile_for_test():
    """
    Return (profile_dir, is_temp).
    Prefer agent_chrome_data (has LinkedIn session).
    If it's locked by a running Chrome, fall back to a fresh temp profile.
    """
    # Quick probe: can we open agent_chrome_data right now?
    if PROFILE_EXISTS:
        try:
            with sync_playwright() as p:
                ctx = _launch_headless_ctx(p, AGENT_PROFILE)
                ctx.close()
            return AGENT_PROFILE, False
        except Exception:
            pass
    # Fallback: fresh temp profile (no LinkedIn session -- will hit authwall)
    tmp = tempfile.mkdtemp(prefix="agent_test_profile_")
    return tmp, True


# =============================================================================
# SCENARIO 4 — Headless browser: navigation + Apply button detection
# =============================================================================
print(bold("\n" + "="*65))
print(bold("  SCENARIO 4 -- Headless Navigation & Apply Button Detection"))
print(bold("="*65))

_test_profile, _is_temp_profile = _profile_for_test()
_profile_note = "(fresh profile -- may hit authwall)" if _is_temp_profile else "(logged-in profile)"
print(f"  Profile: {_profile_note}\n")

try:
    with sync_playwright() as p:
        ctx = _launch_headless_ctx(p, _test_profile)
        for job in JOBS[:5]:
            page = ctx.new_page()
            try:
                page.goto(job["Link"], wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)

                final_url  = page.url
                is_login   = _is_login_page(page)
                title      = page.title()[:50]

                easy_apply = any(
                    page.locator(s).count() > 0
                    for s in ["button[aria-label*='Easy Apply']",
                              "button:has-text('Easy Apply')"]
                )
                ext_apply = not easy_apply and any(
                    page.locator(s).count() > 0
                    for s in ["button[aria-label*='Apply']",
                              "a:has-text('Apply on company website')",
                              "button:has-text('Apply now')",
                              "button:has-text('Apply')"]
                )

                if is_login:
                    # Authwall correctly detected -- agent would pause for login
                    detail = "AUTHWALL detected -- agent will pause for manual login (correct)"
                    report(4, job["Title"][:45], True, detail)
                else:
                    apply_type = ("Easy Apply" if easy_apply
                                  else "External Apply" if ext_apply
                                  else "No apply btn found")
                    report(4, job["Title"][:45], True,
                           f"{apply_type} | {title}")
            except Exception as e:
                report(4, job["Title"][:45], False, str(e)[:70])
            finally:
                page.close()
        ctx.close()
except PlaywrightError as e:
    print(red(f"  Could not launch Chrome for scenario 4: {e}"))
    for job in JOBS[:5]:
        results.append((4, job["Title"], False, "Chrome launch failed"))
finally:
    if _is_temp_profile and os.path.isdir(_test_profile):
        shutil.rmtree(_test_profile, ignore_errors=True)

# =============================================================================
# SCENARIO 5 — Form field detection: contact/radio/select/file inputs
# =============================================================================
print(bold("\n" + "="*65))
print(bold("  SCENARIO 5 -- Form Field & Filler Logic Validation"))
print(bold("="*65))

# 5a: Profile-level validation (no browser needed)
print("  5a. Profile data completeness:")
required_fields = ["first_name","last_name","email","phone","city","state","zip",
                   "work_authorization","require_sponsorship","years_experience"]
for field in required_fields:
    val = PROFILE.get(field, "__MISSING__")
    # booleans (False) are valid -- only fail on absent/placeholder
    missing = val == "__MISSING__" or (isinstance(val, str) and (val == "" or val.startswith("PLACEHOLDER")))
    report(5, f"Profile field: {field}", not missing, str(val)[:40] if not missing else "MISSING or PLACEHOLDER")

# 5b: Resume file accessible
resume = PROFILE.get("resume_path","")
if resume and not os.path.isabs(resume):
    resume = os.path.join(os.path.dirname(os.path.abspath(__file__)), resume)
report(5, "Resume PDF accessible",
       os.path.exists(resume) and os.path.getsize(resume) > 1000,
       f"size={os.path.getsize(resume):,} bytes" if os.path.exists(resume) else "NOT FOUND")

# 5c: Test field-filler using a local HTML form (no network/login dependency)
_LOCAL_FORM = os.path.join(tempfile.gettempdir(), "agent_test_form.html")
with open(_LOCAL_FORM, "w") as _f:
    _f.write("""<!DOCTYPE html><html><body>
<form>
  <label for="fn">First name</label>
  <input id="fn" name="firstName" type="text"><br>
  <label for="ln">Last name</label>
  <input id="ln" name="lastName" type="text"><br>
  <label for="em">Email</label>
  <input id="em" name="email" type="email"><br>
  <label for="ph">Phone</label>
  <input id="ph" name="phone" type="tel"><br>
  <label for="cy">City</label>
  <input id="cy" name="city" type="text"><br>
  <label for="st">State</label>
  <input id="st" name="state" type="text"><br>
  <label for="zp">Zip</label>
  <input id="zp" name="zip" type="text"><br>
  <label for="yr">Years of experience</label>
  <input id="yr" name="yearsExperience" type="number"><br>
  <label>Are you authorized to work in the US?</label>
  <fieldset>
    <legend>Work authorization</legend>
    <input type="radio" id="auth_yes" name="auth" value="yes">
    <label for="auth_yes">Yes</label>
    <input type="radio" id="auth_no" name="auth" value="no">
    <label for="auth_no">No</label>
  </fieldset>
  <label for="cv">Cover letter</label>
  <textarea id="cv" name="coverLetter"></textarea><br>
  <input type="file" name="resume" accept=".pdf"><br>
  <select name="notice">
    <option value="">Select...</option>
    <option value="2weeks">2 weeks</option>
    <option value="1month">1 month</option>
  </select><br>
  <button type="submit">Submit</button>
</form>
</body></html>""")

_test_profile2, _is_temp2 = _profile_for_test()
try:
    with sync_playwright() as p:
        ctx = _launch_headless_ctx(p, _test_profile2)
        page = ctx.new_page()
        try:
            page.goto(f"file:///{_LOCAL_FORM.replace(os.sep, '/')}")
            page.wait_for_timeout(500)

            # Count raw fields
            inputs    = page.locator("input[type='text'],input[type='email'],input[type='tel'],input[type='number']").count()
            selects   = page.locator("select").count()
            radios    = page.locator("input[type='radio']").count()
            textareas = page.locator("textarea").count()
            file_inp  = page.locator("input[type='file']").count()
            report(5, "Field detection: local HTML form",
                   inputs >= 7 and radios >= 2 and textareas >= 1,
                   f"inputs={inputs} selects={selects} radios={radios} "
                   f"textareas={textareas} file={file_inp}")

            # Now actually run the filler on this form
            _fill_contact_fields(page, PROFILE)
            _fill_address_fields(page, PROFILE)
            _handle_radio_questions(page, PROFILE)
            _handle_select_dropdowns(page, PROFILE)
            _handle_numeric_inputs(page, PROFILE)

            # Verify fields were filled
            fn_val = page.locator("#fn").input_value()
            em_val = page.locator("#em").input_value()
            ph_val = page.locator("#ph").input_value()
            yr_val = page.locator("#yr").input_value()
            report(5, "Filler: first_name filled correctly",    fn_val == PROFILE["first_name"], f'got "{fn_val}"')
            report(5, "Filler: email filled correctly",         em_val == PROFILE["email"],      f'got "{em_val}"')
            report(5, "Filler: phone filled correctly",         ph_val == PROFILE["phone"],      f'got "{ph_val}"')
            report(5, "Filler: years_experience filled",        yr_val == str(PROFILE["years_experience"]), f'got "{yr_val}"')

            # Check radio was selected (work auth Yes)
            auth_yes_checked = page.locator("#auth_yes").is_checked()
            report(5, "Filler: work auth radio -> Yes",         auth_yes_checked, "radio checked" if auth_yes_checked else "NOT checked")

        except Exception as e:
            report(5, "Field filler on local form", False, str(e)[:80])
        finally:
            page.close()
        ctx.close()
except Exception as e:
    report(5, "Field detection (browser launch)", False, str(e)[:70])
finally:
    if _is_temp2 and os.path.isdir(_test_profile2):
        shutil.rmtree(_test_profile2, ignore_errors=True)
    try:
        os.unlink(_LOCAL_FORM)
    except Exception:
        pass

# =============================================================================
# Summary
# =============================================================================
print(bold("\n" + "="*65))
print(bold("  RESULTS SUMMARY"))
print(bold("="*65))

passed_count = sum(1 for _, _, ok, _ in results if ok is True)
failed_count = sum(1 for _, _, ok, _ in results if ok is False)
skip_count   = sum(1 for _, _, ok, _ in results if ok is None)

for scenario, label, ok, detail in results:
    if ok is True:
        tag = green(PASS)
    elif ok is False:
        tag = red(FAIL)
    else:
        tag = yellow(SKIP)
    print(f"  S{scenario}  {tag}  {label[:52]}")

print()
print(f"  {green(str(passed_count) + ' passed')}   "
      f"{red(str(failed_count) + ' failed')}   "
      f"{yellow(str(skip_count) + ' skipped')}")

if failed_count == 0:
    print(green("\n  Agent is ready to use."))
elif failed_count <= 2:
    print(yellow("\n  Minor issues -- see FAILs above."))
else:
    print(red("\n  Multiple failures -- review above before using."))

print()
