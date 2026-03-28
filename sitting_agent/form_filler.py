"""
Form filling logic for LinkedIn Easy Apply, Indeed Apply, and generic ATS forms.

Safety contract:
  - _click_next_safe() will NEVER click a submit/apply/finish button
  - stop_on_review() is the ONLY place the agent stops and waits
  - The agent highlights the Submit button RED but NEVER clicks it

Supported platforms (auto-detected):
  LinkedIn Easy Apply, LinkedIn -> Company ATS
  Indeed Apply, Indeed -> Company ATS
  Greenhouse, Lever, Workday, Taleo, iCIMS, SmartRecruiters, BambooHR, Ashby
  Workable, Recruitee, JazzHR, Breezy, Jobvite, SuccessFactors, Rippling
  Any generic multi-step ATS form
"""

import os
import re
import sys
import time
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sitting_agent.groq_responder import ask_groq

# Prevent re-uploading the resume across steps of the same application
_resume_uploaded = False


# ===========================================================================
# Public entry point
# ===========================================================================

def fill_application(page, profile: dict, job: dict, start_time: float = None):
    """Navigate to the job page, detect the platform, and fill the application."""
    global _resume_uploaded
    _resume_uploaded = False  # Reset per application

    if start_time is None:
        start_time = time.time()

    url = str(job.get("Link", ""))

    if _is_login_page(page):
        if not _wait_for_manual_login(page, url):
            stop_on_review(page)
            return

    if "linkedin.com" in url:
        _fill_linkedin(page, profile, job, start_time=start_time)
    elif "indeed.com" in url:
        _fill_indeed(page, profile, job)
    elif "greenhouse.io" in url or "boards.greenhouse.io" in url:
        _fill_greenhouse(page, profile, job)
    elif "lever.co" in url:
        _fill_lever(page, profile, job)
    elif "myworkdayjobs.com" in url or "workday.com" in url:
        _fill_workday(page, profile, job)
    else:
        _fill_generic_ats(page, profile, job)


# ===========================================================================
# LinkedIn
# ===========================================================================

def _fill_linkedin(page, profile: dict, job: dict, start_time: float = None):
    """Handle LinkedIn job page — Easy Apply modal or external Apply link."""
    if start_time is None:
        start_time = time.time()
    print("[Agent] LinkedIn — looking for Easy Apply button...")
    page.wait_for_timeout(2000)

    for sel in [
        "button[aria-label*='Easy Apply']",
        "button:has-text('Easy Apply')",
        ".jobs-apply-button--top-card",
        ".jobs-s-apply button",
        "button.jobs-apply-button",
        "[data-control-name='jobdetails_topcard_inapply']",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                btn.scroll_into_view_if_needed()
                btn.click()
                page.wait_for_timeout(3000)
                print("[Agent] Easy Apply modal opened")
                _walk_linkedin_wizard(page, profile, job, start_time=start_time)
                return
        except Exception:
            continue

    print("[Agent] No Easy Apply — looking for external Apply link...")
    _navigate_external_apply(page, profile, job)


def _navigate_external_apply(page, profile: dict, job: dict):
    """Click 'Apply on company website' — usually opens in a new tab."""
    for sel in [
        "a:has-text('Apply on company website')",
        "a:has-text('Apply on company site')",
        "button:has-text('Apply on company website')",
        "button:has-text('Apply on company site')",
        "button.jobs-apply-button:not([aria-label*='Easy'])",
    ]:
        try:
            el = page.locator(sel).first
            if not el.is_visible(timeout=2000):
                continue
            print(f"[Agent] External apply: '{el.inner_text().strip()[:50]}'")
            new_page = _click_for_new_tab(page, el)
            target = new_page or page
            label_str = "new tab" if new_page else "same tab"
            print(f"[Agent] Company site ({label_str}): {target.url[:80]}")
            if _is_login_page(target):
                print("[Agent] Company site requires login — manual review.")
                stop_on_review(target)
                return
            _walk_generic_wizard(target, profile, job, label="Company ATS")
            return
        except Exception:
            continue

    print("[Agent] No Apply button found — stopping for manual review.")
    stop_on_review(page)


def _walk_linkedin_wizard(page, profile: dict, job: dict, start_time: float = None):
    """Walk through all LinkedIn Easy Apply wizard steps."""
    if start_time is None:
        start_time = time.time()
    for step_num in range(25):
        page.wait_for_timeout(2000)
        print(f"\n[Agent] LinkedIn Easy Apply — Step {step_num + 1}")

        # 5-minute global timeout
        if time.time() - start_time > 300:
            print("[Agent] ⚠️  5-minute timeout reached — stopping to prevent hanging.")
            stop_on_review(page, reason="5-minute timeout reached")
            return

        # URL drift — LinkedIn silently redirected to an external ATS
        current_url = page.url
        if "linkedin.com" not in current_url:
            _handle_external_redirect(page, profile, current_url)
            return

        if _is_login_page(page):
            print("[Agent] Redirected to login — stopping.")
            stop_on_review(page)
            return

        if _is_linkedin_final_page(page):
            print("[Agent] LinkedIn review/submit page reached.")
            stop_on_review(page)
            return

        _fill_all_visible(page, profile, job)
        page.wait_for_timeout(800)

        # Re-check after fill — some steps reveal the review page once all fields are done
        if _is_linkedin_final_page(page):
            stop_on_review(page)
            return

        if not _click_next_safe(page):
            page.wait_for_timeout(1500)
            if _is_linkedin_final_page(page):
                stop_on_review(page)
            else:
                print("[Agent] No Next button found — stopping for manual review.")
                stop_on_review(page)
            return

    print("[Agent] Maximum steps reached — stopping.")
    stop_on_review(page)


# ===========================================================================
# External ATS redirect handler (LinkedIn wizard escape hatch)
# ===========================================================================

_ATS_MAP = {
    "dayforcehcm.com":          "Dayforce HCM",
    "myworkdayjobs.com":        "Workday",
    "workday.com":              "Workday",
    "icims.com":                "iCIMS",
    "taleo.net":                "Taleo (Oracle)",
    "greenhouse.io":            "Greenhouse",
    "lever.co":                 "Lever",
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
    "hire.trakstar.com":        "Trakstar",
    "breezy.hr":                "Breezy HR",
}


def _handle_external_redirect(page, profile: dict, current_url: str):
    """
    Called when the LinkedIn wizard detects a URL that is no longer linkedin.com.
    Prints a clear warning, attempts a single best-effort field fill, then hands
    off to the human via stop_on_review().
    """
    url_lower = current_url.lower()
    ats_name = "Unknown ATS"
    for domain, name in _ATS_MAP.items():
        if domain in url_lower:
            ats_name = name
            break

    print("\n" + "=" * 65)
    print("  ⚠️   EXTERNAL REDIRECT DETECTED")
    print(f"  LinkedIn redirected to: {ats_name}")
    print(f"  URL: {current_url[:80]}")
    print("=" * 65)
    print("  The agent cannot reliably automate this ATS platform.")
    print("  What the agent WILL do: attempt a single-pass field fill")
    print("  on whatever fields are visible right now, then hand off.")
    print("=" * 65 + "\n")

    # Single best-effort pass — no looping, no pagination
    page.wait_for_timeout(3000)
    _fill_contact_fields(page, profile)
    _try_resume_upload(page, profile)

    stop_on_review(
        page,
        reason=(
            f"Redirected to {ats_name} — manual login may be required. "
            f"URL: {current_url[:80]}"
        ),
    )


# ===========================================================================
# Indeed
# ===========================================================================

def _fill_indeed(page, profile: dict, job: dict):
    """Handle Indeed job page — click the apply button then walk the form."""
    print("[Agent] Indeed — looking for Apply button...")
    page.wait_for_timeout(2000)

    # Indeed-hosted apply flow buttons
    for sel in [
        "#indeedApplyButton",
        "button.ia-IndeedApplyButton",
        "button[data-indeed-apply-jobid]",
        "[id*='indeedApply'] button",
        "button[id*='indeedApply']",
        "span.indeed-apply-widget button",
    ]:
        try:
            btn = page.locator(sel).first
            if not btn.is_visible(timeout=1500):
                continue
            print(f"[Agent] Found Indeed Apply button")
            new_page = _click_for_new_tab(page, btn)
            target = new_page or page
            label_str = "new tab" if new_page else "same tab"
            print(f"[Agent] Indeed Apply -> {label_str}: {target.url[:80]}")
            _fill_indeed_apply(target, profile, job)
            return
        except Exception:
            continue

    # Indeed "Apply on company site" redirect
    for sel in [
        "a:has-text('Apply on company site')",
        "button:has-text('Apply on company site')",
        "a:has-text('Apply now')",
        "button:has-text('Apply now')",
    ]:
        try:
            btn = page.locator(sel).first
            if not btn.is_visible(timeout=1500):
                continue
            print(f"[Agent] Indeed -> company site")
            new_page = _click_for_new_tab(page, btn)
            target = new_page or page
            _walk_generic_wizard(target, profile, job,
                                  label=_detect_ats(target.url) or "Company ATS")
            return
        except Exception:
            continue

    # Fallback: walk whatever is visible on the current page
    print("[Agent] No Indeed-specific button found — walking current page...")
    _walk_generic_wizard(page, profile, job, label="Indeed")


# ===========================================================================
# Generic ATS
# ===========================================================================

def _fill_generic_ats(page, profile: dict, job: dict):
    """
    Handle any generic job/ATS URL.
    Detect whether we're on a job description page (click Apply first)
    or already on an application form (start filling immediately).
    """
    ats = _detect_ats(page.url)
    if ats:
        print(f"[Agent] Detected ATS: {ats}")

    # Already on a form — skip Apply button search
    if _page_has_form_fields(page):
        print("[Agent] Already on application form — starting fill.")
        _walk_generic_wizard(page, profile, job, label=ats or "ATS")
        return

    print("[Agent] Searching for Apply button...")

    apply_selectors = [
        "button:has-text('Apply Now')",
        "a:has-text('Apply Now')",
        "button:has-text('Apply now')",
        "a:has-text('Apply now')",
        "button:has-text('Apply for this job')",
        "a:has-text('Apply for this job')",
        "button:has-text('Apply for this position')",
        "a:has-text('Apply for this position')",
        "button:has-text('Apply for Job')",
        "a:has-text('Apply for Job')",
        "a[class*='applyButton']",
        "button[class*='applyButton']",
        "a[class*='apply-button']",
        "button[class*='apply-button']",
        "button[id*='apply']",
        "a[id*='apply-button']",
    ]

    for sel in apply_selectors:
        try:
            btn = page.locator(sel).first
            if not btn.is_visible(timeout=1200):
                continue
            btn_text = btn.inner_text().strip()[:60]
            print(f"[Agent] Apply button: '{btn_text}'")
            original_url = page.url

            new_page = _click_for_new_tab(page, btn)

            if new_page:
                print(f"[Agent] Apply -> new tab: {new_page.url[:80]}")
                if _is_login_page(new_page):
                    print("[Agent] Login required — manual review.")
                    stop_on_review(new_page)
                    return
                _walk_generic_wizard(new_page, profile, job,
                                      label=_detect_ats(new_page.url) or "Company ATS")
                return

            # Same-tab result
            if page.url != original_url:
                # Navigated to a new URL
                print(f"[Agent] Apply -> {page.url[:80]}")
                if _is_login_page(page):
                    stop_on_review(page)
                    return
                _walk_generic_wizard(page, profile, job,
                                      label=_detect_ats(page.url) or "Company ATS")
                return

            # URL unchanged — check if a JS modal/inline form appeared
            if _page_has_form_fields(page):
                print("[Agent] Apply opened inline form — filling.")
                _walk_generic_wizard(page, profile, job, label=ats or "Company ATS")
                return

            # Nothing happened — try next selector
            continue

        except Exception:
            continue

    # No Apply button worked — walk whatever is on the page
    print("[Agent] No Apply button found — walking current page.")
    _walk_generic_wizard(page, profile, job, label=ats or "Generic ATS")


# ===========================================================================
# Generic multi-step wizard walker
# ===========================================================================

def _walk_generic_wizard(page, profile: dict, job: dict, label: str = "ATS"):
    """
    Walk through any multi-step application form:
    fill all visible fields, click Next/Continue, repeat until submit page.
    """
    print(f"[Agent] Walking {label} form...")

    for step_num in range(25):
        page.wait_for_timeout(2500)
        print(f"\n[Agent] {label} — Step {step_num + 1}  ({page.url[:70]})")

        if _is_login_page(page):
            print("[Agent] Login page detected — manual review.")
            stop_on_review(page)
            return

        if _is_final_submit_page(page):
            print("[Agent] Submit/review page reached.")
            stop_on_review(page)
            return

        _fill_all_visible(page, profile, job)
        page.wait_for_timeout(800)

        # Some ATS reveal the submit button only after all fields are filled
        if _is_final_submit_page(page):
            print("[Agent] Submit page revealed after filling.")
            stop_on_review(page)
            return

        if not _click_next_safe(page):
            page.wait_for_timeout(2000)
            if _is_final_submit_page(page):
                stop_on_review(page)
            else:
                print("[Agent] No Next/Continue button — stopping for manual review.")
                stop_on_review(page)
            return

    print("[Agent] Maximum steps (25) reached — stopping.")
    stop_on_review(page)


# ===========================================================================
# Platform / page detection helpers
# ===========================================================================

def _detect_ats(url: str) -> str:
    """Identify the ATS from the URL for cleaner log labels."""
    u = url.lower()
    if "greenhouse.io" in u or "boards.greenhouse" in u: return "Greenhouse"
    if "lever.co" in u:                                   return "Lever"
    if "myworkdayjobs.com" in u or "workday.com" in u:   return "Workday"
    if "taleo.net" in u:                                  return "Taleo"
    if "icims.com" in u:                                  return "iCIMS"
    if "smartrecruiters.com" in u:                        return "SmartRecruiters"
    if "bamboohr.com" in u:                               return "BambooHR"
    if "successfactors" in u:                             return "SuccessFactors"
    if "jobvite.com" in u:                                return "Jobvite"
    if "ashbyhq.com" in u:                                return "Ashby"
    if "rippling.com" in u:                               return "Rippling"
    if "breezy.hr" in u:                                  return "Breezy"
    if "applytojob.com" in u:                             return "ApplyToJob"
    if "recruitee.com" in u:                              return "Recruitee"
    if "workable.com" in u:                               return "Workable"
    if "jazz.co" in u or "resumatorjobs.com" in u:       return "JazzHR"
    return ""


def _page_has_form_fields(page) -> bool:
    """
    True only when the page IS an application form — not a job description page.
    Requires strong signals to avoid false positives from search boxes.
    """
    try:
        # Signal 1: visible email input — unambiguous form indicator
        for sel in ["input[type='email']", "input[autocomplete='email']",
                    "input[name*='email' i]", "input[id*='email' i]"]:
            try:
                if page.locator(sel).first.is_visible(timeout=300):
                    return True
            except Exception:
                pass
    except Exception:
        pass

    try:
        # Signal 2: file upload (resume) input
        if page.locator("input[type='file']").count() > 0:
            return True
    except Exception:
        pass

    try:
        # Signal 3: a <form> with 2+ visible fields
        for form in page.locator("form").all():
            try:
                count = 0
                for sel in ["input[type='text']", "input[type='tel']",
                             "input[type='number']", "textarea", "select"]:
                    for i in range(min(form.locator(sel).count(), 3)):
                        try:
                            if form.locator(sel).nth(i).is_visible(timeout=200):
                                count += 1
                                break
                        except Exception:
                            pass
                if count >= 2:
                    return True
            except Exception:
                continue
    except Exception:
        pass

    return False


def _is_login_page(page) -> bool:
    """URL-based login/authwall detection — no DOM queries to avoid false positives."""
    try:
        url = page.url.lower()
        if any(k in url for k in ["/authwall", "/login", "/signin", "/sign-in",
                                   "session/new", "account/login", "checkpoint/lg",
                                   "/auth?", "oauth/authorize"]):
            return True
        path = urlparse(url).path.rstrip("/")
        if path in ["/login", "/signin", "/sign-in", "/auth/login", "/auth"]:
            return True
    except Exception:
        pass
    return False


def _is_linkedin_final_page(page) -> bool:
    """Detect LinkedIn Easy Apply final review page."""
    for sel in [
        "button[aria-label='Submit application']",
        "button[aria-label='Submit Application']",
        "button:has-text('Submit application')",
        "button:has-text('Submit Application')",
        "h1:has-text('Review your application')",
        "h2:has-text('Review your application')",
        "h3:has-text('Review your application')",
    ]:
        try:
            if page.locator(sel).count() > 0:
                return True
        except Exception:
            pass
    return False


def _is_final_submit_page(page) -> bool:
    """
    True when the page's only remaining action is the final submit.

    Rule: stop ONLY on LinkedIn-specific signals OR a clearly-final button label
    when NO safe Next/Continue button is also visible on the page.
    Never stop just because button[type='submit'] exists — ATS use that for Next too.
    """
    # LinkedIn — unambiguous
    if _is_linkedin_final_page(page):
        return True

    # Generic: clearly-final label + no Next button visible
    FINAL_LABELS = [
        "Submit Application", "Submit My Application",
        "Send Application", "Complete Application",
        "Finish Application", "Submit and Apply",
    ]
    for label in FINAL_LABELS:
        try:
            if page.locator(f"button:has-text('{label}')").count() > 0:
                if not _has_visible_next_button(page):
                    return True
        except Exception:
            pass
    return False


def _has_visible_next_button(page) -> bool:
    """True if any Next/Continue/Proceed button is currently visible."""
    for lbl in ["Next", "Continue", "Proceed", "Next step", "Next page",
                "Save and continue", "Save & Continue", "Forward"]:
        try:
            if page.locator(f"button:has-text('{lbl}')").first.is_visible(timeout=300):
                return True
        except Exception:
            pass
    return False


# ===========================================================================
# New-tab click helper — prevents the double-click bug
# ===========================================================================

def _click_for_new_tab(page, element):
    """
    Click element exactly once. Return the new page if a tab opened, else None.
    The click happens inside `with expect_page` — it is NEVER repeated in except.
    """
    try:
        with page.context.expect_page(timeout=6000) as info:
            element.click()
        new_page = info.value
        new_page.wait_for_load_state("domcontentloaded", timeout=30000)
        new_page.wait_for_timeout(2000)
        return new_page
    except Exception:
        # No new tab opened; click already fired — wait for same-tab nav to settle
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
        return None


# ===========================================================================
# Manual login pause
# ===========================================================================

def _wait_for_manual_login(page, job_url: str) -> bool:
    print("\n" + "=" * 65)
    print("  MANUAL LOGIN REQUIRED")
    print("=" * 65)
    print("  The page is showing a login or authwall.")
    print()
    print("  1. Switch to the Chrome window")
    print("  2. Log in with your credentials")
    print("  3. Press Enter here when done")
    print("=" * 65)
    try:
        input("\n  >>> Press Enter after logging in... ")
    except (EOFError, KeyboardInterrupt):
        return False
    if not job_url:
        return False
    try:
        page.goto(job_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        if _is_login_page(page):
            print("[Agent] Still on login page — stopping.")
            return False
        print("[Agent] Job page loaded — resuming.")
        return True
    except Exception as e:
        print(f"[Agent] Re-navigation failed: {e}")
        return False


# ===========================================================================
# Unified field-fill pass (runs on every wizard step)
# ===========================================================================

def _fill_all_visible(page, profile: dict, job: dict):
    """Run every field-filling helper against the currently visible page."""
    page.wait_for_timeout(800)
    _fill_contact_fields(page, profile)
    _fill_address_fields(page, profile)
    _fill_work_fields(page, profile)
    _handle_radio_buttons(page, profile)
    _handle_radio_questions(page, profile)
    _handle_select_dropdowns(page, profile)
    _handle_comboboxes(page, profile)
    _handle_checkboxes(page, profile)
    _try_resume_upload(page, profile)
    _handle_numeric_inputs(page, profile)
    _handle_text_questions(page, profile, job)


# ===========================================================================
# Contact fields
# ===========================================================================

def _fill_contact_fields(page, profile: dict):
    phone_raw = profile.get("phone", "")
    phone_digits = re.sub(r"\D", "", phone_raw)  # e.g. "5044352750"

    fields = [
        (
            ["input[id*='firstName' i]", "input[name*='firstName' i]",
             "input[name='first_name']", "input[name='firstname']",
             "input[autocomplete='given-name']",
             "input[placeholder*='First name' i]", "input[aria-label*='First name' i]",
             "input[data-automation-id='legalNameSection_firstName']"],
            profile.get("first_name", ""), "first name",
        ),
        (
            ["input[id*='lastName' i]", "input[name*='lastName' i]",
             "input[name='last_name']", "input[name='lastname']",
             "input[autocomplete='family-name']",
             "input[placeholder*='Last name' i]", "input[aria-label*='Last name' i]",
             "input[data-automation-id='legalNameSection_lastName']"],
            profile.get("last_name", ""), "last name",
        ),
        (
            ["input[type='email']", "input[id*='email' i]", "input[name*='email' i]",
             "input[autocomplete='email']",
             "input[placeholder*='Email' i]", "input[aria-label*='email' i]"],
            profile.get("email", ""), "email",
        ),
        (
            # Formatted phone first; digits-only fallback handled via _fill_phone_smart
            ["input[type='tel']", "input[id*='phone' i]", "input[name*='phone' i]",
             "input[id*='mobile' i]", "input[autocomplete='tel']",
             "input[placeholder*='Phone' i]", "input[aria-label*='Phone' i]",
             "input[data-automation-id='phone']"],
            phone_raw, "phone",
        ),
        (
            ["input[placeholder*='LinkedIn' i]", "input[id*='linkedin' i]",
             "input[name*='linkedin' i]", "input[aria-label*='LinkedIn' i]",
             "input[placeholder*='linkedin.com' i]"],
            profile.get("linkedin_url", ""), "LinkedIn URL",
        ),
        (
            ["input[placeholder*='GitHub' i]", "input[id*='github' i]",
             "input[name*='github' i]", "input[aria-label*='GitHub' i]"],
            profile.get("github_url", ""), "GitHub URL",
        ),
        (
            ["input[placeholder*='Portfolio' i]", "input[placeholder*='personal website' i]",
             "input[id*='website' i]", "input[name*='website' i]",
             "input[aria-label*='website' i]", "input[aria-label*='portfolio' i]"],
            profile.get("website_url", ""), "website/portfolio",
        ),
    ]
    _apply_field_list(page, fields)

    # For phone fields that reject formatted numbers, try digits-only
    if phone_digits and phone_raw:
        _fill_phone_fallback(page, phone_raw, phone_digits)


def _fill_phone_fallback(page, phone_formatted: str, phone_digits: str):
    """
    If a phone field was filled with the formatted number but it didn't stick
    (some ATS accept only digits), retry with digits only.
    """
    for sel in ["input[type='tel']", "input[id*='phone' i]", "input[name*='phone' i]"]:
        try:
            inp = page.locator(sel).first
            if not inp.is_visible(timeout=300):
                continue
            current = inp.input_value()
            if current == phone_formatted:
                # Formatted number accepted — done
                return
            if not current:
                # Not filled at all — try digits only
                inp.click()
                inp.fill(phone_digits)
                inp.dispatch_event("input")
                inp.dispatch_event("change")
                after = inp.input_value()
                if after:
                    print(f"[Agent] Phone (digits only) -> {phone_digits}")
                    return
        except Exception:
            continue


# ===========================================================================
# Address fields
# ===========================================================================

def _fill_address_fields(page, profile: dict):
    fields = [
        (
            ["input[id*='city' i]", "input[name*='city' i]",
             "input[placeholder*='City' i]", "input[aria-label*='City' i]",
             "input[autocomplete='address-level2']"],
            profile.get("city", ""), "city",
        ),
        (
            ["input[id*='state' i]", "input[name*='state' i]",
             "input[placeholder*='State' i]", "input[aria-label*='State' i]",
             "input[autocomplete='address-level1']"],
            profile.get("state", ""), "state",
        ),
        (
            ["input[id*='zip' i]", "input[id*='postal' i]",
             "input[name*='zip' i]", "input[name*='postal' i]",
             "input[placeholder*='Zip' i]", "input[placeholder*='Postal' i]",
             "input[aria-label*='ZIP' i]", "input[autocomplete='postal-code']"],
            profile.get("zip", ""), "ZIP",
        ),
        (
            ["input[id*='address1' i]", "input[name*='address1' i]",
             "input[id*='streetAddress' i]", "input[name*='streetAddress' i]",
             "input[placeholder*='Street address' i]",
             "input[autocomplete='street-address']"],
            # Use only the street portion — not city/state/zip
            _street_only(profile.get("address", "")), "street address",
        ),
    ]
    _apply_field_list(page, fields)
    _fill_country_dropdown(page, profile)
    _fill_state_dropdown(page, profile)


def _street_only(address: str) -> str:
    """Return just the street line from a full address string."""
    if not address:
        return ""
    # Everything before the first comma is the street
    parts = address.split(",")
    return parts[0].strip() if parts else address.strip()


def _fill_country_dropdown(page, profile: dict):
    """Select country in any <select> labelled 'country'."""
    country = profile.get("country", "United States")
    if not country:
        return
    try:
        for sel_elem in page.locator("select").all():
            try:
                if not sel_elem.is_visible(timeout=300):
                    continue
                label = _get_element_label(page, sel_elem).lower()
                if "country" not in label:
                    continue
                current = sel_elem.input_value()
                if current and current not in _BLANK_OPTION_VALUES:
                    continue  # Already selected
                for opt in [country, "United States", "United States of America", "US", "USA"]:
                    try:
                        sel_elem.select_option(label=opt)
                        print(f"[Agent] Country -> {opt}")
                        return
                    except Exception:
                        pass
                for val in ["US", "USA", "United States", "united states"]:
                    try:
                        sel_elem.select_option(value=val)
                        print(f"[Agent] Country (value) -> {val}")
                        return
                    except Exception:
                        pass
            except Exception:
                continue
    except Exception:
        pass


def _fill_state_dropdown(page, profile: dict):
    """Select state in any <select> labelled 'state' or 'province'."""
    state = profile.get("state", "")
    if not state:
        return
    try:
        for sel_elem in page.locator("select").all():
            try:
                if not sel_elem.is_visible(timeout=300):
                    continue
                label = _get_element_label(page, sel_elem).lower()
                if not any(k in label for k in ["state", "province", "region"]):
                    continue
                current = sel_elem.input_value()
                if current and current not in _BLANK_OPTION_VALUES:
                    continue
                # Try full name and abbreviation
                state_full = _US_STATE_NAMES.get(state.upper(), state)
                for opt in [state_full, state, state.upper(), state.lower()]:
                    try:
                        sel_elem.select_option(label=opt)
                        print(f"[Agent] State -> {opt}")
                        return
                    except Exception:
                        pass
                for val in [state, state.upper(), state.lower()]:
                    try:
                        sel_elem.select_option(value=val)
                        print(f"[Agent] State (value) -> {val}")
                        return
                    except Exception:
                        pass
            except Exception:
                continue
    except Exception:
        pass


# US state abbreviation -> full name map
_US_STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

# Common "no selection" option values across ATS platforms
_BLANK_OPTION_VALUES = {
    "", "-1", "0", "none", "null", "select", "placeholder",
    "please select", "choose", "-- select --", "select one",
    "-- select one --", "choose one", "n/a",
}


# ===========================================================================
# Work / employment fields
# ===========================================================================

def _fill_work_fields(page, profile: dict):
    fields = [
        (
            ["input[id*='currentTitle' i]", "input[name*='currentTitle' i]",
             "input[id*='jobTitle' i]", "input[name*='jobTitle' i]",
             "input[placeholder*='Current title' i]", "input[placeholder*='Job title' i]",
             "input[aria-label*='Current title' i]", "input[aria-label*='Job title' i]"],
            profile.get("current_title", ""), "current job title",
        ),
        (
            ["input[id*='currentEmployer' i]", "input[name*='currentCompany' i]",
             "input[name*='employer' i]",
             "input[placeholder*='Current employer' i]",
             "input[placeholder*='Current company' i]",
             "input[aria-label*='Current employer' i]"],
            profile.get("current_employer", ""), "current employer",
        ),
    ]
    _apply_field_list(page, fields)


# ===========================================================================
# Core field-fill engine
# ===========================================================================

def _apply_field_list(page, fields: list):
    """
    Fill each field if visible and empty.
    Dispatches React/Vue/Angular change events.
    Falls back to character-by-character typing if .fill() is rejected.
    """
    for selectors, value, label in fields:
        if not value or str(value).startswith("PLACEHOLDER"):
            continue
        for sel in selectors:
            try:
                inp = page.locator(sel).first
                if not inp.is_visible(timeout=600):
                    continue
                current = inp.input_value()
                if current:
                    print(f"[Agent] — {label} already filled ({current[:30]})")
                    break
                inp.click()
                inp.fill(str(value))
                inp.dispatch_event("input")
                inp.dispatch_event("change")
                # Verify value was accepted (React controlled inputs may reject .fill)
                if not inp.input_value():
                    inp.clear()
                    inp.type(str(value), delay=40)
                    inp.dispatch_event("input")
                    inp.dispatch_event("change")
                print(f"[Agent] Filled {label}")
                break
            except Exception:
                continue


def _smart_fill(inp, value: str):
    """Fill a numeric/specific input with event dispatch and Tab."""
    try:
        inp.click()
        inp.fill(value)
        inp.dispatch_event("input")
        inp.dispatch_event("change")
        inp.press("Tab")
    except Exception:
        pass


# ===========================================================================
# Checkboxes (consent / agreement — skip EEO/demographic)
# ===========================================================================

def _handle_checkboxes(page, profile: dict):
    AUTO_CHECK = [
        "i agree", "i understand", "i confirm", "i certify",
        "i acknowledge", "i authorize", "i consent",
        "agree", "accept", "terms", "conditions",
        "privacy", "e-sign", "electronic signature",
        "i have read", "confirm", "acknowledge",
        "authorization", "accurate", "true and correct",
    ]
    SKIP_DEMOGRAPHIC = [
        "eeo", "equal opportunity", "disability", "veteran",
        "race", "gender", "ethnicity", "demographic",
        "voluntary", "self-identify", "self identify",
    ]
    try:
        for cb in page.locator("input[type='checkbox']").all():
            try:
                if not cb.is_visible(timeout=300):
                    continue
                if not cb.is_enabled():
                    continue
                if cb.is_checked():
                    continue
                label = _get_element_label(page, cb).lower()
                if not label:
                    continue
                if any(k in label for k in SKIP_DEMOGRAPHIC):
                    print(f"[Agent] — Skipped demographic checkbox: {label[:55]}")
                    continue
                if any(k in label for k in AUTO_CHECK):
                    cb.check()
                    print(f"[Agent] ✓ Checked: {label[:55]}")
                    continue
                # Unknown checkbox — ask Groq
                try:
                    groq_answer = ask_groq(
                        f"Should the candidate check this checkbox? Label: \"{label}\". "
                        "Answer only 'yes' or 'no'.",
                        profile,
                    ).strip().lower()
                    if groq_answer.startswith("yes"):
                        cb.check()
                        print(f"[Agent] ✓ Checked (Groq): {label[:55]}")
                except Exception:
                    pass
            except Exception:
                continue
    except Exception:
        pass


# ===========================================================================
# Radio buttons
# ===========================================================================

def _handle_radio_questions(page, profile: dict):
    require_sponsorship = profile.get("require_sponsorship", False)
    willing_to_relocate = profile.get("willing_to_relocate", False)

    def _answer_for(text: str):
        """Map a question string to the correct Yes/No answer."""
        t = text.lower()
        if any(k in t for k in [
            "authorized to work", "authorized to legally work",
            "eligible to work", "legally authorized",
            "work in the us", "work in us",
            "employment authorization", "work authorization",
            "right to work", "legally permitted",
        ]):
            return "Yes"
        if any(k in t for k in [
            "sponsorship required", "visa sponsor",
            "require employer sponsor", "require sponsorship",
            "need sponsorship", "need visa",
        ]) or re.search(r"\bsponsor\b", t):
            return "Yes" if require_sponsorship else "No"
        if "relocat" in t:
            return "Yes" if willing_to_relocate else "No"
        return None

    # ── fieldset/legend groups (most ATS) ─────────────────────────────────
    try:
        for group in page.locator("fieldset").all():
            try:
                if not group.is_visible(timeout=300):
                    continue
                question = _fieldset_label(group, page)
                answer = _answer_for(question)
                if not answer:
                    continue
                if any(_safe_checked(r) for r in group.locator("input[type='radio']").all()):
                    continue  # Already answered
                _click_radio_for_answer(group, page, answer, question)
            except Exception:
                continue
    except Exception:
        pass

    # ── Standalone radios not inside a fieldset ────────────────────────────
    try:
        for radio in page.locator("input[type='radio']").all():
            try:
                if not radio.is_visible(timeout=300):
                    continue
                if _safe_checked(radio):
                    continue
                rid = radio.get_attribute("id") or ""
                if not rid:
                    continue
                lbl = page.locator(f"label[for='{rid}']")
                if not lbl.count():
                    continue

                # Get the question from the nearest container, not the radio label
                question_text = _get_radio_question_text(page, radio)
                answer = _answer_for(question_text) if question_text else None
                if not answer:
                    continue

                lbl_text = lbl.inner_text().strip()
                if lbl_text.lower() == answer.lower() or answer.lower() in lbl_text.lower():
                    radio.click()
                    print(f"[Agent] Radio '{question_text[:50]}' -> {lbl_text}")
            except Exception:
                continue
    except Exception:
        pass


def _get_radio_question_text(page, radio) -> str:
    """
    Walk up the DOM from a radio button to find the question text
    (the nearest heading/paragraph/label that is NOT the radio's own label).
    """
    try:
        return radio.evaluate("""el => {
            let parent = el.parentElement;
            for (let depth = 0; depth < 8; depth++) {
                if (!parent) break;
                // Look for a legend, p, span, or div that contains question text
                const candidates = parent.querySelectorAll(
                    'legend, p, span.t-bold, span.fb-dash-form-element__label, ' +
                    '[data-test-form-label-text], .field-label, .form-label, h4, h3'
                );
                for (const c of candidates) {
                    if (c.contains(el)) continue;
                    const t = c.textContent.trim();
                    if (t.length > 8 && t.length < 300) return t;
                }
                parent = parent.parentElement;
            }
            return '';
        }""") or ""
    except Exception:
        return ""


def _fieldset_label(group, page) -> str:
    for sel in ["legend", "span.t-bold", "[data-test-form-label-text]",
                "span.fb-dash-form-element__label", ".field-label"]:
        try:
            el = group.locator(sel).first
            if el.count():
                t = el.inner_text().strip()
                if t:
                    return t
        except Exception:
            pass
    return ""


def _click_radio_for_answer(group, page, answer: str, question: str):
    """Click the radio button whose label best matches `answer`."""
    for radio in group.locator("input[type='radio']").all():
        try:
            rid = radio.get_attribute("id") or ""
            lbl_text = ""
            if rid:
                lbl = page.locator(f"label[for='{rid}']")
                if lbl.count():
                    lbl_text = lbl.inner_text().strip()
            if not lbl_text:
                # Try adjacent text node or parent text
                lbl_text = radio.evaluate(
                    "el => (el.nextSibling && el.nextSibling.textContent "
                    "? el.nextSibling.textContent.trim() "
                    ": el.parentElement ? el.parentElement.textContent.trim() : '')"
                )
            if not lbl_text:
                continue
            if lbl_text.lower() == answer.lower() or answer.lower() in lbl_text.lower():
                radio.click()
                print(f"[Agent] Radio '{question[:50]}' -> {lbl_text[:40]}")
                return
        except Exception:
            continue


def _safe_checked(radio) -> bool:
    try:
        return radio.is_checked()
    except Exception:
        return False


# ===========================================================================
# Select dropdowns
# ===========================================================================

def _handle_select_dropdowns(page, profile: dict):
    require_sponsorship = profile.get("require_sponsorship", False)
    willing_to_relocate = profile.get("willing_to_relocate", False)

    try:
        for sel_elem in page.locator("select").all():
            try:
                if not sel_elem.is_visible(timeout=300):
                    continue
                current = sel_elem.input_value() or ""
                if current.lower().strip() not in _BLANK_OPTION_VALUES:
                    continue  # Already selected a real value

                label = _get_element_label(page, sel_elem).lower()
                if not label:
                    continue

                if "country" in label:
                    for opt in ["United States", "United States of America", "US", "USA"]:
                        try:
                            sel_elem.select_option(label=opt)
                            print(f"[Agent] Country -> {opt}")
                            break
                        except Exception:
                            pass

                elif "state" in label or "province" in label:
                    state = profile.get("state", "")
                    state_full = _US_STATE_NAMES.get(state.upper(), state)
                    for opt in [state_full, state, state.upper()]:
                        try:
                            sel_elem.select_option(label=opt)
                            print(f"[Agent] State dropdown -> {opt}")
                            break
                        except Exception:
                            pass

                elif any(k in label for k in ["authorized", "eligible to work",
                                               "legally authorized", "employment authorization",
                                               "work authorization", "work legally"]):
                    _safe_select(sel_elem, ["Yes", "yes"])
                    print("[Agent] Work auth -> Yes")

                elif any(k in label for k in ["sponsor", "visa sponsor"]):
                    ans = "Yes" if require_sponsorship else "No"
                    _safe_select(sel_elem, [ans, ans.lower()])
                    print(f"[Agent] Sponsorship -> {ans}")

                elif "relocat" in label:
                    ans = "Yes" if willing_to_relocate else "No"
                    _safe_select(sel_elem, [ans, ans.lower()])
                    print(f"[Agent] Relocation -> {ans}")

                elif any(k in label for k in ["notice", "availability", "start date",
                                               "when can you start", "earliest start"]):
                    notice = profile.get("notice_period", "Immediately")
                    for opt in [notice, "Immediately", "Immediate",
                                 "2 weeks", "Two weeks", "14 days"]:
                        try:
                            sel_elem.select_option(label=opt)
                            print(f"[Agent] Notice -> {opt}")
                            break
                        except Exception:
                            continue

                elif any(k in label for k in ["employment type", "job type",
                                               "work type", "employment status"]):
                    wt = profile.get("work_type", "Full-time")
                    for opt in [wt, "Full-time", "Full Time", "Permanent"]:
                        try:
                            sel_elem.select_option(label=opt)
                            print(f"[Agent] Employment type -> {opt}")
                            break
                        except Exception:
                            continue

                elif any(k in label for k in ["education", "degree",
                                               "highest education", "highest level"]):
                    edu = profile.get("education_level", "Bachelor's Degree")
                    for opt in [edu, "Bachelor's Degree", "Bachelor's",
                                 "Bachelors", "Bachelor", "4 year degree", "B.S."]:
                        try:
                            sel_elem.select_option(label=opt)
                            print(f"[Agent] Education -> {opt}")
                            break
                        except Exception:
                            continue

                elif any(k in label for k in ["currency", "pay type", "salary currency"]):
                    for opt in ["USD", "US Dollar", "$", "United States Dollar"]:
                        try:
                            sel_elem.select_option(label=opt)
                            print(f"[Agent] Currency -> {opt}")
                            break
                        except Exception:
                            continue

            except Exception:
                continue
    except Exception:
        pass


def _safe_select(sel_elem, labels: list) -> bool:
    for lbl in labels:
        try:
            sel_elem.select_option(label=lbl)
            return True
        except Exception:
            pass
    for lbl in labels:
        try:
            sel_elem.select_option(value=lbl)
            return True
        except Exception:
            pass
    return False


# ===========================================================================
# Comboboxes  ([role='combobox'] — LinkedIn-style and generic)
# ===========================================================================

def _handle_comboboxes(page, profile: dict):
    require_sponsorship = profile.get("require_sponsorship", False)

    try:
        for combo in page.locator("[role='combobox']").all():
            try:
                if not combo.is_visible(timeout=300):
                    continue

                # Detect if already has a real value — works for both input & div comboboxes
                already_filled = False
                try:
                    existing = combo.input_value()
                    if existing and existing.lower().strip() not in _BLANK_OPTION_VALUES:
                        already_filled = True
                except Exception:
                    # div-based combobox — check aria-activedescendant or displayed text
                    try:
                        if combo.get_attribute("aria-activedescendant"):
                            already_filled = True
                        else:
                            text = combo.inner_text().strip()
                            if text and text.lower() not in _BLANK_OPTION_VALUES:
                                already_filled = True
                    except Exception:
                        pass
                if already_filled:
                    continue

                label = _get_element_label(page, combo).lower()
                if not label:
                    continue

                answer = None
                is_location = False

                if any(k in label for k in ["authorized", "eligible", "work in the",
                                             "employment authorization", "legally"]):
                    # Split LinkedIn combos: "currently authorized" vs "future sponsor"
                    if "future" in label and "sponsor" in label:
                        answer = "Yes" if require_sponsorship else "No"
                    elif "currently" in label and "authorized" in label:
                        answer = "Yes"
                    else:
                        answer = "Yes"
                elif "sponsor" in label:
                    answer = "Yes" if require_sponsorship else "No"
                elif "relocat" in label:
                    answer = "Yes" if profile.get("willing_to_relocate", False) else "No"
                elif any(k in label for k in ["notice", "start date"]):
                    answer = profile.get("notice_period", "Immediately")
                elif any(k in label for k in ["city", "location", "zip", "postal",
                                               "where are you", "where do you live",
                                               "current location"]):
                    city = profile.get("city", "")
                    state = profile.get("state", "")
                    if city:
                        answer = f"{city}, {state}" if state else city
                        is_location = True

                if not answer:
                    continue

                combo.click()
                page.wait_for_timeout(800)

                if is_location:
                    try:
                        # Clear and type to trigger autocomplete
                        try:
                            combo.fill("")
                        except Exception:
                            pass
                        combo.type(answer, delay=60)
                        page.wait_for_timeout(1500)
                        opt = page.locator("[role='option']").first
                        if opt.count() and opt.is_visible(timeout=2000):
                            opt.click()
                            print(f"[Agent] Location combobox -> {answer}")
                        else:
                            combo.press("Tab")
                            print(f"[Agent] Location typed (no dropdown): {answer}")
                    except Exception:
                        pass
                    continue

                # Standard yes/no/value combobox
                clicked = False
                for opt_sel in [
                    f"[role='option']:has-text('{answer}')",
                    f"li:has-text('{answer}')",
                    f"[data-value='{answer}']",
                ]:
                    try:
                        opt = page.locator(opt_sel).first
                        if opt.count() and opt.is_visible(timeout=800):
                            opt.click()
                            print(f"[Agent] Combobox '{label[:50]}' -> {answer}")
                            clicked = True
                            break
                    except Exception:
                        pass
                if not clicked:
                    page.keyboard.press("Escape")

            except Exception:
                continue
    except Exception:
        pass


# ===========================================================================
# Resume upload
# ===========================================================================

def _try_resume_upload(page, profile: dict):
    global _resume_uploaded
    if _resume_uploaded:
        return

    resume_path = profile.get("resume_path", "")
    if not resume_path:
        return
    if not os.path.isabs(resume_path):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        resume_path = os.path.join(root, resume_path)
    if not os.path.exists(resume_path):
        print(f"[Agent] Resume not found: {resume_path} — upload manually.")
        return

    try:
        for fi in page.locator("input[type='file']").all():
            try:
                accept = (fi.get_attribute("accept") or "").lower()
                if accept and not any(k in accept for k in
                                      ["pdf", "doc", "application", "*", ".pdf", ".doc"]):
                    continue
                fi.set_input_files(resume_path)
                _resume_uploaded = True
                print(f"[Agent] Resume uploaded: {os.path.basename(resume_path)}")
                page.wait_for_timeout(3000)
                return
            except Exception:
                continue
    except Exception as e:
        print(f"[Agent] Resume upload failed: {e}")


# ===========================================================================
# Numeric inputs (years of experience, salary, GPA)
# ===========================================================================

def _handle_numeric_inputs(page, profile: dict):
    years_exp = str(profile.get("years_experience", "10"))

    try:
        for inp in page.locator("input[type='text'], input[type='number']").all():
            try:
                if not inp.is_visible(timeout=300):
                    continue
                if inp.input_value():
                    continue
                label = _get_element_label(page, inp).lower()
                if not label:
                    continue

                if any(k in label for k in ["years of experience", "years experience",
                                             "how many years", "years with", "years in",
                                             "years of relevant"]):
                    _smart_fill(inp, years_exp)
                    print(f"[Agent] Years of experience -> {years_exp}")

                elif any(k in label for k in ["salary", "compensation", "expected pay",
                                               "desired salary", "pay rate",
                                               "salary expectation", "annual salary"]):
                    sal = str(profile.get("salary_expectation", ""))
                    if sal and not sal.startswith("PLACEHOLDER"):
                        _smart_fill(inp, sal)
                        print(f"[Agent] Salary -> {sal}")

                elif "gpa" in label:
                    gpa = str(profile.get("gpa", ""))
                    if gpa:
                        _smart_fill(inp, gpa)
                        print(f"[Agent] GPA -> {gpa}")

            except Exception:
                continue
    except Exception:
        pass


# ===========================================================================
# Free-text / open-ended questions via Groq
# ===========================================================================

# Words in a field label that mean it's a contact/structural field — skip Groq
_CONTACT_SKIP = {
    "name", "email", "phone", "address", "city", "state", "zip",
    "postal", "linkedin", "website", "url", "street",
    "country", "company", "employer", "github", "portfolio", "area code",
}

# Words that indicate the field is a real open question
_QUESTION_SIGNALS = [
    "describe", "explain", "why", "how", "what", "tell us", "tell me",
    "summarize", "elaborate", "discuss", "share", "provide",
    "background", "experience", "motivation", "interest", "goal",
    "strength", "weakness", "challenge", "achievement", "accomplishment",
    "additional information", "anything else", "comments", "message",
]

# Textarea labels that are always open questions regardless of signals
_ALWAYS_ANSWER = ["cover letter", "letter of intent", "personal statement"]


def _handle_text_questions(page, profile: dict, job: dict):
    """Fill open-ended text fields and textareas using Groq."""

    def _skip(label: str) -> bool:
        lo = label.lower()
        return any(p in lo for p in _CONTACT_SKIP)

    def _is_open(label: str) -> bool:
        lo = label.lower()
        if "?" in label:
            return True
        if any(k in lo for k in _ALWAYS_ANSWER):
            return True
        return any(k in lo for k in _QUESTION_SIGNALS)

    # Textareas
    try:
        for ta in page.locator("textarea").all():
            try:
                if not ta.is_visible(timeout=300):
                    continue
                existing = ta.input_value()
                if existing and len(existing) > 20:
                    continue
                question = _get_element_label(page, ta)
                if not question or len(question) < 6 or _skip(question):
                    continue
                if not _is_open(question):
                    continue
                print(f"[Agent] Q (textarea): {question[:100]}")
                answer = ask_groq(question, profile, job)
                print(f"[Agent] A: {answer[:100]}")
                page.wait_for_timeout(800)
                ta.click()
                ta.fill(answer)
                ta.dispatch_event("input")
                ta.dispatch_event("change")
                print("[Agent] Textarea answered")
            except Exception:
                continue
    except Exception:
        pass

    # Single-line text and number inputs (fast-path + Groq for open questions)
    _CONTACT_IDS = {
        "phone", "email", "city", "first", "last", "linkedin", "name",
        "zip", "postal", "address", "state",
    }

    def _is_contact_input(inp) -> bool:
        for attr in ["id", "name", "placeholder"]:
            val = (inp.get_attribute(attr) or "").lower()
            if any(k in val for k in _CONTACT_IDS):
                return True
        return False

    def _fast_path_answer(label_lo: str) -> str | None:
        """Return a hard-coded answer if the label matches a known field."""
        if any(k in label_lo for k in ["years of experience", "years experience",
                                        "how many years", "years with", "years in",
                                        "years of relevant"]):
            return str(profile.get("years_experience", "10"))
        if any(k in label_lo for k in ["salary", "compensation", "expected pay",
                                        "desired salary", "pay rate",
                                        "salary expectation", "annual salary"]):
            sal = str(profile.get("desired_salary", profile.get("salary_expectation", "")))
            return sal if sal and not sal.startswith("PLACEHOLDER") else None
        if "gpa" in label_lo:
            gpa = str(profile.get("gpa", ""))
            return gpa if gpa else None
        if "github" in label_lo:
            gh = str(profile.get("github_url", ""))
            return gh if gh else None
        if any(k in label_lo for k in ["portfolio", "personal site", "personal website"]):
            pf = str(profile.get("portfolio_url", profile.get("website_url", "")))
            return pf if pf else None
        if any(k in label_lo for k in ["notice period", "start date", "available",
                                        "when can you start", "earliest start"]):
            return str(profile.get("notice_period", "2 weeks"))
        if any(k in label_lo for k in ["overtime", "hours per week"]):
            return "40"
        if "travel" in label_lo and any(k in label_lo for k in ["percent", "%"]):
            return "10"
        return None

    try:
        for inp in page.locator("input[type='text'], input[type='number']").all():
            try:
                if not inp.is_visible(timeout=300):
                    continue
                if inp.input_value():
                    continue
                if _is_contact_input(inp):
                    continue
                label = _get_element_label(page, inp)
                if not label or len(label) < 5 or _skip(label):
                    continue
                label_lo = label.lower()
                is_number = inp.get_attribute("type") == "number"

                # Fast-path: known numeric / structured fields
                fp = _fast_path_answer(label_lo)
                if fp is not None:
                    answer = re.sub(r"[^\d.]", "", fp) if is_number else fp
                    if answer:
                        _smart_fill(inp, answer)
                        print(f"[Agent] ✓ Input field \"{label[:60]}\" → {answer[:40]}")
                    continue

                # Open-question fallback via Groq
                if not _is_open(label):
                    continue
                print(f"[Agent] Q (input): {label[:100]}")
                answer = ask_groq(label, profile, job)
                if is_number:
                    answer = re.sub(r"[^\d.]", "", answer)
                elif "." in answer:
                    answer = answer.split(".")[0].strip() + "."
                if answer:
                    _smart_fill(inp, answer)
                    print(f"[Agent] ✓ Input field \"{label[:60]}\" → {answer[:40]}")
            except Exception:
                continue
    except Exception:
        pass


# ===========================================================================
# Label resolution (aria-labelledby → aria-label → label[for] → DOM walk → placeholder)
# ===========================================================================

def _get_element_label(page, element) -> str:
    """Return the best human-readable label text for any form element."""
    try:
        # 1. aria-labelledby (highest priority — explicit association)
        labelledby = element.get_attribute("aria-labelledby") or ""
        if labelledby:
            parts = []
            for lid in labelledby.split():
                try:
                    el = page.locator(f"#{lid}")
                    if el.count():
                        t = el.inner_text().strip()
                        if t:
                            parts.append(t)
                except Exception:
                    pass
            if parts:
                return " ".join(parts)

        # 2. aria-label
        aria = element.get_attribute("aria-label") or ""
        if aria.strip():
            return aria.strip()

        # 3. <label for="id">
        elem_id = element.get_attribute("id") or ""
        if elem_id:
            safe_id = elem_id.replace(":", r"\:").replace(".", r"\.")
            try:
                lbl = page.locator(f"label[for='{safe_id}']")
                if lbl.count():
                    t = lbl.inner_text().strip()
                    if t:
                        return t
            except Exception:
                pass

        # 4. DOM traversal — previous siblings then parent containers
        result = element.evaluate("""el => {
            // Check immediate previous siblings first
            let prev = el.previousElementSibling;
            for (let i = 0; prev && i < 3; i++, prev = prev.previousElementSibling) {
                const tag = prev.tagName.toLowerCase();
                if (['label','span','p','legend','div','h1','h2','h3','h4'].includes(tag)) {
                    const t = prev.textContent.trim();
                    if (t.length > 3 && t.length < 300) return t;
                }
            }
            // Walk up parent containers
            let parent = el.parentElement;
            for (let depth = 0; depth < 6; depth++) {
                if (!parent) break;
                const candidates = parent.querySelectorAll(
                    'label, legend, ' +
                    'span.t-bold, span.fb-dash-form-element__label, ' +
                    '[data-test-form-label-text], [data-automation-id$="Label"], ' +
                    '.field-label, .form-label, .label-text, ' +
                    '.jobs-easy-apply-form-section__group-subtitle, ' +
                    '[class*="label"], [class*="Label"]'
                );
                for (const c of candidates) {
                    if (c.contains(el)) continue;
                    const t = c.textContent.trim();
                    if (t.length > 3 && t.length < 300) return t;
                }
                parent = parent.parentElement;
            }
            return '';
        }""")
        if result:
            return result

        # 5. Placeholder text as last resort
        return element.get_attribute("placeholder") or ""

    except Exception:
        return ""


# ===========================================================================
# Safe Next/Continue click
# ===========================================================================

def _click_next_safe(page) -> bool:
    """
    Click the wizard's Next/Continue button.
    NEVER clicks Submit / Apply Now / Finish (see FORBIDDEN sets).
    Pass 1: match by exact safe-label list.
    Pass 2: fallback scan for any forward-action button not in the forbidden set.
    """
    FORBIDDEN = {
        "submit application", "submit my application", "send application",
        "apply now", "complete application", "finish application",
        "submit and apply",
    }
    FORBIDDEN_EXACT = {"apply", "submit", "finish"}

    SAFE_LABELS = [
        "Next", "Continue", "Continue to next step", "Continue to next",
        "Save and continue", "Save & Continue", "Save & Next",
        "Next step", "Next page", "Next section",
        "Proceed", "Continue to Application",
        "Review", "Review Application",
        "Next >", ">", "Forward",
    ]

    def _is_forbidden(text: str) -> bool:
        t = text.lower().strip()
        if t in FORBIDDEN_EXACT:
            return True
        return any(f in t for f in FORBIDDEN)

    # Pass 1 — safe label list
    for label in SAFE_LABELS:
        for selector in [
            f"button:has-text('{label}')",
            f"[role='button']:has-text('{label}')",
            f"a:has-text('{label}')",
            f"button[aria-label='{label}']",
            f"button[aria-label*='{label}']",
        ]:
            try:
                btn = page.locator(selector).first
                if not btn.is_visible(timeout=400):
                    continue
                btn_text = btn.inner_text().lower().strip()
                if _is_forbidden(btn_text):
                    continue
                btn.scroll_into_view_if_needed()
                btn.click()
                page.wait_for_timeout(2500)
                print(f"[Agent] Clicked '{btn_text}'")
                return True
            except Exception:
                continue

    # Pass 2 — fallback: any button with a forward-action keyword
    FORWARD = ["next", "continue", "proceed", "forward", "advance"]
    BACKWARD = ["cancel", "back", "previous", "discard", "close",
                 "dismiss", "skip", "save draft", "save for later", "delete"]
    try:
        for btn in page.locator("button, input[type='button'], input[type='submit']").all():
            try:
                if not btn.is_visible(timeout=300):
                    continue
                raw = btn.inner_text() or btn.get_attribute("value") or ""
                btn_text = raw.lower().strip()
                if not btn_text:
                    continue
                if _is_forbidden(btn_text):
                    continue
                if any(k in btn_text for k in BACKWARD):
                    continue
                if any(k in btn_text for k in FORWARD):
                    btn.scroll_into_view_if_needed()
                    btn.click()
                    page.wait_for_timeout(2500)
                    print(f"[Agent] Clicked (fallback) '{btn_text}'")
                    return True
            except Exception:
                continue
    except Exception:
        pass

    return False


# ===========================================================================
# Human-in-the-loop STOP gate — the ONLY termination path
# ===========================================================================

def stop_on_review(page, reason: str = ""):
    """
    Halt the agent. Highlight the Submit button red. Wait for human to review and submit.
    The agent NEVER clicks Submit — that is always left to you.
    """
    print("\n" + "=" * 65)
    print("  AGENT STOPPED — YOUR REVIEW IS REQUIRED")
    print("=" * 65)
    if reason:
        print(f"  Reason: {reason}")
    print("  All detectable fields have been filled.")
    print("  The SUBMIT button is highlighted RED in your browser.")
    print()
    print("  WHAT TO DO:")
    print("  1. Switch to the Chrome browser window")
    print("  2. Review every field carefully")
    print("  3. Fix anything that needs correcting")
    print("  4. Click SUBMIT yourself when you are satisfied")
    print("  5. Return here and press Enter to close the browser")
    print("=" * 65)

    submit_selectors = [
        "button[aria-label='Submit application']",
        "button[aria-label='Submit Application']",
        "button:has-text('Submit application')",
        "button:has-text('Submit Application')",
        "button:has-text('Submit My Application')",
        "button:has-text('Apply Now')",
        "button:has-text('Apply now')",
        "button:has-text('Submit')",
        "button:has-text('Send Application')",
        "button:has-text('Complete Application')",
        "button:has-text('Finish Application')",
        "button:has-text('Finish')",
        "button[type='submit']",
        "input[type='submit']",
    ]

    highlighted = 0
    for sel in submit_selectors:
        try:
            btns = page.locator(sel)
            for i in range(min(btns.count(), 5)):
                try:
                    btns.nth(i).evaluate("""el => {
                        el.style.border          = '4px solid #ff0000';
                        el.style.boxShadow       = '0 0 20px 6px #ff0000';
                        el.style.backgroundColor = '#fff0f0';
                        el.style.color           = '#cc0000';
                        el.style.fontWeight      = 'bold';
                        el.setAttribute('title', 'AGENT STOPPED — click here to submit');
                    }""")
                    highlighted += 1
                except Exception:
                    pass
        except Exception:
            pass

    if highlighted:
        print(f"\n  [{highlighted} Submit button(s) highlighted red in your browser]")
    else:
        print("\n  [No Submit button auto-detected — scroll through the form to locate it]")

    try:
        input("\n  >>> Press Enter in this terminal to close the browser... ")
    except (EOFError, KeyboardInterrupt):
        print("\n[Agent] Session closed.")


# ===========================================================================
# Radio button handler — enhanced (FIX 1)
# Handles Yes/No groups, sponsorship, relocation, hybrid/on-site, and Groq fallback
# ===========================================================================

def _handle_radio_buttons(page, profile: dict):
    """
    Find all visible radio groups, infer the correct answer from the group label,
    and select the matching radio. Falls back to Groq for unrecognised questions.
    Safety: never clicks Submit — only selects radio inputs within groups.
    """
    require_sponsorship = profile.get("require_sponsorship", False)
    willing_to_relocate = profile.get("willing_to_relocate", False)

    def _decide(label_text: str, option_texts: list) -> str | None:
        t = label_text.lower()
        if any(k in t for k in ["authorized", "eligible", "legally", "work in the us",
                                  "work in us", "right to work", "employment authorization"]):
            if "future" in t and "sponsor" in t:
                return "Yes" if require_sponsorship else "No"
            return "Yes"
        if re.search(r"\bsponsor\b", t):
            return "Yes" if require_sponsorship else "No"
        if any(k in t for k in ["relocat", "willing to travel", "open to travel"]):
            return "Yes" if willing_to_relocate else "No"
        if any(k in t for k in ["hybrid", "on-site", "onsite", "in-office", "in office"]):
            return "Yes"
        return None  # Unknown — caller will try Groq

    def _select_radio(group_radios: list, answer: str, label_text: str, page) -> bool:
        """Click the radio whose visible label text matches `answer`."""
        for radio in group_radios:
            try:
                if not radio.is_visible(timeout=300) or not radio.is_enabled():
                    continue
                rid = radio.get_attribute("id") or ""
                lbl_text = ""
                if rid:
                    lbl = page.locator(f"label[for='{rid}']")
                    if lbl.count():
                        lbl_text = lbl.inner_text().strip()
                if not lbl_text:
                    lbl_text = radio.evaluate(
                        "el => el.value || (el.nextSibling && el.nextSibling.textContent"
                        " ? el.nextSibling.textContent.trim() : '')"
                    ) or ""
                val = radio.get_attribute("value") or ""
                if (lbl_text.lower() == answer.lower()
                        or answer.lower() in lbl_text.lower()
                        or val.lower() == answer.lower()):
                    radio.check()
                    selected = lbl_text or val or answer
                    print(f"[Agent] ✓ Radio: {label_text[:60]} → {selected}")
                    return True
            except Exception:
                continue
        return False

    # ── fieldset/legend groups ────────────────────────────────────────────
    try:
        for group in page.locator("fieldset").all():
            try:
                if not group.is_visible(timeout=300):
                    continue
                radios = group.locator("input[type='radio']").all()
                if not radios:
                    continue
                if any(_safe_checked(r) for r in radios):
                    continue  # Already answered
                question = _fieldset_label(group, page)
                if not question:
                    question = _get_element_label(page, radios[0])
                if not question:
                    continue
                option_texts = []
                for r in radios:
                    try:
                        rid = r.get_attribute("id") or ""
                        if rid:
                            lbl = page.locator(f"label[for='{rid}']")
                            if lbl.count():
                                option_texts.append(lbl.inner_text().strip())
                    except Exception:
                        pass

                answer = _decide(question, option_texts)
                if answer is None and len(option_texts) == 2:
                    # Groq fallback — only for binary (2-option) groups
                    try:
                        opts_str = ", ".join(option_texts)
                        groq_q = (
                            f'Job application question (radio): "{question}"\n'
                            f"Options: {opts_str}\n"
                            f"Candidate: {profile.get('custom_question_context', '')}\n"
                            "Answer with ONLY one of the option texts exactly."
                        )
                        answer = ask_groq(groq_q, profile).strip()
                    except Exception:
                        continue
                if answer is None:
                    continue
                _select_radio(radios, answer, question, page)
            except Exception:
                continue
    except Exception:
        pass

    # ── name-grouped radios not inside a fieldset ─────────────────────────
    try:
        handled_names: set = set()
        for radio in page.locator("input[type='radio']").all():
            try:
                if not radio.is_visible(timeout=300):
                    continue
                name = radio.get_attribute("name") or ""
                if not name or name in handled_names:
                    continue
                # Collect all radios with the same name
                group_radios = page.locator(f"input[type='radio'][name='{name}']").all()
                if any(_safe_checked(r) for r in group_radios):
                    handled_names.add(name)
                    continue
                question = _get_radio_question_text(page, radio)
                if not question:
                    question = _get_element_label(page, radio)
                if not question or len(question) < 5:
                    handled_names.add(name)
                    continue
                option_texts = []
                for r in group_radios:
                    try:
                        rid = r.get_attribute("id") or ""
                        if rid:
                            lbl = page.locator(f"label[for='{rid}']")
                            if lbl.count():
                                option_texts.append(lbl.inner_text().strip())
                    except Exception:
                        pass

                answer = _decide(question, option_texts)
                if answer is None and len(option_texts) == 2:
                    try:
                        opts_str = ", ".join(option_texts)
                        groq_q = (
                            f'Job application question (radio): "{question}"\n'
                            f"Options: {opts_str}\n"
                            f"Candidate: {profile.get('custom_question_context', '')}\n"
                            "Answer with ONLY one of the option texts exactly."
                        )
                        answer = ask_groq(groq_q, profile).strip()
                    except Exception:
                        handled_names.add(name)
                        continue
                if answer is None:
                    handled_names.add(name)
                    continue
                _select_radio(group_radios, answer, question, page)
                handled_names.add(name)
            except Exception:
                continue
    except Exception:
        pass


# ===========================================================================
# Indeed multi-step wizard (FIX 4)
# ===========================================================================

def _fill_indeed_apply(page, profile: dict, job: dict):
    """
    Walk the Indeed hosted application wizard step by step.
    Stops the moment a review/submit page is detected — never clicks Submit.
    """
    print("[Agent] Indeed Apply wizard — starting multi-step fill...")

    for step_num in range(20):
        page.wait_for_timeout(2000)
        print(f"\n[Agent] Indeed — Step {step_num + 1}  ({page.url[:70]})")

        if _is_login_page(page):
            print("[Agent] Login page — stopping for manual review.")
            stop_on_review(page)
            return

        # Detect review/confirmation page
        review_detected = False
        for sel in [
            "button:has-text('Submit')",
            "button[data-testid='IndeedApplyButton-submit']",
        ]:
            try:
                if page.locator(sel).first.is_visible(timeout=500):
                    review_detected = True
                    break
            except Exception:
                pass
        if not review_detected:
            for heading_sel in ["h1", "h2", "h3"]:
                try:
                    for el in page.locator(heading_sel).all():
                        txt = el.inner_text().lower()
                        if any(k in txt for k in ["review", "confirm", "almost done",
                                                   "final step", "check your"]):
                            review_detected = True
                            break
                    if review_detected:
                        break
                except Exception:
                    pass
        if review_detected:
            print("[Agent] Indeed review page detected.")
            stop_on_review(page)
            return

        # Fill all visible fields in prescribed order
        _fill_contact_fields(page, profile)
        _handle_radio_buttons(page, profile)
        _handle_select_dropdowns(page, profile)
        _handle_comboboxes(page, profile)
        _handle_checkboxes(page, profile)
        _try_resume_upload(page, profile)
        _handle_numeric_inputs(page, profile)
        _handle_text_questions(page, profile, job)

        # Re-check after fill
        for heading_sel in ["h1", "h2", "h3"]:
            try:
                for el in page.locator(heading_sel).all():
                    if any(k in el.inner_text().lower() for k in
                           ["review", "confirm", "almost done", "final step"]):
                        print("[Agent] Indeed review page detected after fill.")
                        stop_on_review(page)
                        return
            except Exception:
                pass

        # Try to advance to next step
        next_clicked = _click_next_safe(page)
        if not next_clicked:
            # Try Indeed-specific buttons
            for sel in [
                "button[data-testid='IndeedApplyButton-primary']",
                "button:has-text('Continue')",
                "button:has-text('Next')",
            ]:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=500):
                        btn_text = btn.inner_text().lower().strip()
                        # Safety: never click submit/apply
                        if not any(f in btn_text for f in ["submit", "apply", "finish"]):
                            btn.click()
                            page.wait_for_timeout(2500)
                            next_clicked = True
                            break
                except Exception:
                    pass

        if not next_clicked:
            print("[Agent] No Next button found — stopping for manual review.")
            stop_on_review(page)
            return

    print("[Agent] Indeed: maximum steps reached.")
    stop_on_review(page)


# ===========================================================================
# Greenhouse ATS filler (FIX 7)
# ===========================================================================

def _fill_greenhouse(page, profile: dict, job: dict):
    """Fill a Greenhouse-hosted application form (single-page)."""
    print("[Agent] Greenhouse — filling application form...")
    page.wait_for_timeout(3000)

    # Greenhouse-specific contact selectors (supplement generic ones)
    greenhouse_fields = [
        (["input#first_name"], profile.get("first_name", ""), "first name"),
        (["input#last_name"],  profile.get("last_name", ""),  "last name"),
        (["input#email"],      profile.get("email", ""),      "email"),
        (["input#phone"],      profile.get("phone", ""),      "phone"),
        (
            ["input[name='job_application[location]']",
             "input[id*='location' i]"],
            f"{profile.get('city', '')}, {profile.get('state', '')}",
            "location",
        ),
        (
            ["input[name='job_application[linkedin_url]']"],
            profile.get("linkedin_url", ""), "LinkedIn URL",
        ),
    ]
    _apply_field_list(page, greenhouse_fields)

    # Generic field-fill pass (canonical order)
    _fill_contact_fields(page, profile)
    _fill_address_fields(page, profile)
    _handle_radio_buttons(page, profile)
    _handle_select_dropdowns(page, profile)
    _handle_comboboxes(page, profile)
    _handle_checkboxes(page, profile)
    _try_resume_upload(page, profile)
    _handle_numeric_inputs(page, profile)
    _handle_text_questions(page, profile, job)

    # Cover letter textarea
    try:
        for ta in page.locator("textarea").all():
            try:
                if not ta.is_visible(timeout=300):
                    continue
                if ta.input_value() and len(ta.input_value()) > 20:
                    continue
                lbl = _get_element_label(page, ta).lower()
                if any(k in lbl for k in ["cover letter", "why", "tell us"]):
                    answer = ask_groq(lbl or "Why are you interested in this role?",
                                      profile, job)
                    ta.click()
                    ta.fill(answer)
                    ta.dispatch_event("input")
                    print("[Agent] Greenhouse cover letter filled")
                    break
            except Exception:
                continue
    except Exception:
        pass

    stop_on_review(page)


# ===========================================================================
# Lever ATS filler (FIX 7)
# ===========================================================================

def _fill_lever(page, profile: dict, job: dict):
    """Fill a Lever-hosted application form (single-page)."""
    print("[Agent] Lever — filling application form...")
    page.wait_for_timeout(2000)

    lever_fields = [
        (["input[name='name']"],   profile.get("name", ""),          "name"),
        (["input[name='email']"],  profile.get("email", ""),         "email"),
        (["input[name='phone']"],  profile.get("phone", ""),         "phone"),
        (["input[name='org']"],    profile.get("current_company",
                                               profile.get("current_employer", "")), "company"),
        (["input[name='urls[LinkedIn]']", "input[placeholder*='LinkedIn' i]"],
         profile.get("linkedin_url", ""), "LinkedIn URL"),
        (["input[name='urls[GitHub]']"],
         profile.get("github_url", ""), "GitHub URL"),
        (["input[name='urls[Portfolio]']"],
         profile.get("portfolio_url", ""), "Portfolio URL"),
    ]
    _apply_field_list(page, lever_fields)

    # Cover letter / comments textarea
    try:
        for ta in page.locator("textarea[name='comments'], textarea[name='cover_letter'],"
                               " textarea[placeholder*='cover' i],"
                               " textarea[placeholder*='message' i]").all():
            try:
                if not ta.is_visible(timeout=500):
                    continue
                if ta.input_value() and len(ta.input_value()) > 20:
                    continue
                question = _get_element_label(page, ta) or "Why are you interested in this role?"
                answer = ask_groq(question, profile, job)
                ta.fill(answer)
                ta.dispatch_event("input")
                print("[Agent] Lever cover letter / comments filled")
                break
            except Exception:
                continue
    except Exception:
        pass

    _fill_contact_fields(page, profile)
    _handle_radio_buttons(page, profile)
    _handle_select_dropdowns(page, profile)
    _handle_comboboxes(page, profile)
    _handle_checkboxes(page, profile)
    _try_resume_upload(page, profile)
    _handle_numeric_inputs(page, profile)
    _handle_text_questions(page, profile, job)

    stop_on_review(page)


# ===========================================================================
# Workday ATS filler (FIX 7)
# ===========================================================================

def _fill_workday(page, profile: dict, job: dict):
    """
    Best-effort Workday filler.
    Workday is a heavy React SPA; we walk up to 5 pages filling what we can.
    """
    print("[Agent] Workday — waiting for React app to load...")
    page.wait_for_timeout(3000)

    for step_num in range(5):
        print(f"\n[Agent] Workday — Step {step_num + 1}")

        if _is_login_page(page):
            stop_on_review(page)
            return

        # Detect Workday review page
        review_detected = False
        for sel in [
            "button[aria-label*='Submit' i]",
            "button:has-text('Submit')",
        ]:
            try:
                if page.locator(sel).first.is_visible(timeout=500):
                    review_detected = True
                    break
            except Exception:
                pass
        if not review_detected:
            try:
                for h in page.locator("h1, h2").all():
                    if "review" in h.inner_text().lower():
                        review_detected = True
                        break
            except Exception:
                pass
        if review_detected:
            print("[Agent] Workday review page detected.")
            stop_on_review(page)
            return

        _fill_contact_fields(page, profile)
        _fill_address_fields(page, profile)
        _fill_work_fields(page, profile)
        _handle_radio_buttons(page, profile)
        _handle_select_dropdowns(page, profile)
        _handle_comboboxes(page, profile)
        _handle_checkboxes(page, profile)
        _try_resume_upload(page, profile)
        _handle_numeric_inputs(page, profile)
        _handle_text_questions(page, profile, job)

        # Workday "Save and Continue" / "Next"
        advanced = False
        for sel in [
            "button:has-text('Save and Continue')",
            "button:has-text('Next')",
            "button:has-text('Continue')",
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=800):
                    btn_text = btn.inner_text().lower().strip()
                    if not any(f in btn_text for f in ["submit", "apply", "finish"]):
                        btn.click()
                        page.wait_for_timeout(3000)
                        advanced = True
                        break
            except Exception:
                pass

        if not advanced:
            if not _click_next_safe(page):
                print("[Agent] No Next/Continue on Workday — stopping for manual review.")
                stop_on_review(page)
                return

    print("[Agent] Workday: maximum pages reached.")
    stop_on_review(page)
