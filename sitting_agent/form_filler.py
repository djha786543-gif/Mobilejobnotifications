"""
Form filling logic for LinkedIn Easy Apply, Indeed Apply, and generic forms.

Safety contract:
  - _click_next_safe() will NEVER click a submit/apply/finish button
  - stop_on_review() is the only place the agent intentionally stops
  - The agent highlights the Submit button but NEVER clicks it
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sitting_agent.groq_responder import ask_groq


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fill_application(page, profile: dict, url: str):
    """Route to the correct form filler based on job URL."""
    if "linkedin.com" in url:
        _fill_linkedin_easy_apply(page, profile)
    elif "indeed.com" in url:
        _fill_indeed_apply(page, profile)
    else:
        _fill_generic(page, profile)


# ---------------------------------------------------------------------------
# LinkedIn Easy Apply
# ---------------------------------------------------------------------------

def _fill_linkedin_easy_apply(page, profile: dict):
    print("[Agent] Looking for Easy Apply button...")

    clicked = False
    for sel in [
        "button:has-text('Easy Apply')",
        "button[aria-label*='Easy Apply']",
        ".jobs-apply-button",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible():
                btn.scroll_into_view_if_needed()
                btn.click()
                page.wait_for_timeout(2000)
                print("[Agent] ✓ Easy Apply modal opened")
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        print("[Agent] ⚠️  No 'Easy Apply' button found.")
        print("[Agent]    This job may redirect to the company's own site.")
        print("[Agent]    Attempting generic form fill on current page...")
        _fill_generic(page, profile)
        return

    # Walk through the wizard — max 15 steps
    for step_num in range(15):
        page.wait_for_timeout(2000)
        print(f"\n[Agent] --- Step {step_num + 1} ---")

        # SAFETY CHECK FIRST — always check before doing anything
        if _is_review_page(page):
            stop_on_review(page)
            return

        _fill_contact_fields(page, profile)
        _handle_work_auth_dropdowns(page, profile)
        _try_resume_upload(page, profile)
        _handle_text_questions(page, profile)

        advanced = _click_next_safe(page)
        if not advanced:
            page.wait_for_timeout(1500)
            # Final review check before giving up
            if _is_review_page(page):
                stop_on_review(page)
            else:
                print("[Agent] No more navigation buttons found — stopping here.")
                stop_on_review(page)
            return

    print("[Agent] Maximum steps reached — stopping.")
    stop_on_review(page)


def _is_review_page(page) -> bool:
    """Detect LinkedIn's final review page using multiple indicators."""
    indicators = [
        "button[aria-label='Submit application']",
        "button[aria-label='Submit Application']",
        "button:has-text('Submit application')",
        "button:has-text('Submit Application')",
        "h3:has-text('Review your application')",
        "h2:has-text('Review your application')",
    ]
    for ind in indicators:
        try:
            if page.locator(ind).count() > 0:
                return True
        except Exception:
            pass
    return False


# ---------------------------------------------------------------------------
# Indeed Apply
# ---------------------------------------------------------------------------

def _fill_indeed_apply(page, profile: dict):
    print("[Agent] Indeed apply flow — filling visible fields...")
    page.wait_for_timeout(2000)
    _fill_contact_fields(page, profile)
    _handle_work_auth_dropdowns(page, profile)
    _try_resume_upload(page, profile)
    _handle_text_questions(page, profile)
    stop_on_review(page)


# ---------------------------------------------------------------------------
# Generic form fallback
# ---------------------------------------------------------------------------

def _fill_generic(page, profile: dict):
    print("[Agent] Generic form — filling recognisable fields...")
    page.wait_for_timeout(2000)
    _fill_contact_fields(page, profile)
    _handle_work_auth_dropdowns(page, profile)
    _try_resume_upload(page, profile)
    _handle_text_questions(page, profile)
    stop_on_review(page)


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------

def _fill_contact_fields(page, profile: dict):
    """Fill standard contact fields: phone, city, LinkedIn URL."""
    fields = [
        (
            ["input[id*='phoneNumber']", "input[name*='phone']",
             "input[placeholder*='Phone']", "input[aria-label*='Phone']",
             "input[type='tel']"],
            profile.get("phone", ""),
            "phone",
        ),
        (
            ["input[id*='city']", "input[name*='city']",
             "input[placeholder*='City']", "input[aria-label*='City']"],
            profile.get("city", ""),
            "city",
        ),
        (
            ["input[placeholder*='LinkedIn']", "input[id*='linkedin']",
             "input[aria-label*='LinkedIn profile']",
             "input[aria-label*='LinkedIn URL']"],
            profile.get("linkedin_url", ""),
            "LinkedIn URL",
        ),
        (
            ["input[id*='firstName']", "input[name*='firstName']",
             "input[placeholder*='First name']", "input[aria-label*='First name']"],
            profile.get("first_name", ""),
            "first name",
        ),
        (
            ["input[id*='lastName']", "input[name*='lastName']",
             "input[placeholder*='Last name']", "input[aria-label*='Last name']"],
            profile.get("last_name", ""),
            "last name",
        ),
        (
            ["input[id*='email']", "input[name*='email']",
             "input[type='email']", "input[placeholder*='Email']"],
            profile.get("email", ""),
            "email",
        ),
    ]

    for selectors, value, label in fields:
        if not value or str(value).startswith("PLACEHOLDER"):
            continue
        for sel in selectors:
            try:
                inp = page.locator(sel).first
                if inp.count() > 0 and inp.is_visible():
                    current = inp.input_value()
                    if not current:  # Only fill if the field is empty
                        inp.fill(value)
                        print(f"[Agent] ✓ Filled {label}: {value[:40]}")
                    else:
                        print(f"[Agent] — {label} already filled: {current[:40]}")
                    break
            except Exception:
                continue


def _handle_work_auth_dropdowns(page, profile: dict):
    """
    Handle work-authorisation and sponsorship <select> dropdowns.
    LinkedIn also uses div-based custom dropdowns — handled separately.
    """
    require_sponsorship = profile.get("require_sponsorship", False)

    # --- Standard HTML <select> ---
    try:
        selects = page.locator("select").all()
        for sel_elem in selects:
            try:
                if not sel_elem.is_visible():
                    continue

                # Get associated label text
                label_text = _get_element_label(page, sel_elem).lower()

                if any(k in label_text for k in
                       ["authorized", "eligible", "work in the", "work legally"]):
                    sel_elem.select_option(label="Yes")
                    print("[Agent] ✓ Work authorisation dropdown → Yes")

                elif "sponsor" in label_text:
                    answer = "Yes" if require_sponsorship else "No"
                    sel_elem.select_option(label=answer)
                    print(f"[Agent] ✓ Sponsorship dropdown → {answer}")

            except Exception:
                continue
    except Exception:
        pass

    # --- LinkedIn custom div-dropdowns (role="combobox") ---
    try:
        combos = page.locator("select, [role='combobox']").all()
        for combo in combos:
            try:
                if not combo.is_visible():
                    continue
                label_text = _get_element_label(page, combo).lower()

                if any(k in label_text for k in
                       ["authorized", "eligible", "work in the"]):
                    # Try clicking to open, then pick "Yes"
                    combo.click()
                    page.wait_for_timeout(500)
                    yes_opt = page.locator("[role='option']:has-text('Yes')").first
                    if yes_opt.count() > 0:
                        yes_opt.click()
                        print("[Agent] ✓ Work auth combobox → Yes")

            except Exception:
                continue
    except Exception:
        pass


def _try_resume_upload(page, profile: dict):
    """Upload the resume PDF if a file input is present."""
    resume_path = profile.get("resume_path", "")
    if not resume_path:
        return

    # Resolve relative path from project root
    if not os.path.isabs(resume_path):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        resume_path = os.path.join(root, resume_path)

    if not os.path.exists(resume_path):
        print(f"[Agent] ⚠️  Resume not found: {resume_path}")
        print(f"[Agent]    Place the PDF at that path and re-run.")
        return

    try:
        file_inputs = page.locator("input[type='file']").all()
        for fi in file_inputs:
            try:
                fi.set_input_files(resume_path)
                print(f"[Agent] ✓ Resume uploaded: {os.path.basename(resume_path)}")
                page.wait_for_timeout(2000)
                return          # Upload once only
            except Exception:
                continue
    except Exception as e:
        print(f"[Agent] ⚠️  Could not upload resume: {e}")
        print(f"[Agent]    Upload manually: {resume_path}")


def _handle_text_questions(page, profile: dict):
    """Fill free-text custom questions using Groq."""
    try:
        textareas = page.locator("textarea").all()
        for ta in textareas:
            try:
                if not ta.is_visible():
                    continue
                if ta.input_value():   # Already has content
                    continue

                question = _get_element_label(page, ta)
                if not question or len(question) < 8:
                    continue

                print(f"[Agent] Custom question: {question[:100]}")
                answer = ask_groq(question, profile)
                print(f"[Agent] Groq answer: {answer[:100]}")

                # 3-second pause so user can see what's about to be typed
                page.wait_for_timeout(3000)
                ta.fill(answer)
                print("[Agent] ✓ Custom question answered")

            except Exception:
                continue
    except Exception:
        pass


def _get_element_label(page, element) -> str:
    """Find the question/label text for a form element."""
    try:
        # aria-labelledby
        labelledby = element.get_attribute("aria-labelledby")
        if labelledby:
            lbl = page.locator(f"#{labelledby}")
            if lbl.count() > 0:
                return lbl.inner_text().strip()

        # aria-label
        aria = element.get_attribute("aria-label") or ""
        if aria.strip():
            return aria.strip()

        # <label for="id">
        elem_id = element.get_attribute("id")
        if elem_id:
            lbl = page.locator(f"label[for='{elem_id}']")
            if lbl.count() > 0:
                return lbl.inner_text().strip()

        # Walk up the DOM for nearby text
        return element.evaluate("""el => {
            // Check previous siblings
            let prev = el.previousElementSibling;
            while (prev) {
                const t = prev.textContent.trim();
                if (t.length > 8 && t.length < 500) return t;
                prev = prev.previousElementSibling;
            }
            // Check parent's label-like children
            const parent = el.parentElement;
            if (parent) {
                const candidates = parent.querySelectorAll('label, span.t-14, p, legend');
                for (const c of candidates) {
                    const t = c.textContent.trim();
                    if (t.length > 8 && t.length < 500) return t;
                }
            }
            return '';
        }""")
    except Exception:
        return ""


def _click_next_safe(page) -> bool:
    """
    Click the wizard's Next/Continue button.
    SAFETY: contains an explicit deny-list — will never click anything
    that looks like a Submit, Apply, or Finish button.
    """
    # Buttons we are allowed to click
    safe_labels = [
        "Next",
        "Continue",
        "Continue to next step",
        "Save and continue",
        "Next step",
    ]
    # Patterns we absolutely refuse to click (prevents accidental submission)
    forbidden = [
        "submit", "apply now", "send application",
        "finish", "complete", "done", "review and submit",
    ]

    for label in safe_labels:
        for selector in [
            f"button:has-text('{label}')",
            f"button[aria-label='{label}']",
            f"button[aria-label*='{label}']",
        ]:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_visible():
                    btn_text = btn.inner_text().lower()
                    if any(f in btn_text for f in forbidden):
                        print(f"[Agent] ⛔ Refusing to click '{btn_text}' — matches forbidden pattern")
                        return False
                    btn.click()
                    print(f"[Agent] ✓ Advanced to next step")
                    return True
            except Exception:
                continue

    return False


# ---------------------------------------------------------------------------
# Human-in-the-loop STOP gate — the core safety mechanism
# ---------------------------------------------------------------------------

def stop_on_review(page):
    """
    Stops the agent and hands control to the human.
    Highlights the Submit button in red — but NEVER clicks it.
    Waits for the human to press Enter in the terminal when done.
    """
    print("\n" + "=" * 65)
    print("  ✋  AGENT STOPPED — YOUR REVIEW IS REQUIRED  ✋")
    print("=" * 65)
    print("  All fields have been filled.")
    print("  The SUBMIT button is highlighted RED in your browser.")
    print()
    print("  WHAT TO DO NOW:")
    print("  1. Switch to the browser window")
    print("  2. Read through every field carefully")
    print("  3. Edit anything that needs correction")
    print("  4. Click SUBMIT yourself when you are happy")
    print("  5. Come back to this terminal and press Enter to close")
    print("=" * 65)

    # Visually highlight all submit-like buttons
    submit_selectors = [
        "button[aria-label='Submit application']",
        "button[aria-label='Submit Application']",
        "button:has-text('Submit application')",
        "button:has-text('Submit Application')",
        "button:has-text('Apply Now')",
        "button:has-text('Submit')",
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
                        el.style.border          = '5px solid #ff0000';
                        el.style.boxShadow       = '0 0 18px #ff0000';
                        el.style.backgroundColor = '#fff0f0';
                        el.style.color           = '#cc0000';
                        el.style.fontWeight      = 'bold';
                        el.setAttribute('title', 'AGENT STOPPED HERE — click to submit');
                    }""")
                    highlighted += 1
                except Exception:
                    pass
        except Exception:
            pass

    if highlighted:
        print(f"\n  [{highlighted} Submit button(s) highlighted red in your browser]")
    else:
        print("\n  [Submit button not found — scroll through the form to locate it]")

    try:
        input("\n  >>> Press Enter here in the terminal to close the browser when done... ")
    except (EOFError, KeyboardInterrupt):
        print("\n[Agent] Session closed.")
