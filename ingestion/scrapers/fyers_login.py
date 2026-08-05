"""
ingestion/scrapers/fyers_login.py

Phase: 0.5 (FYERS Historical Backfill / Daily Cutover)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/pipeline_scheduler.py (schedule_fyers_login,
    daily early-morning job), scripts/fyers_staged_backfill.py

Unattended daily FYERS login: drives the FYERS web login form (client_id +
FYERS_PIN + a pyotp-generated TOTP code from FYERS_TOTP_SECRET) via
Playwright, captures the redirected auth_code, and exchanges it for an
access token using the EXISTING, already-tested
ingestion.scrapers.fyers_backfill.FYERSBackfill.exchange_auth_code() —
this module only automates the browser steps a human would otherwise do
by hand once a day; it does not reimplement any of the OAuth2 token
exchange or caching logic.

[AS BUILT / KNOWN BLOCKER, 2026-08-04] Confirmed live against the real
login page (https://login.fyers.in/): FYERS' login form is protected by
Cloudflare Turnstile (`<script src="https://challenges.cloudflare.com/
turnstile/v0/api.js">`, a `captcha-container` widget with a hidden
`cf-turnstile-response` input). Turnstile is specifically designed to
detect and block headless/scripted browsers — this is NOT a selector
problem, and this module does not attempt to defeat it (that would mean
actively circumventing FYERS' anti-bot/security measure, likely also a
ToS violation). As a result, fully unattended daily login via this
script is NOT currently reliable/usable — 2026-08-04 user decision:
production daily login uses the existing manual/non-interactive CLI
instead (`python3 -m ingestion.scrapers.fyers_backfill login` / `...
exchange <url>`, run by hand once a day). This module is kept, unused,
for a possible future semi-automated mode (fill PIN/TOTP, pause for a
human to clear the Turnstile widget, then continue) — see
run_browser_login's docstring. schedule_fyers_login is registered but its
job will currently fail at the Turnstile step every time; this is
expected until/unless a semi-automated flow replaces the fully-automated
one.

[AS BUILT / VERIFIED 2026-08-04 against the live login page] The
client-ID step selectors below are confirmed correct against
https://login.fyers.in/ — including the fix for FYERS' markup reusing
id="fy_client_id" for a second, hidden field inside a "Forgot PIN?" form
on the same page (scoped to #clientIdForm to disambiguate). The page also
defaults to a "Mobile number" tab; a "Client ID" radio tab must be
clicked first to reveal #clientIdForm — see _SELECTOR_CLIENT_ID_TAB.
PIN/TOTP-step selectors are NOT yet verified (the flow doesn't reach that
page while Turnstile blocks progress) — verify these the same way
(--visible / FYERS_LOGIN_DEBUG_DIR) if/when a semi-automated flow is built.

Rate limiting: none needed — this is one login per day, not a data-volume
endpoint.
"""

import logging
import os
from pathlib import Path

from config.settings import FYERS_CLIENT_LOGIN_ID, FYERS_PIN, FYERS_TOTP_SECRET
from ingestion.scrapers.browser import browser_page
from ingestion.scrapers.fyers_backfill import FYERSBackfill

logger = logging.getLogger(__name__)

# Optional: set to a directory to auto-save a screenshot + page HTML on
# any step failure, for fixing selectors without re-running blind.
FYERS_LOGIN_DEBUG_DIR = os.environ.get("FYERS_LOGIN_DEBUG_DIR")

# [AS BUILT — see module docstring for verification status of each]
_SELECTOR_CLIENT_ID_TAB = "label:has-text('Client ID')"
_SELECTOR_CLIENT_ID_INPUT = "#clientIdForm #fy_client_id"
_SELECTOR_CLIENT_ID_SUBMIT = "#clientIdSubmit"
_SELECTOR_PIN_INPUT = "#login-pin"
_SELECTOR_PIN_SUBMIT = "#verifyPinSubmit"
_SELECTOR_TOTP_INPUT = "#totp_code"
_SELECTOR_TOTP_SUBMIT = "#confirmOtpSubmit"

_STEP_TIMEOUT_MS = 20_000


class FyersLoginError(RuntimeError):
    """Raised when the automated browser login flow cannot complete."""


def _totp_code() -> str:
    if not FYERS_TOTP_SECRET:
        raise FyersLoginError(
            "FYERS_TOTP_SECRET is not set in .env — required for unattended "
            "FYERS login. Fill it in from FYERS' 2FA/TOTP setup."
        )
    import pyotp

    return pyotp.TOTP(FYERS_TOTP_SECRET).now()


def _save_debug_artifacts(page, step: str) -> None:
    if not FYERS_LOGIN_DEBUG_DIR:
        return
    debug_dir = Path(FYERS_LOGIN_DEBUG_DIR)
    debug_dir.mkdir(parents=True, exist_ok=True)
    try:
        page.screenshot(path=str(debug_dir / f"fyers_login_failed_{step}.png"))
        (debug_dir / f"fyers_login_failed_{step}.html").write_text(page.content())
    except Exception as exc:  # pragma: no cover - best-effort debug aid only
        logger.warning(f"fyers_login: could not save debug artifacts for step '{step}': {exc}")


def run_browser_login(auth_url: str, headless: bool = True) -> str:
    """
    Drive the FYERS login form and return the URL the browser is finally
    redirected to (containing '?auth_code=...').

    [AS BUILT, 2026-08-04] Reaches the Client-ID -> PIN step but currently
    BLOCKS at FYERS' Cloudflare Turnstile widget — see module docstring.
    Not usable end-to-end for unattended daily login until a
    semi-automated (human clears Turnstile, script continues) mode is
    built here.

    Parameters
    ----------
    auth_url : str
        The FYERS OAuth2 authorization URL
        (FYERSBackfill.get_authorization_url()).
    headless : bool
        False opens a visible browser window — useful for a one-time
        selector-verification run (see module docstring).

    Returns
    -------
    str
        The final redirected URL.

    Raises
    ------
    FyersLoginError
        If any login step fails (missing element, FYERS rejects
        PIN/TOTP, etc). Debug artifacts are saved first if
        FYERS_LOGIN_DEBUG_DIR is set.
    """
    if not FYERS_PIN:
        raise FyersLoginError("FYERS_PIN is not set in .env — required for unattended FYERS login.")
    if not FYERS_CLIENT_LOGIN_ID:
        raise FyersLoginError(
            "FYERS_CLIENT_LOGIN_ID is not set in .env — required for unattended FYERS login "
            "(the login-PAGE Client ID, distinct from FYERS_APP_ID)."
        )

    with browser_page(headless=headless) as page:
        page.goto(auth_url, timeout=_STEP_TIMEOUT_MS)

        try:
            # Page defaults to the "Mobile number" tab — switch to "Client ID"
            # to reveal #clientIdForm before it becomes interactable.
            page.click(_SELECTOR_CLIENT_ID_TAB, timeout=_STEP_TIMEOUT_MS)
            page.wait_for_selector(_SELECTOR_CLIENT_ID_INPUT, timeout=_STEP_TIMEOUT_MS)
            page.fill(_SELECTOR_CLIENT_ID_INPUT, FYERS_CLIENT_LOGIN_ID)
            page.click(_SELECTOR_CLIENT_ID_SUBMIT)
        except Exception as exc:
            _save_debug_artifacts(page, "client_id")
            raise FyersLoginError(
                f"FYERS login: client-ID step failed ({exc}) — the login page's markup "
                "may not match the selectors in fyers_login.py; see module docstring."
            ) from exc

        try:
            page.wait_for_selector(_SELECTOR_PIN_INPUT, timeout=_STEP_TIMEOUT_MS)
            page.fill(_SELECTOR_PIN_INPUT, FYERS_PIN)
            page.click(_SELECTOR_PIN_SUBMIT)
        except Exception as exc:
            _save_debug_artifacts(page, "pin")
            raise FyersLoginError(f"FYERS login: PIN step failed ({exc})") from exc

        try:
            page.wait_for_selector(_SELECTOR_TOTP_INPUT, timeout=_STEP_TIMEOUT_MS)
            page.fill(_SELECTOR_TOTP_INPUT, _totp_code())
            page.click(_SELECTOR_TOTP_SUBMIT)
        except Exception as exc:
            _save_debug_artifacts(page, "totp")
            raise FyersLoginError(f"FYERS login: TOTP step failed ({exc})") from exc

        try:
            page.wait_for_url(lambda url: "auth_code=" in url, timeout=_STEP_TIMEOUT_MS)
        except Exception as exc:
            _save_debug_artifacts(page, "redirect")
            raise FyersLoginError(
                f"FYERS login: never redirected to a URL containing 'auth_code=' ({exc})"
            ) from exc

        return page.url


def daily_login(headless: bool = True, force: bool = False) -> str:
    """
    Ensure a valid same-day FYERS access token exists, logging in via the
    browser only if necessary.

    [AS BUILT, 2026-08-04] The browser-login fallback path currently
    blocks on Cloudflare Turnstile — see module docstring. Production use
    today should call FYERSBackfill's own non-interactive
    login/exchange CLI by hand instead
    (`python3 -m ingestion.scrapers.fyers_backfill login` / `... exchange
    <url>`). This function is kept for when a semi-automated flow makes
    the browser path usable again.

    Parameters
    ----------
    headless : bool
    force : bool
        If True, re-run the browser login even if a cached token is
        already valid (for testing/debugging the login flow itself).

    Returns
    -------
    str
        The valid access token (freshly obtained or already-cached).

    Spec References
    ----------------
    SPEC-PIPE-001.
    """
    fb = FYERSBackfill(non_interactive=True)

    if not force:
        try:
            return fb.get_access_token()
        except RuntimeError:
            pass  # no valid cached/env token — fall through to browser login

    auth_url = fb.get_authorization_url()
    redirected_url = run_browser_login(auth_url, headless=headless)
    return fb.exchange_auth_code(redirected_url)


def _cli() -> None:
    """`python3 -m ingestion.scrapers.fyers_login [--visible] [--force]`"""
    import argparse

    parser = argparse.ArgumentParser(description="Unattended FYERS daily login (Playwright)")
    parser.add_argument(
        "--visible", action="store_true", help="Run with a visible browser window (debugging)."
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-login even if a cached token is already valid."
    )
    args = parser.parse_args()

    token = daily_login(headless=not args.visible, force=args.force)
    print(f"FYERS access token ready ({'refreshed' if args.force else 'cached-or-refreshed'}): "
          f"{token[:12]}...")


if __name__ == "__main__":
    _cli()
