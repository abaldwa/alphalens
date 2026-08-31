"""
ingestion/scrapers/browser.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-001, SPEC-SOLID-001
Owner: Platform / Ingestion
Consumers: ingestion/scrapers/amfi_holdings.py (per-AMC fetch_fn implementations)

Shared headless-browser utility (Playwright/Chromium) for sources whose
real data is only reachable via client-side JavaScript — confirmed across
multiple AMC portfolio-disclosure pages during P2.2 (SBI Mutual Fund's
`/portfolios` page renders its download links into custom-styled
`<select>` widgets that only populate after JS runs; ICICI Prudential's
downloads page is a full React/MUI single-page app). `requests` +
BeautifulSoup (this project's scraper pattern everywhere else —
screener.py, bhavcopy.py) cannot see this content at all.

[AS BUILT] This machine's OS (Ubuntu 26.04 "Resolute Raccoon") is newer
than any version Playwright 1.60.0 has a registered Chromium build table
entry for — `playwright install chromium` and every browser launch fail
outright with "Playwright does not support chromium on ubuntu26.04-x64"
otherwise. `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64` (a real,
documented override Playwright's own registry.js exposes for exactly
this situation — found by reading the installed package's source, not
guessed) makes it treat this host as the latest officially-supported
Ubuntu LTS, which is glibc/ABI-compatible enough for the downloaded
Chromium build to run correctly. Set here, once, before `playwright` is
imported anywhere, so every caller gets it automatically — no per-script
boilerplate required. The browser binary itself must additionally be
installed once with the same env var set:
    PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64 python3 -m playwright install chromium
"""

import logging
import os
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

os.environ.setdefault("PLAYWRIGHT_HOST_PLATFORM_OVERRIDE", "ubuntu24.04-x64")

from playwright.sync_api import Page, sync_playwright  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_VIEWPORT = {"width": 1366, "height": 768}


@contextmanager
def browser_page(
    user_agent: str = DEFAULT_USER_AGENT,
    viewport: Optional[Dict[str, int]] = None,
    headless: bool = True,
) -> Iterator[Page]:
    """
    Context manager yielding a single Playwright page, browser closed on exit.

    Parameters
    ----------
    user_agent : str, optional
        Defaults to a realistic desktop Chrome UA — several AMC sites
        (verified live: hdfcfund.com is the one confirmed exception, see
        amfi_holdings.py) return non-2xx responses to obviously-automated
        or missing user agents.
    viewport : dict, optional
        Defaults to DEFAULT_VIEWPORT.
    headless : bool

    Yields
    ------
    playwright.sync_api.Page

    Spec References
    ----------------
    SPEC-PIPE-001.

    Raises
    ------
    playwright._impl._errors.Error
        If the browser fails to launch (e.g. not installed — see module docstring).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=user_agent, viewport=viewport or DEFAULT_VIEWPORT)
        page = context.new_page()
        try:
            yield page
        finally:
            browser.close()


def set_select_by_label(page: Page, container_selector: str, label_text: str) -> None:
    """
    Set a (possibly visually-hidden, JS-custom-styled) <select>'s value by
    matching one of its <option> labels exactly, then dispatch a `change`
    event so the page's own JS reacts to it.

    Several AMC sites (SBI Mutual Fund confirmed live) render native
    `<select>` elements with `display:none` and a custom-styled dropdown
    widget on top — Playwright's `select_option()` requires the element
    to be visible, so this evaluates inside the page instead.

    Parameters
    ----------
    page : playwright.sync_api.Page
    container_selector : str
        CSS selector for the element CONTAINING the `<select>` (the
        `<select>` itself is matched as `f"{container_selector} select"`).
    label_text : str
        Exact (after `.trim()`) option text to select.

    Returns
    -------
    None

    Raises
    ------
    playwright._impl._errors.Error
        If no <select> matches, or if propagated from the page (e.g. the
        evaluated JS itself raises when no matching option is found —
        surfaces as a generic Playwright error wrapping the JS exception).
    """
    page.eval_on_selector(
        f"{container_selector} select",
        """(el, label) => {
            const opt = Array.from(el.options).find(o => o.textContent.trim() === label);
            if (!opt) throw new Error('option not found: ' + label);
            el.value = opt.value;
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        label_text,
    )
