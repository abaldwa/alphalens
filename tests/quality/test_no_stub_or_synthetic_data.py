"""
tests/quality/test_no_stub_or_synthetic_data.py

Phase: cross-cutting (all phases)
Specs: CLAUDE.md Absolute Rule 6 (no synthetic/mocked/procedurally-generated
       data anywhere in production code, ever, and no fallback to it)
Owner: project-wide quality gate
Consumers: CI / `pytest tests/quality/`

These are static "fitness functions", not behavioural tests: they grep and
AST-walk production source (everything except tests/, alphalens_docs/,
.venv, and similar) for patterns that have historically indicated a
fabricated-data fallback or an unfinished stub implementation, and fail on
any occurrence that isn't in the ALLOWLIST below.

The allowlist is the single place that tracks today's known,
BuildLog.md-documented real-data gaps (e.g. analogue_miner.py's synthetic
33-feature vectors, the empty systems/technical_analysis scaffold). It
exists so this test can be strict without blocking on debt that's already
visible elsewhere — but it must shrink over time. Do not add a new entry
without a corresponding BuildLog.md "Real data sourcing" section explaining
why the real fix isn't done yet; do not loosen a regex just to silence a
new finding.

PIT Assumptions
----------------
None — pure static analysis, no data access.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Iterator


REPO_ROOT = Path(__file__).resolve().parents[2]

# Only scan directories that ship production code paths. Excludes tests/,
# alphalens_docs/, baselines/, code_reviews/, dashboard mocks, etc.
SCAN_DIRS = [
    "config", "contracts", "datastore", "features", "ingestion",
    "scripts", "systems", "backtest", "dashboard",
]

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules", "catboost_info"}

# contracts/interfaces.py legitimately defines abstract methods that raise
# NotImplementedError by design (IModel, IClassificationModel, etc.) — that
# is the interface contract, not an unfinished implementation.
STUB_CHECK_EXCLUDE_DIRS = {"contracts"}


def _iter_py_files(dirs: list[str]) -> Iterator[Path]:
    for d in dirs:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _rel(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


# ---------------------------------------------------------------------------
# 1. Synthetic / fabricated data fallbacks
# ---------------------------------------------------------------------------

# Matches numpy/random data-generation calls. Algorithm seeding
# (`random_state=42`, `np.random.default_rng(seed)` used only to seed a
# model/shuffle) is NOT itself a violation — what matters is whether the
# *output* of the call is used as a stand-in for real market/fundamental
# data. That judgment call is why this list feeds a human-reviewed
# allowlist rather than an auto-pass/fail on regex alone.
_SYNTHETIC_DATA_RE = re.compile(
    r"np\.random\.(normal|uniform|randint|choice|dirichlet|permutation|rand|randn)\(|"
    r"rng\.(normal|uniform|randint|choice|dirichlet|permutation|rand|randn)\("
)
_KEYWORD_RE = re.compile(r"\b(placeholder|synthetic|fake|dummy)\b", re.IGNORECASE)

# The codebase has a strong, deliberate convention of documenting the
# *absence* of synthetic data ("there is no synthetic-data fallback",
# "synthetic fallback has been removed") — those comments are evidence of
# compliance, not violations, and would otherwise dominate the diff. Skip
# any line matching a negation around the keyword.
_NEGATION_RE = re.compile(
    r"(\bno\b|\bnot\b|\bnever\b|\bwithout\b)[^.\n]{0,60}\b(synthetic|fabricat\w*|placeholder|fake|dummy)\b"
    r"|\b(synthetic|fabricat\w*|placeholder|fake|dummy)\b[^.\n]{0,60}"
    r"\b(removed|removal|policy|raises|fallback\b.{0,20}\b(removed|raise))\b",
    re.IGNORECASE,
)

# file (relative to repo root) -> set of substrings; a flagged line is
# allowed through if any of these substrings appears on that line OR on the
# few lines around it carry one of these markers. Keep entries narrow.
SYNTHETIC_DATA_ALLOWLIST: dict[str, set[str]] = {
    # BuildLog.md "Real data sourcing — Multibagger historical archive
    # features": real company names/return facts, explicitly-documented
    # SYNTHETIC (not measured) 33-feature vectors used only for analogue
    # cosine-similarity matching. Tracked fix: backfill real OHLCV at each
    # entry's historical date and recompute real features.
    "systems/ml_signal_engine/models/multibagger/analogue_miner.py": {
        "AVANTI FEEDS", "RELAXO", "PAGE INDUSTRIES", "BAJAJ FINANCE",
        "HISTORICAL_MULTIBAGGER_ARCHIVE", "stock_name",
    },
    # Random-feature overfit/leak test: shuffling real feature *columns* to
    # verify the model can't beat chance on noise. This is a model-integrity
    # test, not a data fallback — the values being permuted are real.
    "backtest/overfit_checks.py": {"rng.permutation"},
    # Random-buy baseline for the strategy confidence framework: samples
    # which REAL tickers to evaluate on a given date (for the "what would
    # an unconditional random buy have scored" control), never fabricates
    # a price/return value — same class of use as overfit_checks.py above.
    "backtest/strategy_confidence.py": {"rng.choice"},
    # Subsamples real out-of-fold rows for SHAP-speed reasons (TabNet
    # feature-selection validator) — not data fabrication.
    "systems/ml_signal_engine/models/training/feature_selection.py": {"rng.choice"},
    # Module docstring *mentioning* removed code for historical context
    # ("a previous version ... fabricated fake OOF via `rng.dirichlet()`/
    # `rng.choice()` ... removed") — the call sites themselves are gone;
    # this is prose, not a live data-fabrication path.
    "backtest/run_phase3_backtest.py": {"rng.dirichlet()`/`rng.choice()`"},
    # Monte Carlo DCF (SPEC-VAL-004): sampling WACC around its computed base
    # value is the model's actual purpose (uncertainty propagation), not a
    # fabricated-data fallback — base_wacc itself comes from real financials.
    "systems/damodaran_valuation/scenarios/monte_carlo.py": {"self._rng.normal"},
    # Downsamples real (ticker, date) training pairs when there are more than
    # max_samples — not data fabrication, just row selection for memory limits.
    "systems/ml_signal_engine/models/deep/tft_model.py": {"rng.choice"},
}

# Keyword-only allowlist (placeholder/synthetic/fake/dummy as a word, where
# the surrounding code is legitimate — e.g. a documented archive entry, an
# intentional no-op, or an honest-NaN explanatory comment).
KEYWORD_ALLOWLIST: dict[str, set[str]] = {
    # [H4, 2026-08-18] UnifiedGeneratorRefactorPlan.md §19 deleted the
    # per-ticker HMM / grace-cycle knobs from MomentumAdapter. In these three
    # research scripts that knob was ONE axis of a grid, so the axis was
    # collapsed to a single fixed value rather than the whole script being
    # dropped. The word describes a collapsed SWEEP AXIS -- one code path
    # where there used to be several -- not fabricated data.
    "scripts/run_momentum_refinement.py": {"one placeholder value"},
    "scripts/run_momentum_refinement_v2.py": {"one placeholder value"},
    "scripts/run_momentum_grid2.py": {"one placeholder value"},
    "systems/ml_signal_engine/models/multibagger/analogue_miner.py": {
        "SYNTHETIC", "synthetic"
    },
    "systems/ml_signal_engine/models/forensic/forensic_ml.py": {
        "KNOWN_FRAUD_ARCHIVE", "KNOWN_CLEAN_ARCHIVE", "synthetic", "Synthetic",
        # Real, named historical fraud case (Satyam, 2009) in the
        # documented fraud archive — "fake cash" describes the real fraud
        # type, not a fabricated-data stand-in.
        "Fictitious revenue + fake cash",
    },
    "systems/ml_signal_engine/inference/retrain_phase2.py": {"synthetic"},
    "systems/ml_signal_engine/inference/train_all_phase1.py": {"synthetic"},
    "datastore/api/db.py": {"pass"},
    "features/pattern_scores.py": {"pass"},
    # Historical note: a since-fixed schema column used to be typed as a
    # placeholder VARCHAR; the real-valued column has shipped since P1.6.
    "datastore/schema/create_signals.py": {"Phase 0.2 placeholder typed"},
    # DI-design comment ("tests can substitute a fake") — describes test
    # injection capability, not a production data fallback.
    #
    # [2026-08-13] The two "placeholder column" phrases describe columns that
    # are deliberately all-NaN because that category is not computed on this
    # path. A NaN that says "not computed" is the opposite of fabricated data
    # — it is the honest alternative to inventing a value.
    "features/matrix_builder.py": {
        "tests can substitute a fake",
        "raw per-ticker record DataFrame including a placeholder",
        "placeholder columns instead)",
    },
    # [2026-08-13] Names fundamentals columns that are 100% NULL table-wide
    # and never populated by any ingestion path — a documented data gap, not
    # a stand-in value.
    "ingestion/scheduler/daily_pipeline.py": {
        "Structural metadata/placeholder columns that are 100% NULL",
    },
    # [2026-08-13] A validator slot that intentionally does only shape/sanity
    # checks because FYERS is the trusted source here; it validates real data
    # rather than substituting any.
    "scripts/fyers_staged_backfill.py": {"Placeholder validator slot"},
    # [2026-08-13] Describes the bug being fixed: one fabricated 187x gain in
    # the trade book inflated later trades. Naming the defect, not shipping it.
    "scripts/prorate_trades_from_date.py": {"inflated - one fake 187x gain"},
    # [2026-08-13] States the script profiles the REAL feature path rather
    # than a synthetic one — an assertion that no synthetic data is used.
    "scripts/profile_one_feature_date.py": {"rather than a synthetic path"},
    # [2026-08-13] HTML placeholder= attributes on a search input in generated
    # reports. Browser UI text, not data.
    "scripts/convert_momentum_csv_to_html.py": {'placeholder="Filter rows'},
    "backtest/trade_book_html.py": {'placeholder="Filter rows'},
    # [2026-08-13] Notes that these pure helpers are unit-testable against
    # small synthetic trade lists — describing test inputs, which is exactly
    # where synthetic data is legitimate.
    "backtest/technical_reporting.py": {"synthetic trade lists"},
    # Detects an unedited literal ".env placeholder" string in a real
    # credentials file to decide whether to fall back to OAuth2 login —
    # the placeholder being checked for is the user's own un-filled
    # template value, not fabricated market data.
    "ingestion/scrapers/fyers_backfill.py": {
        "catching both an unedited", "placeholder value — falling back",
    },
    # Describes a one-off incident where a synthetic test fixture (not
    # production data) crashed this function; the fix (excluding before
    # scoring) is real and already implemented above this docstring.
    "systems/ml_signal_engine/inference/daily_inference.py": {
        "a synthetic, unrealistic"
    },
    # Module docstring describing now-removed code for historical context
    # (same justification as this file's SYNTHETIC_DATA_ALLOWLIST entry
    # above) — the fabrication itself is gone, this is prose about it.
    "backtest/run_phase3_backtest.py": {"fabricated fake OOF via"},
    # Damodaran's own valuation terminology ("synthetic rating" = a
    # bond-rating proxy derived from a company's interest-coverage ratio,
    # per his published ratings-spread table) — not a fabricated-data
    # stand-in.
    "systems/damodaran_valuation/dcf/wacc.py": {"synthetic rating"},
    # Prose describing the *previous* fixed-date-only holiday-calendar
    # implementation that this module supersedes — historical context in a
    # comment/docstring, not a live data-fabrication path.
    "config/nse_holidays.py": {
        "old fixed-date-only placeholder",
    },
    # Prose note describing Trendlyne Premium's own UI rendering a literal
    # "xxx" placeholder string for locked/paywalled data — describes the
    # third-party source's behavior being detected, not fabricated data
    # produced by this codebase.
    "datastore/schema/create_normalised.py": {
        'still renders "xxx" placeholder',
    },
    # Docstring describing the manual-backfill script recording a real,
    # honest placeholder *note* (not fabricated numeric data) in
    # corporate_actions when a bonus is known to have occurred but the
    # ratio couldn't be confirmed — an honesty marker, not a data stand-in.
    "scripts/align_remaining_to_fyers.py": {
        'placeholder note ("a bonus has',
    },
    # sklearn's own `DummyClassifier` baseline-comparator import — a
    # standard scikit-learn utility class name, not fabricated/synthetic
    # data.
    "systems/ml_signal_engine_gainer/models/multibagger/multibagger_model.py": {
        "from sklearn.dummy import DummyClassifier",
    },
    "systems/ml_signal_engine_gainer/models/signal/signal_ranker.py": {
        "from sklearn.dummy import DummyClassifier",
    },
    "systems/ml_signal_engine/models/multibagger/multibagger_model.py": {
        "from sklearn.dummy import DummyClassifier",
    },
    # Prose describing a deliberately unglamorous *naming* choice for a
    # future retirement path, not a fabricated-data stand-in.
    "datastore/api/routers/paper_trading_unified.py": {
        "unglamorous placeholder name",
    },
    # Docstring instructing callers to pass a REAL benchmark series rather
    # than a synthetic one — the word appears only in a negative
    # instruction, not a fabrication path in this module.
    "features/regime_signal.py": {
        "rather than a synthetic",
    },
    # Docstring instructing implementers never to silently default to a
    # dummy value — a warning against fabrication, not fabrication itself.
    "backtest/paper_trading/live_runner.py": {
        "to a dummy horizon bucket",
    },
    # Comment explaining why class-weighting was chosen over a SMOTE-style
    # resampling step ("could leak synthetic correlation") — a risk being
    # avoided, not a fabricated-data path in this module.
    "systems/ml_signal_engine/models/signal/meta_labeler.py": {
        "could leak synthetic",
    },
}


def _is_allowlisted(rel_path: str, line: str, table: dict[str, set[str]]) -> bool:
    markers = table.get(rel_path)
    if not markers:
        return False
    return any(marker in line for marker in markers)


def test_no_unallowlisted_synthetic_data_generation():
    violations = []
    for path in _iter_py_files(SCAN_DIRS):
        rel = _rel(path)
        if rel.startswith("tests" + str(Path("/"))):
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _SYNTHETIC_DATA_RE.search(line) and not _is_allowlisted(rel, line, SYNTHETIC_DATA_ALLOWLIST):
                violations.append(f"{rel}:{lineno}: {line.strip()}")
    assert not violations, (
        "Found numpy/rng data-generation calls outside the documented allowlist "
        "(CLAUDE.md Absolute Rule 6 — no synthetic data, ever, no fallback). "
        "Either remove the fabricated-data fallback (raise instead, per "
        "backtest/run_phase1_backtest.py's _fetch_real_benchmark() pattern) or "
        "add a narrowly-scoped, justified entry to SYNTHETIC_DATA_ALLOWLIST "
        "referencing a BuildLog.md \"Real data sourcing\" section:\n"
        + "\n".join(violations)
    )


def test_no_unallowlisted_stub_keywords():
    violations = []
    for path in _iter_py_files(SCAN_DIRS):
        rel = _rel(path)
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        lines = text.splitlines()
        for lineno, line in enumerate(lines, start=1):
            if not _KEYWORD_RE.search(line):
                continue
            # Negation phrasing ("there is no synthetic-data fallback")
            # routinely wraps across a line break in this codebase's
            # prose-style comments — check the keyword's line joined with
            # the line before and after it, not just the line alone.
            window = " ".join(lines[max(0, lineno - 2):lineno + 1])
            if _NEGATION_RE.search(window):
                continue
            if not _is_allowlisted(rel, line, KEYWORD_ALLOWLIST):
                violations.append(f"{rel}:{lineno}: {line.strip()}")
    assert not violations, (
        "Found 'placeholder'/'synthetic'/'fake'/'dummy' outside the documented "
        "allowlist. Either it's a genuine fabricated-data stand-in (fix it, "
        "per CLAUDE.md Absolute Rule 6) or it's benign and needs a narrow, "
        "justified entry in KEYWORD_ALLOWLIST:\n" + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# 2. Stub function bodies (NotImplementedError / bare pass / Ellipsis-only)
# ---------------------------------------------------------------------------

def _is_stub_body(body: list[ast.stmt]) -> str | None:
    stmts = [s for s in body if not isinstance(s, ast.Expr) or not isinstance(s.value, ast.Constant)]
    if not stmts:
        return "empty/docstring-only body"
    if len(stmts) == 1:
        stmt = stmts[0]
        if isinstance(stmt, ast.Pass):
            return "bare `pass` body"
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and stmt.value.value is Ellipsis:
            return "`...`-only body"
        if isinstance(stmt, ast.Raise) and isinstance(stmt.exc, (ast.Call, ast.Name)):
            name = stmt.exc.func.id if isinstance(stmt.exc, ast.Call) and isinstance(stmt.exc.func, ast.Name) else (
                stmt.exc.id if isinstance(stmt.exc, ast.Name) else ""
            )
            if name == "NotImplementedError":
                return "raises NotImplementedError"
    return None


# function qualnames (module_rel_path::FunctionName) already known and
# tracked as legitimately-incomplete scaffolding (BuildLog.md / phase
# delivery plan Weeks 33-38 — TA and Damodaran systems are 0% built).
# [2026-08-18] typing.Protocol methods are no longer listed here individually.
# They used to be ("backtest/core/engine.py::generate_signals" and
# "::feature_vector"), and the cost of that showed up immediately: adding one
# more Protocol method to the same file broke this test for someone who had
# written entirely correct code. A `...` body inside a Protocol is not
# incomplete work, it is the only form the language offers for declaring a
# structural interface -- so it is recognised structurally now, by
# _protocol_method_lines() below, rather than name by name.
STUB_FUNCTION_ALLOWLIST: set[str] = {
    # [H4, 2026-08-18] Scripts whose ENTIRE premise was a knob §19 deleted
    # from MomentumAdapter (per-ticker HMM regime; grace cycles). They now
    # raise NotImplementedError with a message saying so, which is the
    # honest behaviour -- the alternative was reporting a comparison in
    # which every variant is identical. UnifiedGeneratorRefactorPlan.md
    # (~line 1306) records the decision to RETAIN rather than delete them,
    # because the class backs published results. Allowlisted because these
    # are deliberately-terminal scripts, not unimplemented work.
    "scripts/run_band_best_hmm_regime_sweep.py::main",
    "scripts/run_momentum_grace_comparison.py::run",
}

# Whole packages that are intentionally-empty scaffolding today (Weeks
# 33-38 of alphalens_docs/11_phase_delivery_plan.md — not yet built). Listed
# explicitly so a NEW empty package elsewhere is still caught.
# (systems/fundamental_analysis was deleted 2026-07-10 per F3 — it was a
# dead stub package with zero imports; real logic lives in
# features/fundamental_composites.py.)
KNOWN_STUB_PACKAGES: set[str] = set()


def _protocol_method_lines(tree: ast.AST) -> set[int]:
    """Line numbers of methods declared inside a `class X(Protocol)`.

    Matched on the base-class name so both `Protocol` and `typing.Protocol`
    are recognised. Keyed by line number rather than by name because two
    classes in one module may legitimately declare the same method name and
    only one of them may be a Protocol.
    """
    lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        is_protocol = any(
            (isinstance(b, ast.Name) and b.id == "Protocol")
            or (isinstance(b, ast.Attribute) and b.attr == "Protocol")
            for b in node.bases
        )
        if not is_protocol:
            continue
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                lines.add(item.lineno)
    return lines


def test_no_unallowlisted_stub_function_bodies():
    violations = []
    for path in _iter_py_files(SCAN_DIRS):
        rel = _rel(path)
        if set(Path(rel).parts) & STUB_CHECK_EXCLUDE_DIRS:
            continue
        if any(rel.startswith(pkg) for pkg in KNOWN_STUB_PACKAGES):
            continue
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        protocol_lines = _protocol_method_lines(tree)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                reason = _is_stub_body(node.body)
                if reason is None:
                    continue
                if node.lineno in protocol_lines:
                    continue
                qualname = f"{rel}::{node.name}"
                if qualname in STUB_FUNCTION_ALLOWLIST:
                    continue
                violations.append(f"{rel}:{node.lineno}: def {node.name}() — {reason}")
    assert not violations, (
        "Found stub function bodies (bare pass / Ellipsis / unconditional "
        "NotImplementedError) outside contracts/ (where that's the intended "
        "abstract-interface pattern) and outside the known-incomplete "
        "TA/Damodaran/FA system scaffolds. Implement it, delete it, or add a "
        "justified entry to STUB_FUNCTION_ALLOWLIST:\n" + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# 3. Empty package scaffolds under systems/ — must match the known set
# ---------------------------------------------------------------------------

def _package_has_real_code(pkg_dir: Path) -> bool:
    """True if any .py file under pkg_dir (recursively) has executable
    statements beyond a module docstring."""
    for py_file in pkg_dir.rglob("*.py"):
        if EXCLUDE_DIR_PARTS & set(py_file.parts):
            continue
        try:
            tree = ast.parse(py_file.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        body = tree.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]  # drop module docstring
        if body:
            return True
    return False


def test_empty_system_scaffolds_match_known_set():
    """
    Fails if a NEW empty (__init__.py-only) package appears under systems/
    that isn't already tracked in KNOWN_STUB_PACKAGES — and reminds you to
    shrink KNOWN_STUB_PACKAGES (here AND in test_no_unallowlisted_stub_
    function_bodies above) once a listed system actually gets implemented.
    """
    systems_dir = REPO_ROOT / "systems"
    empty_packages = set()
    for child in sorted(systems_dir.iterdir()):
        if not child.is_dir() or EXCLUDE_DIR_PARTS & set(child.parts):
            continue
        if not _package_has_real_code(child):
            empty_packages.add(f"systems/{child.name}")

    unexpected = empty_packages - KNOWN_STUB_PACKAGES
    assert not unexpected, f"New empty system scaffold(s) not yet tracked: {unexpected}"

    now_implemented = KNOWN_STUB_PACKAGES - empty_packages
    assert not now_implemented, (
        f"{now_implemented} now has real code — remove from KNOWN_STUB_PACKAGES "
        "in this file (both occurrences) so stub-body scanning covers it again."
    )
