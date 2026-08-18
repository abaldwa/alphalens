"""
tests/integration/test_live_backtest_parity.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, A3)
Owner: Platform / Backtest
Consumers: CI / `pytest tests/integration/`, and Phases C, D, E as their
           acceptance test.

WHY THIS HARNESS EXISTS
-----------------------
Every other gate in tests/quality/ is STATIC: it proves two code paths are
not textually duplicated. None of them can prove the two paths AGREE. The
divergences in UnifiedGeneratorRefactorPlan.md §1.2 are behavioural — the
live path and the backtest adapter can share every primitive and still
select different stocks, because the live path skips the filter chain that
runs between "score the universe" and "decide what to hold".

This harness closes that. It feeds the SAME universe and the SAME date to
both paths and diffs the selected sets. Its output — not its pass/fail — is
the deliverable for Phases C, D and E: the plan's §5 rule is that no live
behaviour change ships without its parity diff reviewed first.

WHAT IT MEASURED ON DAY ONE  (2026-08-18)
-----------------------------------------
The plan's §1.2 predicted this would fail immediately for momentum. It does
not, and the reason matters more than the prediction did.

  * For the `all_risk` category the two paths agree EXACTLY (Jaccard 1.000,
    15/15 names, rank band 3 on 2026-08-14). That is correct, not a bug in
    the harness: `build_category_presets` defines all_risk as "unfiltered
    baseline (zero kwargs)", and every MomentumAdapter filter defaults to
    off/None. With no filters configured, select_buy_pool's chain is a
    no-op, so ranking then taking the top N is genuinely the same rule.
    This is now asserted as a hard invariant -- it is a real property worth
    protecting, and it was previously only assumed.

  * The divergence is NOT that live ranks differently. It is that
    `features/momentum_live.py` cannot express a filtered category AT ALL.
    Its STRATEGIES entries carry only {band_id, label, rank_start,
    rank_end, strategy_id} -- no category, no top_n, no lookback, no
    filters -- while the registry declares four cumulative categories
    (all_risk / balanced / risk_managed / max_defensive). Three of those
    four are unrepresentable live.

  * The `balanced` case ALSO shows zero difference today, and that is not
    reassurance. At the production liquidity floor (0.1cr, the value
    scripts/run_momentum_recommended_strategies.py actually runs) nothing
    is excluded: the least liquid name in rank band 8 still trades 0.18cr.
    The filtered and unfiltered rules coincide by accident of parameter
    values, not by design -- raise the floor to 25cr and the held set moves
    by 6-8 names (measured, band 7 and 8).

So the risk is sharper than "live picks drift": capital allocated to a
balanced or max_defensive strategy would run **completely unfiltered**
live, because the live path has no way to know the category exists. Today
that is invisible because the floor does not bind. It is asserted
structurally below rather than as a per-date diff for exactly that reason
-- a diff-based test would pass today and keep passing until the day
someone tightens a filter, which is the day it would matter.

REAL DATA ONLY
--------------
Both paths read the real normalised DuckDB. Nothing here fabricates prices
or universes — a parity result computed on invented data would prove
nothing about production (CLAUDE.md Absolute Rule 6). When the database is
absent or locked, these tests SKIP rather than fall back to synthetic
inputs.

PIT Assumptions
---------------
`as_of_date` is resolved to the latest real trading day present in
ohlcv_adjusted, and every read is as-of that date. `rank_band_tickers` is
called without `include_delisted`, matching the live path exactly — see
features/momentum_live.py's docstring for why the live path must differ
from a backtest here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date
from typing import Any, Dict, List, Optional, Set

import pytest

pytest.importorskip("pandas")
pytest.importorskip("duckdb")

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

from config.settings import DUCKDB_PATH  # noqa: E402

# The liquidity floor the real momentum backtests run with. Deliberately not
# config.settings.MIN_ADTV_CR: that is 0.0 under the active universe profile,
# so using it would apply a filter that filters nothing.
RECOMMENDED_MIN_ADTV_CR = 0.1  # scripts/run_momentum_recommended_strategies.py:111


# ---------------------------------------------------------------------------
# Parity report
# ---------------------------------------------------------------------------


@dataclass
class ParityReport:
    """The diff between what the backtest would hold and what the live path
    selects, for one strategy on one date."""

    channel: str
    strategy_id: str
    category: str
    as_of_date: str
    universe_size: int
    backtest_selection: Set[str] = field(default_factory=set)
    live_selection: Set[str] = field(default_factory=set)

    @property
    def only_backtest(self) -> Set[str]:
        """Held by the measured rule, missed by the live path."""
        return self.backtest_selection - self.live_selection

    @property
    def only_live(self) -> Set[str]:
        """Bought live, never evaluated by any backtest."""
        return self.live_selection - self.backtest_selection

    @property
    def agreed(self) -> Set[str]:
        return self.backtest_selection & self.live_selection

    @property
    def jaccard(self) -> float:
        union = self.backtest_selection | self.live_selection
        if not union:
            return 1.0
        return len(self.agreed) / len(union)

    def describe(self) -> str:
        return (
            f"\n=== PARITY DIFF: {self.channel} / {self.strategy_id} "
            f"[{self.category}] @ {self.as_of_date} ===\n"
            f"  universe scored      : {self.universe_size}\n"
            f"  backtest would hold  : {len(self.backtest_selection)}\n"
            f"  live would hold      : {len(self.live_selection)}\n"
            f"  agreed               : {len(self.agreed)}\n"
            f"  overlap (Jaccard)    : {self.jaccard:.3f}\n"
            f"  ONLY backtest ({len(self.only_backtest)}): {sorted(self.only_backtest)}\n"
            f"  ONLY live     ({len(self.only_live)}): {sorted(self.only_live)}\n"
        )


# ---------------------------------------------------------------------------
# Real-data fixtures
# ---------------------------------------------------------------------------


def _connect() -> Optional[Any]:
    """Read-only connection to the normalised DB, or None.

    Returns None rather than raising when the scheduler holds the write
    lock: DuckDB refuses even read_only connections while another process
    has the database open for writing, and a locked database is an
    environment condition, not a parity failure."""
    if not DUCKDB_PATH.exists():
        return None
    try:
        return duckdb.connect(str(DUCKDB_PATH), read_only=True)
    except Exception:
        return None


@pytest.fixture(scope="module")
def conn() -> Any:
    c = _connect()
    if c is None:
        pytest.skip(
            f"normalised DuckDB unavailable or locked ({DUCKDB_PATH}); parity "
            "needs real market data and must never run on fabricated inputs"
        )
    yield c
    c.close()


@pytest.fixture(scope="module")
def as_of_date(conn) -> str:
    """The most recent real trading day in ohlcv_adjusted."""
    row = conn.execute("SELECT max(date) FROM ohlcv_adjusted").fetchone()
    if not row or row[0] is None:
        pytest.skip("ohlcv_adjusted is empty; no real date to compare on")
    return str(row[0])


# ---------------------------------------------------------------------------
# Momentum parity
# ---------------------------------------------------------------------------


def build_momentum_parity_report(
    conn: Any, as_of_date: str, strategy_id: str, category: str = "all_risk",
) -> ParityReport:
    """Run both momentum paths over one identical universe and diff them.

    The whole point is that BOTH sides receive the same `universe` list and
    the same date, so any difference in the result is attributable to the
    selection RULE and nothing else. `compute_daily_ranking` takes a
    `universe` override for exactly this reason, and the adapter is given a
    price panel built from that same list.
    """
    from backtest.adapters.momentum_adapter import MomentumAdapter
    from backtest.core.horizon import HorizonBucket
    from features import momentum_live
    from features.momentum_signal import load_price_panel, load_volume_panel
    from features.momentum_universe import rank_band_tickers

    cfg = momentum_live.get_strategy(strategy_id)

    # ONE universe, shared. Built the way the live path builds it.
    universe: List[str] = rank_band_tickers(
        conn, as_of_date, cfg["rank_start"], cfg["rank_end"],
    )
    if not universe:
        pytest.skip(
            f"no PIT market-cap universe for rank band "
            f"{cfg['rank_start']}-{cfg['rank_end']} at {as_of_date}"
        )

    # The live path's own parameters, read the way the live path now reads
    # them (C1: from strategy_registry.definition_json). Taking them from the
    # same source the live path uses means this diff measures the SELECTION
    # RULE and never a parameter mismatch the harness introduced itself.
    declared = momentum_live.strategy_params(strategy_id)
    top_n = int(declared["top_n"])
    lookback_months = int(declared["lookback_months"])

    # Panel wide enough for the trailing window, from the same universe.
    start = (pd.Timestamp(as_of_date) - pd.Timedelta(days=lookback_months * 31 + 120)).date()
    panel = load_price_panel(conn, universe, str(start), as_of_date)
    if panel.empty:
        pytest.skip(f"no real price history for the band universe at {as_of_date}")

    # The registry's four categories are cumulative filter stacks over the
    # same ranking. all_risk is the unfiltered baseline (zero kwargs) --
    # build_category_presets' own words -- so it is the one category the
    # live path can currently reproduce.
    filter_kwargs: Dict[str, Any] = {}
    if category != "all_risk":
        volume_panel = load_volume_panel(conn, universe, str(start), as_of_date)
        if volume_panel.empty:
            pytest.skip(f"no real volume history to apply the {category} ADTV floor")
        # Only the liquidity floor is applied here. The quality gate and
        # regime filters need panels this harness has no real source for at
        # this date, and fabricating them would make the diff meaningless.
        #
        # The floor is RECOMMENDED_MIN_ADTV_CR, the value the real momentum
        # runs use (scripts/run_momentum_recommended_strategies.py:111),
        # NOT config.settings.MIN_ADTV_CR -- which is 0.0 under the current
        # universe profile and would make this a silent no-op. An earlier
        # draft of this harness used it and reported a meaningless parity.
        filter_kwargs = {
            "volume_panel": volume_panel,
            "min_adtv_cr": RECOMMENDED_MIN_ADTV_CR,
        }

    adapter = MomentumAdapter(
        price_panel=panel, top_n=top_n, lookback_months=lookback_months,
        **filter_kwargs,
    )
    signals = adapter.generate_signals(universe, pd.Timestamp(as_of_date).date(), HorizonBucket.Y1)
    # SignalAction is a plain str alias ("buy" | "sell" | "forced_close" |
    # "hold"), not an Enum -- compared as a lowercase string deliberately.
    backtest_selection = {s.ticker for s in signals if s.action == "buy"}

    ranking = momentum_live.compute_daily_ranking(
        conn, as_of_date, strategy_id=strategy_id, universe=universe,
    )
    live_selection: Set[str] = (
        set(ranking.loc[ranking["in_top_n"], "ticker"]) if not ranking.empty else set()
    )

    return ParityReport(
        channel="momentum",
        strategy_id=strategy_id,
        category=category,
        as_of_date=as_of_date,
        universe_size=len(universe),
        backtest_selection=backtest_selection,
        live_selection=live_selection,
    )


@pytest.fixture(scope="module")
def momentum_report(conn, as_of_date) -> ParityReport:
    """The unfiltered baseline: the one category the live path can express."""
    return build_momentum_parity_report(
        conn, as_of_date, momentum_live_default_strategy_id(), category="all_risk",
    )


@pytest.fixture(scope="module")
def momentum_balanced_report(conn, as_of_date) -> ParityReport:
    """A filtered category, which the live path has no way to represent."""
    return build_momentum_parity_report(
        conn, as_of_date, momentum_live_default_strategy_id(), category="balanced",
    )


def _live_strategies() -> List[Dict[str, Any]]:
    from features import momentum_live

    return list(momentum_live.STRATEGIES)


def momentum_live_default_strategy_id() -> str:
    from features import momentum_live

    return momentum_live.DEFAULT_STRATEGY_ID


def momentum_live_default_top_n() -> int:
    from features import momentum_live

    return int(momentum_live.strategy_params(momentum_live_default_strategy_id())["top_n"])


def test_harness_feeds_both_paths_the_same_inputs(momentum_report):
    """Guards the harness itself before anything is concluded from it.

    A parity diff is only evidence if both sides really saw the same
    universe. If the adapter silently scored a smaller set (a short price
    panel, say), the diff would be an artefact of the harness and every
    conclusion drawn from it would be wrong."""
    assert momentum_report.universe_size > 0
    assert momentum_report.backtest_selection or momentum_report.live_selection, (
        "Neither path selected anything; the harness is not exercising the "
        "selection rules and the diff would be vacuously equal."
    )
    assert len(momentum_report.backtest_selection) <= momentum_live_default_top_n()
    assert len(momentum_report.live_selection) <= momentum_live_default_top_n()


def test_parity_diff_is_reported(momentum_report, momentum_balanced_report, capsys):
    """Always-run reporter. Phases C/D/E require the diff as a reviewable
    artefact (§5), so it is printed whether or not parity holds -- a diff
    that only appears on failure is unavailable exactly when someone is
    deciding whether to ship the fix."""
    with capsys.disabled():
        print(momentum_report.describe())
        print(momentum_balanced_report.describe())
    assert 0.0 <= momentum_report.jaccard <= 1.0


def test_unfiltered_momentum_is_already_at_parity(momentum_report):
    """MEASURED, not assumed (see this module's docstring).

    For the unfiltered `all_risk` category the live ranking and the
    backtested adapter select the same names from the same universe. This
    is the invariant §3 demands."""
    assert momentum_report.backtest_selection == momentum_report.live_selection, (
        momentum_report.describe()
    )


@pytest.mark.parametrize(
    "strategy_id",
    [s["strategy_id"] for s in _live_strategies()],
)
def test_every_band_matches_the_backtested_rule(conn, as_of_date, strategy_id):
    """C3: parity to zero for Momentum, across every live rank band.

    Band 3 alone was not enough. Before C2 the live path and the adapter
    agreed on bands 1-4 and 6 while DISAGREEING on bands 7 and 8 -- 8 names
    out of 15 in band 7, 6 in band 8 -- and a band-3-only test reported
    perfect parity throughout. The bands that diverged are exactly the
    illiquid ones, because the old live path counted each ticker's own last
    126 TRADING ROWS (ROW_NUMBER partitioned by ticker) while the backtest
    used the shared calendar window. Names that trade every day are
    unaffected; names with gaps reached further back in calendar time and
    scored a different trailing return -- up to 14 percentage points.

    Parametrizing over every band is what makes this test able to fail for
    the reason it exists."""
    report = build_momentum_parity_report(conn, as_of_date, strategy_id)
    assert report.backtest_selection == report.live_selection, report.describe()


def test_the_harness_would_detect_a_filter_induced_divergence(conn, as_of_date):
    """Sensitivity guard -- WITHOUT THIS, EVERY PARITY PASS ABOVE IS WORTHLESS.

    The two parity results above are both "no difference". That is only
    evidence if this harness is capable of reporting a difference at all. A
    filter kwarg silently ignored by the adapter, a panel that failed to
    load, a universe that collapsed to nothing -- each would produce a
    perfect, entirely meaningless parity score.

    So: raise the liquidity floor until it MUST bind, and assert the
    selection actually moves. Measured on 2026-08-14, band 3: a 100cr floor
    changes the held set. If this test ever goes quiet, the parity results
    above stop meaning anything and must not be trusted."""
    from backtest.adapters.momentum_adapter import MomentumAdapter
    from backtest.core.horizon import HorizonBucket
    from features import momentum_live
    from features.momentum_signal import load_price_panel, load_volume_panel
    from features.momentum_universe import rank_band_tickers

    cfg = momentum_live.get_strategy(momentum_live_default_strategy_id())
    universe = rank_band_tickers(conn, as_of_date, cfg["rank_start"], cfg["rank_end"])
    if not universe:
        pytest.skip("no universe for the sensitivity check")
    start_d = (pd.Timestamp(as_of_date) - pd.Timedelta(days=6 * 31 + 120)).date()
    panel = load_price_panel(conn, universe, str(start_d), as_of_date)
    volume = load_volume_panel(conn, universe, str(start_d), as_of_date)
    if panel.empty or volume.empty:
        pytest.skip("no real price/volume history for the sensitivity check")

    as_of = pd.Timestamp(as_of_date).date()

    def _held(floor: float) -> Set[str]:
        adapter = MomentumAdapter(
            price_panel=panel, top_n=momentum_live_default_top_n(), lookback_months=6,
            volume_panel=volume, min_adtv_cr=floor,
        )
        return {
            sig.ticker
            for sig in adapter.generate_signals(universe, as_of, HorizonBucket.Y1)
            if sig.action == "buy"
        }

    unfiltered = _held(0.0)
    # A floor above the band's median traded value must exclude names.
    heavily_filtered = _held(1000.0)
    assert unfiltered != heavily_filtered, (
        "Raising the ADTV floor to 1000cr did not change the selection. The "
        "adapter is ignoring min_adtv_cr, or the volume panel is empty -- "
        "either way the parity results in this module are vacuous."
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "PHASE-C2: features/momentum_live.py cannot APPLY a filtered "
        "category. C1 gave it the registry's declared parameters and an "
        "explicit category=all_risk, but it still has no filter chain to "
        "run, while strategy_registry "
        "declares four cumulative categories (all_risk, balanced, "
        "risk_managed, max_defensive). Three of the four are unrepresentable "
        "live, so a balanced or max_defensive strategy would run COMPLETELY "
        "UNFILTERED in production while its backtest applied the whole chain. "
        "When C2 routes the live path through select_buy_pool, this becomes "
        "an XPASS and strict=True fails the run: delete the marker and keep "
        "the assertion."
    ),
)
def test_live_path_can_express_every_registry_category():
    """The real momentum gap, stated as the invariant rather than as a diff.

    A per-date selection diff cannot capture this: the live path does not
    merely pick different names for a filtered strategy, it has no way to
    know the strategy is filtered. Today that is masked -- the production
    ADTV floor (0.1cr) does not bind on any current rank band, so the
    filtered and unfiltered selections coincide by accident of parameter
    values, not by design. Tighten the floor, add the quality gate, or move
    to a less liquid band and the two silently part company.

    That accident is exactly why this is asserted structurally."""
    from features import momentum_live

    declared_categories = {"all_risk", "balanced", "risk_managed", "max_defensive"}
    expressible = {
        str(strategy.get("category"))
        for strategy in momentum_live.STRATEGIES
        if strategy.get("category") is not None
    }
    assert expressible == declared_categories, (
        "features/momentum_live.py can express "
        f"{sorted(expressible) or 'NO categories'}, but strategy_registry "
        f"declares {sorted(declared_categories)}. Every strategy in a "
        "category the live path cannot express runs unfiltered live."
    )


# ---------------------------------------------------------------------------
# Technical and Fundamental: harness slots, deliberately not yet written
# ---------------------------------------------------------------------------


TECHNICAL_PARITY_TEMPLATE = "A1"
TECHNICAL_TOP_N = 10


def build_technical_parity_report(as_of_date: str, template: str = TECHNICAL_PARITY_TEMPLATE) -> ParityReport:
    """Diff the live technical holdings path against the backtested rule.

    backtest_selection : TechnicalAdapter.generate_signals — the rule that
        was measured, called directly, exactly as BacktestOrchestrator calls it.
    live_selection : the SAME adapter reached through LiveSignalRunner (D2),
        which is now the live holdings path.

    A non-zero diff here means the runner is doing selection of its own,
    which is precisely what it must never do. This is a real measurement
    rather than a tautology: the runner could filter, re-sort, truncate, or
    drop sells, and each of those would show up as a diff.
    """
    from backtest.adapters.technical_adapter import TechnicalAdapter
    from backtest.core.horizon import HorizonBucket
    from backtest.core.live_signal_runner import LiveSignalRunner
    from systems.technical_analysis.screener.engine import ScreenerEngine

    day = _date.fromisoformat(as_of_date)
    engine = ScreenerEngine()
    universe = [r.ticker for r in engine.screen(template, as_of_date, limit=500)]

    backtest_adapter = TechnicalAdapter(template_name=template, top_n=TECHNICAL_TOP_N)
    backtest_selection = {
        s.ticker for s in backtest_adapter.generate_signals(universe, day, HorizonBucket.D21)
        if s.action == "buy"
    }

    live_adapter = TechnicalAdapter(template_name=template, top_n=TECHNICAL_TOP_N)
    runner = LiveSignalRunner(
        "technical", f"ta_{template}", horizon_bucket=HorizonBucket.D21,
        persist_signals=False, enforce_readiness=False,
    )
    live_selection = set(runner.target_holdings(live_adapter, universe, day))

    return ParityReport(
        channel="technical", strategy_id=f"ta_{template}", category="template",
        as_of_date=as_of_date, universe_size=len(universe),
        backtest_selection=backtest_selection, live_selection=live_selection,
    )


@pytest.fixture(scope="module")
def technical_report(as_of_date) -> ParityReport:
    report = build_technical_parity_report(as_of_date)
    if not report.universe_size:
        pytest.skip(
            f"template {TECHNICAL_PARITY_TEMPLATE} matched nothing on {as_of_date}; "
            "no real selection to diff (never fabricated)"
        )
    return report


def test_technical_live_selection_matches_the_backtested_rule(technical_report):
    """D2/D4: the live holdings path must select exactly the backtested set.

    LiveSignalRunner is deliberately thin — it assembles today's inputs and
    delegates. The moment it starts deciding anything, live technical
    holdings stop being what was backtested, which is the failure this whole
    refactor exists to remove.
    """
    assert technical_report.only_live == set() and technical_report.only_backtest == set(), (
        technical_report.describe()
    )


def test_technical_holdings_are_a_subset_of_the_alert_feed(as_of_date):
    """The two questions Technical conflates, measured (D1/D3).

    `ta_signals` answers "what matched the template today"; the adapter
    answers "what should be held". The second must be a selection FROM the
    first — a held name that never matched would mean the two paths evaluate
    the template differently, which is exactly the drift D3 removes by
    making them share one ScreenerEngine evaluation.
    """
    from systems.technical_analysis.screener.engine import ScreenerEngine, is_full_match

    engine = ScreenerEngine()
    alert_matches = {
        r.ticker for r in engine.screen(TECHNICAL_PARITY_TEMPLATE, as_of_date, limit=500)
        if is_full_match(r.score)
    }
    if not alert_matches:
        pytest.skip(f"no full matches for {TECHNICAL_PARITY_TEMPLATE} on {as_of_date}")

    report = build_technical_parity_report(as_of_date)
    assert report.backtest_selection <= alert_matches, (
        f"held but never a full match on {as_of_date}: "
        f"{sorted(report.backtest_selection - alert_matches)}"
    )
    assert len(report.backtest_selection) <= TECHNICAL_TOP_N


def test_the_alert_feed_and_the_screener_share_one_evaluation(as_of_date):
    """D3/D4: `ta_signals` and the screener must answer from the same code.

    The alert checker used to run its own loop over the engine's PRIVATE
    _load_df/_screen_df, so a change to how the screener evaluates a template
    did not necessarily reach the alert feed. It now calls the engine's
    public screen_all(). This asserts the result: for the same template and
    date, the alert feed's matched set is exactly what screen() returns.
    """
    from systems.technical_analysis.alerts.daily_alert_checker import DailyAlertChecker
    from systems.technical_analysis.screener.engine import ALL_MATCHES_LIMIT, ScreenerEngine

    checker = DailyAlertChecker()
    resolved, results = checker.evaluate(as_of_date)
    if resolved is None:
        pytest.skip(f"no feature Parquet for {as_of_date}")

    alerted = [r.ticker for r in results.get(TECHNICAL_PARITY_TEMPLATE, [])]
    screened = [
        r.ticker for r in ScreenerEngine().screen(
            TECHNICAL_PARITY_TEMPLATE, resolved, limit=ALL_MATCHES_LIMIT,
        )
    ]
    assert alerted == screened, (
        f"the alert feed and the screener disagree about {TECHNICAL_PARITY_TEMPLATE} "
        f"on {resolved}: only-alerted={sorted(set(alerted) - set(screened))}, "
        f"only-screened={sorted(set(screened) - set(alerted))}"
    )


FUNDAMENTAL_PARITY_PRESET = "quality_compounder"


def build_fundamental_parity_report(as_of_date: str, preset: str = FUNDAMENTAL_PARITY_PRESET) -> ParityReport:
    """Diff the live fundamentals screener against the backtested rule.

    backtest_selection : FundamentalAdapter.select_candidates — everyone the
        preset matches, the same call BacktestOrchestrator's adapter makes
        before entry filters and the top_n cut.
    live_selection : GET /fundamental/screener's own answer, via the router's
        _matched_tickers.

    E2 made the router a reader over that method, so this should be zero. It
    is not a tautology: the router still owns which universe it passes, which
    date it resolves, and (for the bespoke presets) whether it supplies the DB
    connection the PIT path needs — each of which can silently change the set.
    """
    from datastore.api.routers import fundamentals as router
    from datastore.api.utils.feature_store import read_feature_day
    from backtest.adapters.fundamental_adapter import FundamentalAdapter

    panel = read_feature_day(as_of_date)
    if panel is None:
        return ParityReport(
            channel="fundamental", strategy_id=preset, category="preset",
            as_of_date=as_of_date, universe_size=0,
        )

    universe = [str(t) for t in panel["ticker"]]
    adapter = FundamentalAdapter(preset=preset, sector_lookup=router._sector_map())
    backtest_selection = set(adapter.select_candidates(universe, _date.fromisoformat(as_of_date)))
    live_selection = set(router._matched_tickers(preset, panel, as_of_date))

    return ParityReport(
        channel="fundamental", strategy_id=preset, category="preset",
        as_of_date=as_of_date, universe_size=len(universe),
        backtest_selection=backtest_selection, live_selection=live_selection,
    )


def test_fundamental_live_selection_matches_the_backtested_rule(as_of_date):
    """E2/E3: the screener endpoint and the backtest must match the same set.

    Before E2 the router re-implemented the adapter's dispatch, and it had
    already drifted once: composite-score strategies were evaluated live with
    no sector exclusion at all, which the adapter applies.
    """
    from datastore.api.utils.feature_store import resolve_date

    feature_date = resolve_date(as_of_date) or resolve_date(None)
    if feature_date is None:
        pytest.skip("no fundamental feature Parquet available")

    report = build_fundamental_parity_report(feature_date)
    if not report.universe_size:
        pytest.skip(f"no feature panel on {feature_date}")
    assert report.only_live == set() and report.only_backtest == set(), report.describe()
