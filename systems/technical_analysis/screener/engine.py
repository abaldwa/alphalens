"""
systems/technical_analysis/screener/engine.py

Phase: 3.x (Technical Analysis Screener)
Specs: SPEC-TA-005
Owner: Technical Analysis / Screener
Consumers: datastore/api/routers/technical.py,
           systems/technical_analysis/alerts/daily_alert_checker.py

Screening engine that evaluates named or custom condition templates against
the daily feature Parquet store (config.settings.FEATURES_DAILY_DIR).

Reads real feature Parquets only — no synthetic data in production paths
(SPEC-QUALITY-003 + no-stub/synthetic-data policy). Test fixtures using
synthetic DataFrames are permitted only in tests/unit/test_ta_screener.py
as explicitly stated in SPEC-SYS-006's testing exemption.

Architecture:
    ScreenerEngine.screen(template_name) → loads Parquet → applies conditions
    → returns sorted ScreenerResult list (all conditions must match: score=1.0)

Condition dict format (SPEC-TA-005):
    {"feature": "rsi_14", "op": "lt", "value": 30}
    {"feature": "sma_200_ratio", "op": "gt", "value": 1.0}
    {"feature": "roc_10", "op": "top_pct", "value": 0.20}   # cross-sectional
    {"feature": "bb_width_pct", "op": "bottom_pct", "value": 0.25}

Supported ops:
    lt, gt, lte, gte, eq           — column vs scalar
    between                         — column in [lo, hi] (value=[lo, hi])
    top_pct                         — column >= quantile(1-value); cross-sectional
    bottom_pct                      — column <= quantile(value); cross-sectional

Feature-name safety (SPEC-TA-005): if a feature column is absent from the
Parquet (older backfill date, or feature not yet computed), the condition is
treated as unmet — the condition still counts toward total_conditions but NOT
toward matched_conditions. This is the only tolerated NaN/missing behaviour;
no synthetic fill-ins are ever applied in production paths.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from config.settings import FEATURES_DAILY_DIR
from datastore.api.utils.feature_store import resolve_date
from systems.technical_analysis.screener.templates import ScreenerTemplate

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result / info types
# ---------------------------------------------------------------------------


@dataclass
class ScreenerResult:
    """One stock matched by a screener run.

    Parameters
    ----------
    ticker : str
        NSE ticker symbol.
    date : str
        Feature date (YYYY-MM-DD) used for this run.
    template_name : str
        Template identifier, e.g. "A1", "E2", "S004".
    matched_conditions : int
        Number of conditions the ticker satisfied.
    total_conditions : int
        Total number of conditions in the template.
    score : float
        matched_conditions / total_conditions in [0, 1].
    key_values : dict
        Feature values relevant to the template (for display).

    Spec References
    ---------------
    SPEC-TA-005: screener result schema
    """

    ticker: str
    date: str
    template_name: str
    matched_conditions: int
    total_conditions: int
    score: float
    key_values: Dict[str, float] = field(default_factory=dict)


@dataclass
class TemplateInfo:
    """Summary of a screener template for the /screener/templates listing endpoint.

    Parameters
    ----------
    name : str
        Unique template identifier.
    category : str
        One-letter category code (A-F, S).
    description : str
        Human-readable strategy name.
    condition_count : int
        Number of conditions in the template.

    Spec References
    ---------------
    SPEC-TA-005: template listing endpoint
    """

    name: str
    category: str
    description: str
    condition_count: int


# ---------------------------------------------------------------------------
# ScreenerEngine
# ---------------------------------------------------------------------------


class ScreenerEngine:
    """Evaluates named or custom screener templates against the feature Parquet.

    Parameters
    ----------
    None — paths are read from config.settings (SPEC-QUALITY-003).

    Spec References
    ---------------
    SPEC-TA-005: Custom Technical Screener with 42 Pre-Built Templates
    SPEC-QUALITY-003: no hardcoded paths

    PIT Assumptions
    ---------------
    Reads the Parquet for the specified date (or the latest available day).
    Features carry PITRule.NONE for OHLCV-derived technicals (same-day values
    are knowable) and PITRule.KNOWN_AFTER for fundamentals-derived features
    (handled upstream in matrix_builder.py — this engine reads the already-
    PIT-correct Parquet, not raw tables).
    """

    # Ops that require a second column argument
    _COL_VS_COL_OPS: frozenset[str] = frozenset({"gt_col", "lt_col", "gte_col", "lte_col"})
    # Ops that aggregate across the universe (cross-sectional percentile filters)
    _UNIVERSE_OPS: frozenset[str] = frozenset({"top_pct", "bottom_pct"})

    def __init__(self) -> None:
        # [PERF, 2026-08-02] _load_df previously re-read the full ~2,300-
        # ticker feature Parquet from disk on every single screen()/
        # screen_custom() call, even for the SAME date requested twice in
        # a row — profiling a technical backtest job showed this as the
        # single largest cost in BacktestOrchestrator's daily loop (~35%
        # of run() time). Bounded to the single most-recently-loaded date,
        # same locality argument backtest/run_orchestrator_backtest.py's
        # build_technical_feature_lookup() already relies on: a backtest
        # walks trading days strictly forward, and the live screener API
        # only ever asks for "today" — a date once moved past is never
        # re-requested, so evicting anything but the current date loses
        # nothing while eliminating same-date re-reads (e.g. a rebalance
        # date screened for entries AND checked for exits the same day).
        self._cached_date: Optional[str] = None
        self._cached_df: Optional[pd.DataFrame] = None
        # [PERF, 2026-08-02] populated only by preload_dates() — a date
        # present here (even if the value is None, a genuine missing-file
        # result) short-circuits _load_df with zero disk I/O. Empty by
        # default (preload_dates never called) — every existing caller is
        # completely unaffected.
        self._preloaded: Dict[str, Optional[pd.DataFrame]] = {}

    def _read_parquet_for_date(self, date_str: str) -> Optional[pd.DataFrame]:
        """The raw, uncached disk read for one date — extracted so
        preload_dates() can call it from a thread pool without going
        through _load_df's single-slot-cache bookkeeping (which isn't
        thread-safe and isn't needed here; preload_dates owns its own
        dict, one write per date, no concurrent writes to the same key)."""
        day_path = FEATURES_DAILY_DIR / f"{date_str}.parquet"
        if not day_path.exists():
            logger.warning("Feature Parquet not found for date %s at %s", date_str, day_path)
            return None
        return pd.read_parquet(day_path)

    def preload_dates(self, dates: List[str], max_workers: int = 8) -> None:
        """[PERF, 2026-08-02] Eagerly reads every date in `dates` up front,
        concurrently (ThreadPoolExecutor — pyarrow's read/decode releases
        the GIL enough to give real parallelism: measured ~2.2x on 200
        real feature Parquets at 8 workers vs sequential, plateauing past
        8 on a 14-core machine), instead of the single-slot cache's
        lazy one-at-a-time-as-the-backtest-walks-forward loading.

        Opt-in only (backtest/run_orchestrator_backtest.py's
        --prefetch-feature-parquets flag) — never called by default, so
        _load_df's existing behavior for every other caller (the live
        screener API, any test, any job not passing the flag) is
        completely unaffected. Trades memory (every requested date's
        DataFrame held simultaneously — measured ~0.6MB/date) for speed;
        see that flag's docstring for the sizing rationale.
        """
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(self._read_parquet_for_date, dates)
            for date_str, df in zip(dates, results):
                self._preloaded[date_str] = df

    def _load_df(self, date_str: str) -> Optional[pd.DataFrame]:
        """Load the full feature Parquet for one date.

        Parameters
        ----------
        date_str : str
            Date in YYYY-MM-DD format.

        Returns
        -------
        pd.DataFrame or None
            Full feature matrix with 'ticker' index column, or None if the
            file does not exist for this date.

        Spec References
        ---------------
        SPEC-TA-005: read real Parquet — no synthetic fallback
        """
        if date_str in self._preloaded:
            return self._preloaded[date_str]
        if date_str == self._cached_date:
            return self._cached_df
        df = self._read_parquet_for_date(date_str)
        if df is None:
            return None
        self._cached_date = date_str
        self._cached_df = df
        return df

    def _apply_single_condition(
        self,
        df: pd.DataFrame,
        condition: Dict[str, Any],
        available_cols: frozenset[str],
    ) -> Tuple[pd.Series, bool]:
        """Evaluate one condition dict against the DataFrame, row by row.

        Parameters
        ----------
        df : pd.DataFrame
            The feature DataFrame (one row per ticker).
        condition : dict
            Condition dict with at minimum {"feature": str, "op": str}.
        available_cols : frozenset
            Set of columns actually present in `df`.

        Returns
        -------
        (mask, col_missing) : (pd.Series[bool], bool)
            mask       — True where the condition is met (False for NaN cells).
            col_missing — True if the required feature column was absent from
                          the Parquet (condition treated as unmet per SPEC-TA-005).

        Spec References
        ---------------
        SPEC-TA-005: "if feature not in available_cols → condition treated as unmet"
        """
        feature = condition.get("feature", "")
        op = condition.get("op", "")
        value: Any = condition.get("value")
        feature2 = condition.get("feature2")

        false_mask = pd.Series(False, index=df.index)

        if feature not in available_cols:
            return false_mask, True  # column missing → unmet

        col = df[feature]

        try:
            if op == "lt":
                mask = col < value
            elif op == "gt":
                mask = col > value
            elif op == "lte":
                mask = col <= value
            elif op == "gte":
                mask = col >= value
            elif op == "eq":
                mask = col == value
            elif op == "between":
                lo, hi = value[0], value[1]
                mask = (col >= lo) & (col <= hi)
            elif op == "top_pct":
                # Cross-sectional: keep tickers in the top `value` fraction
                threshold = col.quantile(1.0 - float(value))
                mask = col >= threshold
            elif op == "bottom_pct":
                # Cross-sectional: keep tickers in the bottom `value` fraction
                threshold = col.quantile(float(value))
                mask = col <= threshold
            elif op in self._COL_VS_COL_OPS:
                if not feature2 or feature2 not in available_cols:
                    return false_mask, True
                col2 = df[feature2]
                if op == "gt_col":
                    mask = col > col2
                elif op == "lt_col":
                    mask = col < col2
                elif op == "gte_col":
                    mask = col >= col2
                else:  # lte_col
                    mask = col <= col2
            else:
                logger.warning("Unknown screener op '%s' — condition treated as unmet", op)
                return false_mask, False
        except Exception as exc:
            logger.warning("Condition evaluation failed for feature '%s' op '%s': %s", feature, op, exc)
            return false_mask, False

        return mask.fillna(False), False

    def _screen_df(
        self,
        df: pd.DataFrame,
        template: ScreenerTemplate,
        date_str: str,
        limit: int,
    ) -> List[ScreenerResult]:
        """Apply template conditions to a loaded feature DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Full feature matrix for one date (one row per ticker).
        template : ScreenerTemplate
            Template with name, category, description, and conditions list.
        date_str : str
            Feature date (YYYY-MM-DD), stored in each ScreenerResult.
        limit : int
            Maximum number of results to return (sorted by score desc).

        Returns
        -------
        list of ScreenerResult
            Sorted by score descending (best matches first), then by
            volume_ratio_21d descending within the same score bucket.
            Each result carries key_values populated from the Parquet.

        Spec References
        ---------------
        SPEC-TA-005: screener evaluation logic
        """
        if df.empty:
            return []

        # Ensure 'ticker' column is present
        if "ticker" not in df.columns:
            logger.error("Feature Parquet is missing 'ticker' column on date %s", date_str)
            return []

        available_cols = frozenset(df.columns)
        total_conditions = len(template.conditions)

        if total_conditions == 0:
            return []

        # Accumulate per-row condition match counts
        # Start with zeros; add 1 for each condition met
        match_counts = pd.Series(0, index=df.index, dtype=int)

        for cond in template.conditions:
            mask, _missing = self._apply_single_condition(df, cond, available_cols)
            match_counts += mask.astype(int)

        scores = match_counts / total_conditions

        # Return only full matches (score == 1.0 — all conditions met).
        # The screener is a strict filter: a stock must satisfy every condition
        # to be surfaced. Partial matches are not shown to avoid false positives
        # and keep the output actionable (SPEC-TA-005).
        result_mask = scores >= 1.0 - 1e-9
        result_df = df[result_mask].copy()
        result_df["_score"] = scores[result_mask]
        result_df["_matched"] = match_counts[result_mask]

        # Sort: score desc, then a liquidity/activity proxy desc as secondary
        # (more active first). volume_ratio_21d is preferred, but it is not
        # guaranteed to be present in every day's feature set. Rather than
        # silently falling back to source-Parquet row order (which is
        # ticker-alphabetical and produces a misleading "only A-tickers show
        # up" screener result — see FeatureBacklog T9), we fall through a
        # priority list of liquidity/volume proxy columns and, failing all
        # of those, use a deterministic hash-of-ticker tiebreak so ordering
        # is never simply alphabetical.
        sort_cols = ["_score"]
        sort_asc = [False]
        _VOL_PROXY_PRIORITY = (
            "volume_ratio_21d",
            "volume_ratio_5d",
            "volume_zscore_10d",
            "vol_spike_vs_60d_avg",
            "breakout_volume_ratio",
            "turnover_acceleration",
        )
        vol_col = next((c for c in _VOL_PROXY_PRIORITY if c in available_cols), None)
        if vol_col is not None:
            result_df["_vol"] = df.loc[result_mask, vol_col]
            sort_cols.append("_vol")
            sort_asc.append(False)

        # Deterministic, non-alphabetical final tiebreak (always present).
        result_df["_tiebreak"] = result_df["ticker"].astype(str).map(
            lambda t: int(hashlib.md5(t.encode("utf-8")).hexdigest(), 16) % (2**32)
        )
        sort_cols.append("_tiebreak")
        sort_asc.append(True)

        result_df = result_df.sort_values(sort_cols, ascending=sort_asc).head(limit)

        # Determine display features
        display_features = list(dict.fromkeys(
            template.key_display_features
            + [c.get("feature", "") for c in template.conditions if c.get("feature")]
        ))
        display_features = [f for f in display_features if f and f in available_cols]

        results: List[ScreenerResult] = []
        for _, row in result_df.iterrows():
            key_vals: Dict[str, float] = {}
            for feat in display_features:
                raw = row.get(feat)
                if raw is not None and not (isinstance(raw, float) and np.isnan(raw)):
                    key_vals[feat] = round(float(raw), 6)

            results.append(ScreenerResult(
                ticker=str(row["ticker"]),
                date=date_str,
                template_name=template.name,
                matched_conditions=int(row["_matched"]),
                total_conditions=total_conditions,
                score=round(float(row["_score"]), 4),
                key_values=key_vals,
            ))

        return results

    def screen(
        self,
        template_name: str,
        date: Optional[str] = None,
        limit: int = 50,
    ) -> List[ScreenerResult]:
        """Run a named template against the daily feature store.

        Parameters
        ----------
        template_name : str
            Template identifier, e.g. "A1", "E2", "S004".
        date : str, optional
            YYYY-MM-DD. Defaults to the latest available feature Parquet date.
        limit : int, optional
            Maximum results to return (default 50).

        Returns
        -------
        list of ScreenerResult
            Sorted by score desc, then volume_ratio_21d desc.
            Returns [] if the template is unknown or no feature data is available.

        Raises
        ------
        KeyError
            If `template_name` is not in the template registry.

        Spec References
        ---------------
        SPEC-TA-005: GET /api/v1/ta/screener/run/{template_name}
        """
        # [A95-R2 cutover, 2026-08-15] The template comes from its
        # strategy_registry row, not from the in-memory TEMPLATE_MAP.
        #
        # Proven equivalent before switching: tests/unit/test_registry_templates.py
        # asserts, across all 63 templates, that the stored conditions are
        # byte-identical to templates.py and that category/description/
        # key_display_features/exit_* all match. T15 stored conditions verbatim,
        # so this reads back the same dicts _screen_df already evaluates.
        #
        # Why it is worth a new dependency: the report, the deploy page and the
        # API all explain a strategy from its row. While the screener selected
        # from the Python list there were two declarations of one entry
        # criterion, and nothing would have crashed if they diverged -- the
        # screener would simply have picked stocks on one definition while every
        # surface described another.
        #
        # NOTE this puts a DB read on a path that previously touched only
        # Parquet. It is one indexed lookup on a 63-row table against the
        # Parquet load below, but it does mean the screener now needs the
        # registry to be reachable. Deliberately NOT given a fallback to
        # TEMPLATE_MAP: a fallback would be taken silently on every registry
        # outage, which is how the second declaration would quietly come back.
        from systems.technical_analysis.screener.registry_templates import (
            list_templates as _registry_list_templates,
            load_template,
            template_exists,
        )

        if not template_exists(template_name):
            # KeyError with the available names is a published contract
            # (SPEC-TA-005), so the registry's own DefinitionNotFound is
            # translated rather than allowed to surface here.
            raise KeyError(
                f"Unknown template '{template_name}'. "
                f"Available: {sorted(t.name for t in _registry_list_templates())}"
            )

        template = load_template(template_name)
        resolved = resolve_date(date)
        if resolved is None:
            logger.warning("No feature Parquet available for date '%s'", date)
            return []

        df = self._load_df(resolved)
        if df is None:
            return []

        return self._screen_df(df, template, resolved, limit)

    def screen_custom(
        self,
        conditions: List[Dict[str, Any]],
        date: Optional[str] = None,
        limit: int = 50,
    ) -> List[ScreenerResult]:
        """Run a user-defined list of conditions against the daily feature store.

        Parameters
        ----------
        conditions : list of dict
            Each dict must have {"feature": str, "op": str} and either
            {"value": scalar/list} or {"feature2": str}.
            Example: [{"feature": "rsi_14", "op": "lt", "value": 30}]
        date : str, optional
            YYYY-MM-DD. Defaults to the latest available feature Parquet date.
        limit : int, optional
            Maximum results to return (default 50).

        Returns
        -------
        list of ScreenerResult
            template_name is set to "custom" for all rows.

        Spec References
        ---------------
        SPEC-TA-005: POST /api/v1/ta/screener/custom
        """
        resolved = resolve_date(date)
        if resolved is None:
            logger.warning("No feature Parquet available for date '%s'", date)
            return []

        df = self._load_df(resolved)
        if df is None:
            return []

        # Build a transient template from the custom conditions
        from systems.technical_analysis.screener.templates import ScreenerTemplate  # local import avoids circular

        custom_template = ScreenerTemplate(
            name="custom",
            category="custom",
            description="Custom screener",
            conditions=conditions,
            key_display_features=[c.get("feature", "") for c in conditions if c.get("feature")],
        )

        return self._screen_df(df, custom_template, resolved, limit)

    def list_templates(self) -> List[TemplateInfo]:
        """Return summary metadata for all 42 registered templates.

        Returns
        -------
        list of TemplateInfo
            One entry per template, sorted by name.

        Spec References
        ---------------
        SPEC-TA-005: GET /api/v1/ta/screener/templates
        """
        # [A95-R2 cutover, 2026-08-15] Listed from the registry, same source
        # screen() now resolves against — so the picker cannot offer a template
        # that screen() would then refuse, which is exactly what two
        # declarations of the same set would eventually produce.
        # list_templates() is already name-ordered.
        from systems.technical_analysis.screener.registry_templates import list_templates

        return [
            TemplateInfo(
                name=t.name,
                category=t.category,
                description=t.description,
                condition_count=len(t.conditions),
            )
            for t in list_templates()
        ]
