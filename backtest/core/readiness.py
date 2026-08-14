"""
backtest/core/readiness.py

Owner: Platform / Architecture
Consumers: the one signal generator per channel (momentum, technical, ml,
fundamental), the technical alert checker, daily inference, paper trading,
and the pipeline scheduler jobs that invoke them.

The gate every signal generator must pass before it is allowed to emit
anything: "are my data prerequisites actually met for this as-of date?"

A DELAYED SIGNAL IS BETTER THAN A WRONG SIGNAL. Signals execute at the next
market open, so a generator that refuses at 18:00 and runs again at 22:00
after the backfill catches up costs nothing. A generator that runs anyway on
partial data costs a trade. The concrete failure this prevents: a Technical
strategy whose entry predicates reference 66 indicators, run against a
feature panel where 6 of them are null, silently evaluates those predicates
against NULL and emits a plausible-looking but different signal set. Nothing
in the output says "this was computed on 60 of 66 inputs" -- the ranking just
comes out wrong, and it looks exactly like a normal day.

Two real incidents shaped the checks here:

* FYERS token expiry degraded ingestion to bhavcopy-only. Row counts on
  ohlcv_adjusted looked completely normal; only the `source` column revealed
  it. So coverage-by-count is not sufficient -- `required_source` checks the
  provenance of the rows, not just their existence.
* Scheduler gaps left the newest feature panel days old while everything
  downstream happily read the stale one. So the panel check is keyed to the
  exact as_of_date, never "the latest panel available".

PARTIAL READINESS IS NOT A THING. Any missing input at all means
ready=False. There is deliberately no severity, no threshold, no "95% of the
universe is close enough" -- the moment a caller can decide to proceed on a
degraded input, it eventually will, on the day it matters.

The indicator list a technical check enforces is DERIVED FROM THE REGISTRY
(strategy_registry.entry_criterion_json), never hardcoded here. If it were
hardcoded, revising a strategy to reference a new feature would leave this
gate still green while the new feature was null -- the check would drift away
from the definition it is supposed to protect, silently, and only a hardcoded
list makes that possible.

PIT Assumptions
----------------
Every check is evaluated strictly as of `as_of_date`: fundamentals are
filtered on announcement_date <= as_of_date (SPEC-PIPE-003), and the feature
panel read is the panel FOR that date. No check looks forward.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

CHANNELS = frozenset({"momentum", "technical", "fundamental", "ml"})

# The kinds of prerequisite a generator can be missing. Kept small and closed
# so a consumer (dashboard, alert) can switch on it; `detail` carries the
# specifics.
MISSING_KINDS = frozenset(
    {"ohlcv", "feature_panel", "indicator", "fundamentals", "model_artifact"}
)

# Listing every one of ~800 tickers in a blocked row makes the row unreadable
# and the JSON large; the first few plus a count identifies the failure just
# as well.
_MAX_LISTED = 10

# Fundamentals are quarterly. A ticker whose newest announcement is older
# than this has either stopped reporting or the scraper has been broken for a
# quarter -- either way a fundamental strategy must not rank on it.
DEFAULT_FUNDAMENTAL_STALENESS_DAYS = 200

#: Fyers OHLCV coverage begins here. Earlier data is legacy, carries
#: source NULL, and includes 518 known-broken corporate-action adjustments
#: (A99-A102). The provenance check below is therefore only meaningful from
#: this date forward -- see check_ohlcv.
FYERS_COVERAGE_START = date(2017, 1, 1)

# The columns features/financial_ratios.py::derive_all_ratios is responsible
# for. They were dead code for months (nothing called the deriver) while the
# raw fields they come from were present, so "fundamentals rows exist" was
# true and "derived ratios exist" was false at the same time. Hence a
# separate check rather than folding it into the row-existence one.
DERIVED_RATIO_COLUMNS: tuple[str, ...] = (
    "ebit",
    "net_debt",
    "debt_to_ebitda",
    "fcf_margin",
    "capex_intensity",
)


class ReadinessError(ValueError):
    """The readiness check itself could not be performed (bad channel,
    unknown strategy). Distinct from ready=False, which is a successful check
    with a negative answer -- conflating the two would let a typo'd channel
    name read as 'not ready yet, try later' forever."""


@dataclass(frozen=True)
class MissingInput:
    """One unmet prerequisite.

    `expected` records what the check wanted (the panel path, the source
    name, the artifact key) so an operator reading a blocked row can act on it
    without re-running the checker to find out.
    """

    kind: str
    detail: str
    expected: Optional[str] = None

    def __post_init__(self) -> None:
        if self.kind not in MISSING_KINDS:
            raise ReadinessError(
                f"unknown missing-input kind {self.kind!r}; valid: {sorted(MISSING_KINDS)}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Readiness:
    """The verdict. `ready` is derived, not asserted: it is exactly
    `not missing`, so there is no way to construct a 'ready with missing
    inputs' result by mistake."""

    ready: bool
    channel: str
    as_of_date: date
    missing: List[MissingInput] = field(default_factory=list)
    checked_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def build(
        cls,
        *,
        channel: str,
        as_of_date: date,
        missing: Sequence[MissingInput],
    ) -> "Readiness":
        missing_list = list(missing)
        return cls(
            ready=not missing_list,
            channel=channel,
            as_of_date=as_of_date,
            missing=missing_list,
            checked_at=datetime.now(),
        )

    def missing_json(self) -> str:
        return json.dumps([m.to_dict() for m in self.missing])

    def reason(self) -> str:
        """One-line summary for a log or an alert body."""
        if self.ready:
            return f"{self.channel} ready for {self.as_of_date}"
        parts = "; ".join(f"{m.kind}: {m.detail}" for m in self.missing)
        return f"{self.channel} BLOCKED for {self.as_of_date} -- {parts}"


# ---------------------------------------------------------------------------
# registry-derived indicator extraction
# ---------------------------------------------------------------------------


def extract_predicate_features(entry_criterion: Iterable[Dict[str, Any]]) -> List[str]:
    """Pull the ordered, de-duplicated feature names out of a predicate list.

    This is the whole reason the technical check cannot drift: the same
    entry_criterion_json the engine evaluates is the list the gate demands be
    non-null. Order is preserved so the blocked-row detail reads in the same
    order as the strategy definition an operator is looking at.

    Nested predicate groups ({"any"/"all": [...]}) are walked rather than
    skipped -- a feature referenced only inside an OR branch is still a
    feature the strategy reads, and skipping it would reintroduce exactly the
    silent-NULL failure this module exists to prevent.
    """
    seen: Dict[str, None] = {}

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            feature = node.get("feature")
            if isinstance(feature, str) and feature:
                seen.setdefault(feature, None)
            for group_key in ("any", "all", "conditions", "predicates"):
                if group_key in node:
                    walk(node[group_key])
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item)

    walk(list(entry_criterion))
    return list(seen)


def strategy_indicators(
    key: str,
    *,
    version: Optional[int] = None,
    as_of: Optional[date] = None,
    registry_db_path: Optional[Path] = None,
    registry_conn: Any = None,
) -> List[str]:
    """The features a registered strategy's entry predicates reference.

    `as_of` is forwarded to get_strategy so a historical re-check reads the
    definition as it stood, not as it stands now.
    """
    from strategies.registry import get_strategy

    row = get_strategy(
        key, version=version, as_of=as_of, db_path=registry_db_path, conn=registry_conn
    )
    if row is None:
        raise ReadinessError(f"strategy {key!r} is not in the registry")
    return extract_predicate_features(row.get("entry_criterion") or [])


# ---------------------------------------------------------------------------
# the checker
# ---------------------------------------------------------------------------


class ReadinessChecker:
    """Per-channel prerequisite checks.

    Every path (market DB, registry DB, feature panel dir, model registry
    JSON) is injectable, because the tests must never touch the real DuckDB --
    project policy is that not even a temporary test row goes into it. The
    production defaults come from config.settings.
    """

    def __init__(
        self,
        *,
        market_db_path: Optional[Path] = None,
        market_conn: Any = None,
        registry_db_path: Optional[Path] = None,
        registry_conn: Any = None,
        features_daily_dir: Optional[Path] = None,
        model_registry_path: Optional[Path] = None,
        required_ohlcv_source: Optional[str] = "fyers",
        fundamental_staleness_days: int = DEFAULT_FUNDAMENTAL_STALENESS_DAYS,
    ) -> None:
        self.market_db_path = market_db_path
        self.market_conn = market_conn
        self.registry_db_path = registry_db_path
        self.registry_conn = registry_conn
        self.features_daily_dir = features_daily_dir
        self.model_registry_path = model_registry_path
        # None disables the provenance check. Left ON by default: the FYERS
        # token expiry incident produced normal-looking row counts, and the
        # only signal that anything was wrong was this column.
        self.required_ohlcv_source = required_ohlcv_source
        self.fundamental_staleness_days = fundamental_staleness_days

    # -- public API ---------------------------------------------------------

    def check(
        self,
        channel: str,
        as_of_date: date,
        *,
        universe: Sequence[str],
        strategy_key: Optional[str] = None,
        strategy_version: Optional[int] = None,
        model_name: Optional[str] = None,
        model_features: Sequence[str] = (),
    ) -> Readiness:
        """Run the checks for `channel` and return the verdict.

        Channels are cumulative: technical includes momentum's prerequisites,
        ml includes technical's, because a generator that reads a feature
        panel also reads the prices the panel was built from.
        """
        if channel not in CHANNELS:
            raise ReadinessError(f"unknown channel {channel!r}; valid: {sorted(CHANNELS)}")
        if not universe:
            raise ReadinessError("universe is empty; nothing to check readiness against")

        missing: List[MissingInput] = []

        if channel in ("momentum", "technical", "ml"):
            missing.extend(self.check_ohlcv(as_of_date, universe))

        if channel in ("technical", "ml"):
            indicators: List[str] = []
            if strategy_key:
                # NOTE the deliberate absence of as_of. Two different clocks
                # were being conflated here, and it made this gate
                # unsatisfiable for every historical backtest:
                #
                #   * registry validity (valid_from/valid_to) runs on
                #     AUTHORING time -- when a human declared the strategy;
                #   * as_of_date runs on MARKET time -- the session being
                #     simulated.
                #
                # Every registry row was authored on 2026-08-13, so a run
                # over 2009-2026 asked "was this strategy valid in 2013?",
                # got None, and raised "strategy 'technical:A2' is not in the
                # registry" -- for a row that exists and is active. Measured
                # 2026-08-14: all 126 technical jobs failed this way the
                # moment enforce_readiness was on (the parallel queue had
                # been setting it False, which is why it went unnoticed).
                #
                # `version` is what provides point-in-time correctness: the
                # run records the exact version it executed, so re-reading
                # that version reproduces the definition regardless of when
                # the lookup happens. Filtering by market date on top of a
                # pinned version adds nothing and excludes everything.
                indicators = strategy_indicators(
                    strategy_key,
                    version=strategy_version,
                    registry_db_path=self.registry_db_path,
                    registry_conn=self.registry_conn,
                )
            elif channel == "technical":
                raise ReadinessError(
                    "technical readiness needs strategy_key: the indicator list is "
                    "derived from the strategy's entry predicates, never hardcoded"
                )
            if channel == "ml":
                # The model reads its whole feature set, not just whatever
                # predicates gate it afterwards.
                indicators = list(indicators) + [
                    f for f in model_features if f not in indicators
                ]
            missing.extend(self.check_feature_panel(as_of_date, indicators))

        if channel == "ml":
            if not model_name:
                raise ReadinessError("ml readiness needs model_name to locate its artifact")
            missing.extend(self.check_model_artifact(as_of_date, model_name))

        if channel == "fundamental":
            missing.extend(self.check_fundamentals(as_of_date, universe))

        result = Readiness.build(channel=channel, as_of_date=as_of_date, missing=missing)
        if not result.ready:
            logger.warning(result.reason())
        return result

    # -- individual checks --------------------------------------------------

    def check_ohlcv(self, as_of_date: date, universe: Sequence[str]) -> List[MissingInput]:
        """ohlcv_adjusted must cover every universe ticker ON as_of_date, from
        the expected source."""
        tickers = list(dict.fromkeys(universe))
        with self._market() as conn:
            rows = conn.execute(
                "SELECT ticker, source FROM ohlcv_adjusted WHERE date = ?",
                [as_of_date],
            ).fetchall()

        by_ticker = {t: s for t, s in rows}
        absent = [t for t in tickers if t not in by_ticker]

        missing: List[MissingInput] = []
        if absent:
            missing.append(
                MissingInput(
                    kind="ohlcv",
                    detail=f"no ohlcv_adjusted row on {as_of_date} for {_summarise(absent)}",
                    expected=f"{len(tickers)} tickers on {as_of_date}",
                )
            )

        # Provenance is only checkable where the source exists. Fyers OHLCV
        # begins at FYERS_COVERAGE_START; every row before it is legacy with
        # source NULL, by definition and permanently. Applying the check to
        # those dates would block every backtest date from the 2009 start to
        # 2017 -- most of the track record -- for a condition no ingestion run
        # can ever satisfy. Requiring callers to remember to pass None instead
        # would make the common case the one that has to opt out.
        if self.required_ohlcv_source is not None and as_of_date >= FYERS_COVERAGE_START:
            wrong = [
                t
                for t in tickers
                if t in by_ticker and by_ticker[t] != self.required_ohlcv_source
            ]
            if wrong:
                # This is the FYERS-token-expiry shape: the rows are there,
                # the counts are right, and the data silently came from the
                # bhavcopy fallback instead.
                missing.append(
                    MissingInput(
                        kind="ohlcv",
                        detail=(
                            f"ohlcv_adjusted on {as_of_date} not sourced from "
                            f"{self.required_ohlcv_source!r} for {_summarise(wrong)}"
                        ),
                        expected=f"source = {self.required_ohlcv_source!r}",
                    )
                )
        return missing

    def check_feature_panel(
        self, as_of_date: date, indicators: Sequence[str]
    ) -> List[MissingInput]:
        """The panel parquet for exactly this date must exist, and every
        indicator the strategy references must be present and non-null in it.

        Keyed to the exact date on purpose: a scheduler gap leaves an older
        panel sitting in the directory, and "read the latest panel" would
        happily generate today's signals from last Tuesday's features.
        """
        panel_path = self._panel_path(as_of_date)
        if not panel_path.exists():
            return [
                MissingInput(
                    kind="feature_panel",
                    detail=f"no feature panel for {as_of_date}",
                    expected=str(panel_path),
                )
            ]
        if not indicators:
            return []

        import pandas as pd

        panel: "pd.DataFrame" = pd.read_parquet(panel_path)

        absent = [c for c in indicators if c not in panel.columns]
        missing: List[MissingInput] = []
        if absent:
            missing.append(
                MissingInput(
                    kind="indicator",
                    detail=(
                        f"feature panel for {as_of_date} has no column for "
                        f"{_summarise(absent)}"
                    ),
                    expected=str(panel_path),
                )
            )

        # A column that exists but is entirely null is the 60-of-66 case: the
        # panel loads, the predicate evaluates against NULL, and the strategy
        # emits a different, wrong set of names with no error anywhere.
        all_null = [
            c for c in indicators if c in panel.columns and panel[c].isna().all()
        ]
        if all_null:
            missing.append(
                MissingInput(
                    kind="indicator",
                    detail=(
                        f"feature panel for {as_of_date} is entirely null for "
                        f"{_summarise(all_null)}"
                    ),
                    expected=str(panel_path),
                )
            )
        return missing

    def check_model_artifact(self, as_of_date: date, model_name: str) -> List[MissingInput]:
        """datastore/models/registry.json must carry a current artifact for
        the model, and the file it points at must actually be on disk."""
        registry_path = self._model_registry_path()
        if not registry_path.exists():
            return [
                MissingInput(
                    kind="model_artifact",
                    detail="model registry not found",
                    expected=str(registry_path),
                )
            ]

        try:
            registry = json.loads(registry_path.read_text())
        except json.JSONDecodeError as exc:
            return [
                MissingInput(
                    kind="model_artifact",
                    detail=f"model registry is not valid JSON: {exc}",
                    expected=str(registry_path),
                )
            ]

        entry = registry.get(model_name)
        if not isinstance(entry, dict):
            return [
                MissingInput(
                    kind="model_artifact",
                    detail=f"no registry entry for model {model_name!r}",
                    expected=str(registry_path),
                )
            ]

        missing: List[MissingInput] = []
        saved_path = entry.get("saved_path")
        if not saved_path or not Path(saved_path).exists():
            missing.append(
                MissingInput(
                    kind="model_artifact",
                    detail=f"model {model_name!r} artifact file is absent",
                    expected=str(saved_path) if saved_path else "saved_path",
                )
            )

        # Each entry declares its own retrain cadence. A model past it is
        # running on a world that has moved; that is a refusal, not a warning,
        # because inference will produce confident numbers either way.
        trained_raw = entry.get("last_trained_date")
        interval = entry.get("training_interval_days")
        if trained_raw and interval:
            trained = _as_date(trained_raw)
            if trained is not None:
                age = (as_of_date - trained).days
                if age > int(interval):
                    missing.append(
                        MissingInput(
                            kind="model_artifact",
                            detail=(
                                f"model {model_name!r} last trained {trained} "
                                f"({age}d before {as_of_date})"
                            ),
                            expected=f"retrained within {interval}d",
                        )
                    )
        return missing

    def check_fundamentals(
        self, as_of_date: date, universe: Sequence[str]
    ) -> List[MissingInput]:
        """Every universe ticker needs a point-in-time fundamentals row that
        is both known by as_of_date and recent enough, with its derived ratios
        populated.

        announcement_date is the PIT key (SPEC-PIPE-003) -- filtering on
        quarter_end_date would let a quarter that had not been published yet
        into the ranking.
        """
        tickers = list(dict.fromkeys(universe))
        ratio_nulls = " OR ".join(f"{c} IS NULL" for c in DERIVED_RATIO_COLUMNS)
        sql = f"""
            WITH latest AS (
                SELECT *, row_number() OVER (
                    PARTITION BY ticker ORDER BY announcement_date DESC
                ) AS rn
                FROM fundamentals
                WHERE CAST(announcement_date AS DATE) <= ?
            )
            SELECT ticker,
                   CAST(announcement_date AS DATE) AS announced,
                   ({ratio_nulls}) AS ratios_incomplete
            FROM latest WHERE rn = 1
        """
        with self._market() as conn:
            rows = conn.execute(sql, [as_of_date]).fetchall()

        latest = {t: (announced, bool(incomplete)) for t, announced, incomplete in rows}

        absent = [t for t in tickers if t not in latest]
        stale = [
            t
            for t in tickers
            if t in latest
            and (as_of_date - latest[t][0]).days > self.fundamental_staleness_days
        ]
        no_ratios = [t for t in tickers if t in latest and latest[t][1]]

        missing: List[MissingInput] = []
        if absent:
            missing.append(
                MissingInput(
                    kind="fundamentals",
                    detail=(
                        f"no fundamentals announced on or before {as_of_date} for "
                        f"{_summarise(absent)}"
                    ),
                    expected=f"{len(tickers)} tickers with PIT fundamentals",
                )
            )
        if stale:
            missing.append(
                MissingInput(
                    kind="fundamentals",
                    detail=(
                        f"newest fundamentals older than "
                        f"{self.fundamental_staleness_days}d for {_summarise(stale)}"
                    ),
                    expected=f"announcement within {self.fundamental_staleness_days}d",
                )
            )
        if no_ratios:
            missing.append(
                MissingInput(
                    kind="fundamentals",
                    detail=f"derived ratios missing for {_summarise(no_ratios)}",
                    expected=", ".join(DERIVED_RATIO_COLUMNS),
                )
            )
        return missing

    # -- plumbing -----------------------------------------------------------

    def _market(self) -> Any:
        if self.market_conn is not None:
            return _Borrowed(self.market_conn)
        db_path = self.market_db_path
        if db_path is None:
            from config.settings import DUCKDB_PATH

            db_path = DUCKDB_PATH
        # read_only=True is explicit and load-bearing: a readiness check runs
        # right before a generator and must never be the process that takes
        # the single write lock away from the ingestion job it is waiting on.
        return get_duckdb_connection(db_path, persist=False, read_only=True)

    def _panel_path(self, as_of_date: date) -> Path:
        base = self.features_daily_dir
        if base is None:
            from config.settings import FEATURES_DAILY_DIR

            base = FEATURES_DAILY_DIR
        return Path(base) / f"{as_of_date.isoformat()}.parquet"

    def _model_registry_path(self) -> Path:
        if self.model_registry_path is not None:
            return Path(self.model_registry_path)
        from config.settings import MODEL_REGISTRY_PATH

        return MODEL_REGISTRY_PATH


class _Borrowed:
    """Context manager over a connection we do not own. Closing a borrowed
    connection would break the caller's transaction (and, in tests, discard
    the in-memory database entirely)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __enter__(self) -> Any:
        return self._conn

    def __exit__(self, *exc: Any) -> bool:
        return False


# ---------------------------------------------------------------------------
# recording a refusal
# ---------------------------------------------------------------------------


def record_blocked(
    readiness: Readiness,
    *,
    strategy_key: str,
    strategy_version: int,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> int:
    """Persist a refusal to signal_generation_blocked. Returns rows written.

    Called only for ready=False; recording a pass would write one row per
    strategy per day forever for no diagnostic value. A refusal, by contrast,
    MUST be recorded: an operator seeing no signals today needs to be able to
    tell "the strategy found nothing" from "the strategy was never allowed to
    look", and without this row those two are indistinguishable.
    """
    if readiness.ready:
        return 0

    row = (
        strategy_key,
        int(strategy_version),
        readiness.channel,
        readiness.as_of_date,
        readiness.missing_json(),
        readiness.checked_at,
    )

    with _blocked_conn(db_path, conn) as c:
        # Re-running a blocked generator on the same day must overwrite rather
        # than collide: the scheduler retries several times an evening while
        # the backfill catches up, and each retry's missing list is the
        # interesting one.
        c.execute(
            "DELETE FROM signal_generation_blocked WHERE strategy_key = ? "
            "AND strategy_version = ? AND as_of_date = ?",
            [strategy_key, int(strategy_version), readiness.as_of_date],
        )
        c.execute(
            "INSERT INTO signal_generation_blocked "
            "(strategy_key, strategy_version, channel, as_of_date, missing_json, checked_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            list(row),
        )
    return 1


def list_blocked(
    *,
    as_of_date: Optional[date] = None,
    channel: Optional[str] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> List[Dict[str, Any]]:
    """Read back refusals, missing list already JSON-decoded."""
    sql = "SELECT * FROM signal_generation_blocked WHERE 1=1"
    params: List[Any] = []
    if as_of_date is not None:
        sql += " AND as_of_date = ?"
        params.append(as_of_date)
    if channel is not None:
        sql += " AND channel = ?"
        params.append(channel)
    sql += " ORDER BY as_of_date DESC, strategy_key"

    with _blocked_conn(db_path, conn) as c:
        df = c.execute(sql, params).fetchdf()

    out: List[Dict[str, Any]] = []
    for record in df.to_dict("records"):
        record["missing"] = json.loads(record.pop("missing_json") or "[]")
        out.append(record)
    return out


def _blocked_conn(db_path: Optional[Path], conn: Any) -> Any:
    if conn is not None:
        return _Borrowed(conn)
    if db_path is None:
        from config.settings import BACKTEST_DUCKDB_PATH

        db_path = BACKTEST_DUCKDB_PATH
    # read_only=False stated explicitly: this is a write path, and the
    # tests/quality gate exists so the intent is visible at the call site
    # rather than inherited from a default.
    return get_duckdb_connection(db_path, persist=False, read_only=False)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _summarise(items: Sequence[str]) -> str:
    """"A, B, C (+797 more)" -- readable in a log line and in a stored row."""
    head = list(items[:_MAX_LISTED])
    extra = len(items) - len(head)
    text = ", ".join(head)
    return f"{text} (+{extra} more)" if extra > 0 else text


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None
