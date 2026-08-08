"""
tests/unit/test_momentum_dynamic_report.py

Per-band scoring correctness for the Dynamic Report's recommendation logic:
_score_cohort sets exactly one is_recommended per (band, category) cohort,
and _score_band sets exactly one is_band_most_important per band — the
single best variant across ALL configs in that band (not limited to
category winners).  The global is_most_important is intentionally never set
(2026-08-08: removed per user request for one strategy per band, no global
overall portfolio CAGR).
"""


from scripts.run_momentum_dynamic_report import _score_band, _score_cohort


def _make_variant(band_id: int, strategy: str, sharpe: float, sortino: float,
                  post_tax_cagr: float, max_dd: float) -> dict:
    return {
        "band_id": band_id,
        "strategy": strategy,
        "sharpe": sharpe,
        "sortino": sortino,
        "post_tax_cagr": post_tax_cagr,
        "max_drawdown": max_dd,
        "cagr": post_tax_cagr + 0.05,
        "is_recommended": False,
        "is_band_most_important": False,
        "is_most_important": None,
        "score": 0.0,
        "top_cagr_rank": None,
    }


class TestScoreCohort:
    """_score_cohort: exactly one is_recommended per (band, category) cohort."""

    def test_single_winner_in_cohort(self):
        variants = [
            _make_variant(1, "all_risk", 1.0, 2.0, 0.20, -0.30),
            _make_variant(1, "all_risk", 1.5, 2.5, 0.25, -0.25),
            _make_variant(1, "all_risk", 0.5, 1.5, 0.15, -0.35),
        ]
        _score_cohort(variants)
        recommended = [v for v in variants if v["is_recommended"]]
        assert len(recommended) == 1
        # The variant with best Sharpe + Sortino + CAGR + lowest abs MDD wins.
        assert recommended[0]["strategy"] == "all_risk"

    def test_score_is_set_for_all(self):
        variants = [
            _make_variant(1, "balanced", 1.0, 2.0, 0.20, -0.30),
            _make_variant(1, "balanced", 1.5, 2.5, 0.25, -0.25),
        ]
        _score_cohort(variants)
        assert all(v["score"] is not None for v in variants)


class TestScoreBand:
    """_score_band: exactly one is_band_most_important per band, chosen by
    band-wide z-scored score across ALL configs (not just category winners)."""

    def _band_variants(self):
        """Two categories (all_risk, balanced) with two variants each in band 1.
        Balanced #2 (sharpe=3.0, sortino=4.0, cagr=0.40, MDD=-0.15) is
        clearly the best across all four by any reasonable metric, even though
        it wasn't necessarily the category-z-scored winner (we don't care about
        the category ranking here, only band-wide)."""
        return [
            _make_variant(1, "all_risk",  1.0, 2.0, 0.20, -0.30),
            _make_variant(1, "all_risk",  1.5, 2.5, 0.25, -0.25),
            _make_variant(1, "balanced",  2.0, 3.0, 0.30, -0.20),
            _make_variant(1, "balanced",  3.0, 4.0, 0.40, -0.15),
        ]

    def test_single_most_important_per_band(self):
        band_vars = self._band_variants()
        _score_band(band_vars)
        mi = [v for v in band_vars if v["is_band_most_important"]]
        assert len(mi) == 1

    def test_most_important_is_the_dominant_variant(self):
        band_vars = self._band_variants()
        _score_band(band_vars)
        mi = [v for v in band_vars if v["is_band_most_important"]][0]
        assert mi["strategy"] == "balanced"
        # Confirm its score is the highest in the band.
        assert mi["score"] == max(v["score"] for v in band_vars)

    def test_is_most_important_always_none(self):
        """The global flag is intentionally never set — one strategy per band."""
        band_vars = self._band_variants()
        _score_band(band_vars)
        assert all(v["is_most_important"] is None for v in band_vars)

    def test_two_separate_bands(self):
        """Two bands, each with its own single is_band_most_important."""
        band1 = [
            _make_variant(1, "all_risk", 1.0, 2.0, 0.20, -0.30),
            _make_variant(1, "balanced", 2.0, 3.0, 0.35, -0.20),
        ]
        band2 = [
            _make_variant(7, "all_risk", 1.0, 1.5, 0.15, -0.35),
            _make_variant(7, "balanced", 1.5, 2.5, 0.30, -0.25),
        ]
        _score_band(band1)
        _score_band(band2)
        assert sum(v["is_band_most_important"] for v in band1) == 1
        assert sum(v["is_band_most_important"] for v in band2) == 1

    def test_cohort_and_band_flags_coexist(self):
        """is_recommended (per-category) and is_band_most_important (per-band)
        are independent flags; both can be True on the same variant, or not."""
        band_vars = self._band_variants()
        # Simulate _score_cohort having run first for both categories.
        _score_cohort([v for v in band_vars if v["strategy"] == "all_risk"])
        _score_cohort([v for v in band_vars if v["strategy"] == "balanced"])
        _score_band(band_vars)
        # Exactly two is_recommended (one per category) and one is_band_most_important.
        assert sum(v["is_recommended"] for v in band_vars) == 2
        assert sum(v["is_band_most_important"] for v in band_vars) == 1
