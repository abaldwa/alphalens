"""
tests/unit/test_staging_default_publish_mode.py

Phase: Pipeline & Monitoring Remediation, Phase 3 (A51)
Owner: Platform / Data Layer
Consumers: CI, pytest

Fitness function: the writers A25's 2026-07-09 rollout gave a working
`staged` publish path to (backfill_fundamentals_trendlyne.py,
backfill_fundamentals_nse_xbrl.py, amfi_holdings.sync_duckdb_table) must
default to `staged`, not `direct` — otherwise the N=7 rollback safety net
A25 was built for silently stops covering most of what these writers
actually produce, exactly the gap A25's own writeup flagged
(FeatureBacklog.md:353: "most ingestion sources still default to
--publish-mode direct"). This test fails loudly if a future edit reverts
any of these defaults back to `direct`.

Does NOT assert this for `scripts/load_kaggle_fundamentals.py` (never
had a staged path — A25's rollout didn't cover it) or for
`ingestion/scrapers/corporate_actions.py`'s daily_pipeline.py call site
(deliberately left on `direct` this session — that call site is exercised
by the live, currently-running scheduler process; flipping it requires a
coordinated restart, tracked as a follow-up under A51, not a silent
same-session change).
"""

import inspect

from ingestion.scrapers import amfi_holdings


class TestStagedIsDefaultPublishMode:
    def test_sync_duckdb_table_defaults_to_staged(self):
        sig = inspect.signature(amfi_holdings.sync_duckdb_table)
        assert sig.parameters["publish_mode"].default == "staged"

    def test_trendlyne_backfill_cli_defaults_to_staged(self):
        import scripts.backfill_fundamentals_trendlyne as mod

        source = inspect.getsource(mod)
        assert (
            '"--publish-mode", choices=["direct", "staged"], default="staged"' in source
        ), "backfill_fundamentals_trendlyne.py's --publish-mode default must stay 'staged' (A51)"

    def test_nse_xbrl_backfill_cli_defaults_to_staged(self):
        import scripts.backfill_fundamentals_nse_xbrl as mod

        source = inspect.getsource(mod)
        assert (
            '"--publish-mode", choices=["direct", "staged"], default="staged"' in source
        ), "backfill_fundamentals_nse_xbrl.py's --publish-mode default must stay 'staged' (A51)"
