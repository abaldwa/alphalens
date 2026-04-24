"""
alphalens/scheduler/jobs.py

APScheduler job definitions for all automated runs.

Schedule:
  6:30 PM IST (13:00 UTC) Mon-Fri  — EOD: prices + indicators + cycles + signals + reports
  9:30 AM IST (04:00 UTC) Mon-Fri  — Morning: gap analysis + intraday signals + Telegram
  3:00 PM IST (09:30 UTC) Mon-Fri  — Pre-close: intraday exit check + SL update
  Monday 6:30 PM          —         Fundamental refresh (Screener.in)
  1st of month 6:30 PM    —         Monthly investment review
  Last Sunday 01:00 AM    —         Monthly strategy discovery (genetic algorithm)

Usage:
    from alphalens.scheduler.jobs import create_scheduler
    scheduler = create_scheduler()
    scheduler.start()
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger


def create_scheduler() -> BackgroundScheduler:
    """Create and configure the APScheduler with all AlphaLens jobs."""
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

    # ── 6:30 PM IST — EOD Master Run ────────────────────────────────────
    scheduler.add_job(
        run_eod_master,
        CronTrigger(day_of_week="mon-fri", hour=18, minute=30, timezone="Asia/Kolkata"),
        id="eod_master",
        name="EOD Master Run",
        replace_existing=True,
        misfire_grace_time=1800,    # 30 min grace period
    )

    # ── 9:30 AM IST — Morning Pre-Market Analysis ─────────────────────
    scheduler.add_job(
        run_morning_review,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=30, timezone="Asia/Kolkata"),
        id="morning_review",
        name="Morning Pre-Market Review",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # ── 3:00 PM IST — Pre-Close Review ───────────────────────────────
    scheduler.add_job(
        run_preclose_review,
        CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone="Asia/Kolkata"),
        id="preclose_review",
        name="Pre-Close Intraday Review",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # ── Every Monday 6:30 PM IST — Fundamental Refresh ───────────────
    scheduler.add_job(
        run_fundamental_refresh,
        CronTrigger(day_of_week="mon", hour=18, minute=30, timezone="Asia/Kolkata"),
        id="fundamental_refresh",
        name="Weekly Fundamental Refresh",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # ── 1st of every month 6:30 PM IST — Investment Review ───────────
    scheduler.add_job(
        run_monthly_investment_review,
        CronTrigger(day=1, hour=18, minute=30, timezone="Asia/Kolkata"),
        id="monthly_investment_review",
        name="Monthly Investment Portfolio Review",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # ── Last Sunday of month 01:00 AM IST — Strategy Discovery ───────
    scheduler.add_job(
        run_strategy_discovery,
        CronTrigger(day_of_week="sun", hour=1, minute=0, timezone="Asia/Kolkata"),
        id="strategy_discovery",
        name="Monthly Strategy Discovery (Genetic Algorithm)",
        replace_existing=True,
        misfire_grace_time=7200,
    )

    # ── Continuous: Drawdown Monitor (every 30 min during market hours)
    scheduler.add_job(
        run_drawdown_monitor,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/30",
            timezone="Asia/Kolkata"
        ),
        id="drawdown_monitor",
        name="Intraday Drawdown Monitor",
        replace_existing=True,
    )

    logger.info(f"Scheduler configured with {len(scheduler.get_jobs())} jobs")
    return scheduler


# ── Job Functions ─────────────────────────────────────────────────────────

def run_eod_master():
    """
    6:30 PM IST — Master EOD pipeline.

    Steps (in order):
      1. Fetch today's OHLCV (incremental update)
      2. Fetch latest market context (VIX, DXY, Crude, Nasdaq, Dow)
      3. Calculate technical indicators (last 30 days, all stocks)
      4. Classify market cycles (market → sectors → stocks)
      5. Generate signals for all timeframes
      6. Update portfolio targets/stop-losses
      7. Generate tomorrow's watchlist
      8. Compute P&L snapshots
      9. Send EOD Telegram + Email reports
    """
    logger.info("=" * 60)
    logger.info("EOD MASTER RUN STARTING")
    logger.info("=" * 60)

    try:
        # Step 1: Prices
        logger.info("Step 1/9: Fetching EOD prices...")
        from alphalens.core.ingestion.historical import HistoricalLoader
        loader = HistoricalLoader()
        loader.update_incremental(days_back=5)

        # Step 2: Indicators
        logger.info("Step 2/9: Calculating indicators...")
        from alphalens.core.indicators.calculator import IndicatorCalculator
        calc = IndicatorCalculator()
        calc.calculate_incremental(days=30)

        # Step 3: Compute RS vs Nifty200 for all stocks (relative strength)
        _update_rs_nifty200()

        # Step 4: Cycle classification
        logger.info("Step 3/9: Classifying market cycles...")
        from alphalens.core.cycle.classifier import CycleClassifier
        from alphalens.core.cycle.context import update_cycle_context
        clf     = CycleClassifier()
        results = clf.classify_all_and_store()
        update_cycle_context(results)

        # Step 5: Signal generation (all timeframes)
        logger.info("Step 4/9: Generating signals...")
        from alphalens.core.signals.generator import SignalGenerator
        gen = SignalGenerator()
        gen.generate_all()

        # Step 6: Update portfolio targets/SLs
        logger.info("Step 5/9: Updating portfolio targets/SLs...")
        from alphalens.core.portfolio.reviewer import PortfolioReviewer
        reviewer = PortfolioReviewer()
        reviewer.run_eod_review()

        # Step 7: P&L snapshot
        logger.info("Step 6/9: Computing P&L snapshots...")
        from alphalens.core.portfolio.pnl import PnlTracker
        pnl = PnlTracker()
        pnl.take_snapshot()

        # Step 8: Send notifications
        logger.info("Step 7/9: Sending EOD notifications...")
        _send_eod_notifications()

        logger.info("EOD MASTER RUN COMPLETE")

    except Exception as e:
        logger.exception(f"EOD Master Run failed: {e}")
        _send_error_alert("EOD Master Run", str(e))


def run_morning_review():
    """
    9:30 AM IST — Morning gap analysis and intraday signal generation.

    Steps:
      1. Gap-up / gap-down analysis for all portfolio stocks
      2. Generate intraday signals for today
      3. Update intraday watchlist
      4. Send morning Telegram alert with top 3 intraday setups
    """
    logger.info("MORNING REVIEW STARTING (9:30 AM)")
    try:
        from alphalens.core.portfolio.reviewer import PortfolioReviewer
        from alphalens.core.signals.generator import SignalGenerator

        reviewer = PortfolioReviewer()
        gap_data = reviewer.run_gap_analysis()

        gen = SignalGenerator()
        gen.generate_timeframe("intraday")

        _send_morning_telegram(gap_data)

        logger.info("Morning review complete")
    except Exception as e:
        logger.exception(f"Morning review failed: {e}")


def run_preclose_review():
    """
    3:00 PM IST — Pre-close review for intraday positions.

    Steps:
      1. Check all open intraday portfolio stocks
      2. If target not hit: update SL to breakeven
      3. If SL breached: fire exit alert
      4. Send 3:00 PM Telegram update
    """
    logger.info("PRE-CLOSE REVIEW STARTING (3:00 PM)")
    try:
        from alphalens.core.portfolio.reviewer import PortfolioReviewer
        reviewer = PortfolioReviewer()
        alerts   = reviewer.run_preclose_intraday_check()

        if alerts:
            _send_preclose_telegram(alerts)

        logger.info(f"Pre-close review complete: {len(alerts)} alerts")
    except Exception as e:
        logger.exception(f"Pre-close review failed: {e}")


def run_fundamental_refresh():
    """Monday 6:30 PM — Scrape Screener.in for all 200 stocks."""
    logger.info("FUNDAMENTAL REFRESH STARTING")
    try:
        from alphalens.core.ingestion.fundamental import FundamentalScraper
        scraper = FundamentalScraper()
        stats   = scraper.scrape_all()
        logger.info(f"Fundamental refresh complete: {stats}")
    except Exception as e:
        logger.exception(f"Fundamental refresh failed: {e}")


def run_monthly_investment_review():
    """
    1st of month — Full review of long-term investment portfolio.

    For each long-term holding:
      1. Fetch latest fundamentals and price
      2. Run ML recommendation: Hold / Average Down / Exit
      3. Re-evaluate target and stop-loss
      4. Send detailed email report
    """
    logger.info("MONTHLY INVESTMENT REVIEW STARTING")
    try:
        from alphalens.core.portfolio.reviewer import PortfolioReviewer
        reviewer = PortfolioReviewer()
        report   = reviewer.run_monthly_investment_review()
        _send_monthly_email_report(report)
        logger.info(f"Monthly investment review complete: {len(report.get('holdings', []))} holdings reviewed")
    except Exception as e:
        logger.exception(f"Monthly investment review failed: {e}")


def run_strategy_discovery():
    """
    Last Sunday 01:00 AM — Run genetic algorithm strategy discovery.
    Runs DEAP evolution over strategy parameter space.
    Stores new strategies meeting quality gates in strategies table.
    """
    logger.info("STRATEGY DISCOVERY STARTING (monthly genetic algorithm)")
    try:
        from alphalens.core.strategy.discovery import StrategyDiscovery
        discovery = StrategyDiscovery()
        stats     = discovery.run_monthly_discovery()
        logger.info(f"Strategy discovery complete: {stats}")
        _send_discovery_email(stats)
    except Exception as e:
        logger.exception(f"Strategy discovery failed: {e}")


def run_drawdown_monitor():
    """
    Every 30 min during market hours — Check long-term holdings for 10% drawdown.
    If any holding falls 10% below avg cost → trigger alert + ML recommendation.
    """
    try:
        from alphalens.core.portfolio.reviewer import PortfolioReviewer
        reviewer = PortfolioReviewer()
        alerts   = reviewer.check_drawdown_alerts()

        if alerts:
            logger.info(f"Drawdown alerts: {len(alerts)} triggered")
            for alert in alerts:
                _send_drawdown_alert(alert)
    except Exception as e:
        logger.debug(f"Drawdown monitor error: {e}")


# ── Helper notification dispatchers ──────────────────────────────────────

def _update_rs_nifty200():
    """Compute relative strength vs Nifty200 for all stocks and update indicators table."""
    try:
        from alphalens.core.database import get_duck
        con = get_duck()
        con.execute("""
            UPDATE technical_indicators ti
            SET rs_nifty200 = (
                SELECT
                    (dp.close / LAG(dp.close, 20) OVER (PARTITION BY dp.symbol ORDER BY dp.date) - 1)
                    /
                    NULLIF((mc.nifty200_close / LAG(mc.nifty200_close, 20) OVER (ORDER BY mc.date) - 1), 0)
                FROM daily_prices dp
                JOIN market_context mc ON dp.date = mc.date
                WHERE dp.date = ti.date AND dp.symbol = ti.symbol
                LIMIT 1
            )
            WHERE ti.date >= (SELECT MAX(date) - INTERVAL '30 days' FROM technical_indicators)
        """)
        logger.debug("RS vs Nifty200 updated")
    except Exception as e:
        logger.debug(f"RS update failed: {e}")


def _send_eod_notifications():
    """Dispatch EOD Telegram + Email alerts."""
    try:
        from alphalens.core.notifications.telegram import TelegramNotifier
        from alphalens.core.notifications.email import EmailNotifier
        from alphalens.core.cycle.context import get_cycle_context
        from alphalens.core.database import get_sqlite, SignalLog, Watchlist

        ctx = get_cycle_context()

        # Get today's new signals
        with get_sqlite() as session:
            from datetime import date
            today_signals = session.query(SignalLog).filter(
                SignalLog.generated_at >= str(date.today()),
                SignalLog.signal_type.in_(["buy", "sell"]),
                SignalLog.is_active == True
            ).all()

        tg = TelegramNotifier()
        tg.send_eod_report(ctx, today_signals)

        em = EmailNotifier()
        em.send_eod_report(ctx, today_signals)

    except Exception as e:
        logger.warning(f"EOD notification failed: {e}")


def _send_morning_telegram(gap_data: dict):
    try:
        from alphalens.core.notifications.telegram import TelegramNotifier
        tg = TelegramNotifier()
        tg.send_morning_alert(gap_data)
    except Exception as e:
        logger.warning(f"Morning Telegram failed: {e}")


def _send_preclose_telegram(alerts: list):
    try:
        from alphalens.core.notifications.telegram import TelegramNotifier
        tg = TelegramNotifier()
        tg.send_preclose_alert(alerts)
    except Exception as e:
        logger.warning(f"Pre-close Telegram failed: {e}")


def _send_drawdown_alert(alert: dict):
    try:
        from alphalens.core.notifications.telegram import TelegramNotifier
        from alphalens.core.notifications.email import EmailNotifier
        tg = TelegramNotifier()
        tg.send_drawdown_alert(alert)
        em = EmailNotifier()
        em.send_drawdown_alert(alert)
    except Exception as e:
        logger.warning(f"Drawdown alert failed: {e}")


def _send_monthly_email_report(report: dict):
    try:
        from alphalens.core.notifications.email import EmailNotifier
        em = EmailNotifier()
        em.send_monthly_investment_report(report)
    except Exception as e:
        logger.warning(f"Monthly email failed: {e}")


def _send_discovery_email(stats: dict):
    try:
        from alphalens.core.notifications.email import EmailNotifier
        em = EmailNotifier()
        em.send_strategy_discovery_report(stats)
    except Exception as e:
        logger.warning(f"Discovery email failed: {e}")


def _send_error_alert(job_name: str, error: str):
    try:
        from alphalens.core.notifications.telegram import TelegramNotifier
        tg = TelegramNotifier()
        tg.send_error_alert(job_name, error)
    except Exception:
        pass
