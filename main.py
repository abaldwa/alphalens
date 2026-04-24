"""
main.py — AlphaLens application entry point.

Usage:
    python main.py --init          # First-time setup: create DBs, seed universe
    python main.py --backfill      # Fetch 15yr historical data + calculate indicators
    python main.py --dashboard     # Launch Dash dashboard only
    python main.py --scheduler     # Start APScheduler background jobs only
    python main.py                 # Start everything (dashboard + scheduler)
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger


def setup_logging():
    from config.settings import settings
    Path(settings.logs_dir).mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(
        sys.stdout,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>"
    )
    logger.add(
        f"{settings.logs_dir}/alphalens_{{time:YYYY-MM-DD}}.log",
        level="DEBUG",
        rotation="1 day",
        retention="30 days",
        compression="gz",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )


def cmd_init():
    """First-time setup: initialise databases and seed stock universe."""
    logger.info("=== AlphaLens: First-Time Initialisation ===")
    from alphalens.core.database import init_databases
    from alphalens.core.ingestion.universe import seed_universe_to_db
    from alphalens.core.strategy.library import seed_strategy_library

    init_databases()
    logger.info("Databases initialised")

    count = seed_universe_to_db()
    logger.info(f"Universe seeded: {count} stocks")

    seed_strategy_library()
    logger.info("Strategy library seeded: 12 base strategies")

    logger.info("=== Initialisation complete ===")
    logger.info("Next step: run  python main.py --backfill  to fetch historical data")


def cmd_backfill():
    """Fetch 15yr historical data and calculate all indicators."""
    logger.info("=== AlphaLens: Historical Backfill ===")
    from alphalens.core.ingestion.historical import HistoricalLoader
    from alphalens.core.indicators.calculator import IndicatorCalculator

    # Step 1: Fetch OHLCV
    loader = HistoricalLoader()
    stats  = loader.backfill_all(period="15y")
    logger.info(f"Data loaded: {stats['total_stocks']} stocks, {stats['total_bars']:,} bars")

    # Step 2: Calculate all indicators
    logger.info("Calculating technical indicators (this takes ~10-15 min on SATA HDD)...")
    calc   = IndicatorCalculator()
    result = calc.calculate_all()
    logger.info(f"Indicators: {result['ok']} ok, {len(result['failed'])} failed")

    logger.info("=== Backfill complete ===")
    logger.info("Next step: run  python main.py --train-cycles  to train ML cycle models")


def cmd_dashboard():
    """Launch Dash dashboard."""
    logger.info("Starting AlphaLens Dashboard on http://localhost:8050")
    from alphalens.dashboard.app import create_app
    app = create_app()
    from config.settings import settings
    app.run(
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        debug=not settings.is_production
    )


def cmd_scheduler():
    """Start APScheduler background job runner."""
    logger.info("Starting AlphaLens Scheduler")
    from alphalens.scheduler.jobs import create_scheduler
    scheduler = create_scheduler()
    scheduler.start()
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        import time
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped")


def cmd_all():
    """Start dashboard + scheduler together."""
    import threading
    from alphalens.scheduler.jobs import create_scheduler
    from alphalens.dashboard.app import create_app
    from config.settings import settings

    # Start scheduler in background thread
    scheduler = create_scheduler()
    scheduler.start()
    sched_thread = threading.Thread(target=lambda: None, daemon=True)
    sched_thread.start()
    logger.info("Scheduler started in background")

    # Start dashboard (blocking)
    logger.info(f"Starting Dashboard on http://{settings.dashboard_host}:{settings.dashboard_port}")
    app = create_app()
    app.run(
        host  = settings.dashboard_host,
        port  = settings.dashboard_port,
        debug = not settings.is_production
    )


def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="AlphaLens — Nifty200 ML Trading Intelligence Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  --init          First-time setup (databases + universe + strategy library)
  --backfill      Fetch 15yr historical data + calculate indicators
  --dashboard     Launch dashboard only (http://localhost:8050)
  --scheduler     Start background scheduler only
  (no args)       Start dashboard + scheduler together
        """
    )
    parser.add_argument("--init",        action="store_true", help="First-time initialisation")
    parser.add_argument("--backfill",    action="store_true", help="Historical data backfill")
    parser.add_argument("--dashboard",   action="store_true", help="Launch dashboard only")
    parser.add_argument("--scheduler",   action="store_true", help="Start scheduler only")

    args = parser.parse_args()

    if args.init:
        cmd_init()
    elif args.backfill:
        cmd_backfill()
    elif args.dashboard:
        cmd_dashboard()
    elif args.scheduler:
        cmd_scheduler()
    else:
        cmd_all()


if __name__ == "__main__":
    main()
