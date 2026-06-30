#!/usr/bin/env python3
"""
Paper Trading Metrics Tracker for AlphaLens Framework
SPEC-OBS-004: Paper Trading Performance Tracking
"""

import csv
from pathlib import Path
from typing import Dict, List, Optional

from config.timezone import now_ist


class PaperTradingTracker:
    """Tracks paper trading executions and computes metrics."""

    def __init__(self, logs_dir: str = "./paper_trading/executions"):
        """
        Initialize paper trading tracker.

        Args:
            logs_dir: Directory for paper trading logs
        """
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def log_trade(
        self,
        date: str,
        ticker: str,
        signal_type: str,
        entry_price: float,
        quantity: int,
        entry_time: str,
        exit_price: Optional[float] = None,
        exit_time: Optional[str] = None,
        pnl: Optional[float] = None,
        pnl_pct: Optional[float] = None,
    ) -> None:
        """
        Log a paper trading execution.

        Args:
            date: Trade date (YYYY-MM-DD)
            ticker: Stock ticker
            signal_type: Signal type (BUY, SELL, HOLD)
            entry_price: Entry price
            quantity: Quantity
            entry_time: Entry time (HH:MM:SS)
            exit_price: Exit price (optional)
            exit_time: Exit time (optional)
            pnl: P&L in rupees (optional)
            pnl_pct: P&L percentage (optional)
        """
        # Get or create log file for the date
        log_file = self.logs_dir / f"{date}.csv"

        # Check if file exists to determine if we need to write header
        file_exists = log_file.exists()

        with open(log_file, "a", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "date",
                    "ticker",
                    "signal_type",
                    "entry_price",
                    "quantity",
                    "entry_time",
                    "exit_price",
                    "exit_time",
                    "pnl",
                    "pnl_pct",
                ],
            )

            if not file_exists:
                writer.writeheader()

            writer.writerow({
                "date": date,
                "ticker": ticker,
                "signal_type": signal_type,
                "entry_price": entry_price,
                "quantity": quantity,
                "entry_time": entry_time,
                "exit_price": exit_price or "",
                "exit_time": exit_time or "",
                "pnl": pnl or "",
                "pnl_pct": pnl_pct or "",
            })

    def get_trades_for_date(self, date: str) -> List[Dict]:
        """Get all trades for a specific date."""
        log_file = self.logs_dir / f"{date}.csv"

        if not log_file.exists():
            return []

        trades = []
        with open(log_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trades.append(row)

        return trades

    def compute_metrics(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Compute paper trading metrics.

        Args:
            start_date: Start date filter (YYYY-MM-DD, optional)
            end_date: End date filter (YYYY-MM-DD, optional)

        Returns:
            Dictionary of computed metrics
        """
        all_trades = []

        # Collect all trades
        for log_file in sorted(self.logs_dir.glob("*.csv")):
            with open(log_file, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Filter by date range if specified
                    if start_date and row["date"] < start_date:
                        continue
                    if end_date and row["date"] > end_date:
                        continue

                    all_trades.append(row)

        if not all_trades:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "total_pnl": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
            }

        # Compute metrics
        closed_trades = [t for t in all_trades if t.get("pnl") and t.get("pnl_pct")]

        if not closed_trades:
            return {
                "total_trades": len(all_trades),
                "signals_generated": len(all_trades),
                "closed_trades": 0,
                "note": "No closed trades yet",
            }

        wins = [float(t["pnl"]) for t in closed_trades if float(t["pnl"]) > 0]
        losses = [float(t["pnl"]) for t in closed_trades if float(t["pnl"]) < 0]

        total_wins = sum(wins) if wins else 0
        total_losses = sum(losses) if losses else 0
        avg_win = total_wins / len(wins) if wins else 0
        avg_loss = abs(total_losses / len(losses)) if losses else 0
        profit_factor = total_wins / total_losses if total_losses != 0 else 0

        pnls = [float(t["pnl"]) for t in closed_trades]
        pnl_pcts = [float(t["pnl_pct"]) for t in closed_trades]

        total_pnl = sum(pnls)
        mean_return = sum(pnl_pcts) / len(pnl_pcts) if pnl_pcts else 0

        # Compute Sharpe ratio
        if len(pnl_pcts) > 1:
            variance = sum([(x - mean_return) ** 2 for x in pnl_pcts]) / (len(pnl_pcts) - 1)
            std_dev = variance ** 0.5
            sharpe = (mean_return / std_dev * (252 ** 0.5)) if std_dev > 0 else 0
        else:
            sharpe = 0.0

        # Compute max drawdown
        max_drawdown = self._compute_max_drawdown(pnl_pcts)

        return {
            "total_trades": len(all_trades),
            "signals_generated": len(all_trades),
            "closed_trades": len(closed_trades),
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate": (len(wins) / len(closed_trades) * 100) if closed_trades else 0,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": profit_factor,
            "total_pnl": total_pnl,
            "mean_pnl_pct": mean_return,
            "sharpe": sharpe,
            "max_drawdown": max_drawdown,
        }

    def _compute_max_drawdown(self, returns: List[float]) -> float:
        """Compute maximum drawdown from returns list."""
        if not returns:
            return 0.0

        cumulative = 0.0
        max_cumulative = 0.0
        max_drawdown = 0.0

        for ret in returns:
            cumulative += ret
            if cumulative > max_cumulative:
                max_cumulative = cumulative
            drawdown = (cumulative - max_cumulative) / max_cumulative if max_cumulative != 0 else 0
            if drawdown < max_drawdown:
                max_drawdown = drawdown

        return max_drawdown * 100  # Convert to percentage

    def generate_report(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> str:
        """
        Generate a paper trading report.

        Args:
            start_date: Start date filter (optional)
            end_date: End date filter (optional)

        Returns:
            Markdown report
        """
        metrics = self.compute_metrics(start_date, end_date)

        if "note" in metrics:
            return f"# 📊 Paper Trading Report\n\n{metrics['note']}\n"

        report = "# 📊 Paper Trading Report\n\n"
        report += f"**Period:** {start_date or 'All'} to {end_date or 'All'}\n"
        report += f"**Generated:** {now_ist().isoformat()}\n\n"

        report += "## Summary Metrics\n\n"
        report += f"- **Total Signals:** {metrics['signals_generated']}\n"
        report += f"- **Closed Trades:** {metrics['closed_trades']}\n"
        report += f"- **Winning Trades:** {metrics['winning_trades']}\n"
        report += f"- **Losing Trades:** {metrics['losing_trades']}\n"
        report += f"- **Win Rate:** {metrics['win_rate']:.2f}%\n\n"

        report += "## Performance Metrics\n\n"
        report += f"- **Total P&L:** ₹{metrics['total_pnl']:.2f}\n"
        report += f"- **Avg Win:** ₹{metrics['avg_win']:.2f}\n"
        report += f"- **Avg Loss:** ₹{metrics['avg_loss']:.2f}\n"
        report += f"- **Profit Factor:** {metrics['profit_factor']:.2f}\n"
        report += f"- **Sharpe Ratio:** {metrics['sharpe']:.2f}\n"
        report += f"- **Max Drawdown:** {metrics['max_drawdown']:.2f}%\n"

        return report

    def print_summary(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> None:
        """Print a summary of paper trading metrics."""
        metrics = self.compute_metrics(start_date, end_date)

        print("\n=== Paper Trading Summary ===\n")

        if "note" in metrics:
            print(metrics["note"])
            return

        print(f"Period: {start_date or 'All'} to {end_date or 'All'}\n")
        print(f"Signals Generated: {metrics['signals_generated']}")
        print(f"Closed Trades: {metrics['closed_trades']}")
        print(f"Winning Trades: {metrics['winning_trades']}")
        print(f"Losing Trades: {metrics['losing_trades']}\n")

        print(f"Win Rate: {metrics['win_rate']:.2f}%")
        print(f"Avg Win: ₹{metrics['avg_win']:.2f}")
        print(f"Avg Loss: ₹{metrics['avg_loss']:.2f}")
        print(f"Profit Factor: {metrics['profit_factor']:.2f}\n")

        print(f"Total P&L: ₹{metrics['total_pnl']:.2f}")
        print(f"Sharpe Ratio: {metrics['sharpe']:.2f}")
        print(f"Max Drawdown: {metrics['max_drawdown']:.2f}%\n")


if __name__ == "__main__":
    # Example usage
    tracker = PaperTradingTracker()

    # Log some sample trades
    tracker.log_trade(
        date="2025-01-15",
        ticker="RELIANCE",
        signal_type="BUY",
        entry_price=2850.50,
        quantity=100,
        entry_time="09:15:00",
        exit_price=2880.75,
        exit_time="15:30:00",
        pnl=3025.00,
        pnl_pct=1.06,
    )

    tracker.log_trade(
        date="2025-01-15",
        ticker="TCS",
        signal_type="BUY",
        entry_price=3400.00,
        quantity=50,
        entry_time="09:20:00",
        exit_price=3350.00,
        exit_time="15:30:00",
        pnl=-2500.00,
        pnl_pct=-1.47,
    )

    # Print summary
    tracker.print_summary()

    # Print report
    print(tracker.generate_report())
