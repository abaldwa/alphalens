#!/usr/bin/env python3
"""
Baseline Metrics Tracker for AlphaLens Framework
SPEC-OBS-002: Baseline Tracking and Trend Analysis
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional

from config.timezone import now_ist


class BaselineTracker:
    """Tracks and manages baseline metrics across phases."""

    def __init__(self, baseline_file: str = "./baselines/baseline_metrics.json"):
        """
        Initialize baseline tracker.

        Args:
            baseline_file: Path to baseline metrics JSON file
        """
        self.baseline_file = Path(baseline_file)
        self.baseline_file.parent.mkdir(parents=True, exist_ok=True)

        if not self.baseline_file.exists():
            self._initialize_baseline_file()

        self.data = self._load_baseline_file()

    def _initialize_baseline_file(self) -> None:
        """Initialize an empty baseline metrics file."""
        initial_data = {
            "metadata": {
                "framework_version": "2.0",
                "created_at": now_ist().isoformat(),
                "description": "Baseline metrics tracking across all phases",
            },
            "phases": {},
            "execution_history": [],
        }

        with open(self.baseline_file, "w") as f:
            json.dump(initial_data, f, indent=2)

    def _load_baseline_file(self) -> Dict[str, Any]:
        """Load baseline metrics from file."""
        try:
            with open(self.baseline_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            self._initialize_baseline_file()
            return json.load(open(self.baseline_file, "r"))

    def _save_baseline_file(self) -> None:
        """Save baseline metrics to file."""
        with open(self.baseline_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def record_phase_metrics(self, phase: str, metrics: Dict[str, Any]) -> None:
        """
        Record metrics for a phase completion.

        Args:
            phase: Phase identifier (e.g., "P0", "P1")
            metrics: Dictionary of metrics to record
        """
        if phase not in self.data["phases"]:
            self.data["phases"][phase] = []

        record = {
            "date": now_ist().isoformat(),
            **metrics,
        }

        self.data["phases"][phase].append(record)
        self.data["execution_history"].append({
            "phase": phase,
            "timestamp": now_ist().isoformat(),
            "coverage": metrics.get("coverage"),
            "sharpe": metrics.get("sharpe"),
            "tests_passed": metrics.get("tests_passed"),
        })

        self._save_baseline_file()
        print(f"✓ Baseline metrics recorded for {phase}")

    def get_latest_metrics(self, phase: str) -> Optional[Dict[str, Any]]:
        """Get the latest metrics for a phase."""
        if phase in self.data["phases"] and self.data["phases"][phase]:
            return self.data["phases"][phase][-1]
        return None

    def get_trend(self, phase: str, metric: str) -> Dict[str, Any]:
        """
        Get trend analysis for a metric across phase executions.

        Args:
            phase: Phase identifier
            metric: Metric name (e.g., "coverage", "sharpe")

        Returns:
            Dictionary containing trend analysis
        """
        if phase not in self.data["phases"] or not self.data["phases"][phase]:
            return {"status": "no_data"}

        values = [
            record.get(metric)
            for record in self.data["phases"][phase]
            if metric in record
        ]

        if not values or len(values) < 1:
            return {"status": "insufficient_data"}

        current = values[-1]
        previous = values[-2] if len(values) > 1 else None

        if previous is None:
            return {
                "status": "first_run",
                "current": current,
                "change": None,
                "trend": "→",
            }

        change = current - previous
        pct_change = (change / previous * 100) if previous != 0 else 0

        trend = "↑" if change > 0 else ("↓" if change < 0 else "→")

        return {
            "status": "calculated",
            "current": current,
            "previous": previous,
            "change": change,
            "pct_change": pct_change,
            "trend": trend,
        }

    def get_comparison(self, phase1: str, phase2: str) -> Dict[str, Any]:
        """
        Compare metrics between two phases.

        Args:
            phase1: First phase identifier
            phase2: Second phase identifier

        Returns:
            Dictionary containing comparison
        """
        metrics1 = self.get_latest_metrics(phase1)
        metrics2 = self.get_latest_metrics(phase2)

        if not metrics1 or not metrics2:
            return {"status": "insufficient_data"}

        comparison = {
            "phase1": phase1,
            "phase2": phase2,
            "metrics": {},
        }

        for key in set(metrics1.keys()) & set(metrics2.keys()):
            if isinstance(metrics1[key], (int, float)):
                v1 = metrics1[key]
                v2 = metrics2[key]
                change = v2 - v1
                pct_change = (change / v1 * 100) if v1 != 0 else 0
                trend = "↑" if change > 0 else ("↓" if change < 0 else "→")

                comparison["metrics"][key] = {
                    "phase1_value": v1,
                    "phase2_value": v2,
                    "change": change,
                    "pct_change": pct_change,
                    "trend": trend,
                }

        return comparison

    def generate_report(self) -> str:
        """Generate a human-readable baseline report."""
        report = "# 📈 Baseline Metrics Report\n\n"

        if not self.data["phases"]:
            return report + "No baseline data available yet.\n"

        report += "## Execution History\n\n"
        report += "| Phase | Date | Coverage | Sharpe | Tests |\n"
        report += "|-------|------|----------|--------|-------|\n"

        for phase in sorted(self.data["phases"].keys()):
            records = self.data["phases"][phase]
            if records:
                latest = records[-1]
                date = latest.get("date", "N/A").split("T")[0]
                coverage = latest.get("coverage", "N/A")
                sharpe = latest.get("sharpe", "N/A")
                tests = f"{latest.get('tests_passed', 0)}/{latest.get('tests_total', 0)}"

                report += f"| {phase} | {date} | {coverage}% | {sharpe} | {tests} |\n"

        # Trend analysis
        report += "\n## Trend Analysis\n\n"
        for phase in sorted(self.data["phases"].keys()):
            trend = self.get_trend(phase, "sharpe")
            if trend["status"] == "calculated":
                report += (
                    f"- **{phase} Sharpe:** {trend['trend']} {trend['current']:.2f} "
                    f"(change: {trend['change']:+.2f}, {trend['pct_change']:+.1f}%)\n"
                )

        return report

    def print_summary(self) -> None:
        """Print a summary of baseline metrics."""
        print("\n=== Baseline Metrics Summary ===\n")

        for phase in sorted(self.data["phases"].keys()):
            metrics = self.get_latest_metrics(phase)
            if metrics:
                print(f"{phase}:")
                print(f"  Coverage: {metrics.get('coverage', 'N/A')}%")
                print(f"  Sharpe: {metrics.get('sharpe', 'N/A')}")
                print(f"  Tests: {metrics.get('tests_passed', 0)}/{metrics.get('tests_total', 0)}")
                print()


if __name__ == "__main__":
    # Example usage
    tracker = BaselineTracker()

    # Record Phase 0 metrics
    tracker.record_phase_metrics("P0", {
        "coverage": 85,
        "tests_passed": 28,
        "tests_total": 28,
        "sharpe": None,
        "win_rate": None,
    })

    # Record Phase 1 metrics
    tracker.record_phase_metrics("P1", {
        "coverage": 86,
        "tests_passed": 64,
        "tests_total": 64,
        "sharpe": 0.95,
        "win_rate": 0.58,
    })

    # Print summary
    tracker.print_summary()

    # Get trend
    trend = tracker.get_trend("P1", "sharpe")
    print(f"P1 Sharpe Trend: {trend}\n")

    # Get comparison
    comparison = tracker.get_comparison("P0", "P1")
    print(f"P0 vs P1 Comparison: {comparison}\n")

    # Generate report
    print(tracker.generate_report())
