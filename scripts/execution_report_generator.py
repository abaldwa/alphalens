#!/usr/bin/env python3
"""
Execution Report Generator for AlphaLens Claude Code Framework
SPEC-OBS-003: Structured Logging and Reporting
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

from config.timezone import now_ist


class ExecutionReportGenerator:
    """Generates and manages execution reports for phase prompts."""

    def __init__(self, project_root: str = "./"):
        """
        Initialize report generator.

        Args:
            project_root: Project root directory (default: current directory)
        """
        self.project_root = Path(project_root)
        self.execution_logs_dir = self.project_root / "execution_logs"
        self.execution_logs_dir.mkdir(exist_ok=True)

    def generate_report(
        self,
        phase: str,
        prompt_name: str,
        status: str,
        execution_data: Dict[str, Any],
    ) -> Path:
        """
        Generate an execution report.

        Args:
            phase: Phase identifier (e.g., "PHASE_0")
            prompt_name: Prompt name (e.g., "P0.1")
            status: Execution status (PASSED, FAILED, PARTIAL)
            execution_data: Dictionary containing all execution metrics

        Returns:
            Path to generated report
        """
        timestamp = now_ist().strftime("%Y%m%d_%H%M%S")
        filename = f"{phase}_{prompt_name}_{timestamp}.md"
        filepath = self.execution_logs_dir / filename

        report_content = self._build_report(
            phase, prompt_name, status, execution_data, timestamp
        )

        with open(filepath, "w") as f:
            f.write(report_content)

        print(f"✓ Execution report saved: {filepath}")
        return filepath

    def _build_report(
        self,
        phase: str,
        prompt_name: str,
        status: str,
        data: Dict[str, Any],
        timestamp: str,
    ) -> str:
        """Build markdown report content."""

        duration = data.get("duration_seconds", 0)
        duration_str = self._format_duration(duration)

        report = f"""# 📊 {phase} — {prompt_name} — Execution Report
**Date:** {now_ist().strftime('%Y-%m-%d %H:%M:%S')} IST | **Duration:** {duration_str} | **Status:** {status}

---

## 🎯 Executive Summary
- **Completion %:** {data.get('completion_pct', 'N/A')}%
- **Key Metrics:**
  - Coverage: {data.get('coverage', 'N/A')}%
  - Tests Passed: {data.get('tests_passed', 0)}/{data.get('tests_total', 0)}
  - Sharpe (if applicable): {data.get('sharpe', 'N/A')}
- **Critical Issues:** {', '.join(data.get('blockers', [])) if data.get('blockers') else 'None'}
- **Decisions Made:** {', '.join(data.get('decisions', [])) if data.get('decisions') else 'None'}

---

## 🔧 Execution Details
- **Prompt ID:** {prompt_name}
- **Agents Used:** {', '.join(data.get('agents', []))}
- **Skills Invoked:** {', '.join(data.get('skills', []))}
- **Environment:** Ubuntu | Python 3.x | Project: ./projects/AlphaLens

### Steps Executed
"""

        for i, step in enumerate(data.get('steps', []), 1):
            status_symbol = '✓' if step.get('status') == 'completed' else '✗'
            report += f"{i}. {status_symbol} {step.get('description', 'Unknown step')}\n"

        report += f"""
### Time Breakdown
- Planning/Agent Analysis: {data.get('time_planning', 'N/A')}
- Implementation: {data.get('time_implementation', 'N/A')}
- Testing: {data.get('time_testing', 'N/A')}
- Code Review: {data.get('time_review', 'N/A')}
- **Total:** {duration_str}

---

## ✅ Tests & Coverage
- **Unit Tests:** {data.get('tests_passed', 0)}/{data.get('tests_total', 0)} PASSED
- **Integration Tests:** {data.get('integration_tests_passed', 0)}/{data.get('integration_tests_total', 0)} PASSED
- **Coverage:** {data.get('coverage', 'N/A')}% (target: ≥85%)
- **Regression Tests:** {'PASS' if data.get('regression_pass') else 'FAIL'}
- **Code Review (Medium):** {data.get('code_review_medium_findings', 0)} findings

### Coverage Report
```
{data.get('coverage_report', 'No coverage data available')}
```

### Failed Tests (if any)
{self._format_failed_tests(data.get('failed_tests', []))}

---

## 📝 Code Changes
- **Files Modified:** {', '.join(data.get('files_modified', [])) if data.get('files_modified') else 'None'}
- **Files Created:** {', '.join(data.get('files_created', [])) if data.get('files_created') else 'None'}
- **Total Lines Changed:** +{data.get('lines_added', 0)} -{data.get('lines_removed', 0)}

---

## 🧪 Paper Trading Integration
- **Signals Generated:** {data.get('signals_generated', 0)}
- **Trades Executed:** {data.get('trades_executed', 0)} (Paper)
- **Win Rate:** {data.get('win_rate', 'N/A')}%
- **Avg Win/Loss:** {data.get('avg_win', 'N/A')}% / {data.get('avg_loss', 'N/A')}%
- **Drawdown:** {data.get('drawdown', 'N/A')}%
- **Sharpe Ratio (Paper):** {data.get('sharpe', 'N/A')}

### Trend vs Previous Phase
- Sharpe: {data.get('sharpe_trend', 'N/A')}
- Win Rate: {data.get('win_rate_trend', 'N/A')}

---

## 📊 Data Quality & Validation
- **DuckDB Rows (ohlcv_adjusted):** {data.get('duckdb_rows', 'N/A')}
- **PIT Compliance:** {'✓ PASS' if data.get('pit_compliance') else '✗ FAIL'}
- **Data Completeness:** {data.get('data_completeness', 'N/A')}%
- **Anomalies Detected:** {data.get('anomalies_detected', 0)}
- **Library Security:** {data.get('library_security_status', 'N/A')}

---

## 📈 Baseline Tracking
- **Baseline Sharpe:** {data.get('baseline_sharpe', 'N/A')}
- **Current Sharpe:** {data.get('current_sharpe', 'N/A')}
- **Trend:** {data.get('baseline_trend', 'N/A')}
- **Deviation:** {data.get('baseline_deviation', 'N/A')}%

---

## ⚠️ Issues & Decisions
- **Blockers:** {self._format_blockers(data.get('blockers', []))}
- **Warnings:** {self._format_warnings(data.get('warnings', []))}

---

## 🔒 Phase Gate Check
{self._format_gate_checks(data.get('gate_checks', {}))}

---

## 🚀 Next Steps & Recommendations
{self._format_recommendations(data.get('recommendations', []))}

**Ready for next phase?** {'YES' if status == 'PASSED' else 'NO — Needs remediation'}

---

**Generated by:** AlphaLens Execution Report Generator v2.0
**Timestamp:** {timestamp}
"""
        return report

    @staticmethod
    def _format_duration(seconds: int) -> str:
        """Format seconds to human-readable duration."""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    @staticmethod
    def _format_failed_tests(failed_tests: List[str]) -> str:
        """Format failed tests section."""
        if not failed_tests:
            return "None"

        return "```\n" + "\n".join(f"- {test}" for test in failed_tests) + "\n```"

    @staticmethod
    def _format_blockers(blockers: List[str]) -> str:
        """Format blockers section."""
        if not blockers:
            return "None"

        return "\n".join(f"- {blocker}" for blocker in blockers)

    @staticmethod
    def _format_warnings(warnings: List[str]) -> str:
        """Format warnings section."""
        if not warnings:
            return "None"

        return "\n".join(f"- {warning}" for warning in warnings)

    @staticmethod
    def _format_gate_checks(gate_checks: Dict[str, bool]) -> str:
        """Format gate checks section."""
        if not gate_checks:
            return "No gate checks defined"

        output = ""
        for check_name, passed in gate_checks.items():
            symbol = "✓ PASS" if passed else "✗ FAIL"
            output += f"- **{check_name}:** {symbol}\n"

        return output

    @staticmethod
    def _format_recommendations(recommendations: List[str]) -> str:
        """Format recommendations section."""
        if not recommendations:
            return "No recommendations at this time."

        return "\n".join(f"{i}. {rec}" for i, rec in enumerate(recommendations, 1))

    def get_latest_report(self, phase: str) -> Optional[Path]:
        """Get the latest execution report for a phase."""
        reports = sorted(
            self.execution_logs_dir.glob(f"{phase}_*.md"),
            reverse=True
        )
        return reports[0] if reports else None

    def list_reports(self, phase: Optional[str] = None) -> List[Path]:
        """List all execution reports, optionally filtered by phase."""
        if phase:
            return sorted(self.execution_logs_dir.glob(f"{phase}_*.md"), reverse=True)
        else:
            return sorted(self.execution_logs_dir.glob("*.md"), reverse=True)


if __name__ == "__main__":
    # Example usage
    generator = ExecutionReportGenerator()

    sample_data = {
        "completion_pct": 95,
        "coverage": 86,
        "tests_passed": 28,
        "tests_total": 28,
        "blockers": [],
        "decisions": ["Use Plan agent for architecture", "Implement with general-purpose agent"],
        "agents": ["Plan", "general-purpose"],
        "skills": ["code-review", "verify"],
        "steps": [
            {"description": "Read documentation", "status": "completed"},
            {"description": "Implement project skeleton", "status": "completed"},
            {"description": "Run tests", "status": "completed"},
        ],
        "duration_seconds": 1235,
        "tests_passed": 28,
        "tests_total": 28,
        "coverage": 86,
        "integration_tests_passed": 5,
        "integration_tests_total": 5,
        "regression_pass": True,
        "code_review_medium_findings": 2,
        "files_modified": ["config/settings.py", "config/universe.py"],
        "files_created": ["config/nse_holidays.py"],
        "lines_added": 450,
        "lines_removed": 10,
        "signals_generated": 0,
        "trades_executed": 0,
        "win_rate": "N/A",
        "avg_win": "N/A",
        "avg_loss": "N/A",
        "drawdown": "N/A",
        "sharpe": "N/A",
        "duckdb_rows": 0,
        "pit_compliance": True,
        "data_completeness": "N/A",
        "anomalies_detected": 0,
        "library_security_status": "✓ SECURE (0 CVEs)",
        "baseline_sharpe": "N/A",
        "current_sharpe": "N/A",
        "baseline_trend": "N/A",
        "baseline_deviation": "N/A",
        "warnings": [],
        "gate_checks": {
            "Coverage ≥85%": True,
            "All tests pass": True,
            "Project structure": True,
        },
        "recommendations": [
            "Proceed to P0.2 — DataStore Schema",
            "Review code review findings before next prompt",
        ],
    }

    report_path = generator.generate_report(
        "PHASE_0",
        "P0.1",
        "PASSED",
        sample_data
    )

    print(f"✓ Sample report generated: {report_path}")
