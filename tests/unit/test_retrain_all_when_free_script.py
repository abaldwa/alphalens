"""
tests/unit/test_retrain_all_when_free_script.py

Regression test for A28(f)/(g): scripts/retrain_all_when_free.sh logged
"exit=0" for every stage even when the underlying python command crashed,
because `echo "... $(date -Iseconds) exit=$?"` evaluates the `date`
command substitution before `$?` is expanded, so `$?` always reflects
`date`'s (always-zero) exit status rather than the python command's.
This silently masked two real retrain_phase2.py failures on 2026-07-06
and made signal_63d's staleness look like a legitimate "no improvement"
outcome instead of the stage never having run.

These tests exercise the actual stage-logging idiom used in the script
(capture `$?` into a variable immediately after the command, before any
other command runs) and guard against the buggy inline `exit=$?` pattern
being reintroduced.
"""
import re
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "retrain_all_when_free.sh"


def _run_stage_log(command: str) -> str:
    """Run the script's fixed logging idiom around `command`, return the log line."""
    snippet = f"""
{command}
rc=$?
echo "=== STAGE END $(date -Iseconds) exit=$rc ==="
"""
    result = subprocess.run(["bash", "-c", snippet], capture_output=True, text=True)
    return result.stdout.strip().splitlines()[-1]


def test_fixed_idiom_captures_failure_exit_code():
    line = _run_stage_log("false")
    assert "exit=1" in line


def test_fixed_idiom_captures_success_exit_code():
    line = _run_stage_log("true")
    assert "exit=0" in line


def test_fixed_idiom_captures_nonzero_python_exit_code():
    line = _run_stage_log("python3 -c 'import sys; sys.exit(3)'")
    assert "exit=3" in line


def test_script_does_not_reintroduce_inline_exit_dollar_question_bug():
    """No stage may compute `exit=$?` in the same echo as a `$(...)` command
    substitution — that always logs the substitution's exit status, not the
    preceding command's."""
    text = SCRIPT.read_text()
    buggy_pattern = re.compile(r'echo\s+"[^"\n]*\$\([^)]*\)[^"\n]*exit=\$\?')
    assert not buggy_pattern.search(text), (
        "retrain_all_when_free.sh reintroduced the exit=$? logging bug "
        "(A28(f)/(g)) — capture $? into a variable before any other command runs"
    )


def test_script_captures_exit_code_for_all_three_stages():
    text = SCRIPT.read_text()
    assert text.count("rc=$?") == 3, "expected an rc=$? capture for each of the 3 retrain stages"
