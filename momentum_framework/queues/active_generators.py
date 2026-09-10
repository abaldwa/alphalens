"""
Single source of truth for which QueueGenerator subclasses make up the
ACTIVE campaign (2026-09-06 remediation session onward).

WHY THIS EXISTS: before this file, tests/test_queue_generators.py was the
only place that listed "all 12 active strategies" (as ALL_GENERATORS) —
any script assembling a real campaign queue would have had to either
import that test module (fragile — tests shouldn't be a library) or
hand-copy the same 12 imports a second time, which is exactly the kind
of drift CLAUDE.md's "No Ad-Hoc Code Generation" rule exists to prevent
(see that rule's R7-ad-hoc-queues history). Both the test suite and
queues/pass_builder.py (real queue assembly) import ACTIVE_GENERATORS
from here now.

R05 and R16 are permanently excluded — see strategies/__init__.py's
module docstring. R16's generator file (r16_target_volatility.py) is
kept for historical/parity reference only, never imported here.
"""

from typing import List, Type

from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.r01_trailing_momentum import R01QueueGenerator
from momentum_framework.strategies.r03_jt_skipmonth import R03QueueGenerator
from momentum_framework.strategies.r07_crash_aware import R07QueueGenerator
from momentum_framework.strategies.r08_bsc_volscale import R08QueueGenerator
from momentum_framework.strategies.r09_mm_volscale import R09QueueGenerator
from momentum_framework.strategies.r10_sector_momentum import R10QueueGenerator
from momentum_framework.strategies.r11_52wk_reversal import R11QueueGenerator
from momentum_framework.strategies.r12_reversal_1mo import R12QueueGenerator
from momentum_framework.strategies.r13_bollinger_reversal import R13QueueGenerator
from momentum_framework.strategies.r14_inverse_volatility import R14QueueGenerator
from momentum_framework.strategies.r15_inverse_variance import R15QueueGenerator
# r16_target_volatility.R16QueueGenerator deliberately NOT imported — R16
# retired 2026-09-06 (Category B2), see that file's module docstring and
# tests/test_queue_generators.py::test_r16_never_generated.
from momentum_framework.strategies.r17_downside_volatility import R17QueueGenerator

ACTIVE_GENERATORS: List[Type[QueueGenerator]] = [
    R01QueueGenerator, R03QueueGenerator, R07QueueGenerator, R08QueueGenerator,
    R09QueueGenerator, R10QueueGenerator, R11QueueGenerator, R12QueueGenerator,
    R13QueueGenerator, R14QueueGenerator, R15QueueGenerator,
    R17QueueGenerator,
]
