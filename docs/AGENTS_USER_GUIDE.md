# AlphaLens Agent System — User Guide

**Last updated:** 2026-08-25

This guide explains the 15 specialist agents available in AlphaLens, how to invoke them, and whether application restart is needed.

---

## Quick Answer: Do I Need to Restart?

**NO.** Agents run in Claude Code's environment—no application restart required. Just invoke them directly.

---

## Agent Roster & Invocation

### **Phase 2: Strategy Audit Agents** ✅ (READY NOW)

#### 1. **momentum-strategy-audit**
**Purpose:** Validate momentum strategies against published research (Jegadeesh & Titman, Fama-French)

**When to invoke:**
- New momentum strategy proposal arrives
- Existing momentum strategy needs validation
- After lookback/regime parameters change

**How to invoke:**
```
"Can you run momentum-strategy-audit on R11 volatility-adjusted momentum?
Here's the strategy code: [paste file or describe parameters]"
```

**What it does:**
1. Fetches momentum research from 2-3 independent sources
2. Compares code against published definitions
3. Validates: lookback periods, ranking, rebalance frequency, universe filters, regime gates
4. Returns: APPROVED / APPROVED WITH NOTES / BLOCKED verdict

**Cost:** ~18-22K tokens  
**Time:** ~20-25 min  
**Parallelizes with:** technical-strategy-audit, fundamental-strategy-audit

---

#### 2. **technical-strategy-audit**
**Purpose:** Validate technical indicator strategies against Wilder, TradingView, academic papers

**When to invoke:**
- New RSI/MACD/Bollinger Bands strategy
- Parameter changes (thresholds, periods)
- Before backtesting technical signals

**How to invoke:**
```
"Run technical-strategy-audit on my RSI-MACD strategy.
Parameters: RSI period=14, thresholds=30/70, MACD fast=12, slow=26"
```

**What it does:**
1. Fetches indicator definitions from 3 sources (academic + industry + trading book)
2. Verifies calculation formula matches published spec
3. Validates: parameters, thresholds, signal logic, regime compatibility, PIT-safety
4. Returns: APPROVED / APPROVED WITH NOTES / BLOCKED verdict

**Cost:** ~18-22K tokens  
**Time:** ~20-25 min  
**Parallelizes with:** momentum-strategy-audit, fundamental-strategy-audit

---

#### 3. **fundamental-strategy-audit**
**Purpose:** Validate valuation/fundamental strategies against Damodaran, Graham & Dodd, Piotroski

**When to invoke:**
- New P/E, P/B, or Piotroski F-Score strategy
- Metric calculation changes
- Before backtesting valuation signals

**How to invoke:**
```
"Run fundamental-strategy-audit on my Piotroski F-Score strategy.
Code: [paste strategy file]"
```

**What it does:**
1. Fetches valuation research from 3 sources
2. Validates metric calculations against published formulas
3. **Critical PIT-safety audit:** ensures announcement-date safety (no forward data)
4. Checks: delisted handling, survivor bias, forecast lag, benchmark selection
5. Returns: APPROVED / APPROVED WITH NOTES / BLOCKED verdict

**Cost:** ~18-22K tokens  
**Time:** ~20-25 min  
**Parallelizes with:** momentum-strategy-audit, technical-strategy-audit

---

### **Phase 3+: Data & Model Audit Agents** (COMING SOON)

#### 4. **data-audit-agent**
**Purpose:** Auto-validate data integrity before backtest completion

**Trigger:** Automatic (no manual invocation needed; runs post-backtest)  
**What it checks:**
- OHLCV source parity (Fyers consistency)
- Point-in-time versioning (universe snapshots dated correctly)
- Feature store lineage (no corruption)
- Signal metadata completeness
- Benchmark data availability

**Cost:** <12K tokens  
**Time:** ~5 min (automatic)

---

#### 5. **signal-parity-agent**
**Purpose:** Verify live signal generation matches backtested signal generation

**When to invoke:**
- Before paper trading gate
- After signal logic changes
- To verify live ≠ backtest drift

**How to invoke:**
```
"Run signal-parity-agent on momentum strategy.
Verify live R1 signals match backtested R1 signals."
```

**Cost:** ~30-40K tokens  
**Time:** ~25 min

---

#### 6. **ml-model-audit-agent**
**Purpose:** Validate ML model training rigor (leakage, overfitting, cross-validation)

**When to invoke:**
- Before retraining CatBoost/Ridge
- After feature engineering changes
- Anytime ML model is modified

**How to invoke:**
```
"Run ml-model-audit-agent on the gainer classifier.
Code: [paste model training file]"
```

**Checks:**
- Training/validation/test separation (no leakage)
- Feature leakage detection (no forward-looking features)
- Walk-forward validation (not random k-fold)
- Generalization on held-out test set
- Data contamination risks

**Cost:** ~25-30K tokens  
**Time:** ~20 min

---

### **Infrastructure & Execution Agents**

#### 7. **memory-management-agent**
**Purpose:** Monitor OOM during backtests, predict memory exhaustion, recommend optimizations

**Trigger:** Automatic (runs concurrent with long backtests)  
**What it does:**
- Monitors memory in real-time (85% threshold alert)
- Predicts OOM 5 minutes early
- Recommends: ticker subsampling, parallel sharding, snapshot usage
- Tracks memory per backtest phase

**Cost:** ~5-8K tokens  
**Time:** Concurrent (no added time)

---

#### 8. **enhanced-backtesting-agent**
**Purpose:** Orchestrate 12 post-backtest integrity checks + ledger audit + fix recommendations

**Trigger:** Automatic (runs after backtest completes)  
**What it does:**
- Runs 12 checks in parallel: walk-forward, PIT, corp-actions, survivorship, costs, liquidity, HPO, fold-stability, benchmarks, random-feature, sector-lookahead, equity-curve
- Ledger invariant audit (tax, negative cash, FY continuity)
- Recommends fixes if checks fail
- Decides mechanism switching (ticker-by-ticker optimization, benchmark parity)

**Cost:** ~20-25K tokens  
**Time:** ~10 min (automatic, post-backtest)

---

### **Core Validation Agents** (PHASE 1 ROSTER)

#### 9. **ml-rigor-reviewer**
**Purpose:** Statistical rigor review (leakage, overfitting, validation soundness)

**How to invoke:**
```
"Run ml-rigor-reviewer on the gainer model.
Is the cross-validation strategy sound?"
```

**Cost:** ~15K tokens  
**Parallelizes with:** domain-expert, backtest-reviewer, backend-data-engineer

---

#### 10. **domain-expert**
**Purpose:** Review strategies against Indian equity market mechanics

**How to invoke:**
```
"Run domain-expert review on R1 momentum.
Does this work for NSE trading mechanics?"
```

**Cost:** ~12K tokens  
**Parallelizes with:** ml-rigor-reviewer, backtest-reviewer

---

#### 11. **backtest-reviewer**
**Purpose:** Verify backtest engine correctness, trade validation, metrics calculation

**How to invoke:**
```
"Run backtest-reviewer on the R1 backtest results.
Are the Sharpe/Calmar calculations correct?"
```

**Cost:** ~15K tokens  
**Parallelizes with:** ml-rigor-reviewer, domain-expert

---

#### 12. **backend-data-engineer**
**Purpose:** Review data infrastructure feasibility (DuckDB, ingestion, PIT storage)

**How to invoke:**
```
"Run backend-data-engineer on the new feature proposal.
Can we store this in the feature-store schema?"
```

**Cost:** ~12K tokens  
**Parallelizes with:** Other reviewers

---

### **Code Quality Agents**

#### 13. **code-reviewer**
**Purpose:** Correctness bugs, simplification, code cleanup

**How to invoke:**
```
"Run code-reviewer on the strategy changes.
Are there any bugs or refactoring opportunities?"
```

**Cost:** ~10K tokens

---

#### 14. **frontend-a11y-reviewer**
**Purpose:** Accessibility, responsive layout, light/dark theme consistency

**How to invoke:**
```
"Run frontend-a11y-reviewer on the new backtest report UI.
Is it accessible and responsive?"
```

**Cost:** ~10K tokens

---

### **Decision Agents**

#### 15. **skeptic-tester**
**Purpose:** Hunt for failure modes, policy violations, edge cases

**How to invoke:**
```
"Run skeptic-tester on this strategy proposal.
What could go wrong?"
```

**Cost:** ~8K tokens

---

## How to Invoke Agents

### **Method 1: Direct Invocation (Recommended)**

Just ask in conversation:
```
"Run momentum-strategy-audit on R11.
Here's the code: [paste strategy file]"
```

Claude will:
1. Detect the agent request
2. Spawn the agent in the background
3. Return status updates as the agent works
4. Provide final verdict/results

### **Method 2: Parallel Invocation (High-Stakes Decisions)**

Invoke multiple agents together:
```
"Run momentum-strategy-audit, technical-strategy-audit, and ml-rigor-reviewer on R13.
Parallelize them."
```

**When to parallelize:**
- High-stakes decision (strategy gate, live trading, major model change)
- Agents are independent (don't depend on each other's output)
- Time justified (serial would take >10 min; parallel saves ~7 min)

**Example: Strategy Proposal (R13 Volatility-Adjusted Momentum)**
```
Invoke in parallel:
- momentum-strategy-audit (validates against research)
- ml-rigor-reviewer (statistical rigor)
- domain-expert (Indian market mechanics)
- backtest-reviewer (engine correctness)
- signal-parity-agent (live ≠ backtest drift)

Total time: ~25 min parallel vs. ~90 min serial
Token cost: ~135K
```

---

## Do I Need to Restart the Application?

### **API Server**
No restart needed. Agents run in Claude Code's environment, separate from the FastAPI server.
- API lives at `http://127.0.0.1:8123`
- Start with: `python -m uvicorn datastore.api.main:app --port 8123`
- Agents don't touch it

### **Backtest Engine**
No restart needed. Agents review *existing* backtest code and results.
- To run backtest: `python3 backtest/run_strategy_queue.py [queue_file]`
- Agents can audit it while it runs

### **Data Pipeline / Scheduler**
No restart needed. Agents are read-only; they don't modify data.
- Scheduler runs via systemd: `systemctl --user status alphalens-scheduler.service`
- Agents query data but don't write

### **Frontend**
No restart needed. Dashboard runs separately at dev or production build.
- Dev server: `npm run dev` (from `frontend/`)
- Agents don't interact with frontend

**Summary:** ✅ No restarts required. Agents are sandboxed in Claude Code.

---

## Automatic Agents (Run Without Asking)

These agents trigger automatically—no invocation needed:

| Agent | Trigger | When | Cost |
|-------|---------|------|------|
| **data-audit-agent** | Post-backtest | After every backtest completes | <12K |
| **memory-management-agent** | Concurrent with backtest | During long-running backtests | 5-8K |
| **enhanced-backtesting-agent** | Post-backtest | After backtest completes (12 checks) | 20-25K |

---

## Invocation Examples

### Example 1: New Strategy Proposal (Momentum)

**User:** "I have R13 volatility-adjusted momentum. Ready to backtest?"

**Invoke:** 
```
momentum-strategy-audit (external research validation)
+ ml-rigor-reviewer (statistical rigor)
+ domain-expert (market mechanics)
→ Parallel execution: ~25 min
```

**Agent verdict:** APPROVED / APPROVED WITH NOTES / BLOCKED
**Next step:** If APPROVED → schedule backtest immediately

---

### Example 2: Post-Backtest Audit

**Trigger:** Backtest completes

**Automatic invocation:**
```
enhanced-backtesting-agent (12 integrity checks in parallel)
+ data-audit-agent (data parity verification)
→ Parallel execution: ~10 min
```

**Output:** Pass/fail on integrity checks + recommendations

---

### Example 3: Model Retraining

**User:** "Retrain CatBoost gainer model on new data"

**Invoke:**
```
ml-model-audit-agent (training rigor: leakage, overfitting)
+ ml-rigor-reviewer (statistical soundness)
+ backend-data-engineer (data ready?)
→ Parallel execution: ~15 min
```

**Verdict:** APPROVED to retrain / BLOCKED (fix data first)

---

## Agent Status & Monitoring

### Check Agent Status
```bash
# View running agents
claude /agents list

# Check specific agent
claude /agents status momentum-strategy-audit
```

### View Agent Output
- Agents return final verdict + detailed report in conversation
- Intermediate steps streamed in real-time
- Full logs saved to `.claude/logs/` (if configured)

---

## Troubleshooting

### "Agent failed to complete"
- Check network (agents fetch external research)
- Try again—transient failures are rare
- Escalate to skeptic-tester if blocked

### "Verdict is BLOCKED"
- Read the agent's detailed justification
- Fix the issue (e.g., PIT violation, missing parameter check)
- Re-invoke agent to re-audit

### "Agent is taking too long"
- Normal for strategy audits (~20-25 min due to research fetching)
- Expected for backtest audits (~10 min for 12 parallel checks)
- Don't interrupt—let it complete

---

## Next Steps

### Phase 2 (NOW): Trial Strategy Audit Agents ✅
- momentum-strategy-audit ready for R-family momentum strategies
- technical-strategy-audit ready for TA indicator strategies
- fundamental-strategy-audit ready for valuation strategies

**How to start Phase 3:** Submit first strategy proposal + invoke appropriate agent(s)

### Phase 3+ (UPCOMING): Data/Model Agents
- data-audit-agent (automatic post-backtest)
- signal-parity-agent (pre-paper-trading gate)
- ml-model-audit-agent (model retraining validation)

---

## See Also

- [AGENTS.md](../AGENTS.md) — Full technical specification
- [docs/agents/strategy-audit-prompts.md](agents/strategy-audit-prompts.md) — Audit prompt templates
- [docs/agents/test-scenario-r1-audit.md](agents/test-scenario-r1-audit.md) — Example R1 audit output
- [CLAUDE.md § Model & Agent Selection](../CLAUDE.md#model--agent-selection) — Model routing rules
