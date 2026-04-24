"""
alphalens/core/strategy/discovery.py

Monthly genetic algorithm strategy discovery using DEAP.

Process:
  1. Define parameter space (ranges for each tunable parameter)
  2. Initialize population of random parameter combinations
  3. Evaluate each candidate via walk-forward backtest on 30 stocks
  4. Select survivors (tournament selection)
  5. Apply crossover and mutation
  6. Repeat for N generations
  7. Survivors passing quality gates → stored as new strategies in DB

Each strategy template has a parameter_space defining what can evolve.
The genetic algorithm only mutates parameters — not the structural logic.
This gives interpretable, actionable strategies (not black-box).

Usage:
    disc = StrategyDiscovery()
    stats = disc.run_monthly_discovery()
"""

import json
import random
import uuid
from datetime import datetime
from typing import Any

import numpy as np
from loguru import logger

from alphalens.core.database import get_duck
from alphalens.core.strategy.library import STRATEGY_DEFINITIONS

# GA hyperparameters — tuned for 8GB RAM laptop
POPULATION_SIZE = 30      # individuals per generation
N_GENERATIONS   = 20      # generations to run
TOURNAMENT_SIZE = 3       # selection pressure
CROSSOVER_PROB  = 0.7     # probability of crossover
MUTATION_PROB   = 0.2     # probability of mutating each parameter
EVAL_SYMBOLS    = 20      # symbols to evaluate each candidate on

# Fitness thresholds
MIN_SHARPE      = 1.0
MIN_WIN_RATE    = 0.52
MIN_TRADES      = 50

# Parameter spaces per strategy template
# (param_name, min_val, max_val, step, type)
TEMPLATE_PARAM_SPACES = {
    "EMA Crossover Momentum": [
        ("ema_fast",         6,   15,  1,   "int"),
        ("ema_slow",        15,   50,  1,   "int"),
        ("adx_threshold",   15,   35,  1,   "int"),
        ("rsi_min",         45,   60,  1,   "int"),
        ("rsi_exit",        70,   80,  1,   "int"),
        ("atr_sl_mult",    1.5,  3.0, 0.1, "float"),
        ("atr_target_mult", 2.0,  5.0, 0.5, "float"),
        ("volume_min_ratio", 1.0, 2.5, 0.1, "float"),
    ],
    "Supertrend Breakout": [
        ("atr_period",       7,  20,  1,   "int"),
        ("multiplier",     2.0, 4.5, 0.1, "float"),
        ("volume_min_ratio", 1.0, 2.5, 0.1, "float"),
        ("atr_target_mult", 1.5, 4.0, 0.5, "float"),
    ],
    "RSI Mean Reversion": [
        ("rsi_entry",       25,  40,  1,   "int"),
        ("rsi_exit",        50,  65,  1,   "int"),
        ("bb_pct_entry",  0.05, 0.25, 0.05, "float"),
        ("atr_sl_mult",    1.0, 2.5,  0.1, "float"),
        ("atr_target_mult", 1.5, 4.0, 0.5, "float"),
    ],
    "52-Week High Breakout": [
        ("near_high_threshold", -3.0, 0.0, 0.5, "float"),
        ("volume_min_ratio",     1.5,  3.0, 0.1, "float"),
        ("trail_stop_pct",       5.0, 15.0, 0.5, "float"),
        ("adx_min",             15,   30,   1,   "int"),
        ("atr_target_mult",      2.0,  6.0, 0.5, "float"),
    ],
    "Bollinger Band Squeeze Breakout": [
        ("squeeze_lookback",  60,  180,  10, "int"),
        ("volume_min_ratio",  1.2,  3.0, 0.1, "float"),
        ("rsi_exit",          70,   85,  1,   "int"),
        ("atr_sl_mult",      1.5,  3.0,  0.1, "float"),
        ("atr_target_mult",   2.0,  5.0, 0.5, "float"),
    ],
    "Turtle Trading 20-10": [
        ("entry_period",   15,   30,   1,   "int"),
        ("exit_period",     7,   20,   1,   "int"),
        ("atr_period",     14,   30,   1,   "int"),
        ("atr_sl_mult",   1.5,  3.5,  0.1, "float"),
        ("adx_min",        10,   25,   1,   "int"),
    ],
}


class StrategyDiscovery:

    def __init__(self):
        self.con = get_duck()
        self._backtester = None  # lazy import

    def _get_backtester(self):
        if self._backtester is None:
            from alphalens.core.strategy.backtester import Backtester
            self._backtester = Backtester()
        return self._backtester

    # ── Public API ─────────────────────────────────────────────────────────

    def run_monthly_discovery(self) -> dict:
        """
        Run the full monthly strategy discovery pipeline.
        Returns summary of discovered strategies.
        """
        from alphalens.core.ingestion.universe import get_all_symbols
        import random as rng

        all_symbols = get_all_symbols()
        eval_symbols = rng.sample(all_symbols, min(EVAL_SYMBOLS, len(all_symbols)))

        logger.info(
            f"Strategy Discovery: {len(TEMPLATE_PARAM_SPACES)} templates, "
            f"pop={POPULATION_SIZE}, gen={N_GENERATIONS}, "
            f"eval on {len(eval_symbols)} symbols"
        )

        all_new_strategies = []
        total_tested       = 0
        t_start            = datetime.now()

        for template_name, param_space in TEMPLATE_PARAM_SPACES.items():
            logger.info(f"Evolving template: {template_name}")
            try:
                new_strats, n_tested = self._evolve_template(
                    template_name, param_space, eval_symbols
                )
                all_new_strategies.extend(new_strats)
                total_tested += n_tested
                logger.info(
                    f"  {template_name}: {n_tested} tested, "
                    f"{len(new_strats)} new strategies discovered"
                )
            except Exception as e:
                logger.warning(f"  {template_name}: evolution failed — {e}")

        duration = (datetime.now() - t_start).total_seconds() / 60

        logger.info(
            f"Discovery complete: {total_tested} total tested, "
            f"{len(all_new_strategies)} new strategies, "
            f"{duration:.1f} min"
        )

        return {
            "new_strategies":  all_new_strategies,
            "total_tested":    total_tested,
            "duration_minutes": round(duration, 1),
            "templates_run":   len(TEMPLATE_PARAM_SPACES),
        }

    # ── Genetic Algorithm Core ────────────────────────────────────────────

    def _evolve_template(self, template_name: str, param_space: list,
                          eval_symbols: list) -> tuple:
        """
        Run genetic evolution for one strategy template.
        Returns (list of new strategy dicts, total candidates evaluated).
        """
        # Find base strategy definition
        base_strat = next(
            (s for s in STRATEGY_DEFINITIONS if s["name"] == template_name), None
        )
        if not base_strat:
            return [], 0

        # ── Initialise population ─────────────────────────────────────
        population  = [self._random_individual(param_space) for _ in range(POPULATION_SIZE)]
        fitness_cache = {}
        total_tested  = 0

        # ── Evolution loop ────────────────────────────────────────────
        for gen in range(N_GENERATIONS):
            # Evaluate fitness for unevaluated individuals
            for ind in population:
                key = self._individual_key(ind)
                if key not in fitness_cache:
                    fitness = self._evaluate_individual(
                        ind, base_strat, param_space, eval_symbols
                    )
                    fitness_cache[key] = fitness
                    total_tested += 1

            # Sort by fitness (Sharpe ratio)
            population.sort(
                key=lambda x: fitness_cache.get(self._individual_key(x), {}).get("sharpe", -99),
                reverse=True
            )

            top_fitness = fitness_cache.get(self._individual_key(population[0]), {})
            logger.debug(
                f"  Gen {gen+1}/{N_GENERATIONS}: best Sharpe={top_fitness.get('sharpe', 0):.3f} "
                f"WinRate={top_fitness.get('win_rate', 0)*100:.1f}%"
            )

            # Early stopping if top individual is very good
            if top_fitness.get("sharpe", 0) > 2.0 and top_fitness.get("win_rate", 0) > 0.60:
                logger.debug(f"  Early stop at gen {gen+1}")
                break

            # ── Selection + reproduction ──────────────────────────────
            new_population = [population[0]]   # elitism: keep best

            while len(new_population) < POPULATION_SIZE:
                parent1 = self._tournament_select(population, fitness_cache)
                parent2 = self._tournament_select(population, fitness_cache)

                if random.random() < CROSSOVER_PROB:
                    child1, child2 = self._crossover(parent1, parent2, param_space)
                else:
                    child1, child2 = list(parent1), list(parent2)

                child1 = self._mutate(child1, param_space)
                child2 = self._mutate(child2, param_space)

                new_population.append(child1)
                if len(new_population) < POPULATION_SIZE:
                    new_population.append(child2)

            population = new_population

        # ── Extract survivors that pass gates ─────────────────────────
        survivors      = []
        seen_configs   = set()

        for ind in population:
            key     = self._individual_key(ind)
            fitness = fitness_cache.get(key, {})

            if (fitness.get("sharpe", 0)   >= MIN_SHARPE and
                fitness.get("win_rate", 0) >= MIN_WIN_RATE and
                fitness.get("n_trades", 0) >= MIN_TRADES):

                # Deduplicate similar parameter sets
                rounded = tuple(round(v, 1) for v in ind)
                if rounded in seen_configs:
                    continue
                seen_configs.add(rounded)

                # Check it's meaningfully different from existing strategies
                if not self._is_duplicate(template_name, ind, param_space):
                    new_strat = self._build_strategy(
                        base_strat, ind, param_space, fitness
                    )
                    self._store_strategy(new_strat)
                    survivors.append(new_strat)

        return survivors, total_tested

    # ── GA Operators ──────────────────────────────────────────────────────

    def _random_individual(self, param_space: list) -> list:
        """Create a random individual (list of parameter values)."""
        ind = []
        for _, min_v, max_v, step, dtype in param_space:
            if dtype == "int":
                val = random.randint(int(min_v), int(max_v))
            else:
                steps = int((max_v - min_v) / step)
                val   = round(min_v + random.randint(0, steps) * step, 4)
            ind.append(val)
        return ind

    def _tournament_select(self, population: list,
                            fitness_cache: dict) -> list:
        """Tournament selection — pick best of K random individuals."""
        tournament = random.sample(population, min(TOURNAMENT_SIZE, len(population)))
        tournament.sort(
            key=lambda x: fitness_cache.get(self._individual_key(x), {}).get("sharpe", -99),
            reverse=True
        )
        return list(tournament[0])

    def _crossover(self, parent1: list, parent2: list,
                    param_space: list) -> tuple:
        """Uniform crossover — each gene independently inherited."""
        child1 = []
        child2 = []
        for i in range(len(param_space)):
            if random.random() < 0.5:
                child1.append(parent1[i])
                child2.append(parent2[i])
            else:
                child1.append(parent2[i])
                child2.append(parent1[i])
        return child1, child2

    def _mutate(self, individual: list, param_space: list) -> list:
        """Random mutation — perturb each gene with probability MUTATION_PROB."""
        mutated = list(individual)
        for i, (_, min_v, max_v, step, dtype) in enumerate(param_space):
            if random.random() < MUTATION_PROB:
                if dtype == "int":
                    delta = random.choice([-2, -1, 1, 2])
                    mutated[i] = int(np.clip(mutated[i] + delta, min_v, max_v))
                else:
                    delta = random.choice([-step, -step/2, step/2, step])
                    mutated[i] = round(float(np.clip(mutated[i] + delta, min_v, max_v)), 4)
        return mutated

    def _individual_key(self, individual: list) -> str:
        return str([round(v, 3) for v in individual])

    # ── Fitness Evaluation ────────────────────────────────────────────────

    def _evaluate_individual(self, individual: list, base_strat: dict,
                               param_space: list, eval_symbols: list) -> dict:
        """Evaluate fitness by backtesting the candidate strategy."""
        candidate = self._build_strategy(base_strat, individual, param_space)
        bt        = self._get_backtester()

        sharpe_scores   = []
        win_rates       = []
        total_trades    = 0

        for symbol in eval_symbols[:10]:   # Quick eval on 10 symbols
            try:
                result = bt.run(
                    strategy_id = candidate["strategy_id"],
                    symbol      = symbol,
                    from_date   = "2015-01-01"   # Last 9 years for speed
                )
                if "error" not in result and result.get("total_trades", 0) >= 5:
                    sharpe_scores.append(result.get("sharpe_ratio", 0))
                    win_rates.append(result.get("win_rate", 0))
                    total_trades += result.get("total_trades", 0)
            except Exception:
                continue

        if not sharpe_scores:
            return {"sharpe": -1.0, "win_rate": 0.0, "n_trades": 0}

        return {
            "sharpe":   float(np.mean(sharpe_scores)),
            "win_rate": float(np.mean(win_rates)),
            "n_trades": total_trades,
        }

    # ── Strategy Building & Storage ───────────────────────────────────────

    def _build_strategy(self, base_strat: dict, individual: list,
                         param_space: list, fitness: dict = None) -> dict:
        """Build a full strategy dict from a base + evolved parameters."""
        params = dict(base_strat.get("parameters", {}))

        # Apply evolved values
        for i, (param_name, *_) in enumerate(param_space):
            params[param_name] = individual[i]

        # Update entry/exit rules with new parameter values
        entry_rules = json.loads(json.dumps(base_strat["entry_rules"]))
        exit_rules  = json.loads(json.dumps(base_strat["exit_rules"]))
        sl_rules    = json.loads(json.dumps(base_strat["stoploss_rules"]))

        # Patch rule values from evolved params
        self._patch_rules(entry_rules, params)
        self._patch_rules(exit_rules,  params)
        if "multiplier" in params:
            sl_rules["multiplier"] = params["multiplier"]
        if "atr_sl_mult" in params:
            sl_rules["multiplier"] = params["atr_sl_mult"]

        gen_num   = self._get_next_generation(base_strat["name"])
        strat_id  = f"G{gen_num:03d}_{base_strat['strategy_id']}"

        strat = {
            **base_strat,
            "strategy_id":  strat_id,
            "name":         f"{base_strat['name']} (Gen {gen_num})",
            "parameters":   params,
            "entry_rules":  entry_rules,
            "exit_rules":   exit_rules,
            "stoploss_rules": sl_rules,
            "discovered_by": "genetic",
            "generation":   gen_num,
        }

        if fitness:
            strat["sharpe_ratio"] = fitness.get("sharpe")
            strat["win_rate"]     = fitness.get("win_rate")

        return strat

    def _patch_rules(self, rules: dict, params: dict):
        """Patch evolved parameter values into rule conditions."""
        for cond in rules.get("conditions", []):
            ind = cond.get("indicator", "")
            if "rsi" in ind and "rsi_entry" in params:
                if cond.get("op") == "<=":
                    cond["value"] = params["rsi_entry"]
                elif cond.get("op") in (">=", ">") and cond.get("value", 0) < 50:
                    cond["value"] = params.get("rsi_min", cond["value"])
            if ind == "adx_14" and "adx_threshold" in params:
                cond["value"] = params["adx_threshold"]
            if ind == "volume_ratio" and "volume_min_ratio" in params:
                cond["value"] = params["volume_min_ratio"]
            if ind == "bb_pct_b" and "bb_pct_entry" in params:
                cond["value"] = params["bb_pct_entry"]
            if ind == "pct_from_52w_high" and "near_high_threshold" in params:
                cond["value"] = params["near_high_threshold"]

    def _get_next_generation(self, template_name: str) -> int:
        """Get the next generation number for a template."""
        result = self.con.execute("""
            SELECT MAX(generation) FROM strategies
            WHERE name LIKE ? AND discovered_by = 'genetic'
        """, [f"{template_name}%"]).fetchone()
        return (result[0] or 0) + 1

    def _is_duplicate(self, template_name: str,
                       individual: list, param_space: list) -> bool:
        """Check if a very similar strategy already exists in DB."""
        existing = self.con.execute("""
            SELECT parameters FROM strategies
            WHERE name LIKE ? AND discovered_by = 'genetic'
        """, [f"{template_name}%"]).fetchall()

        for (params_json,) in existing:
            try:
                ex_params = json.loads(params_json)
                similarity = 0
                for i, (name, *_) in enumerate(param_space):
                    if abs(ex_params.get(name, 0) - individual[i]) < 0.2:
                        similarity += 1
                if similarity / len(param_space) > 0.9:
                    return True
            except Exception:
                continue
        return False

    def _store_strategy(self, strategy: dict):
        """Store a newly discovered strategy in DuckDB."""
        self.con.execute("""
            INSERT OR IGNORE INTO strategies (
                strategy_id, name, type, description,
                timeframes, best_cycles,
                entry_rules, exit_rules, stoploss_rules, parameters,
                sharpe_ratio, win_rate,
                discovered_by, generation, is_active, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'genetic', ?, true, ?)
        """, [
            strategy["strategy_id"],
            strategy["name"],
            strategy.get("type"),
            strategy.get("description"),
            json.dumps(strategy.get("timeframes", [])),
            json.dumps(strategy.get("best_cycles", [])),
            json.dumps(strategy["entry_rules"]),
            json.dumps(strategy["exit_rules"]),
            json.dumps(strategy["stoploss_rules"]),
            json.dumps(strategy["parameters"]),
            strategy.get("sharpe_ratio"),
            strategy.get("win_rate"),
            strategy.get("generation", 1),
            datetime.now(),
        ])
        logger.info(f"New strategy stored: {strategy['name']} "
                    f"(Sharpe={strategy.get('sharpe_ratio', 0):.2f})")
