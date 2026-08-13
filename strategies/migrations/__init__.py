"""
strategies/migrations/

One module per channel, each turning that channel's hardcoded Python strategy
definitions into strategy_registry rows (T15, ML41, ML42, F7).

Every migration is idempotent and reads the existing Python declarations as
its source, so it can be re-run after a template is added and will register
only what is new. None of them delete the Python source yet -- that happens in
A95, once the backtest actually reads from the registry, so a failed migration
cannot take the screener down with it.
"""
