"""
systems/fundamental_analysis/

The composite-score/peer-ranking package `systems/copilot/strategy_spec.py`
and `features/fundamental_composites.py` both documented as "always meant
to hold" this logic but that was never built. Houses the 5 value/quality
screening strategies (Piotroski-on-Value, Magic Formula, Quality-Value
Composite, FCF Yield + Low Debt, GARP) added on top of the sector-relative
z-scored ratio panel `features/fundamental.py` already computes.

`quality/`, `growth/` hold the new composite-score functions.
`management/`, `peers/` re-export the logic that already existed in
`features/fundamental_composites.py` (management_quality_score,
select_peers) so all four promised locations are real, without
duplicating that logic.
"""
