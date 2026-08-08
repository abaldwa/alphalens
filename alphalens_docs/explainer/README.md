# AlphaLens Explainer

A single, menu-navigated reference for every scoring system in AlphaLens: what each
system computes, how it's derived, and — for technical indicators — whether it
actually drives a decision or is only computed and stored.

## Viewing it

`index.html` is self-contained (no external CSS/JS/fonts). Open it directly:

```bash
xdg-open alphalens_docs/explainer/index.html
```

## Structure

| Section | Status | Covers |
| --- | --- | --- |
| Overview | Ready | Section map and how to read the usage status pills |
| Technical › Indicators | Ready | All 88 daily per-stock features (70 core + 18 advanced), with parameters and downstream-usage status |
| Technical › Screener Templates | Ready | All 42 templates (A–F, S series) with their actual rule conditions, plus exit params by behavioral style |
| Technical › Technical Strategies | Ready | Entry-filter tiers (Balanced / Risk-Managed / Max-Defensive) and cross-style Combo strategies |
| Damodaran Valuation | Ready | 4 DCF variants, India-specific WACC, PE regression, Monte Carlo, 7-stage lifecycle classifier |
| ML Signal Engine | Ready | 13 model families: signal, 8 exit policies, multibagger, P&D, forensic, HMM regime, TFT/BiLSTM, conformal |
| Fundamental Screens | Stub | Quality / Growth / Contrarian / Management strategy families |
| Momentum | Stub | Universe construction, rebalance, portfolio rules |
| Sector Accumulation | Stub | Delivery-weighted per-sector accumulation score |
| Big Investors / MF Holdings | Stub | MF holdings deltas, corporate action features |
| Copilot | Stub | NL strategy builder → spec → backtest |

Stub sections carry a scope preview and their source paths so they can be filled in
without re-researching.

## Product page links

The Technical group in the sidebar links out to the live Technical screens. Those
links assume the Vite dev server default (`http://localhost:5173`) — the frontend is
served separately from the FastAPI API on port 8000. If the app is served from a
different host or port, update the `http://localhost:5173` occurrences in
`index.html`.

Routes are flat and hyphenated (e.g. `/technical-screener`,
`/technical-recommended-strategies`), matching `frontend/src/app/router.tsx`. There is
no `/technical-indicators` route — indicator documentation lives here in the Explainer,
not as a product screen.

## Keeping it accurate

Content was compiled from source, not from prior docs. When these change, update the
matching section:

- `features/technical.py` — the 70 core features (`CORE_TECHNICAL_FEATURES`)
- `features/advanced_technical.py` — the 18 advanced features
- `systems/technical_analysis/screener/templates.py` — the 42 templates, `TEMPLATE_STYLE`, `STYLE_EXIT_PARAMS`
- `systems/ml_signal_engine/models/` — model families
- `systems/damodaran_valuation/` — valuation models and WACC

The usage status pills (Used / Not used) reflect a grep-based audit of whether each
feature is named in a screener rule, exit condition, ranking formula, or served by the
API/UI. Features marked "Not used" are still fed to ML models in bulk — they're just
never hand-picked for a specific signal. Re-run that audit after adding screener rules
or model features.
