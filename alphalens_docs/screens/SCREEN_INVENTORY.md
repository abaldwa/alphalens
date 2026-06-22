# AlphaLens — Screen & Component Inventory
## 27 Screens · 5 Applications · Component Library

**Design language:** Light background, modern, trader/investor persona
**Insight focus:** Entry Point · Expected Returns % · Duration · Confidence Level
**Pattern:** Summary view → click-through detail view
**Cross-nav:** Separate apps with quick-links (stock in ML → opens in Technical)

---

## Application Registry

| App ID | Name | Port | Primary Use | Screens |
|--------|------|:----:|-------------|:-------:|
| ML | AlphaLens.ML | 8001 | Daily signals, P&D, exit alerts, multibagger | 5 |
| TA | AlphaLens.Technical | 8002 | Charting, patterns, 42 strategy screeners | 5 |
| FA | AlphaLens.Fundamental | 8003 | Financials, ratios, sector analysis, thesis | 6 |
| VAL | AlphaLens.Valuation | 8004 | Damodaran DCF, intrinsic value, MoS | 4 |
| FOREN | AlphaLens.Forensic | 8005 | Fraud detection, red flags, investigation | 7 |

---

## Screen Inventory

### AlphaLens.ML (5 screens)

| Screen ID | Name | Purpose | Key Components |
|-----------|------|---------|----------------|
| ML-A | Daily Insight Hub | Morning 15-min scan — what to act on today | C-INSIGHT-CARD, C-ALERT-BANNER, C-REGIME-BADGE, C-SIGNAL-TABLE, C-POSITION-MONITOR |
| ML-B | Signal Deep Dive | Per-stock: why this signal, all model scores | C-SHAP-WATERFALL, C-MODEL-SCORES, C-CONFORMAL-CHART, C-INSIGHT-CARD-EXPANDED |
| ML-C | Multibagger Watchlist | Weekly top-20, survival curves, analogues | C-SURVIVAL-CURVE, C-ARCHETYPE-BADGE, C-ANALOGUE-CARD, C-INSIGHT-CARD |
| ML-D | Position Monitor | All held positions, exit urgency, P&L | C-POSITION-ROW, C-URGENCY-METER, C-EXIT-TYPE-BADGE, C-PNL-BAR |
| ML-E | Backtest Dashboard | Walk-forward results, integrity, benchmarks | C-FOLD-CHART, C-INTEGRITY-CHECKLIST, C-BENCHMARK-TABLE |

### AlphaLens.Technical (5 screens)

| Screen ID | Name | Purpose | Key Components |
|-----------|------|---------|----------------|
| TA-A | Interactive Chart | Candlestick + indicators + pattern annotations | C-CHART-CANVAS, C-INDICATOR-PANEL, C-PATTERN-BADGE, C-SR-LEVELS |
| TA-B | Strategy Screener | 42 pre-built templates + custom criteria | C-STRATEGY-CARD, C-CRITERIA-BUILDER, C-RESULTS-TABLE |
| TA-C | Multi-Stock Compare | Up to 5 stocks normalised, RS, correlation | C-OVERLAY-CHART, C-RS-CHART, C-CORRELATION-MATRIX |
| TA-D | Alert Manager | Create/edit price, indicator, pattern alerts | C-ALERT-FORM, C-ALERT-HISTORY |
| TA-E | Market Overview | Sector heatmap, breadth, advance/decline | C-SECTOR-HEATMAP, C-BREADTH-CHART, C-TOP-MOVERS |

### AlphaLens.Fundamental (6 screens)

| Screen ID | Name | Purpose | Key Components |
|-----------|------|---------|----------------|
| FA-A | Financial Dashboard | P&L + BS + CF trends, ratio analysis | C-FINANCIALS-TABLE, C-RATIO-CARD, C-TRAFFIC-LIGHT |
| FA-B | Peer Comparison | Side-by-side up to 8 peers, radar chart | C-PEER-TABLE, C-RADAR-CHART, C-RANKING-BAR |
| FA-C | Sector Deep-Dive | 12 sector-specific metric dashboards | C-SECTOR-METRICS, C-SECTOR-SELECTOR |
| FA-D | Fundamental Screener | Quality/GARP/turnaround/dividend templates | C-CRITERIA-BUILDER, C-RESULTS-TABLE |
| FA-E | Thesis Builder | Guided workflow → PDF export | C-THESIS-WIZARD, C-STRENGTHS-RISKS, C-BULL-BEAR |
| FA-F | Management Quality | Promoter, governance, RPT, auditor analysis | C-MGMT-SCORECARD, C-PLEDGE-CHART, C-RPT-TABLE |

### AlphaLens.Valuation (4 screens)

| Screen ID | Name | Purpose | Key Components |
|-----------|------|---------|----------------|
| VAL-A | Valuation Dashboard | DCF waterfall + assumptions + MoS | C-DCF-WATERFALL, C-ASSUMPTION-SLIDERS, C-MOS-THERMOMETER, C-SENSITIVITY-GRID |
| VAL-B | Relative Valuation | Multiples vs peers + regression line | C-MULTIPLE-TABLE, C-REGRESSION-SCATTER, C-HISTORY-RANGE |
| VAL-C | Batch Valuation | Universe ranked by margin of safety | C-VALUATION-TABLE, C-SECTOR-BUBBLE, C-BARGAIN-BASKET |
| VAL-D | Valuation Accuracy | Historical intrinsic vs actual price | C-ACCURACY-CHART, C-CALIBRATION-TABLE |

### AlphaLens.Forensic (7 screens)

| Screen ID | Name | Purpose | Key Components |
|-----------|------|---------|----------------|
| FOREN-A | Forensic Dashboard | Scores + 4-layer breakdown + trend | C-SCORE-RING, C-LAYER-BREAKDOWN, C-TREND-CHART, C-FLAG-BADGE |
| FOREN-B | Red Flag Drill-Down | 10 clickable investigation panels | C-REDFLAG-CARD, C-DRILL-PANEL, C-BENEISH-COMPONENTS |
| FOREN-C | Benford Visualization | Digit distribution vs expected | C-BENFORD-CHART, C-CHI2-TABLE, C-MAD-INDICATOR |
| FOREN-D | Cash Flow Deep Dive | CFO/NI trend, accruals, anomalies | C-CFO-NI-CHART, C-ACCRUAL-BAR, C-ANOMALY-HIGHLIGHT |
| FOREN-E | Peer Forensic Heatmap | Rows=companies, cols=metrics, color-coded | C-HEATMAP-GRID, C-OUTLIER-BADGE |
| FOREN-F | Investigation Report | Guided report builder → PDF | C-REPORT-WIZARD, C-FINDING-CARD, C-PATTERN-MATCH |
| FOREN-G | Universe Scan | All stocks ranked by risk, filterable | C-RISK-TABLE, C-SECTOR-SUMMARY, C-FLAG-DISTRIBUTION |

---

## Shared Component Library

### Insight Components (appear on recommended stocks only)

| Component ID | Name | Description |
|-------------|------|------------|
| C-INSIGHT-CARD | Insight Summary Card | Compact card: Entry ₹X · Returns +Y% · Duration Zd · Confidence W% |
| C-INSIGHT-CARD-EXPANDED | Insight Detail Card | Expanded: adds conformal interval, regime context, exit plan |
| C-ALERT-BANNER | Priority Alert Banner | CRITICAL (red) / HIGH (amber) / INFO (blue) with action button |
| C-REGIME-BADGE | Market Regime Badge | Bullish/Bearish/Sideways/Volatile with colour + day count |

### Data Display Components

| Component ID | Name | Description |
|-------------|------|------------|
| C-SIGNAL-TABLE | Signal Results Table | Sortable table with ticker, signal, probability, interval, meta, regime, P&D |
| C-SHAP-WATERFALL | SHAP Feature Attribution | Horizontal bar chart: features pushing toward buy/sell |
| C-MODEL-SCORES | All Model Scores Panel | Table: model name, score, status badge |
| C-SURVIVAL-CURVE | Survival Probability Curve | Horizontal bars: probability at 6/12/18/24/36/60 months |
| C-ARCHETYPE-BADGE | Multibagger Archetype | Badge: Long Base / Post-Crash / Quiet Accum / Sector Rotation |
| C-ANALOGUE-CARD | Historical Analogue | Mini card: stock name, entry year, return, duration |
| C-POSITION-ROW | Position Monitor Row | Ticker, P&L%, days held, urgency meter |
| C-URGENCY-METER | Exit Urgency Meter | Horizontal bar 0–100 with color gradient |
| C-EXIT-TYPE-BADGE | Exit Type Label | Thesis Broken / Momentum Exhaustion / P&D Exit / etc. |
| C-PNL-BAR | P&L Progress Bar | Green (profit) or red (loss) bar with % label |
| C-SCORE-RING | Forensic Score Circle | Circular gauge: 0–100 with flag color |
| C-FLAG-BADGE | Flag Status Badge | Green / Amber / Red with label |
| C-TRAFFIC-LIGHT | Ratio Traffic Light | Top quartile (green) / Middle (amber) / Bottom (red) |

### Chart Components

| Component ID | Name | Description |
|-------------|------|------------|
| C-CHART-CANVAS | Price Chart | Candlestick/OHLC/Line with zoom, pan, crosshair |
| C-INDICATOR-PANEL | Indicator Overlay Panel | Toggle indicators on/off, configure parameters |
| C-PATTERN-BADGE | Pattern Annotation | Chart overlay: pattern name + confidence + target |
| C-SR-LEVELS | Support/Resistance Lines | Horizontal lines with strength indicator |
| C-FOLD-CHART | Walk-Forward Fold Chart | Per-fold Sharpe bars with target line |
| C-BENFORD-CHART | Benford Distribution | Bar chart: actual digits vs expected Benford |
| C-DCF-WATERFALL | DCF Value Waterfall | Stacked bar: Revenue → EBIT → FCFF → PV → Intrinsic |
| C-RADAR-CHART | Peer Radar Chart | 6-axis radar: company vs peer median |
| C-SECTOR-HEATMAP | Sector Performance Map | Grid: sectors × metrics, color-coded by performance |
| C-CORRELATION-MATRIX | Stock Correlation Grid | NxN grid with color intensity |

### Navigation Components

| Component ID | Name | Description |
|-------------|------|------------|
| C-APP-HEADER | App Header Bar | App logo, app name, navigation tabs, status, time |
| C-CROSSLINK | Cross-App Link | "View in AlphaLens.Technical →" link on stock cards |
| C-SEARCH-BAR | Universal Stock Search | Type-ahead search across all apps |
| C-TAB-BAR | Screen Tab Navigation | Horizontal tabs within each app |

---

## Cross-App Navigation Map

When a user views a stock in one app, they see quick-links to the same stock in other apps:

```
AlphaLens.ML (RELIANCE signal) →
  "View Chart" → AlphaLens.Technical (RELIANCE chart)
  "View Financials" → AlphaLens.Fundamental (RELIANCE dashboard)
  "View Valuation" → AlphaLens.Valuation (RELIANCE DCF)
  "View Forensic" → AlphaLens.Forensic (RELIANCE score)
```

Cross-links rendered as: `AlphaLens.Technical → RELIANCE` with app icon + arrow.
Each link opens the target app's detail screen for that stock.

---

## Design Tokens (Light Theme)

```css
/* Colours */
--bg-primary: #FFFFFF;
--bg-secondary: #F8F9FC;
--bg-tertiary: #F0F2F8;
--border: #E2E6EF;
--border-hover: #C8CEE0;
--text-primary: #1A1D26;
--text-secondary: #5A6178;
--text-muted: #8E95A8;
--accent-teal: #0A9B8E;
--accent-blue: #3B6BF5;
--accent-purple: #6E56CF;
--accent-green: #1A9338;
--accent-amber: #C27A09;
--accent-red: #D13438;

/* Insight card specific */
--insight-entry: #3B6BF5;     /* Blue for entry point */
--insight-returns: #1A9338;   /* Green for returns */
--insight-duration: #6E56CF;  /* Purple for duration */
--insight-confidence: #0A9B8E;/* Teal for confidence */

/* Typography */
--font-sans: 'Inter', 'SF Pro Display', system-ui, sans-serif;
--font-mono: 'JetBrains Mono', 'SF Mono', monospace;

/* Spacing */
--radius-sm: 6px;
--radius-md: 10px;
--radius-lg: 14px;

/* Shadows */
--shadow-sm: 0 1px 3px rgba(0,0,0,0.06);
--shadow-md: 0 4px 12px rgba(0,0,0,0.08);
--shadow-lg: 0 8px 24px rgba(0,0,0,0.12);
```

---

## File Map

```
screens/
├── SCREEN_INVENTORY.md          ← This file
├── alphalens_ml.html            ← AlphaLens.ML (5 screens)
├── alphalens_technical.html     ← AlphaLens.Technical (5 screens)
├── alphalens_fundamental.html   ← AlphaLens.Fundamental (6 screens)
├── alphalens_valuation.html     ← AlphaLens.Valuation (4 screens)
└── alphalens_forensic.html      ← AlphaLens.Forensic (7 screens)
```
