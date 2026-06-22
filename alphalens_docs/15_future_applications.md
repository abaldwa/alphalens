# AlphaLens Platform — Future Application Requirements (Enriched)
## 4 Independent Apps · 1 Shared DataStore · Enriched from Reference Docs

**Source documents incorporated:**
- AlphaLens_Strategies_Reference.md → 42 strategies feeding APP-1 screener templates
- Damodaran_Valuation_Module.md → 8 models, 59 features, India-specific adjustments for APP-3
- Forensic_Accounting_ML_Specification.md → 84 features, 9 groups, 15+ Indian fraud cases for APP-4
- Fundamental_Data_Sourcing_Guide.md → Sector-specific sources and metrics for APP-2

---

## Architecture Rules (unchanged)

Each app runs on its own port (8001–8004), has its own FastAPI backend, frontend, tests,
requirements.txt, and Dockerfile. Only shared dependency: DataStore API (port 8000) + DataStoreClient SDK.
No app imports code from another. Cross-app data sharing via DataStore signals store only.

---

## APP-1: TECHNICAL ANALYSIS APPLICATION (Port 8001)

### Purpose
Interactive charting, pattern recognition, and 42-strategy screening system.

### SPEC-TA-001 · Interactive Charting Engine
- Chart types: candlestick (default), OHLC bar, line, Heikin-Ashi
- Timeframes: daily, weekly, monthly
- Chart background colour-coded by HMM regime from ML signals
- Volume bars below price chart with delivery % overlay
- Smooth zoom, pan, crosshair cursor
- Source: DataStore API `/api/v1/ohlcv/{ticker}` + `/api/v1/signals/ml/{ticker}`

### SPEC-TA-002 · Indicator Library (20+ indicators)
- Trend: SMA (20/50/100/200), EMA (8/13/21/34/55/89), Supertrend, Ichimoku Cloud, PSAR
- Momentum: RSI (14), MACD (12,26,9), Stochastic (14,3), CCI (20), Williams %R, MFI, ROC
- Volatility: Bollinger Bands (20,2), ATR (14), Keltner Channel, Donchian Channel (20/10)
- Volume: OBV, VWAP (intraday proxy), volume SMA(20), delivery % trend
- EMA Ribbon: 8/13/21/34/55/89 with alignment detection
- All computed from DataStore OHLCV or pre-computed features where available
- User-configurable parameters per indicator

### SPEC-TA-003 · Pattern Detection Engine
Auto-detect and annotate patterns on chart:
- Reversal: Head & Shoulders, Inverse H&S, Double Top/Bottom, Triple Top/Bottom
- Continuation: Flags, Pennants, Wedges (rising/falling), Rectangles, Triangles (ascending/descending/symmetric)
- Candlestick: Hammer, Shooting Star, Engulfing, Doji, Morning Star, Evening Star, Harami
- Breakout: Cup & Handle, Saucer Bottom, Darvas Box, IBD Base Pattern
- Each pattern: confidence score (0–100) + expected target price + historical accuracy

### SPEC-TA-004 · Support & Resistance
- Methods: pivot points (daily/weekly/monthly), horizontal from price clusters, trendline-based
- Anchored VWAP from key events (earnings, pivot highs, 52-week high)
- Display strength (how many times tested)
- Breakout alerts with volume confirmation

### SPEC-TA-005 · Custom Technical Screener with 42 Pre-Built Templates
User defines criteria: "RSI < 30 AND close > SMA200 AND volume_ratio > 2.0"
Runs against entire universe in < 5 seconds. Save/name configurations.

**Pre-built screener templates (from AlphaLens_Strategies_Reference.md):**

Category A — Technical Momentum (5 templates):
- A1: Opening Range Breakout + VWAP (orb_period=15min, volume≥1.5×, ATR stops)
- A2: Bollinger Squeeze Breakout (BB Width at 126d low, close > upper, vol≥1.8×)
- A3: MACD Histogram Divergence (10-bar divergence, RSI≥40, histogram rising)
- A4: Williams %R Mean Reversion (3-step rule: mark→wait 5d→enter at -85%)

Category B — Price Action & Pattern (5 templates):
- B1: Jesse Livermore Pivotal Points (15d consolidation <5% range, vol≥2×, pyramid 40/30/30)
- B2: Stan Weinstein Stage 2 (weekly close > rising 30wk SMA, RS new 52wk high)
- B3: IBD Base Pattern Breakout (cup-with-handle / double bottom / flat base, vol≥40% above avg)
- B4: Darvas Box (3-session box formation, breakout above box high)
- B5: Anchored VWAP Support (pullback to AVWAP, RSI 40–60, reversal candle)

Category C — Momentum (7 templates):
- C1: Time-Series Momentum (12m return > 0, close > SMA200, vol-scaled sizing)
- C2: Cross-Sectional Momentum (top 20% by 63d return, above SMA200, monthly rebalance)
- C3: Dual Momentum — Antonacci (absolute 12m > T-bill + relative top 20%)
- C4: CAN SLIM (C+A+N+S+L+I+M all 7 criteria, RS≥80, hard -8% stop)
- C5: 52-Week High Proximity (within 1%, vol≥2×, ADX≥20, trailing 8%)
- C6: EMA Ribbon Alignment (all 6 EMAs in bullish order, RSI 50–70)
- C7: Post-Earnings Drift (EPS surprise≥5%, gap≥2%, vol≥2×, hold 30–60d)

Category D — Reversal (4 templates):
- D1: Connors RSI-2 Mean Reversion (RSI(2)<10, above SMA200, exit at SMA5)
- D2: Long-Horizon Contrarian (bottom 10% by 3–5yr return, viable fundamentals)
- D3: MACD + RSI Dual Divergence (both divergence simultaneously, 10-bar lookback)
- D4: IBD Follow-Through Day (market correction ≥10%, day 4–7 close up ≥1.7%)

Category E — Trend Following & Systematic (8 templates):
- E1: Turtle / Donchian 20-10 (new 20d high, ADX≥15, 2×ATR stop, exit at 10d low)
- E2: Minervini Trend Template (all 8 SEPA criteria, RS≥70, -7% stop)
- E3: Piotroski F-Score (F≥7 buy, F≤2 avoid, annual rebalance)
- E4: Fama-French 5-Factor (size+value+profitability+investment, quarterly rebalance)
- E5: Sector Rotation (top 3 sectors by 63d RS, top 5 stocks per sector)
- E6: Earnings Acceleration (2+ quarters accelerating EPS + revenue, ROE>18%)
- E7: GARP + Dividend Yield (PEG<1.5, yield>2%, payout 30–60%, growth>12%)
- E8: Greenblatt Magic Formula (rank by Earnings Yield + Return on Capital, top 30–50)

Category F — Fundamental (8 templates):
- F1: Low P/E Quality (PE < sector median, ROE>15%, D/E<1.0)
- F2: High ROE + Low Debt (ROE>20%, D/E<0.5, mcap>₹5000Cr)
- F3: Dividend Aristocrat (yield>2%, payout 30–70%, 5yr growth, no cuts)
- F4: Earnings Compounder (3yr EPS CAGR>15%, ROE>18%, revenue CAGR>10%)
- F5: Cash Flow King (CFO/NI>1.2, 5yr FCF positive, FCF yield>4%)
- F6: Turnaround Play (ROE improving 2q+, D/E declining, revenue accelerating)
- F7: Promoter Confidence (holding>55%, increasing 2q, no pledge, ROE>15%)
- F8: Undervalued Growth PEG (PEG<1.0 (Lynch), EPS growth>20%, revenue>15%)

Category S — Core Technical Library (12 templates):
- S001 through S012: EMA Crossover, Supertrend Breakout, RSI Mean Reversion,
  52-Week High Breakout, VWAP Intraday Reversal, Ichimoku Cloud Breakout,
  Turtle 20-10, MACD Histogram, BB Squeeze, Fundamental Value+Momentum,
  Gap and Go, Sector Rotation Momentum

### SPEC-TA-006 · Technical Alerts
- Price alerts, indicator alerts, pattern alerts
- Evaluated daily after pipeline completes
- Dashboard notifications + optional email

### SPEC-TA-007 · Multi-Stock Comparison
- Compare up to 5 stocks normalised to 100 base
- Stock vs sector vs Nifty 50
- Relative strength chart and correlation matrix (rolling 63d)

### SPEC-TA-008 · Write-Back to DataStore
- ta_signals table: date, ticker, pattern_name, pattern_score, support, resistance,
  trend_direction, trend_strength, ta_buy_signal, ta_sell_signal

### Screens: TA-Screen-A (chart+overlays), TA-Screen-B (screener), TA-Screen-C (comparison),
TA-Screen-D (alert manager), TA-Screen-E (market overview heatmap)

### NFR: Chart <2s, screener <5s, pattern detection <10s, export PNG+CSV

---

## APP-2: FUNDAMENTAL ANALYSIS APPLICATION (Port 8002)

### Purpose
Deep-dive fundamental research with 12 sector-specific modules.

### SPEC-FA-001 · Financial Statement Dashboard
- Income statement: revenue, EBITDA, PAT, EPS — quarterly + annual, 5yr trend
- Balance sheet: assets, equity, debt, cash — snapshot + trend
- Cash flow: CFO, CFI, CFF — waterfall chart
- All in ₹ Crores with growth rates (YoY, QoQ, CAGR)
- Anomaly highlighting: revenue jump without CFO support, inventory build-up
- Source: DataStore fundamentals API (Screener.in data)

### SPEC-FA-002 · Ratio Analysis Engine
Profitability: gross/operating/EBITDA/net margin, ROE, ROCE, ROA
Efficiency: asset turnover, inventory/receivable/payable days, CCC
Leverage: D/E, interest coverage, debt/EBITDA, CF to debt
Valuation: PE, PB, PEG, EV/EBITDA, mcap/sales, dividend yield
Liquidity: current ratio, quick ratio
Each ratio: current value, 5yr range, sector average, sector rank, traffic light coding

### SPEC-FA-003 · Peer Comparison Engine
- Auto-select peers from stock_master (sector + mcap proximity)
- Side-by-side table up to 8 peers + radar chart
- Ranking per metric within peer group

### SPEC-FA-004 · Sector-Specific Analysis Modules (12 sectors)
Each sector has unique metrics from Tijori Finance + sector regulators:

| Sector | Key Metrics | External Data Source |
|--------|-------------|---------------------|
| Banking/NBFC | GNPA, NNPA, CASA, NIM, cost-to-income, provision coverage, credit cost, CAR/CRAR, advance growth, NPA divergence (RBI vs reported) | RBI quarterly reports |
| Insurance | Combined ratio, solvency ratio, claim settlement, persistency, AUM growth, VNB margin, embedded value | IRDAI reports |
| IT Services | Revenue/employee, utilisation, attrition, TCV pipeline, offshore/onsite mix, top-client concentration, digital revenue % | Company investor presentations |
| Pharma | R&D/revenue, ANDA pipeline, USFDA observations, API vs formulation mix, chronic vs acute, MR productivity, NLEM exposure | FDA warning letters database |
| FMCG | Volume vs price growth, distribution reach, rural vs urban mix, category market share, ad spend/revenue, same-store growth | Tijori Finance |
| Auto | Capacity utilisation, ASP trend, order book months, export %, EV transition %, dealer inventory days, EBITDA/vehicle | Vahan monthly registrations |
| Infrastructure/EPC | Order book/revenue ratio, order inflow growth, execution rate, debtor days, retention money, working capital days | Company disclosures |
| Metals/Commodities | Realisation/tonne, cost/tonne, EBITDA/tonne, capacity utilisation, reserve life, hedging position | Industry associations |
| Chemicals | Capacity additions, specialty vs commodity mix, customer concentration, import substitution, China+1 score | Tijori Finance |
| Telecom | ARPU, subscriber adds, churn rate, data usage/subscriber, tower tenancy, spectrum holding, capex/subscriber | TRAI quarterly reports |
| Power/Energy | PLF, cost of generation, PPA tenure, fuel mix (renewable %), T&D loss, regulatory asset | CEA generation reports |
| Real Estate | Pre-sales value, collections efficiency, net debt/equity, unsold inventory months, launch pipeline, carpet area price | Company disclosures |

### SPEC-FA-005 · Management Quality Scoring (0–100 composite)
- Promoter holding trend + pledge level and trend
- RPT as % of revenue + RPT growth vs revenue growth
- Auditor continuity + qualification history
- Board independence ratio + director attendance
- Promoter remuneration as % of PAT vs sector peers
- Capital allocation track record (buybacks, dividends vs empire-building)

### SPEC-FA-006 · Investment Thesis Builder
- Guided workflow with structured questions
- Auto-populated strengths (top-quartile metrics) and risks (bottom-quartile + forensic flags)
- Bull/base/bear cases (user-guided)
- Pulls valuation from APP-3 if running
- Exportable as PDF, saved in DataStore

### SPEC-FA-007 · Fundamental Screener (5 pre-built + custom)
- Quality compounder (high ROE, low debt, consistent margins, promoter>50%)
- Turnaround (improving margins, debt reduction, new management)
- GARP (PEG<1.5, revenue growth>20%, ROE>15%)
- Dividend aristocrats (yield>3%, 5yr consistent+growing payout)
- Cash-rich micro-caps (cash>30%×mcap, no debt, promoter>60%)

### SPEC-FA-008 · Write-Back to DataStore
- fa_signals: date, ticker, quality_score, growth_score, mgmt_quality_score,
  sector_rank, investment_thesis_id, fa_rating

### Screens: FA-Screen-A (financial dashboard), FA-Screen-B (peer comparison),
FA-Screen-C (sector deep-dive), FA-Screen-D (fundamental screener),
FA-Screen-E (thesis builder), FA-Screen-F (management quality)

### NFR: Dashboard <3s, peer comparison <3s, screener <5s, thesis exportable as PDF

---

## APP-3: DAMODARAN VALUATION APPLICATION (Port 8003)

### Purpose
Full Damodaran methodology — 6 lifecycle stages, 8 valuation models, 59 features,
India-specific adjustments, Monte Carlo DCF, narrative features, value enhancement tracking.

### SPEC-VAL-001 · Lifecycle Classification (6 stages)
Classify every stock into Damodaran's lifecycle stages:
- Stage 1 YOUNG_GROWTH: revenue growth >30%, margin <10%, no dividends, age <10yr
- Stage 2 HIGH_GROWTH: revenue 3yr CAGR >15%, margin >8%, reinvestment >10%, ROE >12%
- Stage 3 MATURE_GROWTH: revenue 5–15%, margin >5%, payout >15%
- Stage 4 MATURE_STABLE: revenue <8%, margin >5%, payout >30%
- Stage 5 DECLINING: revenue <2% or margin <50% of sector median
- Stage 6 DISTRESSED: negative margin OR interest coverage <1.5 OR Altman Z <1.81
- FINANCIAL_SERVICES: Banks, NBFCs, Insurance, AMCs — separate models (no FCFF/WACC)

User can override classification with justification.

### SPEC-VAL-002 · 8 Valuation Models (selected by lifecycle)

| # | Model | Used For | Key Formula |
|---|-------|---------|-------------|
| 1 | FCFF 2-Stage | High Growth, Mature Growth | EV = PV(FCFF_high_growth) + PV(Terminal_Value). FCFF = EBIT(1-t) + D&A - Capex - ΔNWC |
| 2 | FCFF 3-Stage | Young Growth | Revenue-based. Revenue→target margin→EBIT→FCFF. 3 phases: high→transition→stable |
| 3 | FCFE | Mature Stable | Equity Value = PV(FCFE) + PV(Terminal_Equity). FCFE = NI + D&A - Capex - ΔNWC + Net New Debt |
| 4 | Excess Return (Banks) | Financial Services | Value = BV_equity + PV(Excess Returns). Excess = (ROE - CoE) × BV. NO FCFF for banks |
| 5 | Relative/Regression | All (cross-check) | PE = a + b×Growth + c×Payout + d×Beta. Predicted PE vs actual → under/overvalued |
| 6 | Option Pricing (Merton) | Distressed | Equity = call option on firm value. V×N(d1) - D×e^(-rT)×N(d2) |
| 7 | Commodity Normalized | Metals, Oil & Gas | Use normalised EBIT (10yr avg margin × current revenue) not current EBIT |
| 8 | Monte Carlo DCF | All (probability) | 10,000 simulations sampling growth, margin, WACC from distributions |

### SPEC-VAL-003 · Cost of Capital — India-Specific
- Risk-free rate = India 10yr G-Sec yield MINUS India default spread (Damodaran method)
  - July 2025 published data: G-Sec 6.32%, default spread 2.16%, risk-free 4.16%
- Equity Risk Premium = Mature Market ERP (~4.2%) + India Country Risk Premium (~2.3%) = ~6.5%
- Beta = Industry unlevered beta (from Damodaran annual dataset) relevered: β_levered = β_unlevered × (1 + (1-t) × D/E). Blume adjustment: β_adj = 0.67×β_raw + 0.33×1.0
- Lambda (company-specific country risk): λ = domestic_revenue_pct / avg_domestic_pct. Adjusts CRP for exporters (IT companies face less India risk)
- Synthetic rating for unrated companies: ICR→rating→default spread (Damodaran table from ICR>12.5=AAA to ICR<0.5=C/D)
- WACC = E/(D+E)×CoE + D/(D+E)×CoD×(1-t) with market-value weights

### SPEC-VAL-004 · India-Specific Valuation Adjustments
- Cross-holdings: Value = Operating DCF + Market Value of Cross-Holdings × (1 - Conglomerate Discount). Discount 10–25% for Indian groups (Tata, Reliance, Adani, Birla, Mahindra)
- Governance discount: 0% (gold-standard) to 30–50% (near-fraud). Maps from forensic_risk_score
- Currency mismatch: Flag if >50% revenue in foreign currency but debt 100% INR
- Ind AS 116 lease adjustment for pre/post comparability

### SPEC-VAL-005 · Monte Carlo DCF
- 10,000 simulations sampling: growth (triangular), margin (triangular), WACC (normal ±1%), failure probability (binomial for young companies)
- Outputs: median, P10, P90, probability_undervalued, value_at_risk_5pct
- 5 Monte Carlo features: mc_probability_undervalued, mc_valuation_range, mc_downside_risk, mc_upside_potential, mc_skew

### SPEC-VAL-006 · Narrative Features (from "Narrative and Numbers")
- narrative_type: classify from earnings calls (Growth Story / Turnaround / Cash Cow / Disruption / Platform / Capacity Expansion / Market Share Gain)
- narrative_consistency: same narrative across 4 quarters (0–1)
- narrative_vs_numbers_gap: if "Growth Story" but revenue <10%, gap is HIGH (0–100)
- management_credibility_score: historical guidance accuracy (0–100)
- tam_penetration: revenue / estimated TAM
- competitive_advantage_proxy: ROIC persistence (top quartile 3+ years)
- reinvestment_efficiency: revenue growth / reinvestment rate
- narrative_inflection_flag: narrative type changed this quarter (binary)

### SPEC-VAL-007 · Value Enhancement Tracking (Damodaran's 4 levers)
- Lever 1: margin improvement QoQ (increase cash flows from existing assets)
- Lever 2: reinvestment quality change QoQ = Δ(ROIC × reinvestment rate)
- Lever 3: cost of capital change QoQ = ΔWACC (negative = improving)
- Lever 4: moat strength = ROIC persistence (competitive advantage proxy)
- value_enhancement_score: count of levers improving (0–4)

### SPEC-VAL-008 · 59 Valuation Features for ML
- 15 core: intrinsic_value, valuation_gap_pct, margin_of_safety, wacc, cost_of_equity, roic_minus_wacc, roic_wacc_spread_trend, ev_to_intrinsic_ev, terminal_value_pct, implied_terminal_pe, relative_pe_gap, pb_vs_roe_residual, earnings_yield_minus_bond_yield, lifecycle_stage, valuation_gap_percentile
- 6 scenario: intrinsic_value_base/bull/bear, valuation_range_width, current_price_vs_scenarios, scenario_skew
- 8 narrative: as above
- 5 India-specific: lambda_country_risk, cross_holdings_value_pct, conglomerate_complexity, holding_company_discount, governance_valuation_discount
- 5 commodity/cyclical: earnings_cyclicality, cycle_position, normalized_pe, commodity_price_position, economic_cycle_position
- 4 young company: tam_penetration, revenue_to_breakeven_trajectory, burn_rate_months, probability_of_failure
- 5 Monte Carlo: as above
- 5 value enhancement: as above
- 4 DDM: ddm_intrinsic_value, ddm_vs_dcf_gap, dividend_sustainability_score, dividend_growth_rate_sustainable
- 1 currency: currency_mismatch_risk
- 1 pipeline: pipeline_value_ratio (infra/real estate)

### SPEC-VAL-009 · Annual Damodaran Dataset Download
- 11 datasets from pages.stern.nyu.edu/~adamodar: Betas, country risk premiums, WACC by industry, margins, ROE, reinvestment rates, EV/EBITDA, PE ratios, rating spreads, tax rates, dividend/FCFE data
- Download script runs every January after Damodaran publishes annual update

### SPEC-VAL-010 · Write-Back to DataStore
- valuation_signals: date, ticker, lifecycle_stage, intrinsic_value, valuation_gap_pct, margin_of_safety, wacc, cost_of_equity, terminal_value_pct, dcf_model_type, scenario_bull/base/bear, mc_probability_undervalued, value_enhancement_score

### Screens: VAL-Screen-A (DCF waterfall + assumptions + sliders + MoS thermometer),
VAL-Screen-B (relative valuation vs peers + regression), VAL-Screen-C (batch valuation ranked by MoS),
VAL-Screen-D (historical intrinsic vs actual price accuracy)

### NFR: Single stock <3s, batch 500 stocks <30min, slider override <500ms, PDF/Excel export

### Honest Caveats
- Small changes in terminal growth or WACC produce large intrinsic value changes
- Sensitivity table (±1% WACC × ±1% growth) always displayed
- India ERP and risk-free rate are estimates — labelled as such
- Historical accuracy tracked and displayed honestly
- Terminal value >80% of total = heavily dependent on long-term assumptions

---

## APP-4: FORENSIC ACCOUNTING APPLICATION (Port 8004)

### Purpose
ML-driven forensic scoring (from M-09/M-10) PLUS manual investigation tools.
The full investigation workbench for when ML flags something suspicious.

### SPEC-FOREN-001 · Forensic Score Dashboard (ML-Driven Layer)
- Display M-09 classical scores + M-10 ML ensemble outputs from DataStore
- Composite score (0–100) with 4-layer breakdown:
  - Layer 1 Classical (20%): Beneish M-Score, Altman Z, Piotroski F, Benford MAD
  - Layer 2 ML Fraud (40%): LightGBM trained on Indian fraud cases
  - Layer 3 Anomaly (20%): Isolation Forest anomaly z-score
  - Layer 4 Governance (20%): Promoter/audit/board risk composite
- Flag levels: Green (0–20), Yellow (21–40), Orange (41–60), Red (61–80), Black (81–100)
- 12-quarter score trend chart

### SPEC-FOREN-002 · Indian Fraud Taxonomy (5 categories)
System presents red flags categorised by fraud type with drill-down:

Category 1 — Revenue Manipulation:
- Fictitious revenue (Satyam pattern): revenue growing but CFO stagnant
- Channel stuffing: quarter-end revenue spikes, high returns next quarter
- Round-tripping (IL&FS/DHFL pattern): RPTs cycling cash as revenue
- Premature recognition: unbilled revenue >15% of total

Category 2 — Expense Manipulation:
- Expense capitalisation (Satyam pattern): fixed assets growing faster than revenue
- Under-provisioning (Yes Bank pattern): provision coverage declining vs industry
- Hiding losses in subsidiaries (IL&FS pattern): 300+ subsidiaries, losses not consolidated

Category 3 — Balance Sheet Fraud:
- Fictitious cash (Satyam: ₹5,040Cr fake): interest income inconsistent with cash balance
- Inflated receivables (PC Jeweller/Vakrangee): receivable days 3× sector average
- Inflated inventory (Manpasand): inventory days rising, gross margin improving (suspicious)
- Hidden debt / off-balance-sheet (IL&FS): contingent liabilities growing, complex SPVs

Category 4 — India-Specific Governance:
- Promoter fund siphoning (DHFL: ₹34,000Cr via shell companies): RPT growing, loans to promoter entities
- Pledge spiral (Essel/ADAG): pledge >20% + falling price = margin call death spiral
- Circular shareholding: cross-holdings inflating apparent promoter stake
- Shell company transactions: high RPT entity count, entities at same address

Category 5 — Auditor & Governance Red Flags:
- Mid-year auditor change (not rotation): severe red flag
- Qualified audit opinion
- Independent director resignation with governance concerns
- CFO tenure <12 months + recent change

### SPEC-FOREN-003 · Red Flag Investigation Drill-Down (10 clickable panels)

| # | Red Flag | Drill-Down Content |
|---|----------|-------------------|
| 1 | High Beneish M-Score | 8 M-Score components individually: which one is driving the score (DSRI, GMI, AQI, SGI, DEPI, SGAI, TATA, LVGI) |
| 2 | Low CFO/NI ratio | CFO vs NI comparison over 8 quarters. Accrual quality analysis. Should be >0.8 |
| 3 | Receivable days spike | Trend vs revenue growth. Peer comparison. If 3× sector avg = critical |
| 4 | Promoter pledge spiral | Pledge trend with price overlay. Cascade risk: "If price falls 10%, estimated forced selling of X%" |
| 5 | Auditor change/qualification | Full qualification text. Auditor change history timeline |
| 6 | RPT concentration | RPTs as % of revenue. Nature of transactions. Entity count and relationship map |
| 7 | Benford deviation | Visual digit distribution vs expected Benford curve (see SPEC-FOREN-004) |
| 8 | Revenue-cash divergence | Revenue line vs CFO line chart. Divergence = suspect |
| 9 | Goodwill/intangible growth | Goodwill + intangibles as % of total assets over time. Impairment history |
| 10 | CWIP stuck | CWIP as % of gross block for 8+ quarters. Projects not completing = potential capitalisation |

### SPEC-FOREN-004 · Benford's Law Visualisation
- Bar chart: actual first-digit distribution vs expected Benford (30.1%, 17.6%, 12.5%...)
- Line items analysed: revenue, expenses, receivables, inventory, other income
- Chi-squared test with p-value per line item
- MAD (Mean Absolute Deviation): >0.015 = non-conforming, >0.03 = significant
- Compare this company vs sector average Benford conformity

### SPEC-FOREN-005 · Cash Flow Quality Deep Dive
- CFO/NI ratio trend (8 quarters) — should be >0.8
- Accrual ratio: (NI - CFO) / Total Assets — high = low quality
- Interest income vs cash: implied yield vs market FD rate (Satyam test)
- Tax paid / PBT: should approximate effective tax rate
- Working capital changes: receivables/payables/inventory manipulation detection
- FCF vs reported earnings trend: divergence = investigate
- Anomaly auto-highlighting: "CFO/NI dropped from 0.9 to 0.3 in Q3"

### SPEC-FOREN-006 · Peer Forensic Heatmap
- Rows = companies (stock + 5–8 peers), columns = forensic metrics
- Colour = red/amber/green per metric
- Outlier identification: "Company X has receivable days 3× sector average"

### SPEC-FOREN-007 · Historical Fraud Case Library
15+ confirmed Indian fraud cases with pre-fraud financial fingerprints:
Satyam, DHFL, IL&FS, Yes Bank, PC Jeweller, Vakrangee, Manpasand, Bhushan Steel,
Kingfisher Airlines, ADAG Group, Gitanjali Gems, Ricoh India, Cox & Kings,
CG Power, Karvy Stock Broking + all SEBI fraud penalty recipients

Each case: fraud type, year revealed, pre-fraud financial signals that were visible,
how the system would have detected it.

### SPEC-FOREN-008 · Investigation Report Builder
- Structured report: executive summary, red flags detail, peer comparison,
  historical pattern, pattern match to known fraud, risk assessment, recommendation
- Exportable as PDF
- Saved in DataStore for audit trail

### SPEC-FOREN-009 · Forensic Watchlist & Alerts
- User adds stocks to forensic watchlist
- Auto-alert: flag change (green→amber, amber→red), Beneish M > -1.78,
  pledge increase >5%/quarter, auditor change, CFO/NI < 0.5

### SPEC-FOREN-010 · Universe Forensic Scan
- Full universe ranked by composite score (highest risk first)
- Filter by: flag, sector, tier, specific red flag type
- Export flagged companies as CSV
- Summary: red/amber/green count per tier and sector

### SPEC-FOREN-011 · Write-Back to DataStore
- forensic_investigations: date, ticker, investigator_notes, risk_assessment,
  recommendation, report_pdf_path
- Manual "false positive" feedback stored for ML model improvement

### SPEC-FOREN-012 · 84 Forensic Features (9 Groups)
All features from the Forensic_Accounting_ML_Specification.md:
- Group A: Beneish M-Score components (8) — DSRI, GMI, AQI, SGI, DEPI, SGAI, TATA, LVGI
- Group B: Cash flow quality (10) — CFO/NI, accrual ratio, FCF/revenue, interest_income_vs_cash, tax_paid/PBT, operating_cash_cycle_change, etc.
- Group C: Revenue quality (8) — receivable_days_change, unbilled_revenue_ratio, cash_revenue_ratio, revenue_vs_gst_proxy, etc.
- Group D: Balance sheet quality (12) — inventory_days, goodwill_ratio, CWIP_ratio, contingent_liability_ratio, subsidiary_count, loans_to_related, etc.
- Group E: India governance & promoter risk (15) — pledge, salary, RPT, auditor, board independence, CFO tenure, whistle-blower, etc.
- Group F: Benford's Law (5) — chi2 on revenue/expense/receivables, overall deviation, MAD
- Group G: Distress indicators (8) — Altman Z, interest coverage, debt changes, cash burn, debt maturity wall, pledge spiral risk
- Group H: Cross-validation consistency (10) — employee productivity, GST vs revenue, RoC vs reported, peer outlier score, tax rate anomaly, etc.
- Group I: Market behaviour flags (8) — price-volume divergence, insider selling, institutional exit, abnormal return reversal, sector divergence, etc.

### Screens: FOREN-Screen-A (score dashboard), FOREN-Screen-B (red flag drill-down),
FOREN-Screen-C (Benford visualisation), FOREN-Screen-D (cash flow deep dive),
FOREN-Screen-E (peer heatmap), FOREN-Screen-F (report builder), FOREN-Screen-G (universe scan)

### NFR: Dashboard <3s, Benford <2s, universe scan <30s, PDF export, false positive feedback stored

---

## CROSS-APPLICATION SIGNAL FLOW

```
TA App → ta_signals (patterns, S/R) → ML Engine reads as features (Phase 3+)
FA App → fa_signals (quality, mgmt) → ML Engine reads as features (Phase 4+)
Valuation App → valuation_signals (gap, MoS) → ML Engine reads as features (Phase 3+)
Forensic App → forensic_investigations → FA App reads for thesis risk section
ML Engine → ml_signals (regime, P&D) → TA App reads for chart context
ML Engine → ml_forensic (scores, flags) → Forensic App reads as starting point
Valuation App → intrinsic_value → FA App reads for thesis valuation section
```

## PHASE ALIGNMENT

| App | Build Phase | Prerequisite |
|-----|:-----------:|-------------|
| APP-1 Technical Analysis | Phase 3 (Weeks 27–32) | OHLCV + features in DataStore |
| APP-3 Damodaran Valuation | Phase 3 (Weeks 33–38) | Fundamentals + macro in DataStore |
| APP-4 Forensic Accounting | Phase 3 (Weeks 36–40) | M-09/M-10 scores in signals store |
| APP-2 Fundamental Analysis | Phase 4 (Weeks 39–48) | All data sources flowing, Tijori operational metrics |
