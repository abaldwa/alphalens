# Forensic Accounting ML Model — Indian Companies

## Specification for Detecting Corporate Fraud, Accounting Manipulation, and Financial Red Flags

---

## Objective

Build an ML-based forensic accounting system that continuously monitors all 300+ stocks in the Indian equity universe for signs of financial manipulation, accounting fraud, and governance failure. The system should detect both common and uncommon fraud patterns, generate risk scores for every company, produce actionable alerts, and explain WHY each alert was triggered — enabling investors to exit positions BEFORE the fraud becomes public.

The system learns from India's rich history of corporate fraud — Satyam, DHFL, IL&FS, Yes Bank, PC Jeweller, Vakrangee, Manpasand Beverages, Bhushan Steel, Kingfisher Airlines, and others — by reverse-engineering the financial fingerprints that were visible YEARS before each collapse.

---

## The Indian Corporate Fraud Taxonomy

### Category 1: Revenue Manipulation

The most common form of fraud. Revenue is overstated to show growth that doesn't exist.

| Fraud Pattern | How It Works in India | Real Example | Detection Signals |
|-------------|---------------------|-------------|-------------------|
| **Fictitious revenue** | Create fake invoices, fake customers, or fake projects. Book revenue that was never earned. | Satyam: ₹7,136Cr of revenue was fabricated over multiple years | Revenue growing while operating cash flow stagnant or negative; receivables growing faster than revenue; no corresponding tax payments |
| **Channel stuffing** | Push excess inventory to distributors at quarter-end to inflate sales. Distributors return goods next quarter. | Common in FMCG and pharma (unnamed cases) | Spike in revenue in last month of quarter; high sales returns in first month of next quarter; inventory at distributor level rising |
| **Round-tripping** | Money flows out to a related party, then back as revenue. Creates the illusion of sales. | IL&FS: ₹29,000Cr lent to 66 related entities; DHFL: ₹11,000Cr through 87 shell firms | Revenue from related parties growing; cash flowing to related parties and back; complex web of subsidiaries/associates |
| **Premature revenue recognition** | Book revenue before delivery/service is complete. Common in infra/construction projects. | Common in Indian EPC/construction companies | Revenue growth with no proportionate milestone completions; unbilled revenue growing faster than revenue; low collection efficiency |
| **Bill-and-hold sales** | Invoice customer but hold the goods. Revenue recognized without physical delivery. | — | Inventory not declining despite revenue growth; receivables aging increasing; warehouse/freight costs flat despite revenue growth |

### Category 2: Expense Manipulation

Understating expenses to inflate profits, or capitalizing operating expenses to hide losses.

| Fraud Pattern | How It Works | Real Example | Detection Signals |
|-------------|------------|-------------|-------------------|
| **Expense capitalization** | Move operating costs (salaries, maintenance, marketing) to the balance sheet as assets. Inflates both profit and assets. | Satyam capitalized expenses as "intangible assets" | Fixed assets growing faster than revenue; unusually low depreciation relative to gross block; high capex but no corresponding revenue growth |
| **Understating provisions** | Reduce provision for bad debts, warranties, or contingencies to inflate profit. | Yes Bank: under-provisioned ₹20,000Cr+ in stressed exposures | Provision coverage ratio declining while industry average rising; NPA recognition delayed vs peer banks; qualified audit opinions mentioning provisioning |
| **Hiding losses in subsidiaries** | Transfer bad assets or losses to obscure subsidiaries that aren't consolidated, or consolidated with delay. | IL&FS: losses hidden across 300+ subsidiaries | Large number of subsidiaries (especially overseas); subsidiaries showing losses but parent showing profits; intercompany loans growing; some subsidiaries not audited by same auditor |
| **Depreciation manipulation** | Change depreciation policy (useful life, method) to reduce depreciation expense and inflate profit. | — | Change in depreciation policy disclosed in notes; depreciation/gross block ratio changing significantly YoY; policy change without corresponding industry change |

### Category 3: Balance Sheet Fraud

Fabricating or inflating assets, hiding liabilities.

| Fraud Pattern | How It Works | Real Example | Detection Signals |
|-------------|------------|-------------|-------------------|
| **Fictitious cash / bank balances** | Report cash that doesn't exist. The most brazen form of fraud. | Satyam: ₹5,040Cr of cash was completely fabricated | Cash balance very high but company raising debt; interest income inconsistent with reported cash balance; no bank confirmation of deposits |
| **Inflated receivables** | Create fake receivables from fake customers or delay write-offs. | PC Jeweller, Vakrangee | Receivable days increasing every quarter; receivables growing faster than revenue; large one-time write-offs (the correction) |
| **Inflated inventory** | Overvalue inventory or create fictitious stock to inflate assets. | Manpasand Beverages | Inventory days increasing; inventory growing faster than revenue; gross margin improving while revenue growth slows (suspicious) |
| **Hidden debt / off-balance-sheet** | Keep debt off the books via SPVs, guarantees, or structured instruments. | IL&FS (off-balance-sheet via 300+ entities); Kingfisher Airlines | Contingent liabilities growing in notes to accounts; corporate guarantees to subsidiaries; complex financial instruments in notes |
| **Fictitious fixed assets** | Inflate property, plant and equipment values. | Bhushan Steel: loans taken for capex but money diverted | Asset turnover declining; capex very high relative to peers but revenue not growing; physical verification discrepancies in audit report |

### Category 4: India-Specific Governance Fraud

Unique to the Indian promoter-driven corporate structure.

| Fraud Pattern | How It Works | Real Example | Detection Signals |
|-------------|------------|-------------|-------------------|
| **Promoter fund siphoning** | Promoter extracts cash from company via related party transactions, above-market salaries, or loans to promoter entities. | DHFL: promoters diverted ₹34,000Cr via shell companies | Related party transactions growing; loans to promoter entities; promoter salary/perks far above industry norm; complex promoter group structure |
| **Pledging and stealth exit** | Promoter pledges shares for personal loans, then company stock falls, triggering margin calls and forced selling — destroying retail investors. | Multiple mid-cap crashes (Essel Group, ADAG group) | Promoter pledge rising; pledged shares as % of total holding >20%; share price falling while pledge increasing = death spiral risk |
| **Circular shareholding** | Promoter entities hold shares in each other, inflating apparent promoter holding. Real economic stake is lower than reported. | Common in business groups | Multiple entities in promoter group holding cross-shares; complex holding structure; promoter group % doesn't decline even when entities sell |
| **Shell company transactions** | Use shell companies (no real business, minimal employees, same registered address) to route money. | DHFL: 87 shell firms; IL&FS: 66 related entities | High number of related party entities; entities at same address; entities with minimal turnover acting as major counterparties; entities incorporated recently |
| **Evergreening of loans (for NBFCs/banks)** | Restructure a borrower's loan before it becomes NPA, hiding the true asset quality. Give new loan to repay old loan. | Yes Bank: masked bad loans through evergreening | NPA ratio suspiciously low vs peers; restructured book growing; large borrowers receiving repeated "top-up" loans; divergence between reported and RBI-assessed NPAs |

### Category 5: Auditor and Governance Red Flags

Patterns in the governance structure that enable fraud.

| Fraud Pattern | What to Watch | Detection Signals |
|-------------|-------------|-------------------|
| **Auditor resignation/change** | Mid-term auditor change (not at natural rotation) or resignation = major red flag | auditor_change_flag + timing (mid-year vs year-end) |
| **Qualified audit opinion** | Auditor explicitly flags issues they can't resolve | qualified_audit_opinion_flag |
| **Emphasis of matter** | Auditor draws attention to specific risks without qualifying | Count of emphasis-of-matter paragraphs increasing |
| **Weak board independence** | Low % of independent directors, or independent directors who serve on many boards | Independent director ratio; independent directors with >5 boards; board meeting attendance <75% |
| **Whistle-blower complaints** | Under SEBI's vigil mechanism, whistle-blower complaints must be reported | Disclosure of whistle-blower complaints in annual report |
| **Frequent management changes** | Revolving door of CFOs, Company Secretaries, or auditors | director_resignation_count_4q > 2; CFO tenure < 2 years |

---

## Forensic Accounting Feature Set

### Group A: Classical Forensic Ratios (Beneish M-Score Components — 8 features)

The Beneish M-Score was specifically designed to detect earnings manipulation. Each component captures a different dimension of potential fraud. When the composite M-Score exceeds -1.78, the company is likely a manipulator.

| Feature Name | Formula | What It Detects | Fraud Signal |
|-------------|---------|----------------|-------------|
| `dsri` (Days Sales in Receivables Index) | `(Receivables_t/Revenue_t) / (Receivables_{t-1}/Revenue_{t-1})` | Revenue inflation via fake receivables | DSRI > 1.0 = receivables growing faster than revenue. >1.5 = strong fraud signal |
| `gmi` (Gross Margin Index) | `Gross_Margin_{t-1} / Gross_Margin_t` | Margin pressure leading to manipulation | GMI > 1.0 = margins deteriorating (incentive to manipulate). >1.2 = high risk |
| `aqi` (Asset Quality Index) | `(1 - (CA_t + PPE_t)/TA_t) / (1 - (CA_{t-1} + PPE_{t-1})/TA_{t-1})` | Expense capitalization, asset inflation | AQI > 1.0 = growing proportion of intangible/other assets. >1.3 = suspicious |
| `sgi` (Sales Growth Index) | `Revenue_t / Revenue_{t-1}` | Aggressive growth that may be fabricated | SGI > 1.2 combined with poor cash conversion = suspect |
| `depi` (Depreciation Index) | `Deprec_Rate_{t-1} / Deprec_Rate_t` (where rate = Deprec/(Deprec+PPE)) | Depreciation policy changes to boost profit | DEPI > 1.0 = depreciation slowing (artificially boosting profit) |
| `sgai` (SGA Expense Index) | `(SGA_t/Revenue_t) / (SGA_{t-1}/Revenue_{t-1})` | Cutting real expenses to manage earnings | SGAI < 0.8 = SGA declining disproportionately (may be under-investing or misclassifying) |
| `tata` (Total Accruals to Total Assets) | `(NI_t - CFO_t) / TA_t` | Earnings quality — are profits backed by cash? | High positive TATA = profits not backed by cash flows = potential manipulation |
| `lvgi` (Leverage Index) | `((LTD_t + CL_t)/TA_t) / ((LTD_{t-1} + CL_{t-1})/TA_{t-1})` | Leverage changes that may signal distress | LVGI > 1.0 = leverage increasing, which creates incentive to manipulate earnings |

**Composite Beneish M-Score**:
```
M = -4.84 + 0.920×DSRI + 0.528×GMI + 0.404×AQI + 0.892×SGI
    + 0.115×DEPI - 0.172×SGAI + 4.679×TATA - 0.327×LVGI
```
M > -1.78 → probable manipulator. Use both the composite score AND the individual components as features.

### Group B: Cash Flow Quality Features (10 features)

Cash flow is the hardest thing to fake. Accrual earnings can be manipulated; cash either exists or it doesn't.

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `cfo_to_net_income` | `CFO / Net_Income` (TTM) | Core cash quality. Should be >1.0 for healthy companies. Persistently <0.7 = earnings not backed by cash = RED FLAG |
| `cfo_net_income_divergence` | `(Revenue_Growth_YoY) - (CFO_Growth_YoY)` | Revenue growing but cash flow not growing = potential manipulation |
| `accrual_ratio` | `(Net_Income - CFO) / Total_Assets` | High accruals = earnings are accounting-based, not cash-based. Persistent high accruals = manipulation risk |
| `accrual_ratio_change` | `Accrual_Ratio_t - Accrual_Ratio_{t-4}` | Accruals increasing over time = worsening earnings quality |
| `cash_flow_variability` | `std(quarterly_CFO, 8_quarters) / mean(quarterly_CFO, 8_quarters)` | Highly variable CFO with smooth earnings = earnings are being managed |
| `fcf_to_revenue` | `Free_Cash_Flow / Revenue` (TTM) | Sustainable businesses generate FCF. Persistent negative FCF with positive earnings = invest or manipulate? |
| `capex_to_cfo_ratio` | `Capex / CFO` (TTM) | >1.0 means company spending more on capex than it generates from operations. May signal genuine growth OR asset inflation |
| `interest_income_vs_cash` | `(Interest_Income_Reported × 4 / Avg_Cash_Balance) - Market_FD_Rate` | If implied yield on cash is far from market rates, the cash may be fictitious (Satyam pattern) |
| `tax_paid_to_pbt_ratio` | `Tax_Paid_Cash / PBT` | Should approximate the effective tax rate. Low ratio = profits may be inflated (company paying less tax than reported profits suggest) |
| `operating_cash_cycle_change` | `(Inventory_Days + Receivable_Days - Payable_Days)_t - same_{t-4}` | Lengthening cash cycle with stable revenue = deteriorating working capital quality |

### Group C: Revenue Quality Features (8 features)

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `receivable_days_change` | `Receivable_Days_t - Receivable_Days_{t-4}` | Increasing receivable days = either deteriorating collections or fake revenue |
| `revenue_concentration_change` | Change in top-5 customer revenue concentration (from annual report) | Increasing concentration + increasing revenue = suspicious (dependent on few entities) |
| `unbilled_revenue_ratio` | `Unbilled_Revenue / Total_Revenue` | High unbilled revenue (>15%) = revenue recognized before invoicing = aggressive accounting |
| `deferred_revenue_decline` | `(Deferred_Revenue_t / Revenue_t) - (Deferred_Revenue_{t-1} / Revenue_{t-1})` | Declining deferred revenue ratio = company releasing reserves to boost current revenue |
| `revenue_vs_gst_proxy` | `Revenue_Growth_YoY - Industry_GST_Collection_Growth_YoY` | If company's revenue grows much faster than industry GST collections, the revenue may not be real |
| `cash_revenue_ratio` | `Cash_Received_From_Customers / Revenue` (from cash flow statement) | Should be close to 1.0. Persistently <0.85 = revenue not converting to cash |
| `other_income_ratio` | `Other_Income / Total_Income` | Sudden spike in other income = possible one-time items disguised as recurring revenue |
| `export_revenue_flag` | Binary: 1 if >50% revenue from exports AND no corresponding forex hedging losses/gains in volatile periods | Unverifiable revenue claim. Companies claiming high exports to unverifiable jurisdictions = red flag |

### Group D: Balance Sheet Quality Features (12 features)

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `inventory_days_change` | `Inventory_Days_t - Inventory_Days_{t-4}` | Rising inventory days = either weak demand or inventory inflation |
| `inventory_vs_revenue_growth` | `Inventory_Growth_YoY - Revenue_Growth_YoY` | Inventory growing faster than revenue = potential overstatement or obsolescence |
| `fixed_asset_turnover_change` | `Asset_Turnover_t - Asset_Turnover_{t-4}` | Declining asset turnover despite capex = assets may be inflated or unproductive |
| `goodwill_intangible_ratio` | `(Goodwill + Intangible_Assets) / Total_Assets` | High goodwill/intangibles (>30%) = potential write-down risk. Rising ratio = potential expense capitalization |
| `goodwill_growth_vs_acquisition` | `Goodwill_Growth - Value_of_Acquisitions_This_Year` | Goodwill growing without acquisitions = expense capitalization or overvalued past acquisitions |
| `cwip_ratio` | `Capital_Work_in_Progress / Gross_Block` | High CWIP (>30%) for extended periods = projects not completing = possible capitalization of expenses |
| `cwip_age` | Number of consecutive quarters CWIP has been >20% of gross block | CWIP stuck for 4+ quarters = red flag. Projects should complete and transfer to fixed assets. |
| `contingent_liability_ratio` | `Contingent_Liabilities / Net_Worth` | >50% = material off-balance-sheet risk. Growing ratio = growing hidden liabilities |
| `contingent_liability_growth` | YoY growth in contingent liabilities | Growing faster than business growth = accumulating off-balance-sheet risk |
| `subsidiary_count` | Total number of subsidiaries + associates + joint ventures | Very high count (>50) relative to company size = potential for hiding losses across entities |
| `subsidiary_loan_ratio` | `Loans_to_Subsidiaries / Total_Assets` | High intercompany loans = potential fund diversion or loss hiding |
| `loans_and_advances_to_related` | `Loans_to_Related_Parties / Total_Revenue` | Growing loans to related parties without business justification = fund siphoning |

### Group E: India-Specific Governance & Promoter Risk (15 features)

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `promoter_pledge_pct` | Pledged promoter shares / Total promoter shares × 100 | >20% = elevated risk. >40% = severe risk |
| `promoter_pledge_change` | QoQ change in pledge % | Rising pledge = worsening financial health of promoter entities |
| `promoter_salary_ratio` | `Promoter_Remuneration / Net_Profit` | >10% of net profit to promoter = excessive extraction (check SEBI guidelines) |
| `promoter_salary_vs_peers` | Z-score of promoter salary as % of revenue within sector | Significantly above peers = governance concern |
| `related_party_transaction_intensity` | `RPT_Total_Value / Revenue` × 100 | >10% and rising = significant governance risk |
| `rpt_growth_vs_revenue_growth` | `RPT_Growth_YoY - Revenue_Growth_YoY` | RPTs growing faster than revenue = increasing promoter extraction |
| `rpt_entity_count` | Number of unique related party entities transacted with | Very high count (>20) = complex web for potential round-tripping |
| `auditor_tenure` | Years with current statutory auditor | Very long tenure (>10 years pre-rotation law) = potential loss of independence |
| `auditor_change_mid_year` | Binary: 1 if auditor changed outside normal rotation | MID-YEAR auditor change (not at AGM) = SEVERE red flag |
| `audit_qualification_count` | Count of qualifications + emphasis of matter in last 3 audit reports | Increasing = auditor is increasingly concerned |
| `independent_director_ratio` | % of independent directors on board | Below SEBI minimum (1/3 for listed companies) = non-compliant governance |
| `id_resignation_recent` | Binary: 1 if an independent director resigned in last 2 quarters | ID resignation (especially with reasons citing governance) = serious red flag |
| `board_meeting_attendance` | Average attendance of directors at board meetings | <75% average = weak oversight |
| `cfo_tenure_months` | Months since current CFO was appointed | Very short CFO tenure (<12 months) + recent change = investigate why the previous CFO left |
| `whistle_blower_complaint_flag` | Binary: 1 if whistle-blower complaints were reported in annual report | Indicates internal concerns about fraud or governance |

### Group F: Benford's Law Features (5 features)

Apply Benford's Law to the company's reported financial numbers. Legitimate financial data follows a predictable distribution of first digits (1 appears ~30.1%, 2 ~17.6%, etc.). Manipulated data often deviates.

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `benford_revenue_chi2` | Chi-squared test statistic of first-digit distribution of last 20 quarterly revenue figures against Benford's expected distribution | Deviation from expected = potential manipulation of revenue numbers |
| `benford_expense_chi2` | Same for operating expenses | Manipulated expenses deviate from Benford's Law |
| `benford_receivables_chi2` | Same for quarterly receivable figures | Fictitious receivables often show non-Benford digit patterns |
| `benford_overall_deviation` | Average chi-squared across revenue, expenses, receivables, payables, and inventory | Composite Benford anomaly score |
| `benford_mad` | Mean Absolute Deviation of first-digit frequencies from Benford's expected (across all key financial line items) | MAD > 0.015 = non-conforming. MAD > 0.03 = significant deviation |

### Group G: Distress & Insolvency Features (8 features)

Fraud often occurs in companies under financial distress — they manipulate to survive.

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `altman_z_score` | `1.2×(WC/TA) + 1.4×(RE/TA) + 3.3×(EBIT/TA) + 0.6×(MktCap/TL) + 1.0×(Sales/TA)` | Z < 1.81 = distress zone. Distressed companies have the highest incentive to manipulate. |
| `altman_z_change` | Change in Z-score over 4 quarters | Declining Z-score = worsening financial health = rising manipulation incentive |
| `interest_coverage` | `EBIT / Interest_Expense` | <1.5 = company struggling to service debt. <1.0 = technically in default. |
| `debt_to_equity_change` | QoQ change in debt-to-equity ratio | Rapidly rising D/E = leverage distress building |
| `current_ratio_decline` | Change in current ratio (CA/CL) over 4 quarters | Declining current ratio = liquidity deteriorating. When combined with stable reported profits = suspect |
| `cash_burn_rate` | `Monthly_Operating_Cash_Outflow / Cash_Balance` | Months of cash remaining. <6 months = survival pressure = manipulation incentive |
| `debt_maturity_wall` | % of total debt maturing in next 12 months / Total debt | High proportion maturing soon with limited cash = refinancing risk = distress pressure |
| `promoter_pledge_spiral_risk` | `promoter_pledge_pct × (1 / stock_price_6m_return)` | High pledge + falling stock price = margin call death spiral risk |

### Group H: Cross-Validation & Consistency Features (10 features)

These features cross-check reported numbers against external or internally consistent benchmarks.

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `employee_productivity_anomaly` | `Revenue_Per_Employee vs Sector_Median` (z-score) | Revenue per employee far above sector median = either genuinely superior or fake revenue |
| `employee_cost_vs_headcount` | `(Employee_Cost_Growth - Headcount_Growth) / Headcount_Growth` | If employee cost grows much faster than headcount, salaries may be inflated (possibly to related parties) |
| `gst_vs_revenue_consistency` | Compare company's revenue growth with GST collection growth in its state/sector | Revenue growing much faster than GST collections for the sector = revenue may not be real |
| `roc_filing_vs_reported` | Compare revenue/profit in RoC (Registrar of Companies) filings vs BSE-reported results | Discrepancies between MCA filings and exchange filings = manipulation in at least one set |
| `peer_comparison_outlier_score` | Number of financial ratios where this company is >2 std deviations from sector median | Company is an outlier on 5+ ratios = either genuinely exceptional or manipulating |
| `tax_rate_vs_statutory` | `Effective_Tax_Rate - Statutory_Corporate_Tax_Rate` | Large persistent gap (paying much less tax) = either legitimate deductions or understated profits elsewhere |
| `dividend_vs_cash_flow` | `Dividends_Paid / CFO` | Paying dividends from borrowing (not cash flow) = unsustainable and potentially misleading about financial health |
| `capex_vs_loan_proceeds` | `Capex / (New_Borrowings_This_Year)` | Capex funded entirely by debt with no internal generation = may signal asset inflation (borrowed funds not actually going to capex) |
| `market_cap_vs_book_anomaly` | `(Market_Cap / Book_Value) percentile rank within sector` | Extremely high P/B relative to peers with no fundamental justification = either market bubble or inflated book value |
| `segment_revenue_consistency` | Variance in segment-wise revenue contribution over 8 quarters | Sudden unexplained shifts in segment revenue mix = potential reclassification to hide problems |

### Group I: Market Behavior & Technical Red Flags (8 features)

Price and volume behavior that often accompanies fraud — the market sometimes "knows" before the fraud is revealed.

| Feature Name | Formula | What It Detects |
|-------------|---------|----------------|
| `price_volume_divergence_long` | Correlation of 252-day price trend vs volume trend | Price rising on declining volume for extended period = weak foundations (potentially artificial price support) |
| `insider_selling_intensity` | `(Insider_Sell_Value_6m - Insider_Buy_Value_6m) / Market_Cap` | Net insider selling at scale = insiders know something the market doesn't |
| `short_interest_proxy` | `Futures_Short_Buildup_Days_Last_21d / 21` | Persistent short buildup = informed traders betting against the stock |
| `institutional_exit_rate` | `Max(FII_Holding_4Q_Ago, DII_Holding_4Q_Ago) - Current_Holding` | Institutional investors quietly exiting = smart money leaving |
| `abnormal_return_reversal` | Count of days with >5% daily return followed by >3% reversal within 5 days (last 252 days) | Frequent spike-and-reversal = potential manipulation or P&D |
| `stock_vs_sector_divergence` | `Stock_Return_252d - Sector_Return_252d` when divergence is extreme (>3 std) | Stock massively outperforming sector with no fundamental basis = suspicious |
| `vae_anomaly_score` | From the VAE model in the main system | Stock behaving completely unlike its historical self = regime change or manipulation |
| `hmm_regime_instability` | Average HMM stability score over last 63 days | Persistent low stability = stock's statistical behavior is abnormal |

---

## Composite Fraud Risk Scoring

### Multi-Layer Scoring Architecture

```
Layer 1: CLASSICAL MODELS (rule-based, always-on)
├── Beneish M-Score (8 components → single score)
├── Altman Z-Score (5 components → single score)
├── Benford's Law deviation (5 tests → composite)
└── Output: 3 classical risk indicators

Layer 2: ML FRAUD DETECTOR (trained on Indian fraud cases)
├── LightGBM classifier using all ~76 forensic features
├── Trained on: confirmed fraud cases (Satyam, DHFL, IL&FS, etc.)
│   as positive examples, clean companies as negative examples
├── Uses breakout-first learning: identify the fraud reveal date,
│   look back 2-4 years, learn the pre-fraud fingerprint
└── Output: Fraud probability (0–1) + top 5 SHAP risk factors

Layer 3: ANOMALY DETECTOR (unsupervised, catches unknown fraud types)
├── VAE trained on "normal" financial statement patterns
├── Isolation Forest on forensic feature vectors
├── Flags companies that are statistically unusual WITHOUT
│   needing to match a known fraud pattern
└── Output: Anomaly score (z-score)

Layer 4: GOVERNANCE RISK SCORER (rule + ML hybrid)
├── Weighted scoring of governance features
├── Promoter pledge risk model
├── Auditor risk flags (qualification, change, tenure)
├── Board independence and oversight quality
└── Output: Governance risk score (0–100)

COMPOSITE FORENSIC RISK SCORE = Weighted combination of all 4 layers
├── Classical (20% weight) — always reliable, interpretable
├── ML Fraud (40% weight) — most predictive, learns from real cases
├── Anomaly (20% weight) — catches novel fraud patterns
├── Governance (20% weight) — structural risk assessment
└── Output: FORENSIC RISK SCORE (0–100)
    0–20: Clean (green)
    21–40: Low risk (yellow)
    41–60: Moderate risk (orange) — investigate further
    61–80: High risk (red) — avoid or exit
    81–100: Extreme risk (black) — likely manipulation, BLOCK
```

---

## Training the Forensic ML Model

### Positive Examples (Confirmed Fraud Companies)

Build a comprehensive database of Indian corporate fraud cases:

| Company | Fraud Type | Year Revealed | Pre-Fraud Data Available |
|---------|-----------|---------------|------------------------|
| Satyam Computers | Fictitious revenue, fake cash | 2009 | 2003–2008 (6 years of pre-fraud financials) |
| DHFL | Fund siphoning, shell companies, fake accounts | 2019 | 2014–2018 (5 years) |
| IL&FS | Hidden debt, intercompany fraud | 2018 | 2013–2017 (5 years) |
| Yes Bank | Loan evergreening, under-provisioning | 2020 | 2015–2019 (5 years) |
| PC Jeweller | Inflated receivables, revenue manipulation | 2018 | 2013–2017 (5 years) |
| Vakrangee | Revenue inflation, related party fraud | 2018 | 2013–2017 (5 years) |
| Manpasand Beverages | Revenue fraud, inflated inventory | 2019 | 2014–2018 (5 years) |
| Bhushan Steel | Loan diversion, asset inflation | 2017 | 2012–2016 (5 years) |
| Kingfisher Airlines | Hidden losses, asset inflation | 2012 | 2007–2011 (5 years) |
| ADAG Group (multiple) | Complex inter-entity transactions, pledge spiral | 2018–2019 | 2013–2017 (5 years) |
| Gitanjali Gems | Round-tripping with PNB, fraudulent LCs | 2018 | 2013–2017 (5 years) |
| Ricoh India | Revenue manipulation | 2016 | 2011–2015 (5 years) |
| Cox & Kings | Fund diversion, inflated revenue | 2019 | 2014–2018 (5 years) |
| CG Power | Related party fraud, hidden debt | 2019 | 2014–2018 (5 years) |
| Karvy Stock Broking | Client securities misuse | 2019 | 2014–2018 (5 years) |
| + All companies that received SEBI fraud/manipulation penalties | Various | Various | 5 years pre-penalty |

**Source for confirmed fraud cases**: SEBI orders database, NCLT insolvency filings, forensic audit reports (published in media or SEBI orders), Serious Fraud Investigation Office (SFIO) case reports.

### Negative Examples (Clean Companies)

Select companies that have:
- 10+ years of clean audit opinions (no qualifications)
- Consistent CFO/Net Income ratio >0.8
- Stable promoter holding with zero pledge
- No SEBI actions or penalties
- Institutional (FII+DII) holding >30% (institutional due diligence provides implicit quality certification)

### Breakout-First Learning for Fraud

Apply the same methodology as the main signal models:

1. **Define the outcome**: Fraud revelation (stock drops >50% due to fraud disclosure)
2. **Identify the inflection point**: The date the fraud became public
3. **Look backward**: Study the company's financials 2–4 YEARS before the fraud reveal
4. **Learn the pre-fraud fingerprint**: Which forensic features were elevated?
5. **Train**: The model learns to recognize the same patterns in current data
6. **Explain**: Every alert comes with the specific features that match historical fraud patterns

---

## Integration with Main Trading System

### As a Pre-Filter (Blocking)

- Any stock with Forensic Risk Score > 60 is BLOCKED from all buy recommendations across the main system (signal models, multi-bagger model, journey simulation).
- If a held position's score rises above 60, an immediate EXIT alert is generated (exit type: "Forensic Risk — Thesis Broken").

### As a Feature (Enhancement)

- The `forensic_risk_score` (0–100) is added as a feature to all main system models. This allows the ML models to learn that companies with elevated forensic risk tend to underperform, even before fraud is revealed.
- The individual forensic components (Beneish components, cash flow quality, governance score) are available as features for the multi-bagger model — genuinely clean companies are disproportionately represented among multi-baggers.

### In the RL Agent

- The RL agent's state vector includes `forensic_risk_score` as an input.
- The RL agent learns to reduce position sizes in companies with rising forensic risk, even if other technical/fundamental signals are positive.
- A safety guardrail ensures the RL agent CANNOT override the forensic pre-filter (a stock blocked for forensic risk stays blocked regardless of RL conviction).

---

## Additional Classical Forensic Models

### Piotroski F-Score (Financial Strength Assessment — 9 components)

While Beneish detects manipulation and Altman predicts distress, the Piotroski F-Score measures fundamental financial STRENGTH. Fraudulent companies almost always have declining F-Scores BEFORE the fraud is revealed — the deteriorating fundamentals create the pressure to manipulate.

| Component | Criterion | Score = 1 if | What It Catches |
|-----------|----------|-------------|----------------|
| `f_roa` | Profitability | Net Income > 0 | Basic profitability. Unprofitable companies have highest fraud incentive. |
| `f_cfo` | Cash flow | CFO > 0 | Cash flow positive. Negative CFO with positive NI = accrual manipulation. |
| `f_delta_roa` | Improving profitability | ROA_t > ROA_{t-1} | Deteriorating ROA creates pressure to manipulate. |
| `f_accrual` | Earnings quality | CFO > Net Income | Cash exceeds accrual earnings. CFO < NI = earnings inflated by accruals. |
| `f_delta_leverage` | Deleveraging | LTD/TA_t < LTD/TA_{t-1} | Rising leverage = distress pressure. Deleveraging = improving health. |
| `f_delta_liquidity` | Improving liquidity | Current_Ratio_t > Current_Ratio_{t-1} | Deteriorating liquidity = survival pressure = manipulation incentive. |
| `f_no_dilution` | No equity dilution | Shares_t <= Shares_{t-1} | Frequent equity issuance = possible desperation for funds. |
| `f_delta_margin` | Improving margins | Gross_Margin_t > Gross_Margin_{t-1} | Declining margins create incentive to capitalize expenses or inflate revenue. |
| `f_delta_turnover` | Improving efficiency | Asset_Turnover_t > Asset_Turnover_{t-1} | Declining turnover with rising assets = possible asset inflation. |

**Composite F-Score**: Sum of 9 binary components (0–9). F-Score ≤ 2 = weak company with high fraud risk. F-Score ≥ 7 = strong, low fraud risk.

**Forensic use**: Track F-Score trajectory over 8 quarters. A company whose F-Score drops from 7 to 3 over 2 years is under mounting pressure — the PROBABILITY of manipulation rises with each quarter of F-Score decline.

### Ohlson O-Score (Bankruptcy Probability — 9 factors)

The Ohlson O-Score model analyzes four fundamental factors — company size, financial structure indicators, performance metrics, and current liquidity measures — using a logit model to assess their impact on the likelihood of bankruptcy.

```
O-Score = -1.32 - 0.407×log(TA/GNP_Deflator) + 6.03×(TL/TA)
          - 1.43×(WC/TA) + 0.0757×(CL/CA) - 1.72×X
          - 2.37×(NI/TA) - 1.83×(FFO/TL) + 0.285×Y - 0.521×Z

Where:
  X = 1 if TL > TA (negative book value), else 0
  Y = 1 if net loss for last 2 years, else 0
  Z = (NI_t - NI_{t-1}) / (|NI_t| + |NI_{t-1}|)  (change in net income)
  FFO = Funds from operations (NI + Depreciation)

Probability of bankruptcy = exp(O) / (1 + exp(O))
```

**Output features**:
- `ohlson_o_score`: Raw O-Score value
- `ohlson_bankruptcy_prob`: Probability of bankruptcy within 2 years
- `ohlson_prob_change_4q`: Change in bankruptcy probability over 4 quarters

### Dechow F-Score (Earnings Management — specifically designed for misstatement detection)

The Dechow F-Score (2011) was designed specifically to predict material accounting misstatements, not just manipulation or distress. It was trained on SEC Accounting and Auditing Enforcement Releases (AAERs).

| Component | Variable | What It Measures |
|-----------|---------|-----------------|
| `rsst_accruals` | Richardson-Sloan-Soliman-Tuna accruals (change in non-cash working capital + change in non-current operating assets + change in financial assets) / Avg Total Assets | Comprehensive accrual measure — captures ALL balance sheet manipulation, not just working capital |
| `change_receivables` | ΔReceivables / Avg Total Assets | Revenue inflation via receivables |
| `change_inventory` | ΔInventory / Avg Total Assets | Inventory inflation |
| `pct_soft_assets` | (Total Assets - Cash - PPE) / Total Assets | "Soft" assets (intangibles, goodwill, other) are easier to manipulate |
| `change_cash_sales` | % change in cash revenue (cash from customers / revenue) | Discrepancy between revenue growth and cash revenue growth |
| `change_roa` | ROA_t - ROA_{t-1} | Performance pressure |
| `issuance` | 1 if company issued equity or debt this year | Companies that issue securities have incentive to inflate results |
| `book_to_market` | Book value / Market cap | Low book-to-market firms face more pressure to meet market expectations |
| `abnormal_change_employees` | % change in employees - % change in assets | If assets grow much faster than employees, the "assets" may not be real |

**Composite Dechow F-Score**:
```
F = -7.893 + 0.790×rsst_accruals + 2.518×change_receivables
    + 1.191×change_inventory + 1.979×pct_soft_assets
    + 0.171×change_cash_sales - 0.932×change_roa
    + 1.029×issuance + 0.255×book_to_market
    - 0.189×abnormal_change_employees

Probability of misstatement = exp(F) / (1 + exp(F))
```

**This model is particularly valuable for India** because it captures "soft asset" manipulation (expense capitalization into intangibles/goodwill) and employee-asset divergence (fake capex without corresponding hiring), both of which were present in multiple Indian fraud cases.

### Sloan Accrual Anomaly Score

Sloan (1996) showed that high-accrual firms are more likely to have downside earnings surprises and that low-accrual firms tend to outperform their competitors. This is one of the most robust anomalies in finance — companies with high accruals consistently underperform.

| Feature Name | Formula | Notes |
|-------------|---------|-------|
| `sloan_accrual` | `(Net Income - CFO) / Total Assets` | The core Sloan accrual. >0.10 = high accruals = earnings NOT backed by cash = high risk of future earnings disappointment or manipulation |
| `sloan_accrual_percentile` | Percentile rank of `sloan_accrual` within sector | Top quintile = highest risk. Bottom quintile = highest quality. |
| `sloan_accrual_trend` | Slope of `sloan_accrual` over 8 quarters | Rising accruals over time = steadily worsening earnings quality |
| `balance_sheet_accrual` | `(ΔCA - ΔCash - ΔCL + ΔSTD + ΔTP - Depreciation) / Avg_TA` | Balance-sheet-based accrual (harder to manipulate than income statement accruals because it requires physical balance sheet changes) |

---

## Related Party Transaction (RPT) Knowledge Graph Analysis

(Out of scope for the initial 84-feature Groups A–I build — see "Industry-Specific Forensic Models" and "RPT Graph" sections for the full ~220-feature future scope.)

---

## Temporal Pattern Analysis — How Fraud Evolves Over Time

(Out of scope for the initial 84-feature Groups A–I build — trajectory/LSTM features are future scope.)

---

## Industry-Specific Forensic Models

(Out of scope for the initial 84-feature Groups A–I build — the ~93 industry-specific features across 14 sector sub-models are future scope.)

---

## Forensic Alert Severity Framework

### Alert Classification

| Severity | Score Range | Alert Type | User Action | System Action |
|----------|-----------|-----------|-------------|---------------|
| ⬜ **CLEAR** | 0–20 | No alert | None | Monitor quarterly |
| 🟡 **WATCH** | 21–40 | Quarterly review note | Review at next quarterly results | Add to watchlist, increase monitoring to monthly |
| 🟠 **WARNING** | 41–60 | Investigate further | Detailed forensic review recommended | Reduce confidence in all models for this stock by 30% |
| 🔴 **HIGH RISK** | 61–80 | Active alert | Consider exiting position | Block from new buys, generate exit recommendation for held positions |
| ⬛ **CRITICAL** | 81–100 | Urgent alert | Exit immediately | BLOCK from all systems, force exit alert with urgency=95, flag for all users |

### Compound Alert Escalation

Certain combinations of alerts should automatically escalate severity:

| Condition | Escalation |
|-----------|-----------|
| M-Score > -1.78 AND CFO/NI < 0.5 | +20 to forensic score |
| Promoter pledge > 30% AND pledge rising for 3+ quarters | +15 |
| Auditor changed mid-year AND independent director resigned in same quarter | +25 (almost certainly governance crisis) |
| RPT circular flow detected AND cash balance inconsistent with interest income | +30 (strong round-tripping signal) |
| F-Score declined by 4+ points over 8 quarters AND M-Score crossed threshold | +20 (classic fraud evolution pattern) |
| Company under ASM/GSM surveillance AND P&D score > 50 AND forensic score > 40 | +25 (manipulation + market manipulation) |

---

## Updated Summary

| Item | Count |
|------|-------|
| Forensic feature categories | 9 (original A–I) + 4 classical models (Piotroski, Ohlson, Dechow, Sloan) + RPT graph (12) + trajectory (8) + 14 industry sub-models (~93) |
| Total forensic features (full future scope) | **~220** |
| Core Groups A–I features (this build's scope) | **84** |
| Classical scoring models | 7 (Beneish, Altman, Ohlson, Piotroski, Dechow, Sloan, Benford) |
| Scoring layers | 4 (Classical, ML, Anomaly, Governance) — fused by Meta-Model |
| Historical fraud cases for training | 15+ major cases + all SEBI penalty cases |
| Alert severity levels | 5 (Clear, Watch, Warning, High Risk, Critical) |
| Compound escalation rules | 6 |
| Integration points with main system | 3 (Pre-filter, Feature, RL State) |
