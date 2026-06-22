# AlphaLens — Backtesting Architecture
## Walk-Forward Engine · Transaction Costs · Integrity Rules

---

## Non-Negotiable Rules

1. **Walk-forward only** — never random train/test split on time-series
2. **Point-in-time data** — announcement_date for fundamentals, filing_date for shareholding
3. **Survivorship bias** — include delisted stocks; forced exits at last traded price
4. **Transaction costs** — 0.40–0.50% round-trip (brokerage + STT + GST + slippage)
5. **Liquidity constraints** — skip if ADTV < ₹10L; max order = 5% of ADTV
6. **No HPO on test data** — Optuna runs on validation fold ONLY
7. **Fold stability** — report std(Sharpe) across folds; target < 0.5
8. **Deflated Sharpe Ratio** — apply DSR correction if testing 20+ configurations
9. **4 benchmarks** — beat Nifty buy-hold, equal-weight 50, 6m momentum, random 20-stock

---

## Walk-Forward Folds (5-year data)

```
Fold 1: Train 2021         → Test 2022
Fold 2: Train 2021–22      → Test 2023
Fold 3: Train 2021–23      → Test 2024
Fold 4: Train 2021–24      → Test 2025
Fold 5: Train 2021–25      → Test 2026
```

---

## WalkForwardBacktester Class

```python
class WalkForwardBacktester:
    def __init__(self, feature_store_path, model_trainer,
                 start_year=2021, min_train_years=1):
        self.feature_store_path = feature_store_path
        self.trainer = model_trainer
        self.start_year = start_year
        self.min_train_years = min_train_years

    def run(self) -> dict:
        data = self._load_all_features()  # includes delisted stocks
        years = sorted(data['year'].unique())
        for fold_idx, test_year in enumerate(years[self.min_train_years:]):
            train_data = data[data['year'] < test_year].copy()
            test_data  = data[data['year'] == test_year].copy()
            models = self.trainer.fit(train_data)         # train on past only
            fold_result = self._simulate_year(test_data, models, fold_idx)
        return self._aggregate_results()

    def _simulate_year(self, test_data, models, fold_id) -> dict:
        """Day-by-day simulation reproducing live pipeline exactly."""
        portfolio = Portfolio()
        for date in sorted(test_data['date'].unique()):
            today = test_data[test_data['date'] == date]
            prices = {r['ticker']: r['close'] for _, r in today.iterrows()}
            # Reproduce exact live pipeline order
            regimes  = models['hmm'].predict(today)
            pnd      = models['pnd'].predict_proba(today)
            blocked  = set(today[pnd[:, 1] > 0.6]['ticker'])
            allowed  = today[~today['ticker'].isin(blocked)]
            # Exit held positions
            for ticker in list(portfolio.positions):
                exit_score = models['exit'].predict(today[today['ticker']==ticker])
                if len(exit_score) and exit_score[0] > 80:
                    portfolio.sell(ticker, prices[ticker], date, 'exit_model')
            # Generate new buy signals
            if len(allowed):
                sig = models['signal_21d'].predict_proba(allowed)
                meta = models['meta'].predict_proba(allowed)
                buy_mask = (sig[:, 2] > 0.65) & (meta[:, 1] > 0.50)
                for _, row in allowed[buy_mask].nlargest(5, 'signal_strength').iterrows():
                    price = prices.get(row['ticker'])
                    if price:
                        size = compute_position_size(sig[...], meta[...], ...)
                        qty = int(size / price)
                        if qty > 0:
                            portfolio.buy(row['ticker'], price, qty, date)
        return self._compute_metrics(portfolio)
```

---

## Performance Metrics Targets

| Metric | Target | Direction |
|--------|--------|-----------|
| CAGR | > Nifty + 5% | Higher |
| Sharpe Ratio | > 1.0 | Higher |
| Sharpe std (fold stability) | < 0.5 | Lower |
| Max Drawdown | < 25% | Lower |
| Calmar Ratio | > 0.5 | Higher |
| Win Rate | > 55% | Higher |
| Profit Factor | > 1.5 | Higher |
| Signal Accuracy 5d | > 55% | Higher |
| Multibagger Precision@20 | > 25% hit 2x | Higher |

---

## Overfitting Detection

```python
def deflated_sharpe_ratio(sharpe, n_trials, n_obs):
    """Apply when testing 20+ configurations."""
    from scipy.stats import norm
    e_max = norm.ppf(1 - 1/n_trials)
    dsr = (sharpe - e_max / np.sqrt(n_obs))
    return float(norm.cdf(dsr))

def random_feature_test(model_class, train_data, test_data, n_repeats=10):
    """Model should score ~50% on shuffled features. >55% = noise fitting."""
    accs = []
    for _ in range(n_repeats):
        fake = train_data.copy()
        for col in [c for c in fake.columns if c not in ['date','ticker','target']]:
            fake[col] = np.random.permutation(fake[col].values)
        m = model_class(); m.fit(fake)
        acc = (m.predict(test_data) == test_data['target']).mean()
        accs.append(acc)
    return np.mean(accs)  # Should be ~0.50
```
