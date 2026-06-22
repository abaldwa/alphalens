# AlphaLens — Automation Specification
## Daily Pipeline · Retrain Automation · Monitoring Alerts

---

## Automation Inventory

| Automation | Trigger | Frequency | Platform |
|-----------|---------|-----------|----------|
| Daily pipeline runner | Cron 4:00 PM IST | Every trading day | Laptop |
| Option chain scraper | Cron 3:25 PM IST | Every trading day | Oracle Cloud |
| Bhavcopy downloader | Cron 4:05 PM IST | Every trading day | Oracle Cloud |
| PSI drift check | End of daily pipeline | Every trading day | Laptop |
| Model accuracy monitor | End of daily pipeline | Every trading day | Laptop |
| Weekly multibagger run | Cron Monday 5:45 PM | Weekly | Laptop |
| Model retrain scheduler | Triggered by PSI/accuracy | Per trigger | Laptop |
| AMFI MF holdings | Cron 5th of month | Monthly | Oracle Cloud |
| Oracle keep-alive | Cron every 3 min | Continuous | Oracle Cloud |
| Test suite | Pre-commit hook | Every commit | Laptop |

---

## Retrain Automation Logic

```python
# pipeline/quality/retrain_scheduler.py

class RetrainScheduler:
    """
    Automated retrain triggering based on drift and accuracy signals.
    Implements: snapshot → train → shadow_test → compare → promote protocol.
    """

    RETRAIN_RULES = {
        'signal_5d': {
            'scheduled_days': 30,          # Monthly
            'accuracy_threshold': 0.45,    # Below this = retrain
            'psi_threshold': 0.25,         # Severe drift = retrain
            'psi_window': 10,              # Days PSI must be high before triggering
        },
        'signal_21d': {'scheduled_days': 30, 'accuracy_threshold': 0.45, 'psi_threshold': 0.25, 'psi_window': 10},
        'signal_63d': {'scheduled_days': 90, 'accuracy_threshold': 0.42, 'psi_threshold': 0.25, 'psi_window': 10},
        'pnd_detector': {'scheduled_days': 90, 'accuracy_threshold': 0.70, 'psi_threshold': 0.30, 'psi_window': 5},
        'exit_signal': {'scheduled_days': 30, 'accuracy_threshold': 0.50, 'psi_threshold': 0.25, 'psi_window': 10},
        'multibagger': {'scheduled_days': 90, 'accuracy_threshold': 0.55, 'psi_threshold': 0.30, 'psi_window': 15},
        'hmm': {'scheduled_days': 30, 'accuracy_threshold': None, 'psi_threshold': 0.30, 'psi_window': 5},
    }

    def check_all_models(self, today: str, metric_history: dict) -> List[str]:
        """Returns list of model names that need retraining."""
        to_retrain = []
        for model_name, rules in self.RETRAIN_RULES.items():
            if self._should_retrain(model_name, rules, today, metric_history):
                to_retrain.append(model_name)
        return to_retrain

    def _should_retrain(self, model_name, rules, today, metrics) -> bool:
        m = metrics.get(model_name, {})
        # Scheduled retrain
        days_since = (pd.Timestamp(today) -
                      pd.Timestamp(m.get('last_retrain', '2021-01-01'))).days
        if days_since >= rules['scheduled_days']:
            return True
        # Accuracy trigger
        if rules['accuracy_threshold']:
            rolling_acc = m.get('rolling_63d_accuracy', 1.0)
            if rolling_acc < rules['accuracy_threshold']:
                return True
        # PSI trigger (must persist for N days)
        psi_breach_days = m.get('psi_severe_days', 0)
        if psi_breach_days >= rules['psi_window']:
            return True
        return False

    def execute_retrain(self, model_name: str, data_path: str) -> dict:
        """
        Execute full retrain with shadow testing.
        Returns: {'promoted': bool, 'accuracy_change': float, 'reason': str}
        """
        # Step 1: Snapshot current model
        current_model = ModelRegistry.load(model_name)
        current_acc = evaluate_model(current_model, get_last_63d_data())

        # Step 2: Train new model
        new_model = train_model(model_name, load_all_training_data(data_path))

        # Step 3: Shadow test on last 63 days
        new_acc = evaluate_model(new_model, get_last_63d_data())
        new_cal = evaluate_calibration(new_model, get_last_63d_data())
        new_shap_stability = compare_feature_ranks(current_model, new_model)

        # Step 4: Promote if better on 2 of 3 criteria
        acc_better = new_acc > current_acc
        cal_better = new_cal < 0.05  # Calibration error < 5%
        stable = new_shap_stability > 0.7  # Top-10 features mostly same

        criteria_met = sum([acc_better, cal_better, stable])
        if criteria_met >= 2:
            ModelRegistry.promote(model_name, new_model)
            return {'promoted': True, 'accuracy_change': new_acc - current_acc,
                    'reason': f"Criteria met: {criteria_met}/3"}
        else:
            ModelRegistry.archive_candidate(model_name, new_model)
            return {'promoted': False, 'accuracy_change': new_acc - current_acc,
                    'reason': f"Only {criteria_met}/3 criteria met — kept current model"}
```

---

## Pipeline Error Handling

```python
# scheduler/daily_pipeline.py

class PipelineRunner:
    STEPS = [
        ('download_bhavcopy',     'critical'),   # critical = stop pipeline if fails
        ('validate_data',         'critical'),
        ('adjust_corp_actions',   'critical'),
        ('compute_technical',     'critical'),
        ('compute_macro_pnd',     'warning'),    # warning = continue but alert
        ('load_quarterly',        'warning'),
        ('assemble_features',     'critical'),
        ('quality_check',         'critical'),
        ('run_hmm',               'critical'),
        ('run_pnd_filter',        'critical'),
        ('run_signal_models',     'critical'),
        ('run_meta_conformal',    'warning'),
        ('run_exit_signals',      'warning'),
        ('write_outputs',         'critical'),
    ]

    def run(self, date: str) -> PipelineResult:
        result = PipelineResult(date=date)
        for step_name, severity in self.STEPS:
            try:
                step_fn = getattr(self, step_name)
                step_fn(date)
                result.mark_success(step_name)
            except Exception as e:
                result.mark_failure(step_name, e)
                if severity == 'critical':
                    self.send_alert(f"PIPELINE HALTED at {step_name}: {e}")
                    return result
                else:
                    self.send_alert(f"Pipeline warning at {step_name}: {e}")
        result.finalize()
        return result
```

---

## Alert System

```python
# pipeline/alerts.py

class AlertManager:
    """Send alerts via multiple channels."""

    def send(self, alert_type: str, message: str, data: dict = None):
        alert = Alert(type=alert_type, message=message, data=data,
                       timestamp=datetime.now())
        # Log to file (always)
        self._log(alert)
        # Write to alerts/YYYY-MM-DD.json (always)
        self._persist(alert)
        # Console print (for development)
        self._console(alert)

    # Alert types and their routing
    ALERT_ROUTING = {
        'PIPELINE_FAILED':    ['log', 'file', 'console'],
        'PSI_SEVERE':         ['log', 'file', 'console'],
        'PSI_MODERATE':       ['log', 'file'],
        'RETRAIN_TRIGGERED':  ['log', 'file', 'console'],
        'EXIT_URGENT':        ['log', 'file', 'console'],
        'PND_BLOCKED':        ['log', 'file', 'console'],
        'FORENSIC_RED_FLAG':  ['log', 'file', 'console'],
    }
```

---

## Pre-commit Test Hook

```bash
# .git/hooks/pre-commit
#!/bin/bash
echo "Running AlphaLens test suite..."
python -m pytest tests/ -x -q --tb=short 2>&1
if [ $? -ne 0 ]; then
    echo "❌ Tests failed. Commit blocked."
    exit 1
fi
echo "✅ All tests passed."
```

---

## Crontab (Complete — Laptop Ubuntu)

```cron
# AlphaLens Laptop Crontab
# All times in IST (UTC+5:30)

# Daily pipeline — 4:00 PM IST (10:30 UTC)
30 10 * * 1-5 cd /home/user/alphalens && python scheduler/daily_pipeline.py >> data/logs/cron.log 2>&1

# Weekly multibagger — Monday 5:45 PM IST (12:15 UTC)
15 12 * * 1 cd /home/user/alphalens && python models/multibagger/weekly_run.py >> data/logs/cron.log 2>&1

# Model health check — 7:00 AM IST (01:30 UTC)
30 1 * * 1-5 cd /home/user/alphalens && python pipeline/quality/drift_monitor.py >> data/logs/cron.log 2>&1
```

## Crontab (Complete — Oracle Cloud ARM)

```cron
# AlphaLens Oracle Cloud Crontab
# All times UTC (IST = UTC+5:30)

# Option chain — 3:25 PM IST (09:55 UTC)
55 9 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/option_chain.py >> /home/ubuntu/alphalens/logs/oracle.log 2>&1

# Bhavcopy — 4:05 PM IST (10:35 UTC)
35 10 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/bhavcopy.py >> /home/ubuntu/alphalens/logs/oracle.log 2>&1

# F&O bhavcopy — 4:10 PM IST (10:40 UTC)
40 10 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/fno.py >> /home/ubuntu/alphalens/logs/oracle.log 2>&1

# FII/DII macro — 6:00 PM IST (12:30 UTC)
30 12 * * 1-5 python3 /home/ubuntu/alphalens/pipeline/ingest/macro.py >> /home/ubuntu/alphalens/logs/oracle.log 2>&1

# AMFI MF holdings — 5th of month 8 AM IST (2:30 UTC)
30 2 5 * * python3 /home/ubuntu/alphalens/pipeline/ingest/amfi_holdings.py >> /home/ubuntu/alphalens/logs/oracle.log 2>&1

# Keep-alive — every 3 minutes
*/3 * * * * python3 /home/ubuntu/alphalens/keep_alive.py
```
