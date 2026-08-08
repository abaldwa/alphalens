#!/usr/bin/env python3
import sys
sys.path.insert(0, '/home/amit/projects/AlphaLens')

from ingestion.scheduler.daily_pipeline import step_derive_fundamentals_ratios
from datetime import date

step_derive_fundamentals_ratios(date.today())
print('Done')
