# AlphaLens Cleanup & Organization Guide

**Last Updated**: 2026-08-23  
**Organized By**: Reorganization Task

## Overview

The AlphaLens project directory has been reorganized to separate production data, backups, and temporary files. This guide explains the new structure and how to maintain it.

## Directory Structure

```
/home/amit/projects/AlphaLens/
├── _archive_temp/          ← TEMP: Safe to delete anytime (38M currently)
│   ├── README.md
│   ├── *.log               ← Execution logs from experiments
│   ├── *.csv               ← Analysis and comparison CSVs
│   ├── catboost_info/      ← ML training artifacts
│   └── backtest_cache/     ← Regenerable feature cache
│
├── _backups/               ← BACKUPS: Organized backup storage
│   ├── README.md
│   ├── database/           ← Full database backups (compressed)
│   │   └── alphalens_critical_backup_20260823_160807.tar.gz (14GB)
│   ├── metadata/           ← Configuration exports
│   │   └── queue_*.json
│   └── archive/            ← Old/superseded backups
│
├── logs/                   ← LOGS: Centralized execution logs
│   ├── README.md
│   ├── *.log               ← Current logs (≤30 days old)
│   └── archive/            ← Old logs (>30 days, 80 files)
│
├── datastore/              ← PRODUCTION: Live databases (145GB)
│   ├── normalised/         ← OHLCV, fundamentals, delivery
│   ├── signals/            ← Trading signals database
│   └── backtest_store/     ← Backtest results (excluded from backup)
│
└── [other source directories]
```

## Key Directories

### `_archive_temp/` — Temporary Files (38M)
**Purpose**: Staging area for files to delete  
**Contents**:
- Root-level execution logs (*.log)
- Analysis CSVs (*.csv) from one-off experiments
- CatBoost training artifacts
- Backtest cache spillover

**Cleanup Policy**:
- ✅ Safe to delete anytime — nothing depends on these files
- Can be regenerated if needed (cache rebuilt on next run, logs not needed for production)
- Suggested: Delete weekly/monthly

**Commands**:
```bash
# Delete all temp files
rm -rf _archive_temp/*

# Delete specific category
rm -rf _archive_temp/*.log        # Logs
rm -rf _archive_temp/*.csv        # Analysis CSVs
rm -rf _archive_temp/catboost_info/
rm -rf _archive_temp/backtest_cache/
```

### `_backups/` — Backup Storage (14GB database + metadata)

#### `/database/` — Full Database Backups
- **Current Backup**: `alphalens_critical_backup_20260823_160807.tar.gz` (14GB)
- **Contents**: OHLCV, fundamentals, signals, pipeline logs, model registry
- **Compression**: 40% (23GB → 14GB)
- **Also Stored On**: Backblaze B2 (`backblaze:AlphaLensDataBackUp/`)
- **Monthly Cost**: $0.024 (4GB overage on free tier)

**When to Create**:
- Before major feature releases
- Before risky database migrations
- After quarterly data validation

**Restore Procedure**:
```bash
cd ~
tar -xzf ~/projects/AlphaLens/_backups/database/alphalens_critical_backup_*.tar.gz
# Restores to: ~/.local/share/AlphaLens/data/
```

#### `/metadata/` — Configuration Backups
- Queue definitions (`queue_*.json`)
- Strategy configurations
- Export checkpoints

**Update**: After each queue definition or strategy change

#### `/archive/` — Old Backups
- Previous backup versions
- Keep for 90 days minimum (retention policy)
- Delete after 90 days if no longer needed

**Cleanup**:
```bash
# Archive old backups (older than 90 days)
find _backups/archive -type f -mtime +90 -delete
```

### `logs/` — Centralized Execution Logs (8.6GB)

**Current Logs** (≤30 days): 71 files  
**Archived Logs** (>30 days): 80 files  

**Automatic Archiving**:
- Logs older than 30 days are automatically moved to `logs/archive/`
- Happens daily via cleanup script

**Compression** (Optional):
```bash
# Compress archived logs to save space
tar -czf logs/archived_logs_20260823.tar.gz logs/archive/
rm -rf logs/archive/*.log      # Delete after compression verified
```

**Retention Policy**:
- Keep current logs: ≤30 days (active debugging)
- Keep archived logs: 30-90 days (incident analysis)
- Delete logs: >90 days old

## Cleanup Automation

### Manual Cleanup Script

Run anytime to clean up temporary files and organize logs:

```bash
# Run cleanup
/home/amit/projects/AlphaLens/scripts/cleanup_temp_files.sh

# This will:
# 1. Delete regenerable temp files
# 2. Archive logs older than 30 days
# 3. Report cleanup summary
```

### Automated Cleanup (Optional)

Add to crontab to run weekly:

```bash
# Run cleanup every Sunday at 2 AM
crontab -e

# Add this line:
0 2 * * 0 cd /home/amit/projects/AlphaLens && ./scripts/cleanup_temp_files.sh
```

## Backblaze B2 Details

**Account**: d8e81059c4c7  
**Bucket**: `AlphaLensDataBackUp`  
**Current File**: `alphalens_critical_backup_20260823_160807.tar.gz` (14GB)  
**Monthly Cost**: $0.024 storage (4GB overage) + download egress

### Upload New Backup

```bash
# Copy local backup to B2
rclone copy _backups/database/alphalens_critical_backup_*.tar.gz backblaze:AlphaLensDataBackUp/

# Verify upload
rclone ls backblaze:AlphaLensDataBackUp --human-readable

# Remove old B2 backups (optional)
rclone delete backblaze:AlphaLensDataBackUp/alphalens_critical_backup_20260816_*.tar.gz
```

## Space Summary

| Directory | Size | Purpose | Cleanup Policy |
|-----------|------|---------|---|
| **datastore/** | 145G | Production databases | Keep (live) |
| **_backups/** | 14G | Backup storage | Keep for 90 days |
| **logs/** | 8.6G | Execution logs | Archive >30d, delete >90d |
| **_archive_temp/** | 38M | Temporary files | Delete anytime |
| **Total** | 178G | — | — |

## Git Ignore

Added to `.gitignore`:
```
_archive_temp/
_backups/
```

These directories will not be tracked in version control (as intended — they're local artifacts, not source code).

## Maintenance Calendar

| Frequency | Task | Command |
|-----------|------|---------|
| **Weekly** | Clean temp files | `./scripts/cleanup_temp_files.sh` |
| **Monthly** | Archive old logs | `find logs -mtime +30 -exec mv {} logs/archive/ \;` |
| **Quarterly** | Delete archived logs >90d | `find logs/archive -mtime +90 -delete` |
| **After Releases** | Create backup | Manual: `tar -czf _backups/database/backup_*.tar.gz ...` |
| **After Releases** | Upload to B2 | `rclone copy _backups/database/* backblaze:AlphaLensDataBackUp/` |

## FAQ

**Q: Can I delete everything in `_archive_temp/`?**  
A: Yes, anytime. Files are regenerable or analysis artifacts.

**Q: What if I need logs from 6 months ago?**  
A: Archived logs in `logs/archive/` can be restored from Backblaze B2 backup if critical.

**Q: Do I have to pay for the B2 backup?**  
A: Minimal cost (~$0.024/month for overage). You can reduce backup size if needed.

**Q: Can I delete `_backups/archive/`?**  
A: Only if older than 90 days. Keep at least one backup for recovery.

**Q: How do I restore from backup?**  
A: See "Restore Procedure" above. Extracts to `~/.local/share/AlphaLens/data/`.

## Related Documentation

- [_archive_temp/README.md](_archive_temp/README.md) — Temporary files guide
- [_backups/README.md](_backups/README.md) — Backup storage guide
- [logs/README.md](logs/README.md) — Logging and archival guide
- [scripts/cleanup_temp_files.sh](scripts/cleanup_temp_files.sh) — Automated cleanup script
