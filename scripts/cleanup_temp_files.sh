#!/bin/bash
# Cleanup Script for AlphaLens Temporary Files
# Safely removes regenerable and temporary files to reclaim disk space
# Safe to run weekly/monthly - all files can be regenerated if needed

set -e

PROJECT_ROOT="/home/amit/projects/AlphaLens"
cd "$PROJECT_ROOT"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/cleanup_${TIMESTAMP}.log"

# Create log file
mkdir -p logs
touch "$LOG_FILE"

echo "=== AlphaLens Cleanup Started ===" | tee -a "$LOG_FILE"
echo "Timestamp: $TIMESTAMP" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to safely delete directory
delete_safely() {
    local path=$1
    local description=$2

    if [ -d "$path" ] || [ -f "$path" ]; then
        local size=$(du -sh "$path" 2>/dev/null | awk '{print $1}')
        echo -e "${YELLOW}Deleting${NC}: $description ($size)" | tee -a "$LOG_FILE"
        rm -rf "$path"
        echo -e "${GREEN}✓ Deleted${NC}: $description" | tee -a "$LOG_FILE"
    fi
}

# Function to delete files older than N days
delete_old_files() {
    local dir=$1
    local days=$2
    local pattern=${3:- "*"}
    local description=$4

    if [ -d "$dir" ]; then
        local count=$(find "$dir" -maxdepth 1 -type f -name "$pattern" -mtime +$days 2>/dev/null | wc -l)
        if [ $count -gt 0 ]; then
            echo -e "${YELLOW}Deleting${NC}: $description (>$days days old, $count files)" | tee -a "$LOG_FILE"
            find "$dir" -maxdepth 1 -type f -name "$pattern" -mtime +$days -delete
            echo -e "${GREEN}✓ Deleted${NC}: $description" | tee -a "$LOG_FILE"
        fi
    fi
}

echo "CLEANUP TASKS:" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# 1. Clean temporary files (safe to delete anytime)
echo "1. Cleaning temporary files..." | tee -a "$LOG_FILE"
delete_safely "_archive_temp/backtest_cache" "backtest cache spill"
delete_safely "_archive_temp/catboost_info" "CatBoost training artifacts"
delete_old_files "_archive_temp" 0 "*.log" "execution logs"
delete_old_files "_archive_temp" 0 "*.csv" "analysis CSVs"
echo "" | tee -a "$LOG_FILE"

# 2. Archive old logs (>30 days)
echo "2. Archiving old logs (>30 days)..." | tee -a "$LOG_FILE"
if [ -d "logs" ]; then
    OLD_LOG_COUNT=$(find logs -maxdepth 1 -type f -name "*.log" -mtime +30 2>/dev/null | wc -l)
    if [ $OLD_LOG_COUNT -gt 0 ]; then
        echo "  Found $OLD_LOG_COUNT logs to archive" | tee -a "$LOG_FILE"
        find logs -maxdepth 1 -type f -name "*.log" -mtime +30 -exec mv {} logs/archive/ \; 2>/dev/null || true
        echo -e "${GREEN}✓ Moved${NC} old logs to archive/" | tee -a "$LOG_FILE"
    fi
fi
echo "" | tee -a "$LOG_FILE"

# 3. Optional: Compress archived logs (>90 days)
echo "3. Compressing archived logs..." | tee -a "$LOG_FILE"
if [ -d "logs/archive" ]; then
    ARCHIVE_COUNT=$(find logs/archive -type f -name "*.log" -mtime +90 2>/dev/null | wc -l)
    if [ $ARCHIVE_COUNT -gt 0 ]; then
        echo "  Found $ARCHIVE_COUNT archived logs older than 90 days" | tee -a "$LOG_FILE"
        echo -e "${YELLOW}Note${NC}: Consider deleting logs older than 90 days (retention policy)" | tee -a "$LOG_FILE"
        # Uncomment to auto-delete: find logs/archive -type f -name "*.log" -mtime +90 -delete
    fi
fi
echo "" | tee -a "$LOG_FILE"

# 4. Summary
echo "CLEANUP SUMMARY:" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

ARCHIVE_SIZE=$(du -sh _archive_temp 2>/dev/null | awk '{print $1}')
BACKUP_SIZE=$(du -sh _backups/database 2>/dev/null | awk '{print $1}')
LOGS_SIZE=$(du -sh logs 2>/dev/null | awk '{print $1}')
TOTAL_SIZE=$(du -sh . --exclude=.git --exclude=.venv --exclude=node_modules 2>/dev/null | awk '{print $1}')

echo "Space allocated:" | tee -a "$LOG_FILE"
echo "  • Temporary files: $ARCHIVE_SIZE" | tee -a "$LOG_FILE"
echo "  • Backup files: $BACKUP_SIZE" | tee -a "$LOG_FILE"
echo "  • Log files: $LOGS_SIZE" | tee -a "$LOG_FILE"
echo "  • Total project: $TOTAL_SIZE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

echo -e "${GREEN}=== Cleanup Completed Successfully ===${NC}" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"
