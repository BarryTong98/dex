#!/bin/bash
# Setup crontab for DEX Analytics
# Run this script to automatically configure crontab

# Get the absolute path of the project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Detect Python path
PYTHON_PATH=$(which python3)

if [ -z "$PYTHON_PATH" ]; then
    echo "Error: python3 not found in PATH"
    exit 1
fi

echo "Project directory: $PROJECT_DIR"
echo "Python path: $PYTHON_PATH"
echo ""

# Create crontab entries
CRON_ENTRIES="# DEX Analytics - Daily ETL and Report Generation
# Added by setup_cron.sh on $(date)

# Run ETL every day at 9:00 AM UTC
0 9 * * * cd $PROJECT_DIR && $PYTHON_PATH scripts/daily_etl.py >> logs/etl.log 2>&1

# Generate report at 9:30 AM UTC (after ETL)
30 9 * * * cd $PROJECT_DIR && $PYTHON_PATH scripts/generate_report.py >> logs/etl.log 2>&1
"

echo "The following crontab entries will be added:"
echo "----------------------------------------"
echo "$CRON_ENTRIES"
echo "----------------------------------------"
echo ""

# Ask for confirmation
read -p "Add these entries to crontab? (y/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Backup existing crontab
    BACKUP_FILE="/tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt"
    crontab -l > "$BACKUP_FILE" 2>/dev/null || true
    echo "Backed up existing crontab to: $BACKUP_FILE"

    # Add new entries
    (crontab -l 2>/dev/null; echo "$CRON_ENTRIES") | crontab -

    echo "✅ Crontab entries added successfully!"
    echo ""
    echo "Current crontab:"
    crontab -l
    echo ""
    echo "To remove these entries later, run:"
    echo "  crontab -e"
    echo ""
    echo "To monitor execution:"
    echo "  tail -f $PROJECT_DIR/logs/etl.log"
else
    echo "Cancelled. No changes made to crontab."
    echo ""
    echo "To add manually, run:"
    echo "  crontab -e"
    echo ""
    echo "And add these lines:"
    echo "$CRON_ENTRIES"
fi
