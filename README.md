# DEX Analytics Dashboard

Automated daily ETL pipeline to analyze DEX (Decentralized Exchange) usage across multiple blockchains with interactive HTML reporting.

## Features

- **Multi-chain Support**: BSC, Ethereum, Base, and Solana
- **Automated Daily ETL**: Processes previous day's data every morning
- **Persistent Storage**: Uses DuckDB for efficient data storage
- **Interactive Reports**: HTML dashboard with date and chain selectors
- **Historical Tracking**: Accumulates data over time for trend analysis

## Project Structure

```
dex/
├── config/
│   └── config.yaml          # Configuration file
├── utils/
│   ├── __init__.py
│   └── utils.py            # Data processing utilities
├── data/
│   └── dex_analytics.duckdb # DuckDB database (created on first run)
├── reports/
│   └── index.html          # Generated interactive report
├── scripts/
│   ├── init_database.py    # Initialize database schema
│   ├── daily_etl.py        # Daily ETL script
│   └── generate_report.py  # Report generation script
├── logs/
│   └── etl.log            # ETL execution logs
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Installation

### Prerequisites

- Python 3.8+
- Access to parquet data files at `/server/data/parquet/`

### Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize the database**:
   ```bash
   python scripts/init_database.py
   ```

3. **Verify installation**:
   ```bash
   ls -la data/
   # Should see dex_analytics.duckdb
   ```

## Usage

### First-Time Setup: Initialize Historical Data

**🆕 NEW: Initialization Mode** - Process multiple days of historical data at once:

```bash
# 1. Enable init mode in config/config.yaml
vi config/config.yaml

# Set:
# init:
#   enabled: true
#   days: 7  # Process last 7 days
#   auto_generate_report: true

# 2. Run ETL (will process 7 days + auto-generate report)
python3 scripts/daily_etl.py
```

**What happens:**
- ✅ Processes last 7 days of data (configurable)
- ✅ Automatically generates HTML report
- ✅ Auto-disables init mode after completion
- ⏱️ Takes ~10-20 minutes for 7 days

See [docs/INIT_MODE.md](docs/INIT_MODE.md) for detailed guide.

### Manual Execution

**Run ETL for yesterday's data**:
```bash
python scripts/daily_etl.py
```

**Generate HTML report**:
```bash
python scripts/generate_report.py
```

**View report**:
```bash
# Open in browser
open reports/index.html
# Or for server without GUI
python -m http.server 8000 --directory reports/
# Then visit http://your-server:8000
```

### Automated Execution with Crontab

**Setup crontab**:
```bash
# Edit crontab
crontab -e

# Add these lines (adjust paths as needed):
# Run ETL every day at 9:00 AM UTC
0 9 * * * cd /server/share/barry/dex && /usr/bin/python3 scripts/daily_etl.py >> logs/etl.log 2>&1

# Generate report at 9:30 AM UTC (after ETL completes)
30 9 * * * cd /server/share/barry/dex && /usr/bin/python3 scripts/generate_report.py >> logs/etl.log 2>&1
```

**Verify crontab**:
```bash
crontab -l
```

**Monitor execution**:
```bash
tail -f logs/etl.log
```

## Configuration

Edit `config/config.yaml` to customize:

```yaml
chains:
  - bsc
  - eth
  - base
  - sol

data:
  parquet_base_path: "/server/data/parquet"
  database_path: "data/dex_analytics.duckdb"
  reports_path: "reports"

time:
  daily_run_hour: 9
  daily_run_minute: 0

exclusions:
  tokens:
    - "0xe6DF05CE8C8301223373CF5B969AFCb1498c5528"  # Test token
```

## Interactive Report Features

The generated HTML report includes:

- **Chain Selector**: Switch between BSC, ETH, Base, and Sol
- **Date Selector**: View any historical date or latest data
- **View Types**:
  - Daily View: Aggregated daily statistics
  - Hourly View: Hour-by-hour breakdown
  - Total Statistics: All-time cumulative data
- **Charts**:
  - DEX Usage Distribution (Pie/Bar charts)
  - DEX Usage Over Time (Trend lines)
  - Top DEX Rankings (Horizontal bar chart)
- **Summary Statistics**: Total usage, unique orders, active DEXes

## Database Schema

**dex_usage_hourly**
- Primary key: (chain_id, date, hour, dex_name)
- Tracks DEX usage by hour

**dex_usage_daily**
- Primary key: (chain_id, date, dex_name)
- Tracks DEX usage by day

**dex_usage_total**
- Primary key: (chain_id, dex_name)
- Cumulative statistics across all time

**etl_run_log**
- Tracks ETL execution history and status

## Troubleshooting

### No data in report
```bash
# Check if database exists
ls -la data/dex_analytics.duckdb

# Check database content
python3 << EOF
import duckdb
conn = duckdb.connect('data/dex_analytics.duckdb')
print(conn.execute("SELECT COUNT(*) FROM dex_usage_daily").fetchone())
conn.close()
EOF
```

### Parquet files not found
- Verify parquet_base_path in config.yaml
- Check file permissions: `ls -la /server/data/parquet/chain=bsc/`

### Crontab not running
```bash
# Check crontab logs
tail -100 logs/etl.log

# Verify cron service is running
service cron status

# Test script manually
cd /server/share/barry/dex && python3 scripts/daily_etl.py
```

## Development

### Running in Jupyter Notebook

Create a notebook in `notebooks/` to explore data:

```python
import sys
sys.path.insert(0, '..')

import duckdb
conn = duckdb.connect('../data/dex_analytics.duckdb', read_only=True)

# Query data
df = conn.execute("""
    SELECT * FROM dex_usage_daily
    WHERE chain_id = 'bsc'
    ORDER BY date DESC
    LIMIT 10
""").df()

print(df)
```

### Adding New Chains

1. Add chain ID to `config/config.yaml`
2. Ensure parquet files exist at `/server/data/parquet/chain={new_chain}/`
3. Run ETL: `python scripts/daily_etl.py`

## License

Internal use only

## Contact

For issues or questions, contact the data team.

---

**Last Updated**: 2025-11-20
