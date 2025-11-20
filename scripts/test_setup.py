#!/usr/bin/env python3
"""
Test script to verify the DEX Analytics setup
Run this after installation to check if everything is configured correctly
"""
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Color codes for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


def check_python_version():
    """Check if Python version is 3.8+"""
    print(f"\n{BLUE}[1/8] Checking Python version...{RESET}")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"  {GREEN}✓ Python {version.major}.{version.minor}.{version.micro}{RESET}")
        return True
    else:
        print(f"  {RED}✗ Python version too old: {version.major}.{version.minor}.{version.micro}{RESET}")
        print(f"  {YELLOW}  Required: Python 3.8+{RESET}")
        return False


def check_dependencies():
    """Check if required packages are installed"""
    print(f"\n{BLUE}[2/8] Checking dependencies...{RESET}")
    required = ['duckdb', 'pandas', 'yaml', 'plotly']
    missing = []

    for package in required:
        try:
            if package == 'yaml':
                __import__('yaml')
            else:
                __import__(package)
            print(f"  {GREEN}✓ {package}{RESET}")
        except ImportError:
            print(f"  {RED}✗ {package} not installed{RESET}")
            missing.append(package)

    if missing:
        print(f"\n  {YELLOW}To install missing packages:{RESET}")
        print(f"  pip install {' '.join(missing)}")
        return False

    return True


def check_config_file():
    """Check if config file exists and is valid"""
    print(f"\n{BLUE}[3/8] Checking configuration file...{RESET}")
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"

    if not config_path.exists():
        print(f"  {RED}✗ Config file not found: {config_path}{RESET}")
        return False

    try:
        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f)

        # Check required keys
        required_keys = ['chains', 'data', 'time', 'logging', 'exclusions']
        for key in required_keys:
            if key in config:
                print(f"  {GREEN}✓ {key} configured{RESET}")
            else:
                print(f"  {RED}✗ Missing key: {key}{RESET}")
                return False

        # Show chains
        print(f"    Chains: {', '.join(config['chains'])}")

        return True
    except Exception as e:
        print(f"  {RED}✗ Error reading config: {e}{RESET}")
        return False


def check_directories():
    """Check if required directories exist"""
    print(f"\n{BLUE}[4/8] Checking directory structure...{RESET}")
    base_path = Path(__file__).parent.parent
    required_dirs = ['config', 'utils', 'scripts', 'data', 'reports', 'logs']

    all_exist = True
    for dir_name in required_dirs:
        dir_path = base_path / dir_name
        if dir_path.exists():
            print(f"  {GREEN}✓ {dir_name}/{RESET}")
        else:
            print(f"  {RED}✗ {dir_name}/ not found{RESET}")
            all_exist = False

    return all_exist


def check_database():
    """Check if database exists and is accessible"""
    print(f"\n{BLUE}[5/8] Checking database...{RESET}")

    import yaml
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    db_path = Path(__file__).parent.parent / config['data']['database_path']

    if not db_path.exists():
        print(f"  {YELLOW}⚠ Database not found: {db_path}{RESET}")
        print(f"  {YELLOW}  Run: python scripts/init_database.py{RESET}")
        return False

    try:
        import duckdb
        conn = duckdb.connect(str(db_path), read_only=True)

        # Check tables
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()

        required_tables = ['dex_usage_hourly', 'dex_usage_daily', 'dex_usage_total', 'etl_run_log']
        table_names = [t[0] for t in tables]

        for table in required_tables:
            if table in table_names:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {GREEN}✓ {table} ({count} rows){RESET}")
            else:
                print(f"  {RED}✗ Table missing: {table}{RESET}")
                conn.close()
                return False

        conn.close()
        return True

    except Exception as e:
        print(f"  {RED}✗ Database error: {e}{RESET}")
        return False


def check_parquet_access():
    """Check if parquet files are accessible"""
    print(f"\n{BLUE}[6/8] Checking parquet file access...{RESET}")

    import yaml
    from datetime import datetime, timedelta

    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    parquet_base = config['data']['parquet_base_path']

    # Check if base path exists
    if not os.path.exists(parquet_base):
        print(f"  {RED}✗ Parquet base path not found: {parquet_base}{RESET}")
        print(f"  {YELLOW}  Update 'data.parquet_base_path' in config.yaml{RESET}")
        return False

    print(f"  {GREEN}✓ Base path exists: {parquet_base}{RESET}")

    # Check for recent data
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    found_any = False

    for chain in config['chains']:
        chain_path = f"{parquet_base}/chain={chain}/date={yesterday}"
        if os.path.exists(chain_path):
            files = len([f for f in os.listdir(chain_path) if f.endswith('.parquet')])
            if files > 0:
                print(f"  {GREEN}✓ {chain}: {files} file(s) for {yesterday}{RESET}")
                found_any = True
            else:
                print(f"  {YELLOW}⚠ {chain}: No parquet files for {yesterday}{RESET}")
        else:
            print(f"  {YELLOW}⚠ {chain}: Path not found for {yesterday}{RESET}")

    if not found_any:
        print(f"\n  {YELLOW}⚠ No recent parquet data found{RESET}")
        print(f"  {YELLOW}  This may be normal if data hasn't been generated yet{RESET}")

    return True


def check_scripts():
    """Check if scripts are executable"""
    print(f"\n{BLUE}[7/8] Checking scripts...{RESET}")
    base_path = Path(__file__).parent
    scripts = ['init_database.py', 'daily_etl.py', 'generate_report.py', 'setup_cron.sh']

    all_good = True
    for script in scripts:
        script_path = base_path / script
        if script_path.exists():
            is_executable = os.access(script_path, os.X_OK)
            if is_executable or script.endswith('.py'):
                print(f"  {GREEN}✓ {script}{RESET}")
            else:
                print(f"  {YELLOW}⚠ {script} (not executable){RESET}")
                print(f"    Run: chmod +x scripts/{script}")
        else:
            print(f"  {RED}✗ {script} not found{RESET}")
            all_good = False

    return all_good


def check_utils():
    """Check if utils module is importable"""
    print(f"\n{BLUE}[8/8] Checking utils module...{RESET}")

    try:
        from utils import get_parquet_files, generate_union_sql_from_parquet
        print(f"  {GREEN}✓ utils module imported successfully{RESET}")
        print(f"  {GREEN}✓ get_parquet_files available{RESET}")
        print(f"  {GREEN}✓ generate_union_sql_from_parquet available{RESET}")
        return True
    except Exception as e:
        print(f"  {RED}✗ Error importing utils: {e}{RESET}")
        return False


def main():
    """Run all checks"""
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}  DEX Analytics - Setup Verification{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

    checks = [
        check_python_version,
        check_dependencies,
        check_config_file,
        check_directories,
        check_database,
        check_parquet_access,
        check_scripts,
        check_utils
    ]

    results = []
    for check in checks:
        try:
            result = check()
            results.append(result)
        except Exception as e:
            print(f"  {RED}✗ Unexpected error: {e}{RESET}")
            results.append(False)

    # Summary
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}  Summary{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"\n  {GREEN}✅ All checks passed! ({passed}/{total}){RESET}")
        print(f"\n  {GREEN}You're ready to use DEX Analytics!{RESET}")
        print(f"\n  Next steps:")
        print(f"    1. Initialize database: python scripts/init_database.py")
        print(f"    2. Run ETL: python scripts/daily_etl.py")
        print(f"    3. Generate report: python scripts/generate_report.py")
        print(f"    4. Setup cron: bash scripts/setup_cron.sh")
    else:
        print(f"\n  {YELLOW}⚠ {passed}/{total} checks passed{RESET}")
        print(f"\n  {YELLOW}Please fix the issues above before proceeding.{RESET}")
        print(f"\n  For help, see:")
        print(f"    - README.md")
        print(f"    - DEPLOY.md")

    print()


if __name__ == "__main__":
    main()
