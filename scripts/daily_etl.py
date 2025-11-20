#!/usr/bin/env python3
"""
Daily ETL script to extract DEX usage data and load into DuckDB
Runs daily to process previous day's data
"""
import os
import sys
import yaml
import duckdb
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import get_parquet_files, generate_union_sql_from_parquet


def setup_logging(config):
    """Setup logging configuration"""
    log_file = Path(__file__).parent.parent / config['logging']['log_file']
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def get_date_range(days_back=1):
    """
    Get date range for ETL (previous day by default)

    Args:
        days_back: Number of days back to process (1 = yesterday)

    Returns:
        Tuple of (begin_time, end_time) as strings
    """
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    begin_date = end_date - timedelta(days=days_back)
    end_date = begin_date + timedelta(days=1)

    begin_time = begin_date.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_date.strftime("%Y-%m-%d %H:%M:%S")

    return begin_time, end_time


def extract_dex_usage(conn, chain_id, begin_time, end_time, excluded_tokens):
    """
    Extract DEX usage data from parquet files

    Args:
        conn: DuckDB connection
        chain_id: Chain identifier
        begin_time: Start time
        end_time: End time
        excluded_tokens: List of tokens to exclude

    Returns:
        Tuple of (hourly_df, daily_df, total_df)
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing chain={chain_id} from {begin_time} to {end_time}")

    # Get parquet files
    try:
        parquet_files = get_parquet_files(chain_id, begin_time, end_time)
        if not parquet_files:
            logger.warning(f"No parquet files found for {chain_id}")
            return None, None, None

        logger.info(f"Found {len(parquet_files)} parquet file patterns")

        # Generate union SQL
        union_sql = generate_union_sql_from_parquet(parquet_files)

        # Create temporary view
        conn.execute(f"DROP VIEW IF EXISTS temp_dex_data")
        conn.execute(f"CREATE VIEW temp_dex_data AS {union_sql}")

    except Exception as e:
        logger.error(f"Error loading parquet files: {e}")
        return None, None, None

    # Build exclusion filter
    exclusion_filter = " AND ".join([
        f"inputToken != '{token}' AND outputToken != '{token}'"
        for token in excluded_tokens
    ])

    # Extract hourly data
    logger.info("Extracting hourly data...")
    hourly_query = f"""
    WITH dex_extracted AS (
        SELECT
            date,
            hour,
            orderId,
            JSON_EXTRACT(request, '$.swapInfo.routePlans') as route_plans
        FROM temp_dex_data
        WHERE request IS NOT NULL
        AND {exclusion_filter}
    ),
    dex_flattened AS (
        SELECT
            date,
            hour,
            orderId,
            JSON_EXTRACT(route_plan.value, '$.subRouters') as sub_routers
        FROM dex_extracted,
        JSON_EACH(route_plans) as route_plan
    ),
    dex_details AS (
        SELECT
            date,
            hour,
            orderId,
            JSON_EXTRACT(sub_router.value, '$.dexes') as dexes
        FROM dex_flattened,
        JSON_EACH(sub_routers) as sub_router
    ),
    dex_final AS (
        SELECT
            date,
            hour,
            orderId,
            JSON_EXTRACT(dex.value, '$.dex') as dex_name,
            CAST(JSON_EXTRACT(dex.value, '$.weight') AS INTEGER) as weight
        FROM dex_details,
        JSON_EACH(dexes) as dex
        WHERE JSON_EXTRACT(dex.value, '$.dex') IS NOT NULL
    )
    SELECT
        '{chain_id}' as chain_id,
        CAST(date AS DATE) as date,
        hour,
        REPLACE(dex_name, '"', '') as dex_name,
        COUNT(*) as usage_count,
        SUM(weight) as total_weight,
        COUNT(DISTINCT orderId) as unique_orders
    FROM dex_final
    GROUP BY date, hour, dex_name
    ORDER BY date, hour, usage_count DESC
    """

    try:
        hourly_df = conn.execute(hourly_query).df()
        logger.info(f"Extracted {len(hourly_df)} hourly records")
    except Exception as e:
        logger.error(f"Error extracting hourly data: {e}")
        hourly_df = None

    # Extract daily data
    logger.info("Extracting daily data...")
    daily_query = f"""
    WITH dex_extracted AS (
        SELECT
            date,
            orderId,
            JSON_EXTRACT(request, '$.swapInfo.routePlans') as route_plans
        FROM temp_dex_data
        WHERE request IS NOT NULL
        AND {exclusion_filter}
    ),
    dex_flattened AS (
        SELECT
            date,
            orderId,
            JSON_EXTRACT(route_plan.value, '$.subRouters') as sub_routers
        FROM dex_extracted,
        JSON_EACH(route_plans) as route_plan
    ),
    dex_details AS (
        SELECT
            date,
            orderId,
            JSON_EXTRACT(sub_router.value, '$.dexes') as dexes
        FROM dex_flattened,
        JSON_EACH(sub_routers) as sub_router
    ),
    dex_final AS (
        SELECT
            date,
            orderId,
            JSON_EXTRACT(dex.value, '$.dex') as dex_name,
            CAST(JSON_EXTRACT(dex.value, '$.weight') AS INTEGER) as weight
        FROM dex_details,
        JSON_EACH(dexes) as dex
        WHERE JSON_EXTRACT(dex.value, '$.dex') IS NOT NULL
    ),
    daily_stats AS (
        SELECT
            CAST(date AS DATE) as date,
            REPLACE(dex_name, '"', '') as dex_name,
            COUNT(*) as usage_count,
            SUM(weight) as total_weight,
            COUNT(DISTINCT orderId) as unique_orders
        FROM dex_final
        GROUP BY date, dex_name
    )
    SELECT
        '{chain_id}' as chain_id,
        date,
        dex_name,
        usage_count,
        total_weight,
        unique_orders,
        ROUND(CAST(usage_count AS DECIMAL) * 100.0 / SUM(usage_count) OVER(PARTITION BY date), 2) as percentage
    FROM daily_stats
    ORDER BY date, usage_count DESC
    """

    try:
        daily_df = conn.execute(daily_query).df()
        logger.info(f"Extracted {len(daily_df)} daily records")
    except Exception as e:
        logger.error(f"Error extracting daily data: {e}")
        daily_df = None

    # Extract total data
    logger.info("Extracting total data...")
    total_query = f"""
    WITH dex_extracted AS (
        SELECT
            orderId,
            JSON_EXTRACT(request, '$.swapInfo.routePlans') as route_plans
        FROM temp_dex_data
        WHERE request IS NOT NULL
        AND {exclusion_filter}
    ),
    dex_flattened AS (
        SELECT
            orderId,
            JSON_EXTRACT(route_plan.value, '$.subRouters') as sub_routers
        FROM dex_extracted,
        JSON_EACH(route_plans) as route_plan
    ),
    dex_details AS (
        SELECT
            orderId,
            JSON_EXTRACT(sub_router.value, '$.dexes') as dexes
        FROM dex_flattened,
        JSON_EACH(sub_routers) as sub_router
    ),
    dex_final AS (
        SELECT
            orderId,
            JSON_EXTRACT(dex.value, '$.dex') as dex_name,
            CAST(JSON_EXTRACT(dex.value, '$.weight') AS INTEGER) as weight
        FROM dex_details,
        JSON_EACH(dexes) as dex
        WHERE JSON_EXTRACT(dex.value, '$.dex') IS NOT NULL
    )
    SELECT
        '{chain_id}' as chain_id,
        REPLACE(dex_name, '"', '') as dex_name,
        COUNT(*) as usage_count,
        SUM(weight) as total_weight,
        COUNT(DISTINCT orderId) as unique_orders,
        ROUND(CAST(COUNT(*) AS DECIMAL) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM dex_final
    GROUP BY dex_name
    ORDER BY usage_count DESC
    """

    try:
        total_df = conn.execute(total_query).df()
        logger.info(f"Extracted {len(total_df)} total records")
    except Exception as e:
        logger.error(f"Error extracting total data: {e}")
        total_df = None

    # Cleanup
    conn.execute("DROP VIEW IF EXISTS temp_dex_data")

    return hourly_df, daily_df, total_df


def load_data(conn, hourly_df, daily_df, total_df, run_date):
    """
    Load extracted data into DuckDB tables

    Args:
        conn: DuckDB connection
        hourly_df: Hourly statistics DataFrame
        daily_df: Daily statistics DataFrame
        total_df: Total statistics DataFrame
        run_date: Date being processed
    """
    logger = logging.getLogger(__name__)

    # Load hourly data
    if hourly_df is not None and len(hourly_df) > 0:
        logger.info(f"Loading {len(hourly_df)} hourly records...")
        # Delete existing data for this chain and date
        chain_id = hourly_df['chain_id'].iloc[0]
        conn.execute(f"""
            DELETE FROM dex_usage_hourly
            WHERE chain_id = '{chain_id}' AND date = '{run_date}'
        """)
        # Insert new data (specify columns to exclude created_at)
        conn.execute("""
            INSERT INTO dex_usage_hourly (chain_id, date, hour, dex_name, usage_count, total_weight, unique_orders)
            SELECT chain_id, date, hour, dex_name, usage_count, total_weight, unique_orders FROM hourly_df
        """)
        logger.info("✓ Loaded hourly data")

    # Load daily data
    if daily_df is not None and len(daily_df) > 0:
        logger.info(f"Loading {len(daily_df)} daily records...")
        chain_id = daily_df['chain_id'].iloc[0]
        conn.execute(f"""
            DELETE FROM dex_usage_daily
            WHERE chain_id = '{chain_id}' AND date = '{run_date}'
        """)
        # Insert new data (specify columns to exclude created_at)
        conn.execute("""
            INSERT INTO dex_usage_daily (chain_id, date, dex_name, usage_count, total_weight, unique_orders, percentage)
            SELECT chain_id, date, dex_name, usage_count, total_weight, unique_orders, percentage FROM daily_df
        """)
        logger.info("✓ Loaded daily data")

    # Update total data (upsert)
    if total_df is not None and len(total_df) > 0:
        logger.info(f"Updating {len(total_df)} total records...")
        chain_id = total_df['chain_id'].iloc[0]

        for _, row in total_df.iterrows():
            # Check if record exists
            existing = conn.execute(f"""
                SELECT usage_count, first_seen
                FROM dex_usage_total
                WHERE chain_id = '{row['chain_id']}' AND dex_name = '{row['dex_name']}'
            """).fetchone()

            if existing:
                # Update existing record
                new_count = existing[0] + row['usage_count']
                conn.execute(f"""
                    UPDATE dex_usage_total
                    SET usage_count = {new_count},
                        total_weight = total_weight + {row['total_weight']},
                        unique_orders = unique_orders + {row['unique_orders']},
                        last_updated = CURRENT_TIMESTAMP
                    WHERE chain_id = '{row['chain_id']}' AND dex_name = '{row['dex_name']}'
                """)
            else:
                # Insert new record
                conn.execute(f"""
                    INSERT INTO dex_usage_total
                    (chain_id, dex_name, usage_count, total_weight, unique_orders, percentage, first_seen)
                    VALUES ('{row['chain_id']}', '{row['dex_name']}', {row['usage_count']},
                            {row['total_weight']}, {row['unique_orders']}, {row['percentage']}, '{run_date}')
                """)

        # Recalculate percentages for this chain
        conn.execute(f"""
            UPDATE dex_usage_total t
            SET percentage = ROUND(CAST(t.usage_count AS DECIMAL) * 100.0 / total.sum, 2)
            FROM (
                SELECT SUM(usage_count) as sum
                FROM dex_usage_total
                WHERE chain_id = '{chain_id}'
            ) total
            WHERE t.chain_id = '{chain_id}'
        """)

        logger.info("✓ Updated total data")


def log_etl_run(conn, chain_id, run_date, status, records_processed=0, error_message=None):
    """Log ETL run information"""
    logger = logging.getLogger(__name__)

    run_id = conn.execute("SELECT nextval('etl_run_log_seq')").fetchone()[0]

    conn.execute(f"""
        INSERT INTO etl_run_log
        (run_id, chain_id, run_date, start_time, end_time, status, records_processed, error_message)
        VALUES ({run_id}, '{chain_id}', '{run_date}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                '{status}', {records_processed}, {'NULL' if error_message is None else f"'{error_message}'"})
    """)

    logger.info(f"Logged ETL run: run_id={run_id}, status={status}")


def process_single_day(conn, chains, excluded_tokens, days_back, logger):
    """
    Process a single day's data for all chains

    Args:
        conn: DuckDB connection
        chains: List of chain IDs
        excluded_tokens: List of tokens to exclude
        days_back: Number of days back from today
        logger: Logger instance

    Returns:
        Total records processed
    """
    begin_time, end_time = get_date_range(days_back=days_back)
    run_date = datetime.strptime(begin_time, "%Y-%m-%d %H:%M:%S").date()

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Processing date: {run_date} (days_back={days_back})")
    logger.info(f"Time range: {begin_time} to {end_time}")
    logger.info(f"{'=' * 60}")

    total_records = 0

    for chain_id in chains:
        logger.info(f"\n  Processing chain: {chain_id}")

        try:
            # Extract data
            hourly_df, daily_df, total_df = extract_dex_usage(
                conn, chain_id, begin_time, end_time, excluded_tokens
            )

            # Load data
            if hourly_df is not None or daily_df is not None or total_df is not None:
                load_data(conn, hourly_df, daily_df, total_df, run_date)

                records = (len(hourly_df) if hourly_df is not None else 0) + \
                         (len(daily_df) if daily_df is not None else 0)
                total_records += records

                # Log success
                log_etl_run(conn, chain_id, run_date, 'success', records)
                logger.info(f"  ✅ Successfully processed {chain_id}: {records} records")
            else:
                logger.warning(f"  ⚠️ No data extracted for {chain_id}")
                log_etl_run(conn, chain_id, run_date, 'success', 0)

        except Exception as e:
            logger.error(f"  ❌ Error processing {chain_id}: {e}", exc_info=True)
            log_etl_run(conn, chain_id, run_date, 'failed', 0, str(e))

    return total_records


def main():
    """Main ETL process"""
    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Setup logging
    logger = setup_logging(config)

    # Check if init mode is enabled
    init_mode = config.get('init', {}).get('enabled', False)
    init_days = config.get('init', {}).get('days', 7)
    auto_generate_report = config.get('init', {}).get('auto_generate_report', True)

    if init_mode:
        logger.info("=" * 80)
        logger.info("🚀 INITIALIZATION MODE - Processing Last {} Days".format(init_days))
        logger.info("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info("Starting Daily DEX ETL Process")
        logger.info("=" * 80)

    # Get database path
    db_path = Path(__file__).parent.parent / config['data']['database_path']

    # Connect to database
    logger.info(f"Connecting to database: {db_path}")
    conn = duckdb.connect(str(db_path))

    # Get excluded tokens and chains
    excluded_tokens = config['exclusions']['tokens']
    chains = config['chains']

    total_records = 0

    if init_mode:
        # Process multiple days
        logger.info(f"Processing {init_days} days of historical data...")

        for days_back in range(init_days, 0, -1):
            day_records = process_single_day(conn, chains, excluded_tokens, days_back, logger)
            total_records += day_records

        logger.info("\n" + "=" * 80)
        logger.info(f"✅ Initialization Complete!")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Days processed: {init_days}")
        logger.info("=" * 80)

        # Close database connection before generating report
        conn.close()
        logger.info("Database connection closed")

        # Automatically generate report if enabled
        if auto_generate_report:
            logger.info("\n" + "=" * 80)
            logger.info("📊 Generating HTML Report...")
            logger.info("=" * 80)

            try:
                import subprocess
                report_script = Path(__file__).parent / "generate_report.py"
                result = subprocess.run(
                    [sys.executable, str(report_script)],
                    capture_output=True,
                    text=True,
                    timeout=300
                )

                if result.returncode == 0:
                    logger.info("✅ Report generated successfully!")
                    logger.info(result.stdout)
                else:
                    logger.error(f"❌ Report generation failed: {result.stderr}")

            except Exception as e:
                logger.error(f"❌ Error generating report: {e}")

        # Disable init mode after successful run
        logger.info("\n💡 Disabling init mode in config...")
        config['init']['enabled'] = False
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        logger.info("✅ Init mode disabled. Future runs will process only yesterday's data.")

    else:
        # Normal mode: process yesterday only
        total_records = process_single_day(conn, chains, excluded_tokens, 1, logger)

        logger.info("\n" + "=" * 80)
        logger.info(f"ETL Process Complete - Total records processed: {total_records}")
        logger.info("=" * 80)

        # Close connection
        conn.close()


if __name__ == "__main__":
    main()
