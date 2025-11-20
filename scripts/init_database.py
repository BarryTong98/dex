#!/usr/bin/env python3
"""
Initialize DuckDB database schema for DEX analytics
"""
import os
import sys
import duckdb
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def init_database(db_path):
    """
    Initialize DuckDB database with required tables

    Args:
        db_path: Path to DuckDB database file
    """
    print(f"Initializing database at: {db_path}")

    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Connect to database
    conn = duckdb.connect(db_path)

    # Create tables
    print("Creating tables...")

    # Hourly statistics table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dex_usage_hourly (
            chain_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            hour INTEGER NOT NULL,
            dex_name VARCHAR NOT NULL,
            usage_count INTEGER NOT NULL,
            total_weight BIGINT NOT NULL,
            unique_orders INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chain_id, date, hour, dex_name)
        )
    """)
    print("✓ Created dex_usage_hourly table")

    # Daily statistics table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dex_usage_daily (
            chain_id VARCHAR NOT NULL,
            date DATE NOT NULL,
            dex_name VARCHAR NOT NULL,
            usage_count INTEGER NOT NULL,
            total_weight BIGINT NOT NULL,
            unique_orders INTEGER NOT NULL,
            percentage DECIMAL(5,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chain_id, date, dex_name)
        )
    """)
    print("✓ Created dex_usage_daily table")

    # Total statistics table (aggregated)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dex_usage_total (
            chain_id VARCHAR NOT NULL,
            dex_name VARCHAR NOT NULL,
            usage_count INTEGER NOT NULL,
            total_weight BIGINT NOT NULL,
            unique_orders INTEGER NOT NULL,
            percentage DECIMAL(5,2),
            first_seen DATE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chain_id, dex_name)
        )
    """)
    print("✓ Created dex_usage_total table")

    # ETL run log table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_run_log (
            run_id INTEGER PRIMARY KEY,
            chain_id VARCHAR NOT NULL,
            run_date DATE NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status VARCHAR NOT NULL,  -- 'running', 'success', 'failed'
            records_processed INTEGER,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Created etl_run_log table")

    # Create sequence for run_id
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS etl_run_log_seq START 1
    """)

    # Create indexes for better query performance
    print("Creating indexes...")

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hourly_date
        ON dex_usage_hourly(chain_id, date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_date
        ON dex_usage_daily(chain_id, date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_etl_log
        ON etl_run_log(chain_id, run_date, status)
    """)

    print("✓ Created indexes")

    # Verify tables
    result = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    print("\nDatabase tables:")
    for table in result:
        count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
        print(f"  - {table[0]} ({count} rows)")

    conn.close()
    print("\n✅ Database initialization complete!")


if __name__ == "__main__":
    # Get database path from config
    import yaml

    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    db_path = Path(__file__).parent.parent / config['data']['database_path']

    init_database(str(db_path))
