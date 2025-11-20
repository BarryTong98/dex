"""
Utility functions for DEX data processing
Originally from the Jupyter notebook
"""
from datetime import datetime, timedelta
import base64
from IPython.display import HTML


def get_parquet_files(chain_id, begin_time, end_time):
    """
    Get parquet file paths for a specific chain and time range

    Args:
        chain_id: Chain identifier (bsc, eth, base, sol)
        begin_time: Start time in format "YYYY-MM-DD HH:MM:SS"
        end_time: End time in format "YYYY-MM-DD HH:MM:SS"

    Returns:
        List of parquet file paths
    """
    base_files = get_files_for_suffix(begin_time, end_time, ".parquet")
    chain_files = []
    for filename in base_files:
        chain_file_path = f'/server/data/parquet/chain={chain_id}/{filename}'
        chain_files.append(chain_file_path)
    return chain_files


def chose_period(groupBy):
    """
    Get SQL expression for time grouping

    Args:
        groupBy: 'hour' or 'day'

    Returns:
        SQL date truncation expression
    """
    if groupBy == "hour":
        return "DATE_TRUNC('hour', CAST(BlockTime AS TIMESTAMP))"
    else:
        return "DATE_TRUNC('day', CAST(BlockTime AS TIMESTAMP))"


def generate_union_sql_from_parquet(file_paths):
    """
    Generate UNION ALL SQL for multiple parquet files

    Args:
        file_paths: List of parquet file paths

    Returns:
        SQL UNION ALL statement
    """
    return generate_union_sql(file_paths, "parquet")


def generate_union_sql(file_paths, file_format):
    """
    Generate UNION ALL SQL for multiple files

    Args:
        file_paths: List of file paths
        file_format: 'parquet' or 'ndjson'

    Returns:
        SQL UNION ALL statement
    """
    if not file_paths:
        raise ValueError("文件路径数组不能为空")

    select_statements = []
    for file_path in file_paths:
        select_statements.append(f'SELECT * FROM read_{file_format}("{file_path}")')

    union_sql = ' UNION ALL '.join(select_statements)
    return union_sql


def get_files_for_suffix(begin_time, end_time, suffix):
    """
    Get file patterns for date range with specific suffix

    Args:
        begin_time: Start time in format "YYYY-MM-DD HH:MM:SS"
        end_time: End time in format "YYYY-MM-DD HH:MM:SS"
        suffix: File suffix (e.g., ".parquet")

    Returns:
        List of file path patterns
    """
    try:
        start_date = datetime.strptime(begin_time, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        if start_date > end_date:
            raise ValueError("开始时间不能晚于结束时间")

        file_list = []
        current_date = start_date

        while current_date <= end_date:
            file_name = current_date.strftime(f"date=%Y-%m-%d/hour=*/*{suffix}")
            file_list.append(file_name)
            current_date += timedelta(days=1)

        return file_list

    except ValueError as e:
        if "time data" in str(e):
            raise ValueError(f"时间格式错误，请使用 'YYYY-MM-DD HH:MM:SS' 格式: {e}")
        else:
            raise e


def create_download_link(df, filename="data.csv", title="下载CSV文件"):
    """
    Create HTML download link for DataFrame as CSV

    Args:
        df: pandas DataFrame
        filename: Output filename
        title: Link text

    Returns:
        IPython HTML object with download link
    """
    csv_string = df.to_csv(index=False, encoding='utf-8')
    b64 = base64.b64encode(csv_string.encode('utf-8')).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}" target="_blank">{title}</a>'
    return HTML(href)
