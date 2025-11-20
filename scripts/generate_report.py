#!/usr/bin/env python3
"""
Generate interactive HTML reports from DuckDB data
Creates a standalone HTML file with date and chain selectors
"""
import os
import sys
import yaml
import duckdb
import logging
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


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


def load_data_from_db(conn, chains):
    """
    Load all data from DuckDB for report generation

    Args:
        conn: DuckDB connection
        chains: List of chain IDs

    Returns:
        Dictionary with dataframes for each type
    """
    logger = logging.getLogger(__name__)
    logger.info("Loading data from database...")

    data = {}

    # Load hourly data
    hourly_query = """
        SELECT *
        FROM dex_usage_hourly
        ORDER BY chain_id, date, hour, usage_count DESC
    """
    data['hourly'] = conn.execute(hourly_query).df()
    logger.info(f"Loaded {len(data['hourly'])} hourly records")

    # Load daily data
    daily_query = """
        SELECT *
        FROM dex_usage_daily
        ORDER BY chain_id, date, usage_count DESC
    """
    data['daily'] = conn.execute(daily_query).df()
    logger.info(f"Loaded {len(data['daily'])} daily records")

    # Load total data
    total_query = """
        SELECT *
        FROM dex_usage_total
        ORDER BY chain_id, usage_count DESC
    """
    data['total'] = conn.execute(total_query).df()
    logger.info(f"Loaded {len(data['total'])} total records")

    return data


def create_interactive_html(data, config):
    """
    Create interactive HTML report with Plotly

    Args:
        data: Dictionary with dataframes
        config: Configuration dictionary

    Returns:
        HTML string
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating interactive HTML...")

    chains = config['chains']

    # Prepare data for JavaScript
    daily_df = data['daily']
    hourly_df = data['hourly']
    total_df = data['total']

    # Get available dates
    if len(daily_df) > 0:
        dates = sorted(daily_df['date'].unique())
        latest_date = dates[-1] if dates else None
    else:
        dates = []
        latest_date = None

    # Convert data to JSON for embedding
    daily_json = daily_df.to_json(orient='records', date_format='iso')
    hourly_json = hourly_df.to_json(orient='records', date_format='iso')
    total_json = total_df.to_json(orient='records', date_format='iso')

    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{config['report']['title']}</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        .header {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}

        h1 {{
            color: #2d3748;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .subtitle {{
            color: #718096;
            font-size: 1.1em;
        }}

        .controls {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            margin-bottom: 30px;
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            align-items: center;
        }}

        .control-group {{
            display: flex;
            flex-direction: column;
            gap: 8px;
        }}

        .control-label {{
            font-weight: 600;
            color: #2d3748;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        select {{
            padding: 12px 16px;
            border: 2px solid #e2e8f0;
            border-radius: 8px;
            font-size: 1em;
            color: #2d3748;
            background: white;
            cursor: pointer;
            transition: all 0.3s;
            min-width: 200px;
        }}

        select:hover {{
            border-color: #667eea;
        }}

        select:focus {{
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102,126,234,0.1);
        }}

        .chart-container {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}

        .chart-title {{
            font-size: 1.4em;
            color: #2d3748;
            margin-bottom: 15px;
            font-weight: 600;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }}

        .stat-label {{
            color: #718096;
            font-size: 0.9em;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .stat-value {{
            font-size: 2.5em;
            font-weight: 700;
            color: #667eea;
        }}

        .footer {{
            text-align: center;
            color: white;
            padding: 20px;
            font-size: 0.9em;
        }}

        .loading {{
            text-align: center;
            padding: 40px;
            color: #718096;
        }}

        @media (max-width: 768px) {{
            h1 {{
                font-size: 1.8em;
            }}

            .controls {{
                flex-direction: column;
                align-items: stretch;
            }}

            select {{
                width: 100%;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 {config['report']['title']}</h1>
            <div class="subtitle">Last updated: <span id="lastUpdate">{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</span></div>
        </div>

        <div class="controls">
            <div class="control-group">
                <label class="control-label">Blockchain</label>
                <select id="chainSelect">
                    {"".join([f'<option value="{chain}">{chain.upper()}</option>' for chain in chains])}
                </select>
            </div>

            <div class="control-group">
                <label class="control-label">Date</label>
                <select id="dateSelect">
                    <option value="latest">Latest</option>
                </select>
            </div>

            <div class="control-group">
                <label class="control-label">View Type</label>
                <select id="viewSelect">
                    <option value="daily">Daily View</option>
                    <option value="hourly">Hourly View</option>
                    <option value="total">Total Statistics</option>
                </select>
            </div>
        </div>

        <div class="stats-grid" id="statsGrid"></div>

        <div class="chart-container">
            <div class="chart-title" id="mainChartTitle">DEX Usage Distribution</div>
            <div id="mainChart"></div>
        </div>

        <div class="chart-container">
            <div class="chart-title">DEX Usage Over Time</div>
            <div id="trendChart"></div>
        </div>

        <div class="chart-container">
            <div class="chart-title">Top DEX Rankings</div>
            <div id="rankingChart"></div>
        </div>

        <div class="footer">
            <p>DEX Analytics Dashboard | Data updated daily at {config['time']['daily_run_hour']:02d}:{config['time']['daily_run_minute']:02d} UTC</p>
        </div>
    </div>

    <script>
        // Embedded data
        const dailyData = {daily_json};
        const hourlyData = {hourly_json};
        const totalData = {total_json};

        // Parse dates
        dailyData.forEach(d => d.date = new Date(d.date));
        hourlyData.forEach(d => d.date = new Date(d.date));

        // Get available dates
        const availableDates = [...new Set(dailyData.map(d => d.date.toISOString().split('T')[0]))].sort().reverse();

        // Populate date select
        const dateSelect = document.getElementById('dateSelect');
        availableDates.forEach(date => {{
            const option = document.createElement('option');
            option.value = date;
            option.textContent = date;
            dateSelect.appendChild(option);
        }});

        // Color palette
        const colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#30cfd0'];

        function filterData(chain, date, viewType) {{
            if (viewType === 'daily') {{
                let filtered = dailyData.filter(d => d.chain_id === chain);
                if (date !== 'latest') {{
                    const targetDate = new Date(date);
                    filtered = filtered.filter(d => d.date.toISOString().split('T')[0] === date);
                }}
                return filtered;
            }} else if (viewType === 'hourly') {{
                let filtered = hourlyData.filter(d => d.chain_id === chain);
                if (date !== 'latest') {{
                    filtered = filtered.filter(d => d.date.toISOString().split('T')[0] === date);
                }}
                return filtered;
            }} else {{
                return totalData.filter(d => d.chain_id === chain);
            }}
        }}

        function updateStats(data, viewType) {{
            const statsGrid = document.getElementById('statsGrid');

            if (viewType === 'total') {{
                const totalUsage = data.reduce((sum, d) => sum + d.usage_count, 0);
                const totalOrders = data.reduce((sum, d) => sum + d.unique_orders, 0);
                const dexCount = data.length;

                statsGrid.innerHTML = `
                    <div class="stat-card">
                        <div class="stat-label">Total Usage</div>
                        <div class="stat-value">${{{{totalUsage.toLocaleString()}}}}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Unique Orders</div>
                        <div class="stat-value">${{{{totalOrders.toLocaleString()}}}}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Active DEXes</div>
                        <div class="stat-value">${{{{dexCount}}}}</div>
                    </div>
                `;
            }} else {{
                const totalUsage = data.reduce((sum, d) => sum + d.usage_count, 0);
                const totalOrders = data.reduce((sum, d) => sum + d.unique_orders, 0);
                const dexCount = [...new Set(data.map(d => d.dex_name))].length;

                statsGrid.innerHTML = `
                    <div class="stat-card">
                        <div class="stat-label">Total Usage</div>
                        <div class="stat-value">${{{totalUsage.toLocaleString()}}}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Unique Orders</div>
                        <div class="stat-value">${{{totalOrders.toLocaleString()}}}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Active DEXes</div>
                        <div class="stat-value">${{{dexCount}}}</div>
                    </div>
                `;
            }}
        }}

        function createPieChart(data) {{
            const aggregated = {{}};
            data.forEach(d => {{
                if (!aggregated[d.dex_name]) {{
                    aggregated[d.dex_name] = 0;
                }}
                aggregated[d.dex_name] += d.usage_count;
            }});

            const labels = Object.keys(aggregated);
            const values = Object.values(aggregated);

            const trace = {{
                labels: labels,
                values: values,
                type: 'pie',
                marker: {{
                    colors: colors
                }},
                textposition: 'inside',
                textinfo: 'label+percent',
                hovertemplate: '<b>%{{label}}</b><br>Usage: %{{value}}<br>Percentage: %{{percent}}<extra></extra>'
            }};

            const layout = {{
                height: 500,
                margin: {{ t: 20, b: 20, l: 20, r: 20 }},
                showlegend: true,
                legend: {{
                    orientation: 'v',
                    x: 1.1,
                    y: 0.5
                }}
            }};

            Plotly.newPlot('mainChart', [trace], layout, {{responsive: true}});
        }}

        function createBarChart(data, viewType) {{
            if (viewType === 'hourly') {{
                // Group by hour
                const byHour = {{}};
                data.forEach(d => {{
                    const key = d.hour;
                    if (!byHour[key]) byHour[key] = {{}};
                    if (!byHour[key][d.dex_name]) byHour[key][d.dex_name] = 0;
                    byHour[key][d.dex_name] += d.usage_count;
                }});

                const hours = Object.keys(byHour).sort((a, b) => a - b);
                const dexNames = [...new Set(data.map(d => d.dex_name))];

                const traces = dexNames.map((dex, idx) => ({{
                    x: hours.map(h => `${{{String(h).padStart(2, '0')}}}:00`),
                    y: hours.map(h => byHour[h][dex] || 0),
                    name: dex,
                    type: 'bar',
                    marker: {{ color: colors[idx % colors.length] }}
                }}));

                const layout = {{
                    height: 500,
                    barmode: 'stack',
                    xaxis: {{ title: 'Hour' }},
                    yaxis: {{ title: 'Usage Count' }},
                    margin: {{ t: 20, b: 60, l: 60, r: 20 }}
                }};

                Plotly.newPlot('mainChart', traces, layout, {{responsive: true}});
            }} else {{
                // Group by date
                const byDate = {{}};
                data.forEach(d => {{
                    const key = d.date.toISOString().split('T')[0];
                    if (!byDate[key]) byDate[key] = {{}};
                    if (!byDate[key][d.dex_name]) byDate[key][d.dex_name] = 0;
                    byDate[key][d.dex_name] += d.usage_count;
                }});

                const dates = Object.keys(byDate).sort();
                const dexNames = [...new Set(data.map(d => d.dex_name))];

                const traces = dexNames.map((dex, idx) => ({{
                    x: dates,
                    y: dates.map(d => byDate[d][dex] || 0),
                    name: dex,
                    type: 'bar',
                    marker: {{ color: colors[idx % colors.length] }}
                }}));

                const layout = {{
                    height: 500,
                    barmode: 'stack',
                    xaxis: {{ title: 'Date' }},
                    yaxis: {{ title: 'Usage Count' }},
                    margin: {{ t: 20, b: 60, l: 60, r: 20 }}
                }};

                Plotly.newPlot('mainChart', traces, layout, {{responsive: true}});
            }}
        }}

        function createTrendChart(chain) {{
            const data = dailyData.filter(d => d.chain_id === chain);

            const byDate = {{}};
            data.forEach(d => {{
                const key = d.date.toISOString().split('T')[0];
                if (!byDate[key]) byDate[key] = {{}};
                if (!byDate[key][d.dex_name]) byDate[key][d.dex_name] = 0;
                byDate[key][d.dex_name] += d.usage_count;
            }});

            const dates = Object.keys(byDate).sort();
            const dexNames = [...new Set(data.map(d => d.dex_name))];

            const traces = dexNames.map((dex, idx) => ({{
                x: dates,
                y: dates.map(d => byDate[d][dex] || 0),
                name: dex,
                type: 'scatter',
                mode: 'lines+markers',
                line: {{ color: colors[idx % colors.length] }}
            }}));

            const layout = {{
                height: 400,
                xaxis: {{ title: 'Date' }},
                yaxis: {{ title: 'Usage Count' }},
                margin: {{ t: 20, b: 60, l: 60, r: 20 }},
                hovermode: 'x unified'
            }};

            Plotly.newPlot('trendChart', traces, layout, {{responsive: true}});
        }}

        function createRankingChart(data) {{
            const aggregated = {{}};
            data.forEach(d => {{
                if (!aggregated[d.dex_name]) {{
                    aggregated[d.dex_name] = 0;
                }}
                aggregated[d.dex_name] += d.usage_count;
            }});

            const sorted = Object.entries(aggregated)
                .sort((a, b) => a[1] - b[1])
                .slice(-15);

            const trace = {{
                x: sorted.map(d => d[1]),
                y: sorted.map(d => d[0]),
                type: 'bar',
                orientation: 'h',
                marker: {{ color: '#667eea' }},
                text: sorted.map(d => d[1].toLocaleString()),
                textposition: 'outside'
            }};

            const layout = {{
                height: 500,
                xaxis: {{ title: 'Usage Count' }},
                margin: {{ t: 20, b: 40, l: 150, r: 40 }}
            }};

            Plotly.newPlot('rankingChart', [trace], layout, {{responsive: true}});
        }}

        function updateCharts() {{
            const chain = document.getElementById('chainSelect').value;
            const date = document.getElementById('dateSelect').value;
            const viewType = document.getElementById('viewSelect').value;

            const data = filterData(chain, date, viewType);

            if (data.length === 0) {{
                document.getElementById('mainChart').innerHTML = '<div class="loading">No data available for selected filters</div>';
                document.getElementById('statsGrid').innerHTML = '';
                return;
            }}

            updateStats(data, viewType);

            if (viewType === 'total') {{
                createPieChart(data);
            }} else {{
                createBarChart(data, viewType);
            }}

            createTrendChart(chain);
            createRankingChart(data);
        }}

        // Event listeners
        document.getElementById('chainSelect').addEventListener('change', updateCharts);
        document.getElementById('dateSelect').addEventListener('change', updateCharts);
        document.getElementById('viewSelect').addEventListener('change', updateCharts);

        // Initial load
        updateCharts();
    </script>
</body>
</html>
"""

    return html


def main():
    """Main report generation process"""
    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Setup logging
    logger = setup_logging(config)
    logger.info("=" * 80)
    logger.info("Starting Report Generation")
    logger.info("=" * 80)

    # Get database path
    db_path = Path(__file__).parent.parent / config['data']['database_path']

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Please run scripts/init_database.py first")
        sys.exit(1)

    # Connect to database
    logger.info(f"Connecting to database: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)

    # Load data
    data = load_data_from_db(conn, config['chains'])

    # Close connection
    conn.close()

    # Check if we have data
    if len(data['daily']) == 0 and len(data['hourly']) == 0:
        logger.warning("No data available in database. Please run ETL first.")
        sys.exit(1)

    # Generate HTML
    html = create_interactive_html(data, config)

    # Save report
    reports_path = Path(__file__).parent.parent / config['data']['reports_path']
    reports_path.mkdir(parents=True, exist_ok=True)

    report_file = reports_path / "index.html"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(html)

    logger.info(f"✅ Report generated: {report_file}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
