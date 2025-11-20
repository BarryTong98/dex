# 快速部署指南

## 服务器部署步骤

### 1. 复制项目到服务器

```bash
# 在本地（如果需要）
cd /Users/barry/binance/dex
tar -czf dex-analytics.tar.gz .

# 上传到服务器
scp dex-analytics.tar.gz root@your-server:/server/share/barry/

# 在服务器上解压
ssh root@your-server
cd /server/share/barry
tar -xzf dex-analytics.tar.gz -C dex/
cd dex
```

**或者直接在服务器上初始化Git仓库：**
```bash
ssh root@your-server
cd /server/share/barry/dex
git init
# 然后复制所有文件到这个目录
```

### 2. 安装依赖

```bash
cd /server/share/barry/dex

# 确认Python版本
python3 --version  # 需要 3.8+

# 安装依赖
pip3 install -r requirements.txt

# 或使用虚拟环境（推荐）
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. 调整配置

编辑 `config/config.yaml`：

```bash
vi config/config.yaml
```

确认以下配置正确：
- `parquet_base_path`: 指向你的parquet数据目录
- `chains`: 包含你需要的链
- `exclusions.tokens`: 添加需要排除的token地址

### 4. 初始化数据库

```bash
python3 scripts/init_database.py
```

预期输出：
```
Initializing database at: /server/share/barry/dex/data/dex_analytics.duckdb
Creating tables...
✓ Created dex_usage_hourly table
✓ Created dex_usage_daily table
✓ Created dex_usage_total table
✓ Created etl_run_log table
✓ Created indexes

Database tables:
  - dex_usage_daily (0 rows)
  - dex_usage_hourly (0 rows)
  - dex_usage_total (0 rows)
  - etl_run_log (0 rows)

✅ Database initialization complete!
```

### 5. 测试运行ETL

**首次运行（处理昨天的数据）：**

```bash
python3 scripts/daily_etl.py
```

预期输出：
```
================================================================================
Starting Daily DEX ETL Process
================================================================================
Processing date: 2025-11-19
Time range: 2025-11-19 00:00:00 to 2025-11-20 00:00:00
Connecting to database: /server/share/barry/dex/data/dex_analytics.duckdb

============================================================
Processing chain: bsc
============================================================
Processing chain=bsc from 2025-11-19 00:00:00 to 2025-11-20 00:00:00
Found 1 parquet file patterns
Extracting hourly data...
Extracted 245 hourly records
Extracting daily data...
Extracted 28 daily records
Extracting total data...
Extracted 28 total records
Loading 245 hourly records...
✓ Loaded hourly data
Loading 28 daily records...
✓ Loaded daily data
Updating 28 total records...
✓ Updated total data
✅ Successfully processed bsc: 273 records

[重复其他链...]

================================================================================
ETL Process Complete - Total records processed: 1092
================================================================================
```

### 6. 生成报告

```bash
python3 scripts/generate_report.py
```

预期输出：
```
================================================================================
Starting Report Generation
================================================================================
Connecting to database: /server/share/barry/dex/data/dex_analytics.duckdb
Loading data from database...
Loaded 980 hourly records
Loaded 112 daily records
Loaded 112 total records
Generating interactive HTML...
✅ Report generated: /server/share/barry/dex/reports/index.html
================================================================================
```

### 7. 验证报告

**方法1：使用Python简单HTTP服务器**
```bash
cd /server/share/barry/dex/reports
python3 -m http.server 8000

# 然后在浏览器访问：
# http://your-server-ip:8000/index.html
```

**方法2：复制到Web服务器目录**
```bash
# 如果有Nginx或Apache
cp reports/index.html /var/www/html/dex-analytics.html
# 访问: http://your-server/dex-analytics.html
```

**方法3：直接下载查看**
```bash
# 在本地
scp root@your-server:/server/share/barry/dex/reports/index.html ~/Desktop/
open ~/Desktop/index.html
```

### 8. 设置定时任务

**自动设置（推荐）：**
```bash
cd /server/share/barry/dex
bash scripts/setup_cron.sh
```

**手动设置：**
```bash
crontab -e

# 添加以下内容：
0 9 * * * cd /server/share/barry/dex && /usr/bin/python3 scripts/daily_etl.py >> logs/etl.log 2>&1
30 9 * * * cd /server/share/barry/dex && /usr/bin/python3 scripts/generate_report.py >> logs/etl.log 2>&1
```

**验证crontab：**
```bash
crontab -l
```

### 9. 监控运行

```bash
# 实时查看日志
tail -f /server/share/barry/dex/logs/etl.log

# 查看最近的运行记录
tail -100 /server/share/barry/dex/logs/etl.log

# 检查数据库
python3 << EOF
import duckdb
conn = duckdb.connect('/server/share/barry/dex/data/dex_analytics.duckdb')
print("Daily records:", conn.execute("SELECT COUNT(*) FROM dex_usage_daily").fetchone()[0])
print("Last ETL run:", conn.execute("SELECT * FROM etl_run_log ORDER BY run_id DESC LIMIT 1").fetchone())
conn.close()
EOF
```

## 故障排查

### 问题1：找不到parquet文件

```bash
# 检查路径是否正确
ls -la /server/data/parquet/chain=bsc/date=2025-11-19/

# 调整config.yaml中的parquet_base_path
```

### 问题2：权限错误

```bash
# 给予执行权限
chmod +x scripts/*.py scripts/*.sh

# 检查数据目录权限
ls -la data/
chmod 755 data/
```

### 问题3：Python模块未找到

```bash
# 重新安装依赖
pip3 install --upgrade -r requirements.txt

# 或使用虚拟环境
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 然后更新crontab使用虚拟环境的Python
# /server/share/barry/dex/venv/bin/python3
```

### 问题4：报告显示无数据

```bash
# 1. 检查数据库是否有数据
ls -lh data/dex_analytics.duckdb

# 2. 查询数据
python3 << EOF
import duckdb
conn = duckdb.connect('data/dex_analytics.duckdb', read_only=True)
print(conn.execute("SELECT chain_id, COUNT(*) FROM dex_usage_daily GROUP BY chain_id").fetchall())
conn.close()
EOF

# 3. 重新运行ETL
python3 scripts/daily_etl.py
python3 scripts/generate_report.py
```

### 问题5：DuckDB版本兼容性

```bash
# 检查DuckDB版本
python3 -c "import duckdb; print(duckdb.__version__)"

# 如果版本过低，升级
pip3 install --upgrade duckdb
```

## 高级配置

### 回填历史数据

如果需要处理多天的历史数据：

```python
# 创建 scripts/backfill.py
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.daily_etl import *

# 回填最近7天
for days_back in range(1, 8):
    begin_time, end_time = get_date_range(days_back)
    print(f"\n{'='*60}")
    print(f"Processing: {begin_time}")
    print(f"{'='*60}")

    # ... 运行ETL逻辑
```

### 更改运行时间

编辑 `config/config.yaml`:
```yaml
time:
  daily_run_hour: 8  # 改为8点
  daily_run_minute: 30  # 改为30分
```

然后重新设置crontab。

### 添加新链

1. 编辑 `config/config.yaml`，添加新链ID
2. 确保parquet数据存在
3. 运行ETL测试
4. 查看报告验证

## 维护建议

### 日常检查
- 每周检查一次日志：`tail -200 logs/etl.log`
- 监控数据库大小：`ls -lh data/dex_analytics.duckdb`
- 验证报告更新：检查报告中的"Last updated"时间

### 备份
```bash
# 定期备份数据库
cp data/dex_analytics.duckdb data/dex_analytics.duckdb.backup.$(date +%Y%m%d)

# 或添加到crontab（每周日备份）
0 2 * * 0 cp /server/share/barry/dex/data/dex_analytics.duckdb /server/share/barry/dex/data/backup/dex_analytics.duckdb.$(date +\%Y\%m\%d)
```

### 清理旧日志
```bash
# 保留最近30天的日志
find logs/ -name "*.log" -mtime +30 -delete
```

---

## 完成！

部署完成后，你将拥有：
- ✅ 自动化的每日数据处理流程
- ✅ 持久化的历史数据存储（DuckDB）
- ✅ 交互式HTML报告（支持多链、多日期选择）
- ✅ 完整的日志和错误追踪
- ✅ 易于维护和扩展的架构

如有问题，请查看日志或联系开发团队。
