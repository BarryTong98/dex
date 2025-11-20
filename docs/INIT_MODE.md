# 初始化模式使用指南

## 什么是初始化模式？

初始化模式允许你一次性处理过去多天的历史数据，而不是只处理昨天的数据。这在以下场景很有用：

- 🆕 首次部署系统，需要导入历史数据
- 📊 需要快速建立历史数据趋势
- 🔄 数据库重建后需要回填数据
- 📈 添加新链后需要历史对比

## 快速使用

### 1. 启用初始化模式

编辑 `config/config.yaml`：

```yaml
# Initialization mode
init:
  enabled: true  # 改为 true
  days: 7  # 处理过去7天的数据（可修改）
  auto_generate_report: true  # 完成后自动生成报告
```

### 2. 运行ETL

```bash
python3 scripts/daily_etl.py
```

### 3. 查看结果

初始化完成后：
- ✅ 数据已导入数据库
- 📊 HTML报告已自动生成（如果启用）
- ⚙️ Init模式自动关闭（避免重复运行）

## 详细说明

### 配置参数

| 参数 | 说明 | 默认值 | 示例 |
|-----|------|--------|------|
| `enabled` | 是否启用初始化模式 | `false` | `true` |
| `days` | 要处理的天数 | `7` | `30` |
| `auto_generate_report` | 完成后自动生成报告 | `true` | `false` |

### 工作流程

```
1. 检测 init.enabled = true
   ↓
2. 从最早日期开始处理
   (例如：7天前 → 6天前 → ... → 昨天)
   ↓
3. 对每一天：
   - 处理所有配置的链
   - 提取数据并存入数据库
   - 记录日志
   ↓
4. 如果 auto_generate_report = true:
   - 自动调用 generate_report.py
   - 生成 reports/index.html
   ↓
5. 自动将 init.enabled 改为 false
   (避免下次运行时重复处理)
   ↓
6. 完成！
```

### 执行示例

#### 示例1：处理过去7天

```yaml
init:
  enabled: true
  days: 7
  auto_generate_report: true
```

运行后输出：
```
================================================================================
🚀 INITIALIZATION MODE - Processing Last 7 Days
================================================================================
Processing 7 days of historical data...

============================================================
Processing date: 2025-11-13 (days_back=7)
Time range: 2025-11-13 00:00:00 to 2025-11-14 00:00:00
============================================================

  Processing chain: bsc
  ✅ Successfully processed bsc: 273 records

  Processing chain: eth
  ✅ Successfully processed eth: 185 records

[... 继续处理其他日期和链 ...]

================================================================================
✅ Initialization Complete!
Total records processed: 7654
Days processed: 7
================================================================================

================================================================================
📊 Generating HTML Report...
================================================================================
✅ Report generated successfully!

💡 Disabling init mode in config...
✅ Init mode disabled. Future runs will process only yesterday's data.
```

#### 示例2：处理过去30天（不自动生成报告）

```yaml
init:
  enabled: true
  days: 30
  auto_generate_report: false
```

```bash
# 运行ETL
python3 scripts/daily_etl.py

# 手动生成报告
python3 scripts/generate_report.py
```

## 注意事项

### ⚠️ 重要提示

1. **数据可用性**：确保parquet文件存在
   ```bash
   # 检查数据是否存在
   ls /server/data/parquet/chain=bsc/date=2025-11-*/
   ```

2. **磁盘空间**：处理大量历史数据需要足够的磁盘空间
   ```bash
   # 检查可用空间
   df -h data/
   ```

3. **执行时间**：处理多天数据需要较长时间
   - 7天 ≈ 10-20分钟
   - 30天 ≈ 30-60分钟
   - 取决于数据量和链数量

4. **自动关闭**：Init模式在成功执行后会自动关闭
   - 配置文件会被修改：`enabled: false`
   - 下次运行将恢复正常模式（只处理昨天）

5. **幂等性**：可以安全地重复运行
   - 相同日期的数据会被替换（DELETE + INSERT）
   - 不会产生重复数据

### 🔍 故障排查

#### 问题1：找不到历史数据

```bash
# 检查parquet文件
ls /server/data/parquet/chain=bsc/date=2025-11-*/

# 如果没有数据，减少天数
# 编辑config.yaml，将days改为更小的值
```

#### 问题2：执行时间过长

```bash
# 减少天数
init:
  days: 3  # 从7改为3

# 或者减少链的数量
chains:
  - bsc  # 只处理一个链先测试
```

#### 问题3：磁盘空间不足

```bash
# 检查空间
df -h

# 清理旧日志
find logs/ -name "*.log" -mtime +30 -delete

# 或减少处理天数
```

#### 问题4：Init模式没有自动关闭

```bash
# 手动关闭
vi config/config.yaml
# 将 enabled: true 改为 enabled: false

# 或使用sed
sed -i 's/enabled: true/enabled: false/' config/config.yaml
```

## 高级用法

### 分批处理大量历史数据

如果需要处理很长时间的历史数据（如90天），建议分批处理：

```bash
# 方法1：多次运行
# 第一次：处理最近30天
vi config/config.yaml  # 设置 enabled: true, days: 30
python3 scripts/daily_etl.py

# 第二次：处理31-60天前
vi config/config.yaml  # 设置 enabled: true, days: 30
# 修改代码或使用自定义脚本处理更早的日期

# 方法2：创建自定义脚本
python3 << 'EOF'
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))

from scripts.daily_etl import *
import yaml

config_path = Path("config/config.yaml")
with open(config_path) as f:
    config = yaml.safe_load(f)

logger = setup_logging(config)
db_path = Path(config['data']['database_path'])
conn = duckdb.connect(str(db_path))

# 处理31-60天前
for days_back in range(60, 30, -1):
    print(f"Processing day {days_back}...")
    process_single_day(conn, config['chains'],
                      config['exclusions']['tokens'],
                      days_back, logger)

conn.close()
EOF
```

### 只为特定链初始化

```yaml
# 临时只处理一个链
chains:
  - bsc  # 注释掉其他链

init:
  enabled: true
  days: 7
```

## 验证数据

初始化完成后，验证数据是否正确导入：

```bash
python3 << 'EOF'
import duckdb

conn = duckdb.connect('data/dex_analytics.duckdb', read_only=True)

# 检查数据范围
print("Date range:")
print(conn.execute("""
    SELECT
        MIN(date) as earliest,
        MAX(date) as latest,
        COUNT(DISTINCT date) as days
    FROM dex_usage_daily
""").df())

# 按链统计
print("\nRecords by chain:")
print(conn.execute("""
    SELECT
        chain_id,
        COUNT(*) as records,
        COUNT(DISTINCT date) as days,
        COUNT(DISTINCT dex_name) as dexes
    FROM dex_usage_daily
    GROUP BY chain_id
""").df())

conn.close()
EOF
```

## 最佳实践

1. **首次部署**：
   ```bash
   # 先测试1天
   init:
     enabled: true
     days: 1

   # 确认成功后再增加天数
   init:
     enabled: true
     days: 7
   ```

2. **定期运行**：
   - Init模式只用于首次或特殊情况
   - 日常使用Crontab自动处理昨天的数据

3. **备份数据库**：
   ```bash
   # 运行init之前备份
   cp data/dex_analytics.duckdb data/dex_analytics.duckdb.backup
   ```

4. **监控日志**：
   ```bash
   # 实时查看进度
   tail -f logs/etl.log
   ```

---

**提示**：Init模式是一次性操作，完成后会自动关闭。日常使用无需手动管理。
