# 快速开始 - DEX Analytics

## 🚀 5分钟快速部署

### 第一步：安装依赖
```bash
cd /server/share/barry/dex
pip3 install -r requirements.txt
```

### 第二步：验证安装
```bash
python3 scripts/test_setup.py
```

### 第三步：初始化数据库
```bash
python3 scripts/init_database.py
```

### 第四步：运行ETL（处理昨天的数据）
```bash
python3 scripts/daily_etl.py
```

### 第五步：生成报告
```bash
python3 scripts/generate_report.py
```

### 第六步：查看报告
```bash
# 方法1：启动HTTP服务器
cd reports && python3 -m http.server 8000
# 访问: http://your-server:8000

# 方法2：直接下载到本地查看
scp reports/index.html ~/Desktop/
```

### 第七步：设置定时任务
```bash
bash scripts/setup_cron.sh
```

## 📊 报告功能

打开 `reports/index.html` 可以：

- **选择链**: BSC / ETH / Base / Sol
- **选择日期**: 查看任意历史日期
- **选择视图**: Daily / Hourly / Total
- **查看图表**:
  - 📈 使用分布图（饼图/柱状图）
  - 📉 趋势图（折线图）
  - 🏆 排行榜（横向条形图）

## 🔧 常用命令

### 查看日志
```bash
tail -f logs/etl.log
```

### 查看数据库状态
```bash
python3 -c "
import duckdb
conn = duckdb.connect('data/dex_analytics.duckdb', read_only=True)
print('Daily records:', conn.execute('SELECT COUNT(*) FROM dex_usage_daily').fetchone()[0])
print('Chains:', conn.execute('SELECT DISTINCT chain_id FROM dex_usage_daily').fetchall())
conn.close()
"
```

### 重新生成报告
```bash
python3 scripts/generate_report.py
```

### 手动运行ETL
```bash
python3 scripts/daily_etl.py
```

## 📁 重要文件位置

| 文件/目录 | 说明 |
|---------|------|
| `config/config.yaml` | 配置文件（链、路径等） |
| `data/dex_analytics.duckdb` | DuckDB数据库文件 |
| `reports/index.html` | 生成的HTML报告 |
| `logs/etl.log` | ETL运行日志 |
| `scripts/daily_etl.py` | ETL主脚本 |
| `scripts/generate_report.py` | 报告生成脚本 |

## 🔥 故障排查

### 报告无数据？
```bash
# 检查数据库
ls -lh data/dex_analytics.duckdb

# 重新运行ETL
python3 scripts/daily_etl.py
python3 scripts/generate_report.py
```

### 找不到parquet文件？
```bash
# 检查路径
ls /server/data/parquet/chain=bsc/

# 修改config.yaml中的parquet_base_path
vi config/config.yaml
```

### Crontab不运行？
```bash
# 查看crontab
crontab -l

# 检查日志
tail -100 logs/etl.log

# 手动测试
python3 scripts/daily_etl.py
```

## 📚 更多文档

- **完整文档**: [README.md](README.md)
- **部署指南**: [DEPLOY.md](DEPLOY.md)
- **测试脚本**: `python3 scripts/test_setup.py`

## 🎯 定时任务时间表

| 时间（UTC） | 任务 | 说明 |
|-----------|------|------|
| 09:00 | ETL | 处理昨天的数据 |
| 09:30 | Report | 生成最新报告 |

修改运行时间：编辑 `config/config.yaml` 和重新设置 crontab

## ✅ 验证清单

- [ ] Python 3.8+ 已安装
- [ ] 依赖包已安装（`pip3 install -r requirements.txt`）
- [ ] 数据库已初始化（`data/dex_analytics.duckdb` 存在）
- [ ] Parquet文件可访问
- [ ] ETL成功运行一次
- [ ] 报告已生成（`reports/index.html` 存在）
- [ ] Crontab已配置
- [ ] 可以查看报告

全部完成？🎉 你已经成功部署 DEX Analytics！

---

**需要帮助？** 查看日志 `logs/etl.log` 或阅读 `DEPLOY.md`
