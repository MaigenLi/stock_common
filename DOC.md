# stock_common 模块说明文档

> 股票分析项目的统一数据层，所有上层模块（策略、筛选器、报告）共享此接口。
> 生成时间：2026-04-08

---

## 目录结构

```
stock_common/
├── __init__.py              # 包入口，导出核心 API
├── README.md                # 旧版简要说明
├── tdx_day_reader.py        # 底层：通达信 .day 文件解析 + numpy 缓存
├── complete_kline.py        # 核心：完整 K 线统一接口（含实时补丁逻辑）
├── snapshot.py              # 高层：统一快照（名称 + 板块 + K 线）
├── trend_volume.py          # 策略：趋势放量形态分析
├── fetch_share_base.py      # 数据：股本信息抓取与缓存
├── full_market_scan.py      # 工具：全市场扫描（趋势/放量筛选）
├── post_market_review.py    # 报告：盘后复盘报告生成（定时自动执行）
├── show_kline.py            # 工具：K 线查看与涨幅排序（CLI）
└── show_kline使用指南.md    # show_kline.py 详细使用文档
```

---

## 数据完整性规则（核心原则）

```
1. 非交易日          → 本地目录即完整数据
2. 交易日 15:00 后   → 本地目录即完整数据
3. 交易日 15:00 前   → 本地目录 + 腾讯实时补丁 = 完整数据
```

所有上层接口均遵循此规则，`is_complete` 字段标识数据是否完整。

---

## 数据文件格式

### 通达信 .day 文件

每条记录 **32 字节**，小端序，字段如下：

| 偏移 | 字段 | 类型 | 说明 |
|------|------|------|------|
| 0-3   | date   | uint32  | YYYYMMDD |
| 4-7   | open   | uint32  | 价格 ×100 |
| 8-11  | high   | uint32  | 价格 ×100 |
| 12-15 | low    | uint32  | 价格 ×100 |
| 16-19 | close  | uint32  | 价格 ×100 |
| 20-23 | amount | float32 | **IEEE-754 单精度浮点数**，成交额 |
| 24-27 | volume | uint32  | 成交量(手) ×100 |
| 28-31 | reserved | uint32 | 保留 |

> ⚠️ **成交额字段是单精度浮点数**，解析时必须用 `struct.unpack('<f', ...)` 或 numpy `float32`，不能用整数解析，否则数据会偏差约 3 倍。

### 数据路径

```
通达信数据：  ~/stock_data/vipdoc/{sh,sz,bj}/lday/
numpy 缓存：  ~/.stock_cache/{sh,sz,bj}.index.json + .data.npy
股本缓存：    ~/.stock_cache/shares/shares_cache.json
名称库：      ~/stock_code/results/all_stock_names_final.json
```

---

## 文件详解

---

### 1. tdx_day_reader.py — 底层 .day 文件解析 + numpy 缓存

**职责**：直接读写通达信 .day 二进制文件，构建并管理 numpy memmap 市场级缓存。

**性能优化**：
- numpy memmap 缓存（`~/.stock_cache/`），避免每次读文件解析
- memmap 模式写入，避免大数组在内存中整体复制
- 缓存有效性自动校验（`.day` 文件 mtime 变化时自动重建）
- 市场级索引结构：`{market: {"index": dict, "mm": memmap}}`

**公开 API**：

```python
from stock_common.tdx_day_reader import (
    read_tdx_kline,    # 读取日K线（自动使用缓存）
    print_kline,       # 读取并打印K线（调试/查看用）
    rebuild_all_caches,# 强制重建所有市场缓存
    preload_all_klines,# 批量预加载多只股票K线
)

# 读取最近 N 天数据
data = read_tdx_kline("600036", days=10)

# 读取到指定日期为止
data = read_tdx_kline("600036", days=20, end_date="2026-04-02")

# 打印K线（人类可读格式）
print_kline("600036", days=5, end_date="2026-04-02")

# 强制重建缓存
rebuild_all_caches()

# 批量预加载（供 trend_volume 或 rank 使用）
data_map = preload_all_klines(["600036", "000001"], days=80, workers=30)
```

**内部函数**（外部不应直接调用）：
- `_normalize_code(code)` → `(market, pure_code)`
- `_find_file(code)` → `Path` 对象
- `_parse_record(chunk)` → dict
- `_load_all_records_from_file(fp)` → list[dict]
- `_build_market_cache(market)` / `_ensure_cache_valid(market)`

**缓存结构**：
- `sh.index.json` / `sh.data.npy`：上交所市场索引 + float32 数据
- `sz.index.json` / `sz.data.npy`：深交所
- `bj.index.json` / `bj.data.npy`：北交所

---

### 2. complete_kline.py — 完整 K 线统一接口

**职责**：在 `tdx_day_reader` 基础上，根据"数据完整性规则"自动判断是否需要联网补当天的实时数据。

**公开 API**：

```python
from stock_common import (
    get_complete_kline,       # 单只股票完整K线（含实时补丁逻辑）
    get_complete_kline_batch, # 批量完整K线（多线程）
    get_complete_kline_df,    # 返回 pandas DataFrame
    get_latest_kline,         # 只取最后一根K线
    get_latest_kline_batch,    # 批量最后一根K线
    fetch_tencent_realtime_bar,# 手动获取腾讯实时补丁（调试用）
    read_local_tdx_daily,      # 只读本地（排错用）
    normalize_stock_code,       # 标准化股票代码
    get_interface_usage_rules,  # 返回使用规则列表
    CompleteKlineResult,        # 返回结果 dataclass
    LatestKlineResult,         # 最新K线结果 dataclass
)

# 单只股票完整K线
result = get_complete_kline("600036")
if result.is_complete:
    df = result.data  # pandas DataFrame
    print(result.source, result.reason)
    print("本地最新:", result.local_latest_date)
    print("实时补丁:", result.realtime_date)

# 批量
results = get_complete_kline_batch(["600036", "000001"], max_workers=8)

# DataFrame 版（便于直接计算）
df = get_complete_kline_df("600036", days=60)
df["ma5"] = df["close"].rolling(5).mean()

# 最后一根K线
latest = get_latest_kline("600036")
if latest.is_complete:
    print(latest.kline["date"], latest.kline["close"])
```

**CompleteKlineResult 字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | str | 股票代码 |
| `data` | pd.DataFrame | K线数据，含 date/open/high/low/close/amount/volume |
| `source` | str | 数据来源描述 |
| `local_latest_date` | Timestamp | 本地数据最新日期 |
| `realtime_date` | Timestamp | 实时补丁日期（如用了的话） |
| `used_realtime_patch` | bool | 是否用了实时补丁 |
| `expected_realtime_patch` | bool | 是否预期需要实时补丁 |
| `is_complete` | bool | 数据是否完整 |
| `reason` | str | 完整性说明 |
| `realtime_snapshot` | dict | 实时快照原始数据 |

---

### 3. snapshot.py — 统一快照（推荐入口）

**职责**：提供包含**股票名称 + 板块信息 + 最新K线**的统一快照，是所有上层模块的推荐数据入口。

**内部架构**：
- 名称获取：本地 JSON 名称库（`all_stock_names_final.json`），无网络依赖
- 板块获取：优先使用 `stock_trend/core/stock_sector.py`（若存在），否则使用内置 `_SimpleSectorFetcher`（支持联网腾讯板块字段）
- K线获取：内部调用 `complete_kline` 接口

**公开 API**：

```python
from stock_common import (
    get_stock_snapshot,        # 单只股票统一快照（最高层入口）
    get_stock_snapshot_batch,   # 批量统一快照
    get_sector_snapshot,        # 板块快照（热点/分类）
    get_stock_sector_info,      # 获取单只股票板块信息
    get_local_stock_name,       # 仅获取名称
    StockSnapshot,               # dataclass
)

# 单只股票推荐入口
snap = get_stock_snapshot("600036")
print(snap.code, snap.name)
print(snap.sector_info["main_sector"])
if snap.latest_kline:
    print(snap.latest_kline["close"])
print("完整数据:", snap.is_complete, snap.reason)

# 批量
snaps = get_stock_snapshot_batch(["600036", "000001", "002594"])
for code, snap in snaps.items():
    print(code, snap.name, snap.sector_info.get("main_sector"))

# 仅板块信息
info = get_stock_sector_info("600036")
print(info["main_sector"], info["sectors"])

# 仅名称
name = get_local_stock_name("600036")
```

**StockSnapshot 字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | str | 股票代码 |
| `name` | str | 股票名称 |
| `sector_info` | dict | 板块信息（含 main_sector/sectors/热点） |
| `latest_kline` | dict | 最新一根K线（date/close/open/high/low/volume/amount） |
| `source` | str | 数据来源 |
| `is_complete` | bool | 数据是否完整 |
| `used_realtime_patch` | bool | 是否用了实时补丁 |
| `reason` | str | 完整性说明 |
| `latest_result` | LatestKlineResult | 原始最新K线结果 |
| `full_kline_result` | CompleteKlineResult | 完整K线原始结果（可选） |

---

### 4. trend_volume.py — 趋势放量形态分析

**职责**：判断股票是否处于"上升趋势 + 阶段放量"形态，输出量能指标（换手率、量比等）。

**核心判断逻辑**：
1. **上升趋势**：收盘价在 MA5/MA10/MA20 多头排列之上，MA20 方向向上
2. **阶段放量**：近5日平均成交量 > 前20日平均成交量 × 1.5
3. **量比**：当日成交量 / 前5日均量
4. **换手率**：从本地股本缓存计算（`~/.stock_cache/shares/shares_cache.json`）

**公开 API**：

```python
from stock_common.trend_volume import (
    is_trend_volume,          # 单码分析（独立使用，自己读文件）
    is_trend_volume_batch,    # 批量分析（30线程并发）
    preload_all_klines,       # 一次性预加载（零重复IO，供 rank 复用）
    analyze_from_cache,       # 从预加载数据中分析（show_kline.py 内部使用）
)

# 单码
result = is_trend_volume("sh600036")
if result["is_trend_volume"]:
    print("上升趋势+放量")

# 批量（一次性读完全部，极速）
results = is_trend_volume_batch(codes_list)

# 极速版：预加载 + 分析（show_kline.py --trend-volume 内部用法）
data_map = preload_all_klines(candidates, days=80, workers=30)
for code in candidates:
    tv = analyze_from_cache(code, data_map)
    if tv.get("is_trend_volume"):
        print(code, tv["volume_ratio"])
```

**返回字段**（dict）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `is_trend_volume` | bool | 是否趋势放量 |
| `is_uptrend` | bool | 是否上升趋势 |
| `is_volume_increasing` | bool | 是否量能放大 |
| `is_breakout` | bool | 是否突破平台（今日收盘 > 20日最高） |
| `volume_ratio` | float | 量比（当日/前5日均量） |
| `turnover_rate` | float | 换手率（%） |
| `score` | float | 综合评分 |
| `ma_info` | dict | 均线信息 |

---

### 5. fetch_share_base.py — 股本数据抓取

**职责**：从腾讯财经抓取全市场股票的股本、换手率、市盈率、市净率等数据，缓存到本地供换手率计算使用。

**数据来源**：腾讯财经 `qt.gtimg.cn`

**缓存路径**：`~/.stock_cache/shares/shares_cache.json`（有效期 24 小时）

**CLI 用法**：

```bash
# 全量抓取（约5-10分钟，更新所有股票）
python fetch_share_base.py --full

# 增量更新（只更新时间戳超过1小时的）
python fetch_share_base.py --update

# 单股票查询
python fetch_share_base.py --code sh600036

# 定时任务模式（带日志）
python fetch_share_base.py --full --report
```

**抓取字段**：name / close / prev_close / open / volume / amount / turnover / pe / pb / total_market_cap / flow_market_cap / high / low / date

---

### 6. full_market_scan.py — 全市场扫描

**职责**：在全市场 5000+ 股票中按技术指标筛选符合条件的股票，可配置短期/长期窗口、严格/宽松参数。

**CLI 用法**：

```bash
# 默认参数（60天窗口，中等严格度）
python full_market_scan.py

# 严格参数
python full_market_scan.py --strict

# 宽松参数
python full_market_scan.py --loose

# 120天长期窗口
python full_market_scan.py --long-window

# 严格 + 120天窗口
python full_market_scan.py --strict --long-window

# 自定义并发数
python full_market_scan.py --workers 30

# 关闭联网获取板块（纯离线）
python full_market_scan.py --offline-sector
```

**参数说明**：
- `--strict`：更严格的价格/涨幅阈值，筛出更强势的股票
- `--loose`：宽松阈值，覆盖面更广
- `--long-window`：120天窗口，偏重均线多头 + 形态识别 + 趋势持续性

**输出**：扫描结果保存到 `stock_reports/scan_YYYYMMDD_HHMMSS.txt`

---

### 7. post_market_review.py — 盘后复盘报告

**职责**：每日 17:00 自动执行，生成复盘报告并发送邮件。是定时任务 cron 的执行脚本。

**CLI 用法**：

```bash
# 生成报告并发送邮件
python post_market_review.py

# 仅生成报告（不发送邮件，用于调试）
python post_market_review.py --save-only

# 发送到指定邮箱
python post_market_review.py --to another@email.com
```

**邮件配置**（环境变量或 `~/.openclaw/.env`）：
```
QQ_EMAIL=maigenmuzi@qq.com
QQ_PASS=xxxx（QQ邮箱授权码）
```
SMTP: smtp.qq.com (587, TLS)

**定时任务配置**（已通过 cron 自动化）：
```json
{
  "name": "每日盘后复盘报告",
  "schedule": "0 17 * * 1-5",
  "payload": "python3 stock_common/post_market_review.py --save-only"
}
```

---

### 8. show_kline.py — K 线查看与涨幅排序（CLI 工具）

**职责**：命令行工具，支持单个/批量查询指定股票的K线，以及全市场涨幅排序筛选。

**详细用法**见同目录 `show_kline使用指南.md`，以下为核心用法速查：

```bash
# 查看最近N天K线
python show_kline.py --code 600036 --days 5

# 指定日期K线
python show_kline.py --code 600036 --date 2026-04-02 --days 1

# 详细交易日信息
python show_kline.py --code 600036 --date 2026-04-02 --detail

# 批量查询（从文件）
python show_kline.py --file my_stocks.txt --days 10

# 涨幅筛选（全市场，指定日期）
python show_kline.py --rank --date 2026-04-02 --gain 5.0 --top 50

# 涨幅 + 趋势放量筛选
python show_kline.py --rank --date 2026-04-02 --gain 3.0 --top 20 --trend-volume

# 涨幅 + 趋势放量 + 突破平台
python show_kline.py --rank --date 2026-04-02 --gain 3.0 --top 20 --trend-volume --breakout

# 涨幅 + 换手率过滤
python show_kline.py --rank --date 2026-04-02 --gain 5.0 --top 30 --turnover 3.0

# 输出到文件
python show_kline.py --rank --date 2026-04-02 --gain 5.0 --top 50 --output top_codes.txt
```

> ⚠️ 注意：`--days 1 --date 非交易日` 时，正确提示"非交易日或无数据"，不会用前一个交易日的数据凑数（2026-04-08 修复）。

---

## 接口分层图

```
┌─────────────────────────────────────────────────────────────┐
│                      上层应用                               │
│  full_market_scan / post_market_review / show_kline.py     │
└─────────────────────┬───────────────────────────────────────┘
                      │ get_stock_snapshot / get_stock_snapshot_batch
┌─────────────────────▼───────────────────────────────────────┐
│               snapshot.py（推荐入口）                        │
│   提供: 股票名称 + 板块信息 + 最新K线 + 完整K线            │
└─────────────────────┬───────────────────────────────────────┘
                      │ get_complete_kline / get_latest_kline
┌─────────────────────▼───────────────────────────────────────┐
│           complete_kline.py（完整K线接口）                 │
│   规则：非交易日/15:00后用本地，盘中用本地+腾讯补丁       │
└─────────────────────┬───────────────────────────────────────┘
                      │ read_tdx_kline（优先走缓存）
┌─────────────────────▼───────────────────────────────────────┐
│             tdx_day_reader.py（底层文件读写）              │
│   numpy memmap 缓存 ~/.stock_cache/                         │
│   缓存无效时直接解析 .day 二进制文件                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 代码规范化

### 股票代码格式

所有接口统一支持以下格式（内部自动标准化）：

| 输入格式 | 示例 | 识别为 |
|----------|------|--------|
| 纯数字 | `600036` | sh（上交所） |
| `sh` 前缀 | `sh600036` | sh（上交所） |
| `sz` 前缀 | `sz000001` | sz（深交所） |
| `bj` 前缀 | `bj430001` | bj（北交所） |

纯数字自动判断：0/3 开头 → 深市，4/8/9 开头 → 北市，其他 → 沪市。

### 返回值中的日期格式

- `str` 日期：统一 `YYYY-MM-DD` 格式
- `datetime`：内部计算用，解析后返回字符串

---

## 性能参考

| 操作 | 方式 | 耗时 |
|------|------|------|
| 单码读取（本地缓存命中） | numpy memmap | < 50ms |
| 单码读取（缓存未命中） | 读 .day 文件 | < 200ms |
| 批量读取 5000+ 股票 | 多线程顺序 | ~30s |
| 涨幅筛选 Phase 1 | 每文件仅读最后2条 | ~30s（全市场） |
| 涨幅 + 趋势放量 Phase 2 | 读完整历史 + 分析 | ~300s |
| 预加载 100 只股票历史 | numpy memmap | ~10s |
| 实时补丁获取 | 腾讯财经 API | ~100ms/只 |
