# stock_common

股票项目共享接口。

## 完整K线规则

1. **非交易日**：本地目录就是完整数据
2. **交易日 15:00 后**：本地目录就是完整数据
3. **交易日 15:00 前**：本地目录 + 联网获取的当天数据，才是完整数据

## 推荐入口

### 0) 单只股票统一快照（推荐的最高层入口）

```python
from stock_common import get_stock_snapshot

snapshot = get_stock_snapshot('600000')
print(snapshot.code, snapshot.name)
print(snapshot.sector_info['main_sector'])
print(snapshot.latest_kline['close'] if snapshot.latest_kline else None)
print(snapshot.is_complete, snapshot.reason)
```

### 0.1) 批量统一快照

```python
from stock_common import get_stock_snapshot_batch

snapshots = get_stock_snapshot_batch(['600000', '000001', '002594'])
for code, snapshot in snapshots.items():
    print(code, snapshot.name, snapshot.sector_info['main_sector'])
```

### 1) 单只股票：完整K线

```python
from stock_common import get_complete_kline

result = get_complete_kline('600000')
if result.is_complete:
    df = result.data
    print(result.source, result.reason)
```

### 2) 单只股票：只取最后一根K线

```python
from stock_common import get_latest_kline

latest = get_latest_kline('600000')
if latest.is_complete and latest.kline:
    print(latest.kline['date'], latest.kline['close'])
```

### 3) 批量扫描

```python
from stock_common import get_complete_kline_batch

results = get_complete_kline_batch(['600000', '000001', '002594'], max_workers=8)
for code, result in results.items():
    if result.is_complete:
        df = result.data
```

### 4) 批量取最新K线

```python
from stock_common import get_latest_kline_batch

latest_map = get_latest_kline_batch(['600000', '000001', '002594'])
for code, latest in latest_map.items():
    if latest.is_complete and latest.kline:
        print(code, latest.kline['close'])
```

## 快照接口使用规则

- 策略层、报告层、推荐系统默认优先用 `get_stock_snapshot()`
- 批量扫描默认优先用 `get_stock_snapshot_batch()`
- 只在你明确只需要K线而不需要名称/板块时，才退回到 `get_complete_kline()` 或 `get_latest_kline()`
- 如果 `snapshot.is_complete == False`，不要把这份快照当成正式完整数据继续分析

## 不推荐的做法

- 不要在新策略里自己手写 `.day` + 实时行情拼接逻辑
- 不要在交易日 15:00 前把纯本地 `.day` 当成完整K线
- 如果 `is_complete=False`，不要把结果当成完整K线继续分析

## 低层接口

- `read_local_tdx_daily(code)`：只读本地离线数据
- `fetch_tencent_realtime_bar(code)`：获取当天实时补丁

这两个接口主要用于调试、排错、数据校验。
