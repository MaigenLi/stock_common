"""
通达信 .day 文件解析接口
=========================
从本地离线目录读取股票日K线数据，支持 numpy 内存映射缓存

数据目录: ~/stock_data/vipdoc/
缓存目录: ~/.stock_cache/
缓存格式: float32 numpy memmap，每股票最近250条记录
"""

import struct
import datetime
import os
import json
import time as time_module
import shutil
from pathlib import Path
from typing import Literal, Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

# 默认数据目录
TDX_DATA_DIR = Path(os.path.expanduser("~/stock_data/vipdoc"))
STOCK_CACHE_DIR = Path(os.path.expanduser("~/.stock_cache"))

# 通达信.day文件格式说明:
# 每条记录 32 字节，小端序 <，字段如下:
#   0-3:   date    (uint32, YYYYMMDD)
#   4-7:   open    (uint32, 价格×100)
#   8-11:  high    (uint32, 价格×100)
#   12-15: low     (uint32, 价格×100)
#   16-19: close   (uint32, 价格×100)
#   20-23: amount  (float,  IEEE-754单精度浮点，成交额)
#   24-27: volume  (uint32, 成交量(手)×100)
#   28-31: reserved (uint32, 保留)
RECORD_SIZE = 32

# 缓存配置
RECORDS_PER_STOCK = 250  # 每股票缓存最近250条（约1年交易日）
DTYPE = np.float32
FIELDS = 8  # date, open, high, low, close, amount, volume, reserved

# 缓存文件名
INDEX_SUFFIX = ".index.json"
DATA_SUFFIX = ".data.npy"

# 模块级缓存（进程内重用，避免重复 JSON 解析 + mmap）
# 结构: {market: {"index": dict, "mm": memmap}}
_market_cache: Dict[str, dict] = {}


# ============================================================
#  核心二进制解析（保持不变，作为构建缓存的数据源）
# ============================================================

def _normalize_code(code: str) -> tuple[str, str]:
    code = code.strip().lower()
    if code.startswith("sh"):
        return ("sh", code[2:])
    elif code.startswith("sz"):
        return ("sz", code[2:])
    elif code.startswith("bj"):
        return ("bj", code[4:])
    elif len(code) == 6:
        first = code[0]
        if first in ("0", "3"):
            return ("sz", code)
        elif first in ("4", "8", "9"):
            return ("bj", code)
        else:
            return ("sh", code)
    raise ValueError(f"无法识别的股票代码: {code}")


def _find_file(code: str) -> Path:
    market, pure = _normalize_code(code)
    file_path = TDX_DATA_DIR / market / "lday" / f"{market}{pure}.day"
    if not file_path.exists():
        raise FileNotFoundError(f"找不到数据文件: {file_path}")
    return file_path


def _parse_record(chunk: bytes) -> dict:
    date, open_, high, low, close, amount, volume, _ = struct.unpack('<IIIIIfII', chunk)
    return {
        'date':   datetime.datetime.strptime(str(date), '%Y%m%d'),
        'open':   open_ / 100.0,
        'high':   high / 100.0,
        'low':    low / 100.0,
        'close':  close / 100.0,
        'amount': float(amount),
        'volume': volume / 100.0,
    }


def _load_all_records_from_file(file_path: Path) -> list:
    with open(file_path, 'rb') as f:
        data = f.read()
    records = []
    for i in range(0, len(data), RECORD_SIZE):
        chunk = data[i:i + RECORD_SIZE]
        if len(chunk) < RECORD_SIZE:
            break
        records.append(_parse_record(chunk))
    return records


# ============================================================
#  缓存管理
# ============================================================

def _get_cache_paths(market: str) -> Tuple[Path, Path]:
    """获取指定市场的缓存文件路径"""
    index_path = STOCK_CACHE_DIR / f"{market}{INDEX_SUFFIX}"
    data_path = STOCK_CACHE_DIR / f"{market}{DATA_SUFFIX}"
    return index_path, data_path


def _get_market_day_dir(market: str) -> Path:
    return TDX_DATA_DIR / market / "lday"


def _collect_day_files(market: str) -> Dict[str, Path]:
    """收集市场上所有 .day 文件"""
    day_dir = _get_market_day_dir(market)
    if not day_dir.exists():
        return {}
    files = {}
    prefix = f"{market}"
    for fp in day_dir.glob(f"{prefix}*.day"):
        pure = fp.stem[len(prefix):]  # 去掉市场前缀
        files[pure] = fp
    return files


def _build_market_cache(market: str, verbose: bool = True) -> bool:
    """
    为指定市场构建/重建缓存。
    返回是否成功。
    """
    index_path, data_path = _get_cache_paths(market)
    day_files = _collect_day_files(market)

    if not day_files:
        return False

    if verbose:
        print(f"  [{market}] 构建缓存中 ({len(day_files)} 只股票)...", flush=True)

    # 第1步：收集每只股票的记录
    # 格式: {pure_code: [(date_ts, open, high, low, close, amount, volume), ...]}
    all_data = {}  # code -> list of records
    for pure, fp in day_files.items():
        try:
            recs = _load_all_records_from_file(fp)
            # 只保留最近 RECORDS_PER_STOCK 条
            recs = recs[-RECORDS_PER_STOCK:]
            if recs:
                all_data[pure] = recs
        except Exception:
            continue

    if not all_data:
        return False

    # 准备连续存储
    sorted_codes = sorted(all_data.keys())
    total_records = sum(len(all_data[c]) for c in sorted_codes)

    # 第2步：构建 index 和数据
    index_data = {}  # code -> {offset, count, mtimes}
    data_list = []   # flat list of records

    mtimes = {}
    for pure, fp in day_files.items():
        mtimes[pure] = int(fp.stat().st_mtime)

    offset = 0
    for pure in sorted_codes:
        recs = all_data[pure]
        count = len(recs)
        index_data[pure] = {
            "offset": offset,
            "count": count,
            "mtime": mtimes.get(pure, 0),
        }
        for r in recs:
            data_list.append((
                r['date'].timestamp(),
                r['open'],
                r['high'],
                r['low'],
                r['close'],
                r['amount'],
                r['volume'],
                0.0,  # reserved
            ))
        offset += count

    # 第3步：保存
    STOCK_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # index JSON
    meta = {
        "version": 1,
        "market": market,
        "built_at": time_module.time(),
        "records_per_stock": RECORDS_PER_STOCK,
        "stocks": index_data,
    }
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=1, ensure_ascii=False)

    # 数据 numpy（float32，内存映射写入）
    arr = np.array(data_list, dtype=DTYPE)
    # 用 memmap 模式写入，避免大数组在内存中复制
    with open(data_path, 'wb') as f:
        np.save(f, arr)
        f.flush()
        os.fsync(f.fileno())

    if verbose:
        print(f"  [{market}] 缓存完成: {len(sorted_codes)} 只, {total_records} 条记录, "
              f"{arr.nbytes / 1024**2:.1f} MB", flush=True)
    # 立即写入模块缓存（含 day_files）
    _, data_path = _get_cache_paths(market)
    mm = np.load(data_path, mmap_mode='r')
    day_files = _collect_day_files(market)
    _market_cache[market] = {"index": meta, "mm": mm, "day_files": day_files}
    return True


def _load_cache_index(market: str) -> Optional[dict]:
    """加载缓存 index（模块级缓存），若不存在或损坏返回 None"""
    if market in _market_cache:
        return _market_cache[market]["index"]
    index_path, _ = _get_cache_paths(market)
    if not index_path.exists():
        return None
    try:
        with open(index_path, 'r', encoding='utf-8') as f:
            index = json.load(f)
        # 缓存到模块级（含 day_files 避免重复 glob）
        _, data_path = _get_cache_paths(market)
        if data_path.exists():
            mm = np.load(data_path, mmap_mode='r')
            day_files = _collect_day_files(market)
            _market_cache[market] = {"index": index, "mm": mm, "day_files": day_files}
        return index
    except Exception:
        return None


def _is_cache_valid(market: str, index: dict) -> bool:
    """
    检查缓存是否有效。
    
    策略：只检查缓存文件自身的 mtime（WSL2 下 glob+全量 stat 太慢）。
    同时对少量随机股票做 spot-check（最多 10 只）。
    """
    import random
    index_path, data_path = _get_cache_paths(market)
    # 1. 缓存文件自身是否被外部改过
    if not (index_path.exists() and data_path.exists()):
        return False
    cache_mtime = int(index_path.stat().st_mtime)
    if cache_mtime != int(index.get("built_at", 0)):
        return False
    # 2. spot-check：随机抽 5 只股票验证 mtime（最多 5ms × 5 = 25ms）
    stocks = index.get("stocks", {})
    day_files = _market_cache.get(market, {}).get("day_files")
    if day_files is None:
        day_files = _collect_day_files(market)
    samples = random.sample(list(stocks.items()), min(5, len(stocks)))
    for pure, info in samples:
        if pure not in day_files:
            return False
        cur = int(day_files[pure].stat().st_mtime)
        if cur != info.get("mtime", -1):
            return False
    return True


def _ensure_cache_valid(market: str, force_rebuild: bool = False) -> bool:
    """确保缓存有效，无效则惰性重建"""
    index = _load_cache_index(market)
    if index is None or force_rebuild or not _is_cache_valid(market, index):
        return _build_market_cache(market)
    return True


# ============================================================
#  公开 API
# ============================================================

def read_tdx_kline(
    code: str,
    days: Optional[int] = None,
    end_date: Optional[Literal["today"] | str | datetime.date] = None,
    include_full: bool = False,
) -> list[dict]:
    """
    读取通达信日K线数据（自动使用 numpy 缓存）

    Parameters
    ----------
    code : str
        股票代码，支持:
        - 纯代码: "600862", "000001"
        - 带前缀: "sh600862", "sz000001", "bj430001"
    days : int, optional
        获取最近多少天数据（从 end_date 往前算）
        若不指定，则返回 end_date 之前的所有历史数据
    end_date : str or date, optional
        截止日期，默认为今天

    Returns
    -------
    list[dict]
        K线数据列表，每条包含:
        date (str), open, high, low, close, amount, volume
        按日期升序排列
    """
    market, pure = _normalize_code(code)

    # 尝试模块缓存
    index = _load_cache_index(market)
    cache_hit = (market in _market_cache and
                 index is not None and
                 pure in index.get("stocks", {}))

    if cache_hit:
        info = index["stocks"][pure]
        offset = info["offset"]
        count = info["count"]
        try:
            mm = _market_cache[market]["mm"]
            slice_data = mm[offset:offset + count].astype(np.float64)
            records = _slice_to_records(slice_data, count)
            return _filter_by_date(records, end_date, days)
        except Exception:
            pass
    else:
        # 缓存不存在或失效，惰性重建
        _ensure_cache_valid(market)
        # 再试一次
        if market in _market_cache:
            index = _market_cache[market]["index"]
            if pure in index.get("stocks", {}):
                info = index["stocks"][pure]
                offset = info["offset"]
                count = info["count"]
                try:
                    mm = _market_cache[market]["mm"]
                    slice_data = mm[offset:offset + count].astype(np.float64)
                    records = _slice_to_records(slice_data, count)
                    return _filter_by_date(records, end_date, days)
                except Exception:
                    pass

    # 兜底：直接从 .day 文件读取
    fp = _find_file(code)
    all_records = _load_all_records_from_file(fp)
    return _filter_by_date(all_records, end_date, days)


def _slice_to_records(slice_data: np.ndarray, count: int) -> list:
    """将 numpy 切片转换为 dict records 列表"""
    records = []
    for i in range(count):
        r = slice_data[i]
        ts, open_, high, low, close, amount, volume = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
        records.append({
            'date': datetime.datetime.fromtimestamp(ts),
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'amount': amount,
            'volume': volume,
        })
    return records


def _filter_by_date(records: list, end_date, days: Optional[int]) -> list:
    """对记录列表按日期和天数过滤"""
    if end_date is None or end_date == "today":
        end_dt = datetime.datetime.now()
    elif isinstance(end_date, str):
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    elif isinstance(end_date, datetime.date):
        end_dt = datetime.datetime.combine(end_date, datetime.time())
    else:
        raise TypeError(f"end_date 类型不支持: {type(end_date)}")

    filtered = [r for r in records if r['date'] <= end_dt]

    if days is not None:
        filtered = filtered[-days:]

    result = []
    for r in filtered:
        result.append({
            'date':   r['date'].strftime('%Y-%m-%d'),
            'open':   round(r['open'], 2),
            'high':   round(r['high'], 2),
            'low':    round(r['low'], 2),
            'close':  round(r['close'], 2),
            'volume': round(r['volume'], 0),
            'amount': round(r['amount'], 2),
        })
    return result


def print_kline(code: str, days: int = 10, end_date: str = "today") -> list[dict]:
    """打印K线数据的便捷函数"""
    data = read_tdx_kline(code, days=days, end_date=end_date)
    if not data:
        print(f"未找到数据: {code}")
        return []

    print(f"\n{'='*80}")
    print(f"股票代码: {code}  |  最近 {len(data)} 天  |  截止: {data[-1]['date']}")
    print(f"{'='*80}")
    print(f"{'日期':<12} {'开盘':>8} {'最高':>8} {'最低':>8} {'收盘':>8} {'成交量(手)':>12} {'成交额':>14}")
    print("-" * 80)
    for r in data:
        print(f"{r['date']:<12} {r['open']:>8.2f} {r['high']:>8.2f} {r['low']:>8.2f} "
              f"{r['close']:>8.2f} {r['volume']:>12.0f} {r['amount']:>14.2f}")
    print("-" * 80)
    return data


def rebuild_all_caches(verbose: bool = True) -> Dict[str, bool]:
    """强制重建所有市场缓存，返回每市场的结果"""
    if verbose:
        print("🔨 强制重建所有市场缓存...")
    markets = ['sh', 'sz', 'bj']
    results = {}
    for m in markets:
        ok = _build_market_cache(m, verbose=verbose)
        results[m] = ok
    return results


def preload_all_klines(
    codes: list,
    days: int = 80,
    workers: int = 30,
    progress: bool = True,
) -> Dict[str, list]:
    """
    一次性预加载多只股票的K线数据（优先使用缓存）。
    返回 {code: [records...]}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from stock_common.tdx_day_reader import read_tdx_kline as _read_tdx

    # 先把代码按市场分组
    by_market = {}
    for code in codes:
        m, pure = _normalize_code(code.strip())
        if m not in by_market:
            by_market[m] = []
        by_market[m].append((code, pure))

    # 确保缓存有效
    for m in by_market:
        _ensure_cache_valid(m)

    result = {}
    total = len(codes)
    done = 0

    for code, _ in [(c, p) for m, grp in by_market for c, p in grp]:
        done += 1
        if progress and done % 500 == 0:
            print(f"  预加载进度: {done}/{total}", flush=True)
        try:
            records = _read_tdx(code, days=days)
            result[code] = records
        except Exception:
            result[code] = []
    return result


if __name__ == "__main__":
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "600862"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    t0 = time_module.time()
    data = print_kline(code, days=days)
    print(f"\n⏱️  耗时: {time_module.time() - t0:.3f}s")
    if not data:
        print("⚠️  未获取到数据")
