"""
K线形态趋势量能分析通用接口
============================
判断股票是否处于"上升趋势 + 阶段放量"形态

使用说明:
  from stock_common.trend_volume import is_trend_volume, is_trend_volume_batch

  # 单码
  result = is_trend_volume("sh600036")

  # 批量（一次性读完全部股票，极速）
  results = is_trend_volume_batch(codes_list)

  # 极速版：从预加载的 dict 直接分析（零磁盘IO）
  data_map = preload_all_klines(codes_list)  # 一次性读完全部
  result = analyze_from_cache(code, data_map)  # 纯内存计算

接口说明:
  is_trend_volume()       - 单码分析（独立使用，自己读文件）
  is_trend_volume_batch() - 批量分析（30线程并发读文件）
  preload_all_klines()    - 一次性预加载全部股票K线（零重复IO）
  analyze_from_cache()    - 从预加载数据中分析（供 rank_stocks 复用）
"""

import sys
import time
import struct
import datetime
from pathlib import Path
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from stock_common.tdx_day_reader import (
    TDX_DATA_DIR, RECORD_SIZE, _normalize_code, _find_file, _parse_record,
    _load_cache_index, _ensure_cache_valid, _market_cache,
)

# ============================================================
#  实时换手率获取（腾讯财经API）
# ============================================================

def _fetch_turnover_rate(code: str, timeout: float = 3.0) -> Optional[float]:
    """
    从腾讯财经实时API获取真实换手率。

    换手率计算公式：
        换手率(%) = 成交额(万元) / 流通市值(万元) × 100
                   = field[37] / (field[44] × 10000) × 100

    Parameters
    ----------
    code : str
        股票代码（支持 sh/sz 前缀或纯数字）
    timeout : float
        请求超时（秒）

    Returns
    -------
    float or None
        换手率（%），如今日无成交或API失败返回 None
    """
    try:
        import requests
        normalized = _normalize_code_for_api(code)
        resp = requests.get(
            f"http://qt.gtimg.cn/q={normalized}",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "http://finance.qq.com/"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None
        text = resp.text.strip()
        if '="' not in text:
            return None
        payload = text.split('="', 1)[1].rstrip('";')
        fields = payload.split('~')
        if len(fields) < 45:
            return None
        amount_wan = _safe_float(fields[37])      # 成交额（万元）
        flow_cap = _safe_float(fields[44])        # 流通市值（亿元）
        if amount_wan is None or flow_cap is None or flow_cap <= 0:
            return None
        turnover = amount_wan / (flow_cap * 1e4) * 100
        return round(turnover, 2)
    except Exception:
        return None


def _normalize_code_for_api(code: str) -> str:
    """标准化代码为腾讯API格式（sh600036）"""
    m, p = _normalize_code(code)
    return f"{m}{p}"


def _safe_float(s: str, default=None):
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


# ============================================================
#  预加载：一次性读取全部股票数据到内存
# ============================================================

def preload_all_klines(
    codes: List[str],
    days: int = 80,
    workers: int = 30,
    progress: bool = True,
) -> Dict[str, list]:
    """
    一次性加载全部股票的K线数据到内存。

    比逐个调用 read_tdx_kline 快 N 倍，因为：
    1. 避免同一文件被多次读取（Phase 1 + Phase 2）
    2. 多线程并发预读

    Parameters
    ----------
    codes : list
        股票代码列表
    days : int
        加载最近多少天（默认80，够算MA60）
    workers : int
        并发数（默认30）

    Returns
    -------
    dict
        {code: [ {date, open, high, low, close, volume, amount}, ... ]}
        按 date 升序排列
    """
    codes = [c.strip() for c in codes if c.strip()]
    total = len(codes)
    result_map: Dict[str, list] = {}
    done = 0
    t0 = time.time()

    # 先确保各市场缓存就绪（惰性构建/验证）
    markets_seen = set()
    for c in codes:
        m, _ = _normalize_code(c)
        markets_seen.add(m)
    for m in markets_seen:
        _ensure_cache_valid(m)

    def load_one(code: str) -> tuple:
        try:
            data = _load_kline_raw(code, days)
            return code, data
        except Exception:
            return code, []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(load_one, c): c for c in codes}
        for future in as_completed(futures):
            done += 1
            if progress and (done % 500 == 0 or done == total):
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  预加载: {done}/{total} ({done*100//total}%)  "
                      f"已用时{elapsed:.0f}s  剩余约{eta:.0f}s", flush=True)
            code, data = future.result()
            result_map[code] = data

    elapsed = time.time() - t0
    if progress:
        print(f"  预加载完成: {total} 只，耗时 {elapsed:.1f}s，"
              f"数据总量 ~{sum(len(v) for v in result_map.values()):,} 条记录")
    return result_map


def _load_kline_raw(code: str, days: int) -> list:
    """内部：优先从 numpy 缓存读取，缓存失效则直接读 .day 文件"""
    market, pure = _normalize_code(code)
    # 尝试从 numpy 缓存读取
    index = _load_cache_index(market)
    if (market in _market_cache and
            index is not None and pure in index.get("stocks", {})):
        info = index["stocks"][pure]
        offset, count = info["offset"], info["count"]
        mm = _market_cache[market]["mm"]
        slice_data = mm[offset:offset + count].astype(np.float64)
        records = []
        for i in range(count):
            r = slice_data[i]
            records.append({
                'date': datetime.datetime.fromtimestamp(r[0]),
                'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4],
                'amount': r[5], 'volume': r[6],
            })
        return records[-days:] if len(records) > days else records

    # 兜底：直接读 .day 文件
    file_path = TDX_DATA_DIR / market / "lday" / f"{market}{pure}.day"
    with open(file_path, 'rb') as f:
        data = f.read()
    records = []
    for i in range(0, len(data), RECORD_SIZE):
        chunk = data[i:i + RECORD_SIZE]
        if len(chunk) < RECORD_SIZE:
            break
        r = _parse_record(chunk)
        records.append(r)
    return records[-days:] if len(records) > days else records


# ============================================================
#  核心分析逻辑（纯函数，接收 dict/list 数据）
# ============================================================

def analyze_from_cache(
    code: str,
    data_map: Dict[str, list],
    trend_days: int = 20,
    vol_lookback: int = 5,
    vol_ratio_threshold: float = 1.5,
    require_ma60_rising: bool = False,
    breakout_lookback: int = 20,
    require_breakout: bool = False,
    turnover_min: float = 0.0,
) -> dict:
    """
    从预加载的 dict 中分析趋势放量。

    完全在内存中运行，零磁盘 IO。

    Parameters
    ----------
    code : str
        股票代码
    data_map : dict
        preload_all_klines() 返回的内存数据
    trend_days : int
        趋势判断天数（默认20）
    vol_lookback : int
        量能窗口（默认5）
    vol_ratio_threshold : float
        放量阈值（默认1.5）
    require_ma60_rising : bool
        是否要求MA60上升（严格模式）
    breakout_lookback : int
        平台突破参考期，默认20天（不算今天）
    require_breakout : bool
        是否要求突破平台（今日收盘 > 过去N天最高价），默认False
    turnover_min : float
        换手率最低要求（%），默认0.0表示不限制
    """
    import pandas as pd

    raw = data_map.get(code) or data_map.get(code.strip()) or []
    if not raw:
        return {"code": code, "is_trend_volume": False, "error": "数据不存在"}

    lookback = max(trend_days, 60, breakout_lookback + 1)
    if len(raw) < lookback:
        return {"code": code, "is_trend_volume": False, "error": f"数据不足{lookback}天"}

    df = pd.DataFrame(raw).sort_values('date').reset_index(drop=True)
    recent = df.iloc[-lookback:].copy().reset_index(drop=True)

    # ---- 均线 ----
    recent['ma5']  = recent['close'].rolling(5).mean()
    recent['ma10'] = recent['close'].rolling(10).mean()
    recent['ma20'] = recent['close'].rolling(20).mean()
    recent['ma60'] = recent['close'].rolling(60).mean()

    # ---- 量能 ----
    recent['vol_ma5'] = recent['volume'].rolling(5).mean()
    recent['volume_ratio'] = recent['volume'] / recent['vol_ma5']

    # ---- 涨跌 ----
    recent['change_pct'] = recent['close'].pct_change() * 100

    latest = recent.iloc[-1]
    last_n = recent.iloc[-trend_days:]

    # ---- 趋势信号 ----
    up_days = int((last_n['change_pct'] > 0).sum())
    trend_strength = up_days / trend_days

    ma5_above_ma10 = latest['ma5'] > latest['ma10']
    ma10_above_ma20 = latest['ma10'] > latest['ma20']
    ma5_above_ma20 = latest['ma5'] > latest['ma20']
    ma_bullish = ma5_above_ma10 and ma10_above_ma20
    price_above_ma20 = latest['close'] > latest['ma20']

    # MA60 斜率
    ma60_rising = False
    if require_ma60_rising and len(recent) >= 66:
        ma60_vals = recent['ma60'].iloc[-5:].values
        if not any(pd.isna(v) for v in ma60_vals):
            slope = (ma60_vals[-1] - ma60_vals[0]) / 4
            ma60_rising = slope > 0

    is_uptrend = trend_strength >= 0.6 and (ma_bullish or price_above_ma20)
    if require_ma60_rising:
        is_uptrend = is_uptrend and ma60_rising

    # ---- 放量信号 ----
    current_vol_ratio = float(latest['volume_ratio']) if not pd.isna(latest['volume_ratio']) else 0.0
    avg_vol_ratio = float(recent['volume_ratio'].iloc[-vol_lookback:].mean())
    vol_trend_up = recent['volume_ratio'].iloc[-1] > recent['volume_ratio'].iloc[-vol_lookback]
    is_volume_increasing = (
        avg_vol_ratio >= vol_ratio_threshold
        or (vol_trend_up and current_vol_ratio >= 1.2)
    )

    # ---- 平台突破信号 ----
    # 过去 breakout_lookback 天（不含今天）的最高价
    recent_no_today = recent.iloc[-breakout_lookback - 1:-1]
    recent_high = recent_no_today['high'].max()
    is_breakout = float(latest['close']) > float(recent_high)

    # ---- 换手率（腾讯实时API，真实值）----
    # 公式：成交额(万元) / 流通市值(万元) × 100
    # = field[37] / (field[44] × 10000) × 100
    turnover_rate = _fetch_turnover_rate(code)

    # ---- 综合 ----
    interval_change = float(
        (latest['close'] - recent.iloc[-trend_days]['close'])
        / recent.iloc[-trend_days]['close'] * 100
    )

    if len(recent) >= 2:
        last_day = recent.iloc[-1]
        prev_day = recent.iloc[-2]
        day_change = float((last_day['close'] - prev_day['close']) / prev_day['close'] * 100)
    else:
        day_change = 0.0

    is_trend_volume = bool(is_uptrend and is_volume_increasing)

    return {
        "code": code,
        "is_trend_volume": is_trend_volume,
        "is_uptrend": is_uptrend,
        "is_volume_increasing": is_volume_increasing,
        "volume_ratio": round(current_vol_ratio, 2),
        "avg_volume_ratio": round(avg_vol_ratio, 2),
        "trend_strength": round(float(trend_strength), 2),
        "change_pct": round(interval_change, 2),
        "day_change_pct": round(day_change, 2),
        "ma5_above_ma20": bool(ma5_above_ma20),
        "price_above_ma20": bool(price_above_ma20),
        "ma60_rising": bool(ma60_rising),
        "is_breakout": is_breakout,
        "recent_high": round(float(recent_high), 2),
        "turnover_rate": turnover_rate,
        "days": len(recent),
    }


# ============================================================
#  单码接口（自己读文件，供独立使用）
# ============================================================

def is_trend_volume(
    code: str,
    trend_days: int = 20,
    vol_lookback: int = 5,
    vol_ratio_threshold: float = 1.5,
    require_ma60_rising: bool = False,
    breakout_lookback: int = 20,
    require_breakout: bool = False,
    turnover_min: float = 0.0,
) -> dict:
    """判断单只股票是否处于"上升趋势 + 阶段放量"形态"""
    from stock_common.tdx_day_reader import read_tdx_kline

    try:
        data = read_tdx_kline(code, days=trend_days + 60)
    except FileNotFoundError:
        return {"code": code, "is_trend_volume": False, "error": "数据文件不存在"}

    if not data or len(data) < trend_days:
        return {"code": code, "is_trend_volume": False, "error": "数据不足"}

    import pandas as pd
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    return analyze_from_cache(
        code, {code: df.to_dict('records')},
        trend_days=trend_days,
        vol_lookback=vol_lookback,
        vol_ratio_threshold=vol_ratio_threshold,
        require_ma60_rising=require_ma60_rising,
        breakout_lookback=breakout_lookback,
        require_breakout=require_breakout,
        turnover_min=turnover_min,
    )


# ============================================================
#  批量接口（30线程并发读文件，供 is_trend_volume 替代）
# ============================================================

def is_trend_volume_batch(
    codes: list,
    trend_days: int = 20,
    vol_lookback: int = 5,
    vol_ratio_threshold: float = 1.5,
    require_ma60_rising: bool = False,
    breakout_lookback: int = 20,
    require_breakout: bool = False,
    turnover_min: float = 0.0,
    workers: int = 30,
    progress: bool = True,
) -> list:
    """批量分析（并发读文件）"""
    codes = [c.strip() for c in codes if c.strip()]
    total = len(codes)
    results = [None] * total
    done = 0
    t0 = time.time()

    def fetch_and_analyze(idx_code):
        idx, code = idx_code
        import pandas as pd
        from stock_common.tdx_day_reader import read_tdx_kline
        try:
            data = read_tdx_kline(code, days=trend_days + 60)
        except FileNotFoundError:
            return idx, {"code": code, "is_trend_volume": False, "error": "文件不存在"}
        if not data or len(data) < trend_days:
            return idx, {"code": code, "is_trend_volume": False, "error": "数据不足"}
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        res = analyze_from_cache(
            code, {code: df.to_dict('records')},
            trend_days=trend_days,
            vol_lookback=vol_lookback,
            vol_ratio_threshold=vol_ratio_threshold,
            require_ma60_rising=require_ma60_rising,
            breakout_lookback=breakout_lookback,
            require_breakout=require_breakout,
            turnover_min=turnover_min,
        )
        return idx, res

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_and_analyze, (i, c)): (i, c) for i, c in enumerate(codes)}
        for future in as_completed(futures):
            done += 1
            if progress and (done % 200 == 0 or done == total):
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  趋势放量: {done}/{total} ({done*100//total}%)  "
                      f"已用时{elapsed:.0f}s  剩余约{eta:.0f}s", flush=True)
            idx, res = future.result()
            results[idx] = res

    return results


# ============================================================
#  便捷打印
# ============================================================

def print_trend_volume(code: str, **kwargs):
    """打印单码趋势量能分析结果"""
    result = is_trend_volume(code, **kwargs)
    c = result['code']

    if "error" in result:
        print(f"❌ {c}: {result['error']}")
        return result

    signal = "✅ 上升趋势+放量" if result['is_trend_volume'] else "❌ 非上升趋势放量"
    trend_txt = "强势" if result['is_uptrend'] else "弱势"
    vol_txt = "放量" if result['is_volume_increasing'] else "缩量"

    print(f"\n{'='*60}")
    print(f"  {c}  趋势量能分析")
    print(f"{'='*60}")
    print(f"  信号:   {signal}")
    print(f"  趋势:   {trend_txt} (强度 {result['trend_strength']:.0%})")
    print(f"  量能:   {vol_txt} (量比 {result['volume_ratio']:.2f}, 均量比 {result['avg_volume_ratio']:.2f})")
    print(f"  区间涨跌: {result['change_pct']:+.2f}%")
    print(f"  MA5>MA20: {'是' if result['ma5_above_ma20'] else '否'}  |  "
          f"价格>MA20: {'是' if result['price_above_ma20'] else '否'}  |  "
          f"MA60上升: {'是' if result['ma60_rising'] else '否'}")
    print(f"{'='*60}")
    return result


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "sh600036"
    print_trend_volume(code)
