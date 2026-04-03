#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场股票扫描脚本
=================
支持两种窗口模式：
  - 短期窗口（默认60天）：偏重三日动量 + 趋势强度
  - 长期窗口（120天）：偏重均线多头 + 形态识别 + 趋势持续性

用法：
  python full_market_scan.py                  # 中等参数（默认60天窗口）
  python full_market_scan.py --strict         # 严格参数
  python full_market_scan.py --loose          # 宽松参数
  python full_market_scan.py --long-window    # 切换到120天窗口模式
  python full_market_scan.py --strict --long-window   # 严格+120天
  python full_market_scan.py --loose --long-window    # 宽松+120天
  python full_market_scan.py --workers 30     # 自定义线程数
  python full_market_scan.py --offline-sector        # 关闭联网获取板块

输出：
  stock_reports/scan_YYYYMMDD_HHMMSS.txt
"""

import argparse
import os
import sys
import time
from datetime import date as Date, datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

WORKSPACE = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(WORKSPACE))

from stock_common import get_stock_snapshot

STOCK_CODES_FILE = Path.home() / "stock_code" / "results" / "stock_codes.txt"
RESULTS_DIR = WORKSPACE / "stock_reports"
MAX_WORKERS_DEFAULT = 20
BATCH_SIZE = 100

# ── 窗口长度 ──────────────────────────────────────────────
DEFAULT_WINDOW = 60
LONG_WINDOW = 120

# ── 短期窗口参数（原有逻辑）───────────────────────────────

STRICT_PARAMS_60 = {
    'window': 60,
    'price_above_ma': 'ma10',
    'min_three_day_change': 5.0,
    'max_three_day_change': 25.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.2,
    'consecutive_volume': False,
    'max_ten_day_change': 30.0,
    'min_price': 5.0,
    'max_price': 150.0,
    'min_score': 80.0,
}

LOOSE_PARAMS_60 = {
    'window': 60,
    'price_above_ma': 'ma5',
    'min_three_day_change': 3.0,
    'max_three_day_change': 30.0,
    'min_up_days': 2,
    'min_volume_ratio': 0.8,
    'consecutive_volume': False,
    'max_ten_day_change': 50.0,
    'min_price': 3.0,
    'max_price': 200.0,
    'min_score': 60.0,
}

NORMAL_PARAMS_60 = {
    'window': 60,
    'price_above_ma': 'ma5',
    'min_three_day_change': 3.0,
    'max_three_day_change': 30.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.0,
    'consecutive_volume': False,
    'max_ten_day_change': 40.0,
    'min_price': 3.0,
    'max_price': 200.0,
    'min_score': 70.0,
}

# ── 120天窗口参数（中期趋势 + 形态模式）──────────────────

# 严格：要求完整均线多头排列 + 连续放量
STRICT_PARAMS_120 = {
    'window': 120,
    'price_above_ma': 'ma20',
    'min_three_day_change': 3.0,
    'max_three_day_change': 25.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.3,
    'consecutive_volume': True,
    'max_ten_day_change': 30.0,
    'min_price': 5.0,
    'max_price': 150.0,
    'min_score': 75.0,
    # ── 120天专用参数 ────────────────────────────────────
    'min_ma_count': 3,          # 均线多头排列数量（MA5>MA10>MA20>MA60，至少3条）
    'min_ma60_up_days': 15,     # 近20天内MA60上升的天数（越多越好）
    'max_ma_glue_ratio': 0.05,  # 均线粘合度上限（标准差/均值 < 5%认为粘合）
    'allow_pullback': True,      # 允许回踩后启动（回踩到均线止跌）
    'pullback_ma': 'ma20',      # 回踩哪条均线
    'min_break_ratio': 0.02,    # 突破幅度下限（收盘/均线 - 1 > 2%）
}

# 宽松：门槛低一些，覆盖更多形态
LOOSE_PARAMS_120 = {
    'window': 120,
    'price_above_ma': 'ma10',
    'min_three_day_change': 2.0,
    'max_three_day_change': 30.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.0,
    'consecutive_volume': False,
    'max_ten_day_change': 40.0,
    'min_price': 3.0,
    'max_price': 200.0,
    'min_score': 60.0,
    # ── 120天专用参数 ────────────────────────────────────
    'min_ma_count': 2,          # 至少2条均线多头
    'min_ma60_up_days': 10,
    'max_ma_glue_ratio': 0.08,
    'allow_pullback': True,
    'pullback_ma': 'ma20',
    'min_break_ratio': 0.015,
}

NORMAL_PARAMS_120 = {
    'window': 120,
    'price_above_ma': 'ma20',
    'min_three_day_change': 3.0,
    'max_three_day_change': 30.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.1,
    'consecutive_volume': False,
    'max_ten_day_change': 35.0,
    'min_price': 3.0,
    'max_price': 200.0,
    'min_score': 68.0,
    # ── 120天专用参数 ────────────────────────────────────
    'min_ma_count': 3,
    'min_ma60_up_days': 12,
    'max_ma_glue_ratio': 0.06,
    'allow_pullback': True,
    'pullback_ma': 'ma20',
    'min_break_ratio': 0.02,
}


# ── 均线粘合度计算 ────────────────────────────────────────

def calc_ma_glue_ratio(ma_list):
    """
    计算均线粘合度。
    ma_list: [ma5, ma10, ma20, ma60]
    返回 std/mean，越小越粘合。
    """
    if len(ma_list) < 2:
        return 1.0
    vals = [v for v in ma_list if v and v > 0]
    if len(vals) < 2:
        return 1.0
    mean = sum(vals) / len(vals)
    if mean <= 0:
        return 1.0
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    return (variance ** 0.5) / mean


def calc_ma60_trend(ma60_series):
    """
    计算MA60近20日的趋势方向。
    返回：'rising' / 'falling' / 'flat'
    """
    if len(ma60_series) < 5:
        return 'flat'
    recent = ma60_series[-5:]
    first, last = recent[0], recent[-1]
    if last > first * 1.02:
        return 'rising'
    elif last < first * 0.98:
        return 'falling'
    return 'flat'


def count_ma60_rising_days(ma60_vals, lookback=20):
    """统计近 lookback 天内 MA60 上升的天数"""
    if len(ma60_vals) < lookback:
        lookback = len(ma60_vals)
    count = 0
    for i in range(-lookback, 0):
        if ma60_vals[i] > ma60_vals[i - 1]:
            count += 1
    return count


def detect_pullback(df, pullback_ma, ma_dict):
    """
    检测回踩形态：
    - 近期（5-20天内）有显著回调到 pullback_ma 附近
    - 之后企稳止跌
    返回 True/False
    """
    pullback_val = ma_dict.get(pullback_ma)
    if not pullback_val or pullback_val <= 0:
        return False

    # 近20日低点
    low_vals = df['low'].values[-20:]
    min_idx = low_vals.argmin()
    # 距离今天至少5天，最多20天
    days_ago = 20 - min_idx
    if days_ago < 5 or days_ago > 20:
        return False

    # 低点是否贴近均线（误差5%以内）
    min_low = low_vals[min_idx]
    if min_low >= pullback_val * 1.05:  # 低点高出均线5%以上，不是回踩
        return False

    # 之后是否在均线附近企稳（近5日价格是否回到均线附近）
    recent_closes = df['close'].values[-5:]
    avg_recent = recent_closes.mean()
    # 企稳：近期收盘价在均线1.05倍以内
    return avg_recent < pullback_val * 1.05


def detect_breakout(df, ma_dict, price_above_ma, params):
    """
    检测突破形态。
    检测价格是否对指定均线形成有效突破。
    返回突破幅度（收盘价 / 均线 - 1）
    """
    ma_key = params.get('price_above_ma', 'ma20')
    ma_val = ma_dict.get(ma_key)
    if not ma_val or ma_val <= 0:
        return 0.0
    latest_close = float(df.iloc[-1]['close'])
    return (latest_close / ma_val) - 1.0


# ── 均线发散角度计算 ──────────────────────────────────────

def calc_ma_divergence(closes: list, lookback: int = 20) -> dict:
    """
    计算均线发散程度和速度。

    返回:
        divergence_score:  发散得分（0~100），越大表示发散越明显
        ma_spread_now:      当前 MA5-MA60 的差值（绝对值）
        ma_spread_pct:     当前 MA5 相对 MA60 的偏离百分比
        spread_trend:       'diverging' / 'converging' / 'stable'
        angle_score:        发散角度得分（0~10）
    """
    import numpy as np

    n = min(lookback, len(closes) - 60 if len(closes) > 60 else len(closes) - 5)
    if n < 10:
        return {
            'divergence_score': 0, 'ma_spread_now': 0,
            'ma_spread_pct': 0, 'spread_trend': 'stable', 'angle_score': 0,
        }

    # 逐日计算 MA5 和 MA60
    ma5_series = []
    ma60_series = []
    for i in range(n):
        idx = -(n - i)
        if idx >= -len(closes):
            c5 = closes[idx:idx + 5] if idx + 5 <= 0 else closes[max(0, idx):]
            c60 = closes[idx:idx + 60] if idx + 60 <= 0 else closes[max(0, idx):]
            if len(c5) >= 5 and len(c60) >= 60:
                ma5_series.append(np.mean(c5))
                ma60_series.append(np.mean(c60[-60:]))

    if len(ma5_series) < 10:
        return {
            'divergence_score': 0, 'ma_spread_now': 0,
            'ma_spread_pct': 0, 'spread_trend': 'stable', 'angle_score': 0,
        }

    # 当前发散程度
    spread_now = ma5_series[-1] - ma60_series[-1]
    spread_pct = spread_now / ma60_series[-1] if ma60_series[-1] > 0 else 0

    # 起始发散程度
    spread_start = ma5_series[0] - ma60_series[0]
    spread_start_pct = spread_start / ma60_series[0] if ma60_series[0] > 0 else 0

    # 发散趋势
    spread_change = spread_now - spread_start
    if spread_change > 0.02 * ma60_series[-1]:
        spread_trend = 'diverging'
    elif spread_change < -0.02 * ma60_series[-1]:
        spread_trend = 'converging'
    else:
        spread_trend = 'stable'

    # 发散角度得分：综合考虑发散幅度和速度
    # 发散越大、速度越快，分数越高（满分10）
    abs_spread_pct = abs(spread_pct)
    # 速度：发散的变化量 / 时间
    speed = abs(spread_change) / n if n > 0 else 0
    speed_normalized = min(speed / (0.01 * ma60_series[-1]), 1.0) if ma60_series[-1] > 0 else 0

    # 综合得分（满分10）
    angle_score = min(abs_spread_pct / 0.15 * 5 + speed_normalized * 5, 10)

    return {
        'divergence_score': float(min(abs_spread_pct * 500, 100)),  # 偏离15%=100分
        'ma_spread_now': float(spread_now),
        'ma_spread_pct': float(spread_pct),
        'spread_trend': spread_trend,
        'angle_score': float(angle_score),
    }


# ── 底部形态识别 ──────────────────────────────────────────

def detect_bottom_formation(df, lookback: int = 60) -> dict:
    """
    识别底部反转形态（适用于120天窗口）。

    检测以下形态：
    - 双底（W底）：两个相近的低点，中间有反弹
    - 突破下降趋势线：价格从低位反弹后突破近期下降趋势
    - 底部放量：见底时成交量明显放大
    - 三底/多底：多次在相近位置获得支撑

    返回:
        has_bottom:    是否识别到底部形态
        bottom_type:  'double_bottom' / 'triple_bottom' / 'trend_break' / 'volume_bottom' / None
        bottom_score:  底部信号强度（0~100）
        bottom_days_ago: 底部距今天数
    """
    import numpy as np

    n = min(lookback, len(df) - 1)
    if n < 20:
        return {'has_bottom': False, 'bottom_type': None,
                'bottom_score': 0, 'bottom_days_ago': None}

    lows = df['low'].values[-n:]
    closes = df['close'].values[-n:]
    volumes = df['volume'].values[-n:]

    # ── 1. 双底检测 ────────────────────────────────────────
    # 找局部最低点
    min_val = np.min(lows)
    min_indices = np.where(lows <= min_val * 1.02)[0]  # 2%容差
    if len(min_indices) >= 2:
        # 检查是否有至少两个明显分离的低点
        gaps = np.diff(min_indices)
        significant_gaps = np.where(gaps >= n * 0.15)[0]  # 间隔至少15%窗口
        if len(significant_gaps) >= 1:
            bottom_type = 'double_bottom'
            bottom_days_ago = n - 1 - min_indices[-1]
            # 底部反弹幅度：第二个低点之后的反弹
            if min_indices[-1] < n - 1:
                rebound = (closes[-1] - lows[min_indices[-1]]) / lows[min_indices[-1]]
            else:
                rebound = (closes[-1] - lows[min_indices[-2]]) / lows[min_indices[-2]]
            bottom_score = min(50 + rebound * 200, 100)
            return {
                'has_bottom': True, 'bottom_type': bottom_type,
                'bottom_score': float(bottom_score),
                'bottom_days_ago': int(bottom_days_ago),
            }

    # ── 2. 三底/多底检测 ────────────────────────────────────
    # 在后1/3窗口内找多个相近低点
    recent_third = lows[:n // 3] if n >= 30 else lows
    if len(recent_third) >= 5:
        min_recent = np.min(recent_third)
        near_min_indices = np.where(lows <= min_recent * 1.03)[0]
        if len(near_min_indices) >= 3:
            return {
                'has_bottom': True, 'bottom_type': 'triple_bottom',
                'bottom_score': 60.0,
                'bottom_days_ago': int(n - 1 - near_min_indices[0]),
            }

    # ── 3. 突破下降趋势线 ──────────────────────────────────
    # 用最近20天数据检测
    trend_lookback = min(20, n)
    recent_closes = closes[-trend_lookback:]
    recent_lows = lows[-trend_lookback:]
    if len(recent_closes) >= 10:
        # 简单趋势线：最低点 和 次低点 连线
        slope = (recent_lows[-1] - recent_lows[0]) / (len(recent_lows) - 1)
        # 价格已突破趋势线？
        trend_line_now = recent_lows[0] + slope * (len(recent_lows) - 1)
        if slope < 0 and closes[-1] > trend_line_now * 1.02:
            return {
                'has_bottom': True, 'bottom_type': 'trend_break',
                'bottom_score': 55.0, 'bottom_days_ago': int(trend_lookback - 1),
            }

    # ── 4. 底部放量 ────────────────────────────────────────
    avg_vol = np.mean(volumes[:n // 2]) if n >= 10 else np.mean(volumes)
    recent_vol = np.mean(volumes[-5:]) if len(volumes) >= 5 else volumes[-1]
    if avg_vol > 0 and recent_vol / avg_vol >= 2.0:
        # 找到放量对应的低点
        min_vol_idx = np.argmin(volumes[:len(volumes) // 2])
        bottom_days_ago = int(n - 1 - min_vol_idx)
        if bottom_days_ago <= 30:  # 底部距今不超过30天
            return {
                'has_bottom': True, 'bottom_type': 'volume_bottom',
                'bottom_score': 45.0, 'bottom_days_ago': bottom_days_ago,
            }

    return {'has_bottom': False, 'bottom_type': None, 'bottom_score': 0, 'bottom_days_ago': None}


# ── 评估函数 ─────────────────────────────────────────────

def evaluate_stock(code: str, params: dict, allow_sector_online: bool = True):
    """
    评估单只股票，返回 None 或结果字典。
    """
    window = params.get('window', 60)
    try:
        snap = get_stock_snapshot(code, include_full_kline=True,
                                  allow_sector_online=allow_sector_online)
        df = snap.full_kline_result.data if snap.full_kline_result else None

        if df is None or len(df) < max(30, window):
            return None

        df = df.tail(window).copy()
        df = df[df['close'] > 0].copy()

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else latest

        # ── 排除最新交易日无成交或数据非今天的个股 ────────────────
        latest_vol = float(latest.get('volume') or 0)
        latest_amt = float(latest.get('amount') or 0)
        latest_date = latest['date']
        if hasattr(latest_date, 'date'):
            latest_date = latest_date.date()

        today = Date.today()
        if latest_vol <= 0 or latest_amt <= 0 or latest_date != today:
            return None

        latest_price = float(latest['close'])
        latest_change = (latest_price - float(prev['close'])) / float(prev['close']) * 100

        # ── 价格过滤 ─────────────────────────────────────────
        if latest_price < params['min_price'] or latest_price > params['max_price']:
            return None

        # ── 均线计算 ─────────────────────────────────────────
        closes = df['close'].values
        ma5 = float(closes[-5:].mean()) if len(closes) >= 5 else None
        ma10 = float(closes[-10:].mean()) if len(closes) >= 10 else None
        ma20 = float(closes[-20:].mean()) if len(closes) >= 20 else None
        ma60 = float(closes[-60:].mean()) if len(closes) >= 60 and window >= 60 else None

        ma_dict = {'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60}

        if ma5 is None or ma10 is None or ma20 is None:
            return None
        if window >= 60 and ma60 is None:
            return None

        # ── 均线多头排列 ─────────────────────────────────────
        # 统计满足 ma_n > ma_(n+1) 的数量
        ma_order = [(ma5, ma10), (ma10, ma20)]
        if window >= 60 and ma60:
            ma_order.append((ma20, ma60))
        ma_bullish_count = sum(1 for a, b in ma_order if a is not None and b is not None and a > b)

        # 价格在指定均线上方
        ma_key = params.get('price_above_ma', 'ma5' if window == 60 else 'ma20')
        price_above_ma_val = ma_dict.get(ma_key)
        if price_above_ma_val is None:
            return None
        if latest_price <= price_above_ma_val:
            return None

        # ── 三日数据 ─────────────────────────────────────────
        if len(df) < 4:
            return None
        three_day_df = df.iloc[-4:]
        three_day_change = (
            float(three_day_df.iloc[-1]['close']) - float(three_day_df.iloc[0]['close'])
        ) / float(three_day_df.iloc[0]['close']) * 100

        if (three_day_change < params['min_three_day_change']
                or three_day_change > params['max_three_day_change']):
            return None

        up_days = sum(
            1 for i in range(1, 4)
            if float(three_day_df.iloc[i]['close']) > float(three_day_df.iloc[i - 1]['close'])
        )
        if up_days < params['min_up_days']:
            return None

        # ── 十日数据 ─────────────────────────────────────────
        if len(df) < 11:
            return None
        ten_day_df = df.iloc[-11:]
        ten_day_change = (
            float(ten_day_df.iloc[-1]['close']) - float(ten_day_df.iloc[0]['close'])
        ) / float(ten_day_df.iloc[0]['close']) * 100
        if ten_day_change > params['max_ten_day_change']:
            return None

        # ── 量能分析 ─────────────────────────────────────────
        amounts = df['amount'].values
        amount_ratios_3d = []
        for i in range(-4, -1):
            if float(amounts[i]) > 0 and float(amounts[i - 1]) > 0:
                amount_ratios_3d.append(float(amounts[i]) / float(amounts[i - 1]))
        avg_amount_ratio_3d = sum(amount_ratios_3d) / len(amount_ratios_3d) if amount_ratios_3d else 0

        if avg_amount_ratio_3d < params['min_volume_ratio']:
            return None

        consecutive_volume = all(
            float(amounts[i]) > float(amounts[i - 1]) for i in range(-3, 0)
        )
        consecutive_up = all(
            float(df.iloc[i]['close']) > float(df.iloc[i - 1]['close']) for i in range(-3, 0)
        )

        if params.get('consecutive_volume', False) and not consecutive_volume:
            return None

        # ── 评分 ─────────────────────────────────────────────
        score = 0.0

        if window == 60:
            # 60天窗口：原有评分体系
            trend_strength = 0.0
            if latest_price > ma5:   trend_strength += 0.33
            if ma5 > ma10:           trend_strength += 0.33
            if ma5 > ma20:           trend_strength += 0.34

            score += min(three_day_change, 15) / 15 * 25
            score += min(avg_amount_ratio_3d, 2.0) / 2.0 * 25
            score += trend_strength * 30
            if consecutive_up:       score += 10
            if consecutive_volume:    score += 10

        else:
            # 120天窗口：均线多头 + 形态评分体系
            # 1. 均线多头排列（满分25）
            score += min(ma_bullish_count, 4) / 4 * 25

            # 2. 量比（满分20）
            score += min(avg_amount_ratio_3d, 2.5) / 2.5 * 20

            # 3. MA60趋势方向（满分15）
            if window >= 60 and ma60:
                ma60_vals = [float(closes[max(0, i-59):i+1].mean()) if i >= 59 else None
                             for i in range(len(closes))]
                ma60_vals = [v for v in ma60_vals if v is not None]
                ma60_trend = calc_ma60_trend(ma60_vals)
                if ma60_trend == 'rising':    score += 15
                elif ma60_trend == 'flat':     score += 7
                else:                          score += 0

                # MA60持续上升天数加分（满分10）
                ma60_up_days = count_ma60_rising_days(ma60_vals, lookback=20)
                score += min(ma60_up_days, 20) / 20 * 10

            # 4. 均线粘合加分（满分10，粘合越好加分越高）
            if window >= 60 and ma60:
                glue_ratio = calc_ma_glue_ratio([ma5, ma10, ma20, ma60])
                max_glue = params.get('max_ma_glue_ratio', 0.06)
                if glue_ratio < max_glue * 0.5:   score += 10   # 极度粘合
                elif glue_ratio < max_glue:        score += 6    # 轻度粘合
                elif glue_ratio < max_glue * 2:    score += 3    # 略微发散

            # 5. 三日动量（满分10）
            score += min(three_day_change / 15.0, 1.0) * 10

            # 6. 突破加分（满分5）
            break_ratio = detect_breakout(df, ma_dict, latest_price > price_above_ma_val, params)
            score += max(0, min(break_ratio / params.get('min_break_ratio', 0.02), 1.0)) * 5

            # 7. 连续上涨加分（满分5）
            if consecutive_up:   score += 5

            # 8. 均线发散角度（满分5，发散越明显加分越高）
            if window >= 60 and ma60:
                div = calc_ma_divergence(closes.tolist(), lookback=20)
                divergence_score_norm = min(div['angle_score'] / 10.0, 1.0)
                score += divergence_score_norm * 5   # 满分5

            # 9. 底部形态加分（满分5，识别到底部反转形态时加分）
            if window >= 60:
                bottom = detect_bottom_formation(df, lookback=60)
                if bottom['has_bottom']:
                    bottom_score_norm = bottom['bottom_score'] / 100.0
                    score += bottom_score_norm * 5   # 满分5

        if score < params['min_score']:
            return None

        # ── 额外形态信息（返回字典中，但不参与过滤）───────────────
        ma_glue_ratio = None
        if window >= 60 and ma60:
            ma_glue_ratio = calc_ma_glue_ratio([ma5, ma10, ma20, ma60])

        ma60_trend = None
        ma60_up_days = None
        if window >= 60 and ma60:
            ma60_vals = [float(closes[max(0, i-59):i+1].mean()) if i >= 59 else None
                         for i in range(len(closes))]
            ma60_vals = [v for v in ma60_vals if v is not None]
            ma60_trend = calc_ma60_trend(ma60_vals)
            ma60_up_days = count_ma60_rising_days(ma60_vals, lookback=20)

        pullback = None
        if window >= 60 and params.get('allow_pullback', False):
            pullback = detect_pullback(df, params.get('pullback_ma', 'ma20'), ma_dict)

        divergence = None
        bottom_formation = None
        if window >= 60:
            divergence = calc_ma_divergence(closes.tolist(), lookback=20)
            bottom_formation = detect_bottom_formation(df, lookback=60)

        return {
            'code': snap.code,
            'name': snap.name,
            'score': score,
            'window': window,
            'latest_price': latest_price,
            'latest_change': latest_change,
            'three_day_change': three_day_change,
            'ten_day_change': ten_day_change,
            'up_days': up_days,
            'avg_volume_ratio': avg_amount_ratio_3d,
            'consecutive_volume': consecutive_volume,
            'consecutive_up': consecutive_up,
            'ma5': ma5,
            'ma10': ma10,
            'ma20': ma20,
            'ma60': ma60,
            'ma_bullish_count': ma_bullish_count,
            'ma_glue_ratio': ma_glue_ratio,
            'ma60_trend': ma60_trend,
            'ma60_up_days': ma60_up_days,
            'pullback': pullback,
            'divergence': divergence,
            'bottom_formation': bottom_formation,
            'main_sector': snap.sector_info.get('main_sector', '未知'),
            'sector_category': snap.sector_info.get('sector_category', '其他'),
            'sector_hotness': snap.sector_info.get('sector_hotness', 40),
            'sector_popularity': snap.sector_info.get('sector_popularity', 30),
            'sectors': snap.sector_info.get('sectors', []),
            'sector_source': snap.sector_info.get('source', 'local'),
        }
    except Exception:
        return None


# ── 热点标注 ─────────────────────────────────────────────

def heat_icon(hotness: int) -> str:
    if hotness >= 80: return "🔥🔥"
    if hotness >= 60: return "🔥"
    if hotness >= 40: return "♨️"
    return "⚪"


def popularity_icon(popularity: int) -> str:
    if popularity >= 80: return "👥👥"
    if popularity >= 60: return "👥"
    if popularity >= 40: return "👤"
    return "👤"


def ma_trend_icon(trend: str) -> str:
    return {'rising': '↗️', 'flat': '→', 'falling': '↘️'}.get(trend, '?')


# ── 主流程 ────────────────────────────────────────────────

def run_scan(params: dict, max_workers: int = MAX_WORKERS_DEFAULT,
            allow_sector_online: bool = True):
    """执行全市场扫描，返回 (结果列表, 总耗时)。"""
    all_codes = []
    with open(STOCK_CODES_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if line.startswith('sh') or line.startswith('sz'):
                    all_codes.append(line)

    total_stocks = len(all_codes)
    window = params.get('window', 60)
    sector_mode = "联网" if allow_sector_online else "本地"
    print(f"全市场股票数量: {total_stocks}只 | 并行线程: {max_workers} "
          f"| 窗口: {window}天 | 板块: {sector_mode}")

    start_time = time.time()
    selected_stocks = []
    processed_count = 0

    for batch_start in range(0, total_stocks, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_stocks)
        batch_codes = all_codes[batch_start:batch_end]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(evaluate_stock, code, params, allow_sector_online): code
                for code in batch_codes
            }
            for future in as_completed(futures):
                processed_count += 1
                try:
                    result = future.result(timeout=10)
                    if result:
                        selected_stocks.append(result)
                except Exception:
                    pass

                if processed_count % 200 == 0:
                    elapsed = time.time() - start_time
                    progress = processed_count / total_stocks * 100
                    print(f"  进度: {processed_count}/{total_stocks} ({progress:.1f}%) "
                          f"| 筛选出: {len(selected_stocks)}只 | 用时: {elapsed:.0f}s")

    elapsed_time = time.time() - start_time
    pct = len(selected_stocks) / total_stocks * 100 if total_stocks else 0
    print(f"\n✅ 扫描完成！总用时: {elapsed_time:.0f}秒，"
          f"扫描: {total_stocks}只，筛选出: {len(selected_stocks)}只 ({pct:.2f}%)")

    if selected_stocks:
        selected_stocks.sort(key=lambda x: x['score'], reverse=True)

    return selected_stocks, elapsed_time


def save_results(selected_stocks: list, params: dict, elapsed_time: float):
    """保存扫描结果到文件（格式与终端输出一致）。"""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    window = params.get('window', 60)
    filename = f"scan_{window}d_{ts}.txt"
    filepath = RESULTS_DIR / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        sep = "=" * 80
        f.write(f"{sep}\n")
        f.write(f"全市场扫描结果\n")
        f.write(f"窗口: {window}天 | 用时: {elapsed_time:.1f}秒 | 结果: {len(selected_stocks)}只\n")
        f.write(f"{sep}\n")
        f.write("\n")

        for i, s in enumerate(selected_stocks, 1):
            hotness = s['sector_hotness']
            popularity = s['sector_popularity']
            sectors_str = '/'.join(s['sectors']) if s['sectors'] else s['main_sector']
            w = s.get('window', 60)

            f.write(f"{i:3d}. {s['code']} {s['name']} (评分:{s['score']:.1f})\n")
            f.write(f"     价格: {s['latest_price']:7.2f} ({s['latest_change']:+.2f}%)\n")
            f.write(f"     三日: {s['three_day_change']:+.2f}% ({s['up_days']}/3上涨) "
                    f"| 十日: {s['ten_day_change']:+.2f}%\n")
            f.write(f"     量比: {s['avg_volume_ratio']:.2f} "
                    f"| 连涨:{'Y' if s['consecutive_up'] else 'N'} "
                    f"| 连放:{'Y' if s['consecutive_volume'] else 'N'}\n")

            if w == 120:
                ma60_str = f"{s['ma60']:.2f}" if s.get('ma60') else 'N/A'
                f.write(f"     均线: MA5={s['ma5']:.2f} MA10={s['ma10']:.2f} "
                        f"MA20={s['ma20']:.2f} MA60={ma60_str}\n")
                f.write(f"     多头: {s['ma_bullish_count']}条 | "
                        f"MA60:{s.get('ma60_trend','?')} "
                        f"上升日:{s.get('ma60_up_days',0)}天 | "
                        f"粘合:{s.get('ma_glue_ratio', 0):.2%} "
                        f"{'回踩Y' if s.get('pullback') else ''}\n")

                div = s.get('divergence')
                if div:
                    div_icon = {'diverging': '↗', 'converging': '↘', 'stable': '→'}.get(
                        div.get('spread_trend', ''), '?')
                    f.write(f"     发散: {div_icon}{div.get('spread_trend','?')} "
                            f"角度分:{div.get('angle_score',0):.1f} "
                            f"偏离:{div.get('ma_spread_pct',0):.1%}\n")

                bf = s.get('bottom_formation')
                if bf and bf.get('has_bottom'):
                    bf_name = {'double_bottom': '双底W', 'triple_bottom': '多底',
                                'trend_break': '趋势突破', 'volume_bottom': '放量见底'}.get(
                                    bf.get('bottom_type', ''), bf.get('bottom_type', ''))
                    f.write(f"     底部: {bf_name} 信号强度:{bf.get('bottom_score',0):.0f} "
                            f"距今:{bf.get('bottom_days_ago','?')}天\n")
            else:
                f.write(f"     均线: MA5={s['ma5']:.2f} MA10={s['ma10']:.2f} "
                        f"MA20={s['ma20']:.2f}\n")

            f.write(f"     板块: {sectors_str} ({s['sector_category']}) "
                    f"热度:{hotness} 人气:{popularity}\n")
            f.write("\n")

    print(f"💾 结果已保存: {filepath}")
    return filepath


def print_results(selected_stocks: list, params: dict = None, elapsed: float = 0):
    """打印 TOP 结果到终端（格式与 save_results 完全一致）。"""
    w = params.get('window', 60) if params else 60
    print("\n" + "=" * 80)
    print("全市场扫描结果")
    print(f"窗口: {w}天 | 用时: {elapsed:.1f}秒 | 结果: {len(selected_stocks)}只")
    print("=" * 80)

    for i, s in enumerate(selected_stocks, 1):
        hotness = s['sector_hotness']
        popularity = s['sector_popularity']
        sectors_str = '/'.join(s['sectors']) if s['sectors'] else s['main_sector']
        w = s.get('window', 60)

        print(f"{i:3d}. {s['code']} {s['name']} (评分:{s['score']:.1f})")
        print(f"     价格: {s['latest_price']:7.2f} ({s['latest_change']:+.2f}%)")
        print(f"     三日: {s['three_day_change']:+.2f}% ({s['up_days']}/3上涨) "
              f"| 十日: {s['ten_day_change']:+.2f}%")
        print(f"     量比: {s['avg_volume_ratio']:.2f} "
              f"| 连涨:{'Y' if s['consecutive_up'] else 'N'} "
              f"| 连放:{'Y' if s['consecutive_volume'] else 'N'}")

        if w == 120:
            ma60_str = f"{s['ma60']:.2f}" if s.get('ma60') else 'N/A'
            print(f"     均线: MA5={s['ma5']:.2f} MA10={s['ma10']:.2f} MA20={s['ma20']:.2f} MA60={ma60_str}")
            print(f"     多头: {s['ma_bullish_count']}条 | "
                  f"MA60:{s.get('ma60_trend','?')} "
                  f"上升日:{s.get('ma60_up_days',0)}天 | "
                  f"粘合:{s.get('ma_glue_ratio',0):.2%} "
                  f"{'回踩Y' if s.get('pullback') else ''}")

            # 发散角度信息
            div = s.get('divergence')
            if div:
                div_icon = {'diverging': '↗', 'converging': '↘', 'stable': '→'}.get(
                    div.get('spread_trend', ''), '?')
                print(f"     发散: {div_icon}{div.get('spread_trend','?')} "
                      f"角度分:{div.get('angle_score',0):.1f} "
                      f"偏离:{div.get('ma_spread_pct',0):.1%}")

            # 底部形态信息
            bf = s.get('bottom_formation')
            if bf and bf.get('has_bottom'):
                bf_name = {'double_bottom': '双底W', 'triple_bottom': '多底',
                           'trend_break': '趋势突破', 'volume_bottom': '放量见底'}.get(
                               bf.get('bottom_type', ''), bf.get('bottom_type', ''))
                print(f"     底部: {bf_name} 信号强度:{bf.get('bottom_score',0):.0f} "
                      f"距今:{bf.get('bottom_days_ago','?')}天")
        else:
            print(f"     均线: MA5={s['ma5']:.2f} MA10={s['ma10']:.2f} MA20={s['ma20']:.2f}")

        print(f"     板块: {sectors_str} ({s['sector_category']}) "
              f"热度:{hotness} 人气:{popularity}")
        print()


# ── CLI ───────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="全市场股票扫描")
    parser.add_argument('--strict', action='store_true',
                        help='严格参数')
    parser.add_argument('--loose', action='store_true',
                        help='宽松参数')
    parser.add_argument('--long-window', action='store_true',
                        help='使用120天窗口模式（中期趋势+形态）')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS_DEFAULT,
                        help='并行线程数 (默认 %d)' % MAX_WORKERS_DEFAULT)
    parser.add_argument('--offline-sector', action='store_true',
                        help='关闭联网获取板块信息 (提速)')
    args = parser.parse_args()

    # 选择参数集
    if args.long_window:
        base = 'loose' if args.loose else ('strict' if args.strict else 'normal')
        params_map = {
            'strict': STRICT_PARAMS_120,
            'loose': LOOSE_PARAMS_120,
            'normal': NORMAL_PARAMS_120,
        }
        window_name = "120天中期趋势"
    else:
        base = 'loose' if args.loose else ('strict' if args.strict else 'normal')
        params_map = {
            'strict': STRICT_PARAMS_60,
            'loose': LOOSE_PARAMS_60,
            'normal': NORMAL_PARAMS_60,
        }
        window_name = "60天短期动量"

    params = params_map[base]
    allow_sector_online = not args.offline_sector

    print("=" * 80)
    print(f"📈 全市场股票扫描 | 窗口: {window_name} | 参数: {base}")
    print("=" * 80)

    selected, elapsed = run_scan(params, max_workers=args.workers,
                                  allow_sector_online=allow_sector_online)

    if selected:
        print_results(selected, params=params, elapsed=elapsed)
        save_results(selected, params, elapsed)
    else:
        print("❌ 未找到符合条件的股票")
