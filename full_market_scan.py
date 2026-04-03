#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场股票扫描脚本
=================
使用 stock_common 统一接口扫描全市场，筛选趋势放量股票。

用法：
  python full_market_scan.py                  # 中等参数（默认）
  python full_market_scan.py --strict         # 严格参数
  python full_market_scan.py --loose          # 宽松参数
  python full_market_scan.py --strict --workers 30  # 自定义线程数
  python full_market_scan.py --offline-sector        # 关闭联网获取板块

输出：
  stock_reports/scan_YYYYMMDD_HHMMSS.txt
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

WORKSPACE = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(WORKSPACE))

from stock_common import get_stock_snapshot

STOCK_CODES_FILE = Path.home() / "stock_code" / "results" / "stock_codes.txt"
RESULTS_DIR = WORKSPACE / "stock_reports"
MAX_WORKERS_DEFAULT = 20
BATCH_SIZE = 100

# ── 参数配置 ──────────────────────────────────────────────

STRICT_PARAMS = {
    'price_above_ma': 'ma10',
    'min_three_day_change': 5.0,
    'max_three_day_change': 25.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.2,
    'max_ten_day_change': 30.0,
    'min_price': 5.0,
    'max_price': 150.0,
    'min_score': 80.0,
}

LOOSE_PARAMS = {
    'price_above_ma': 'ma5',
    'min_three_day_change': 3.0,
    'max_three_day_change': 30.0,
    'min_up_days': 2,
    'min_volume_ratio': 0.8,
    'max_ten_day_change': 50.0,
    'min_price': 3.0,
    'max_price': 200.0,
    'min_score': 60.0,
}

NORMAL_PARAMS = {
    'price_above_ma': 'ma5',
    'min_three_day_change': 3.0,
    'max_three_day_change': 30.0,
    'min_up_days': 2,
    'min_volume_ratio': 1.0,
    'max_ten_day_change': 40.0,
    'min_price': 3.0,
    'max_price': 200.0,
    'min_score': 70.0,
}


# ── 评估函数 ─────────────────────────────────────────────

def evaluate_stock(code: str, params: dict, allow_sector_online: bool = True):
    """
    评估单只股票，返回 None 或结果字典。

    参数：
        allow_sector_online  是否联网获取板块信息（默认 True，关闭则板块回退默认值）
    """
    try:
        snap = get_stock_snapshot(code, include_full_kline=True, allow_sector_online=allow_sector_online)
        df = snap.full_kline_result.data if snap.full_kline_result else None

        if df is None or len(df) < 30:
            return None

        df = df.tail(60).copy()
        df = df[df['close'] > 0].copy()

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else latest

        # ── 排除最新交易日无成交或数据非今天的个股 ──────────────────
        latest_vol = float(latest.get('volume') or 0)
        latest_amt = float(latest.get('amount') or 0)
        latest_date = latest['date']
        if hasattr(latest_date, 'date'):
            latest_date = latest_date.date()

        from datetime import date as Date
        today = Date.today()
        # 最新数据日期必须等于今天，否则排除（停牌/节假日无数据）
        if latest_vol <= 0 or latest_amt <= 0 or latest_date != today:
            return None
        latest_price = float(latest['close'])
        latest_change = (latest_price - float(prev['close'])) / float(prev['close']) * 100

        # 价格过滤
        if latest_price < params['min_price'] or latest_price > params['max_price']:
            return None

        # 计算均线
        closes = df['close'].values
        ma5 = float(closes[-5:].mean()) if len(closes) >= 5 else None
        ma10 = float(closes[-10:].mean()) if len(closes) >= 10 else None
        ma20 = float(closes[-20:].mean()) if len(closes) >= 20 else None

        if ma5 is None or ma10 is None or ma20 is None:
            return None

        # 价格与均线关系
        price_above_ma5 = latest_price > ma5
        price_above_ma10 = latest_price > ma10
        ma5_above_ma10 = ma5 > ma10

        # 三日数据
        if len(df) < 4:
            return None
        three_day_df = df.iloc[-4:]
        three_day_change = (
            float(three_day_df.iloc[-1]['close']) - float(three_day_df.iloc[0]['close'])
        ) / float(three_day_df.iloc[0]['close']) * 100

        if three_day_change < params['min_three_day_change'] or three_day_change > params['max_three_day_change']:
            return None

        # 上涨天数
        up_days = sum(
            1 for i in range(1, 4)
            if float(three_day_df.iloc[i]['close']) > float(three_day_df.iloc[i - 1]['close'])
        )
        if up_days < params['min_up_days']:
            return None

        # 十日数据
        if len(df) < 11:
            return None
        ten_day_df = df.iloc[-11:]
        ten_day_change = (
            float(ten_day_df.iloc[-1]['close']) - float(ten_day_df.iloc[0]['close'])
        ) / float(ten_day_df.iloc[0]['close']) * 100
        if ten_day_change > params['max_ten_day_change']:
            return None

        # 量能分析（成交额）
        amounts = df['amount'].values

        # 近3日成交额量比
        amount_ratios_3d = []
        for i in range(-4, -1):
            if float(amounts[i]) > 0 and float(amounts[i - 1]) > 0:
                ratio = float(amounts[i]) / float(amounts[i - 1])
                amount_ratios_3d.append(ratio)
        avg_amount_ratio_3d = sum(amount_ratios_3d) / len(amount_ratios_3d) if amount_ratios_3d else 0

        if avg_amount_ratio_3d < params['min_volume_ratio']:
            return None

        # 连续放量 / 连续上涨
        consecutive_volume = all(float(amounts[i]) > float(amounts[i - 1]) for i in range(-3, 0))
        consecutive_up = all(float(df.iloc[i]['close']) > float(df.iloc[i - 1]['close']) for i in range(-3, 0))

        # 趋势强度
        trend_strength = 0.0
        if price_above_ma5:
            trend_strength += 0.25
        if price_above_ma10:
            trend_strength += 0.25
        if ma5_above_ma10:
            trend_strength += 0.25
        if ma5 > ma20:
            trend_strength += 0.25

        # 综合评分
        score = 0.0
        score += min(three_day_change, 15) / 15 * 25
        score += min(avg_amount_ratio_3d, 2.0) / 2.0 * 25
        score += trend_strength * 30
        if consecutive_up:
            score += 10
        if consecutive_volume:
            score += 10

        if score < params['min_score']:
            return None

        # 板块信息（直接从 snapshot 中取，get_stock_snapshot 已处理联网逻辑）
        return {
            'code': snap.code,
            'name': snap.name,
            'score': score,
            'latest_price': latest_price,
            'latest_change': latest_change,
            'three_day_change': three_day_change,
            'ten_day_change': ten_day_change,
            'up_days': up_days,
            'avg_volume_ratio': avg_amount_ratio_3d,
            'consecutive_volume': consecutive_volume,
            'consecutive_up': consecutive_up,
            'trend_strength': trend_strength,
            'price_above_ma5': price_above_ma5,
            'price_above_ma10': price_above_ma10,
            'ma5_above_ma10': ma5_above_ma10,
            'ma5': ma5,
            'ma10': ma10,
            'ma20': ma20,
            # 板块（直接从 snap.sector_info 取，保持一致性）
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
    sector_mode = "联网" if allow_sector_online else "本地"
    print(f"全市场股票数量: {total_stocks}只 | 并行线程: {max_workers} | 板块: {sector_mode}")

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
    """保存扫描结果到文件。"""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"scan_{ts}.txt"
    filepath = RESULTS_DIR / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"# 全市场扫描结果\n")
        f.write(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# 参数: {params}\n")
        f.write(f"# 用时: {elapsed_time:.1f}秒\n")
        f.write(f"\n")

        # 板块分布统计
        sector_count: dict = {}
        for s in selected_stocks:
            sec = s['main_sector']
            sector_count[sec] = sector_count.get(sec, 0) + 1
        sorted_sectors = sorted(sector_count.items(), key=lambda x: x[1], reverse=True)

        f.write(f"# 板块分布（共 {len(sorted_sectors)} 个板块）:\n")
        for sec, cnt in sorted_sectors[:20]:
            f.write(f"#   {sec}: {cnt}只\n")
        f.write(f"\n")

        for s in selected_stocks:
            hotness = s['sector_hotness']
            popularity = s['sector_popularity']
            sectors_str = '/'.join(s['sectors']) if s['sectors'] else s['main_sector']
            f.write(f"{s['code']} {s['name']} (评分:{s['score']:.1f}) "
                     f"三日:{s['three_day_change']:+.2f}% 量比:{s['avg_volume_ratio']:.2f} "
                     f"趋势:{s['trend_strength']:.0%} "
                     f"板块:{sectors_str}({s['sector_category']}) "
                     f"{heat_icon(hotness)}{hotness} {popularity_icon(popularity)}{popularity}\n")

    print(f"💾 结果已保存: {filepath}")
    return filepath


def print_results(selected_stocks: list):
    """打印 TOP 结果到终端。"""
    print("\n" + "=" * 80)
    print("🎯 筛选结果")
    print("=" * 80)

    for i, s in enumerate(selected_stocks, 1):
        hotness = s['sector_hotness']
        popularity = s['sector_popularity']
        sectors_str = '/'.join(s['sectors']) if s['sectors'] else s['main_sector']

        print(f"{i:3d}. {s['code']} {s['name']} (评分:{s['score']:.1f})")
        print(f"     价格: {s['latest_price']:7.2f} ({s['latest_change']:+.2f}%)")
        print(f"     三日: {s['three_day_change']:+.2f}% ({s['up_days']}/3上涨) "
              f"| 十日: {s['ten_day_change']:+.2f}%")
        print(f"     量比: {s['avg_volume_ratio']:.2f} | 趋势强度: {s['trend_strength']:.0%}")
        print(f"     板块: {sectors_str} ({s['sector_category']}) "
              f"{heat_icon(hotness)}热度:{hotness} "
              f"{popularity_icon(popularity)}人气:{popularity}")
        print()


# ── CLI ───────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="全市场股票扫描")
    parser.add_argument('--strict', action='store_true',
                        help='严格参数 (筛选约1-3pct)')
    parser.add_argument('--loose', action='store_true',
                        help='宽松参数 (筛选约10-20pct)')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS_DEFAULT,
                        help='并行线程数 (默认 %d)' % MAX_WORKERS_DEFAULT)
    parser.add_argument('--offline-sector', action='store_true',
                        help='关闭联网获取板块信息 (提速)')
    args = parser.parse_args()

    if args.strict:
        params = STRICT_PARAMS
        param_name = "严格参数"
    elif args.loose:
        params = LOOSE_PARAMS
        param_name = "宽松参数"
    else:
        params = NORMAL_PARAMS
        param_name = "中等参数"

    allow_sector_online = not args.offline_sector

    print("=" * 80)
    print(f"📈 全市场股票扫描 | 参数: {param_name} | 板块联网: {allow_sector_online}")
    print("=" * 80)

    selected, elapsed = run_scan(params, max_workers=args.workers,
                                  allow_sector_online=allow_sector_online)

    if selected:
        print_results(selected)
        save_results(selected, params, elapsed)
    else:
        print("❌ 未找到符合条件的股票")
