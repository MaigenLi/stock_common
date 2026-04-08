#!/usr/bin/env python3
"""
显示指定股票在指定日期的交易信息
用法:
  # 单个代码
  python show_kline.py --code sh600036 --date 2026-04-02

  # 批量从文件读取（每行一个代码）
  python show_kline.py --file codes.txt --date 2026-04-02

  # 只看最近N天（不指定日期则默认今天）
  python show_kline.py --code 600036 --days 5

  # 批量+多日
  python show_kline.py --file codes.txt --date 2026-04-02 --days 5

  # 涨幅筛选排序（指定日期 + 涨幅阈值 + 排名前数量）
  python show_kline.py --rank --date 2026-04-02 --gain 5.0 --top 50
  python show_kline.py --rank --date 2026-04-02 --gain 3.0 --top 100 --output top_codes.txt

  # 涨幅 + 趋势放量双重筛选（批量极速版）
  python show_kline.py --rank --date 2026-04-02 --gain 3.0 --top 20 --trend-volume
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

# stock_common 目录本身即 Python 包路径
# 检测运行上下文：从 stock_common 目录内运行 vs 从 workspace 运行
_SCRIPT_DIR = Path(__file__).parent.resolve()
_IN_STOCK_COMMON_DIR = _SCRIPT_DIR.name == 'stock_common'

if _IN_STOCK_COMMON_DIR:
    # 从 stock_common 目录内运行（如 cd stock_common && python show_kline.py）
    from tdx_day_reader import print_kline, read_tdx_kline
    from trend_volume import preload_all_klines, analyze_from_cache
else:
    # 从 workspace 根目录运行（如 python stock_common/show_kline.py）
    from stock_common.tdx_day_reader import print_kline, read_tdx_kline
    from stock_common.trend_volume import preload_all_klines, analyze_from_cache

DEFAULT_CODE_FILE = '/home/hfie/stock_code/results/stock_codes.txt'


def load_codes_from_file(file_path: str) -> list[str]:
    """从文件加载股票代码列表"""
    codes = []
    with open(os.path.expanduser(file_path), 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                codes.append(line)
    return codes


def show_single(code: str, date: str, days: int):
    """显示单个股票信息"""
    try:
        if date:
            print_kline(code, days=days, end_date=date)
        else:
            print_kline(code, days=days)
    except FileNotFoundError as e:
        print(f"❌ {code}: {e}")
    except Exception as e:
        print(f"❌ {code}: {e}")


def show_detail(code: str, date: str):
    """显示指定日期的详细交易数据"""
    try:
        data = read_tdx_kline(code, days=10, end_date=date)
        if not data:
            print(f"❌ {code}: 未找到 {date} 的数据")
            return

        # 找到最接近目标日期的记录
        target = None
        for r in data:
            if r['date'] == date:
                target = r
                break

        if target is None:
            print(f"⚠️  {code}: {date} 非交易日或无数据，最近可用日期: {data[-1]['date']}")
            target = data[-1]

        # 找前一天算涨跌幅
        idx = data.index(target)
        prev = data[idx - 1] if idx > 0 else None

        # 基本信息
        print(f"\n{'='*60}")
        print(f"  {target['date']} 交易日明细")
        print(f"{'='*60}")
        print(f"  股票代码: {code}")
        print(f"  开盘:     {target['open']:.2f}")
        print(f"  最高:     {target['high']:.2f}")
        print(f"  最低:     {target['low']:.2f}")
        print(f"  收盘:     {target['close']:.2f}")
        print(f"  成交量:   {target['volume']:,.0f} 手")
        print(f"  成交额:   {target['amount']:,.2f} 元")

        if prev:
            change = ((target['close'] - prev['close']) / prev['close']) * 100
            print(f"  昨收:     {prev['close']:.2f}")
            print(f"  涨跌幅:   {change:+.2f}%")
            print(f"  涨跌额:   {target['close'] - prev['close']:+.2f}")
        else:
            print(f"  昨收:     N/A")
            print(f"  涨跌幅:   N/A")

        # 上下影线分析
        body_top = max(target['open'], target['close'])
        body_bottom = min(target['open'], target['close'])
        upper_shadow = target['high'] - body_top
        lower_shadow = body_bottom - target['low']
        body = abs(target['close'] - target['open'])

        print(f"\n  K线形态:")
        print(f"  上影线:   {upper_shadow:.2f} {'▲' if upper_shadow > body else ' '}")
        print(f"  下影线:   {lower_shadow:.2f} {'▼' if lower_shadow > body else ' '}")
        print(f"  实体:     {body:.2f} {'阳线' if target['close'] >= target['open'] else '阴线'}")
        print(f"{'='*60}")

    except FileNotFoundError as e:
        print(f"❌ {code}: {e}")
    except Exception as e:
        print(f"❌ {code}: {e}")


def rank_stocks(date: str, gain_threshold: float, top_n: int,
                output_file: Optional[str], workers: int,
                trend_volume_only: bool = False,
                require_breakout: bool = False,
                turnover_min: float = 0.0):
    """
    涨幅筛选排序（惰性加载版）

    策略：
      - 纯涨幅模式：只读每文件最后2条，顺序执行无并发争抢
      - 趋势放量模式：第一阶段只读2条筛选，第二阶段才对候选股读完整历史

    性能：纯涨幅 ~30s，趋势放量 ~300s（旧版 ~500s+）
    """
    import time
    print(f"\n{'='*70}")
    mode_desc = "涨幅+趋势放量筛选" if trend_volume_only else "涨幅筛选排序"
    print(f"  {mode_desc}  |  日期: {date}  |  涨幅≥{gain_threshold}%  |  取前{top_n}名")
    print(f"{'='*70}")

    codes = load_codes_from_file(DEFAULT_CODE_FILE)
    total = len(codes)
    t0 = time.time()

    # ---------- Phase 1: 快速涨幅筛选（只读最后2条，顺序磁臂友好） ----------
    print(f"\n  [Phase 1] 涨幅筛选中（每文件仅读最后2条）...", flush=True)
    t_p1 = time.time()
    gain_results = []
    done = 0

    for code_raw in codes:
        code = code_raw.strip()
        done += 1
        if done % 1000 == 0 or done == total:
            elapsed = time.time() - t_p1
            rate = done / elapsed if elapsed > 0 else 0
            eta = (total - done) / rate if rate > 0 else 0
            print(f"  进度: {done}/{total} ({done*100//total}%)  已用时{elapsed:.0f}s  剩余{eta:.0f}s", flush=True)

        try:
            # 只读最后2条记录（目标日+昨收），算涨幅
            raw2 = _read_last_n_records(code, 2)
            if not raw2 or len(raw2) < 2:
                continue
            target = raw2[-1]
            prev = raw2[-2]
            if target['date'].strftime('%Y-%m-%d') != date:
                continue
            change = (target['close'] - prev['close']) / prev['close'] * 100
            if change >= gain_threshold:
                gain_results.append({
                    'code': code,
                    'date': date,
                    'close': target['close'],
                    'prev_close': prev['close'],
                    'change_pct': round(change, 2),
                    'volume': target['volume'],
                    'amount': target['amount'],
                    '_close_price': target['close'],  # 供后续用
                })
        except Exception:
            continue

    phase1_time = time.time() - t_p1
    print(f"  [Phase 1] 完成！涨幅≥{gain_threshold}%: {len(gain_results)} 只，耗时 {phase1_time:.1f}s")

    if not gain_results:
        print("  无符合涨幅条件的股票")
        return

    # ---------- Phase 2: 趋势放量筛选（仅对候选股读取完整历史） ----------
    if trend_volume_only:
        print(f"\n  [Phase 2] 趋势放量筛选中（仅对 {len(gain_results)} 只读完整历史）...", flush=True)
        t_p2 = time.time()

        # 一次性预加载候选股的完整数据
        cand_codes = [r['code'] for r in gain_results]
        data_map = preload_all_klines(cand_codes, days=80, workers=min(workers, len(cand_codes)),
                                      progress=True)
        t_load = time.time() - t_p2

        # 分析
        filtered = []
        for r in gain_results:
            tv = analyze_from_cache(
                r['code'], data_map,
                require_breakout=require_breakout,
                turnover_min=turnover_min,
            )
            if tv.get('is_trend_volume', False):
                # 额外硬性过滤：突破平台、换手率
                if require_breakout and not tv.get('is_breakout'):
                    continue
                if turnover_min > 0:
                    tr = tv.get('turnover_rate')
                    if tr is None or tr < turnover_min:
                        continue
                r['_tv'] = tv
                filtered.append(r)
        phase2_time = time.time() - t_p2
        print(f"  [Phase 2] 完成！趋势放量通过: {len(filtered)} 只（加载{t_load:.1f}s + 分析{phase2_time-t_load:.1f}s）")
    else:
        filtered = gain_results

    # ---------- Phase 3: 排序输出 ----------
    filtered.sort(key=lambda x: x['change_pct'], reverse=True)
    top = filtered[:top_n]

    total_time = time.time() - t0
    cond_parts = [f"涨幅≥{gain_threshold}%"]
    if trend_volume_only:
        cond_parts.append("趋势放量")
    if require_breakout:
        cond_parts.append("突破平台")
    if turnover_min > 0:
        cond_parts.append(f"换手率>{turnover_min}%")
    filter_desc = " + ".join(cond_parts) if len(cond_parts) > 1 else cond_parts[0]
    print(f"\n{'='*70}")
    print(f"  扫描完成！共 {total} 只，总耗时 {total_time:.1f}s")
    print(f"  符合条件: {len(filtered)} 只（{filter_desc}）")
    print(f"{'='*70}")
    print(f"  {'排名':>4}  {'代码':<10}  {'收盘价':>8}  {'昨收':>8}  {'涨跌幅':>9}  {'量比':>5}  {'趋势':>4}  {'放量':>4}  {'突破':>4}  {'换手率':>7}  {'成交量(手)':>12}")
    print(f"  {'-'*95}")

    output_lines = []
    for i, r in enumerate(top, 1):
        tv = r.get('_tv', {})
        vol_ratio = tv.get('volume_ratio', None)
        trend_s = '强' if tv.get('is_uptrend') else ('弱' if tv else '')
        vol_s = '是' if tv.get('is_volume_increasing') else ('否' if tv else '')
        breakout_s = '✅' if tv.get('is_breakout') else ('❌' if tv else '')
        turnover = tv.get('turnover_rate', None)
        vol_disp = f"{vol_ratio:.2f}" if isinstance(vol_ratio, float) else ('-' if not tv else '?')
        turnover_disp = f"{turnover:.1f}%" if isinstance(turnover, float) else ('-' if not tv else '?')
        print(f"  {i:>4}  {r['code']:<10}  {r['close']:>8.2f}  {r['prev_close']:>8.2f}  "
              f"{r['change_pct']:>+8.2f}%  {vol_disp:>5}  {trend_s:>4}  {vol_s:>4}  {breakout_s:>4}  "
              f"{turnover_disp:>7}  {r['volume']:>12,.0f}")
        output_lines.append(r['code'])

    print(f"  {'-'*82}")

    if output_file:
        out_path = os.path.expanduser(output_file)
        with open(out_path, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(line + '\n')
        print(f"  ✅ 已输出 {len(output_lines)} 个代码到: {out_path}")
    else:
        print(f"\n  代码: {', '.join(output_lines)}")


def _read_last_n_records(code: str, n: int) -> list:
    """只读取文件最后 n 条记录，极轻量"""
    if _IN_STOCK_COMMON_DIR:
        from tdx_day_reader import _normalize_code, _find_file, _parse_record, RECORD_SIZE
    else:
        from stock_common.tdx_day_reader import _normalize_code, _find_file, _parse_record, RECORD_SIZE
    market, pure = _normalize_code(code)
    fp = _find_file(code)
    with open(fp, 'rb') as f:
        f.seek(0, 2)
        file_size = f.tell()
        # 计算最后 n 条记录的位置
        start_pos = max(0, file_size - RECORD_SIZE * n)
        f.seek(start_pos)
        data = f.read()
    records = []
    for i in range(0, len(data), RECORD_SIZE):
        chunk = data[i:i + RECORD_SIZE]
        if len(chunk) < RECORD_SIZE:
            break
        records.append(_parse_record(chunk))
    # 返回最后 n 条（升序）
    return records[-n:] if len(records) > n else records


def main():
    parser = argparse.ArgumentParser(description='显示股票K线数据')

    # 模式互斥组
    mode_group = parser.add_mutually_exclusive_group(required=False)
    mode_group.add_argument('--code', '-c', type=str, help='单个股票代码')
    mode_group.add_argument('--file', '-f', type=str, nargs='?',
                            const=DEFAULT_CODE_FILE, default=None,
                            help='从文件批量读取股票代码（每行一个）')
    mode_group.add_argument('--rank', action='store_true',
                            help='涨幅筛选排序模式')

    # 通用参数
    parser.add_argument('--date', '-d', type=str, default=None,
                        help='指定日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=10,
                        help='显示最近多少天 (默认10天)')
    parser.add_argument('--detail', action='store_true',
                        help='显示详细交易日信息（仅单码模式）')

    # rank 专属参数
    parser.add_argument('--gain', '-g', type=float, default=5.0,
                        help='涨幅阈值%% (默认5.0)')
    parser.add_argument('--top', '-n', type=int, default=50,
                        help='取前N名 (默认50)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='输出代码到文件')
    parser.add_argument('--workers', '-w', type=int, default=20,
                        help='并发数 (默认20)')
    parser.add_argument('--trend-volume', action='store_true',
                        help='叠加趋势放量筛选（仅在--rank模式生效）')
    parser.add_argument('--breakout', action='store_true',
                        help='要求突破平台（今日收盘 > 过去20天最高价）')
    parser.add_argument('--turnover', type=float, default=0.0,
                        help='换手率阈值%%（默认不限制）')
    parser.add_argument('--rebuild-cache', action='store_true',
                        help='重建 numpy 缓存后退出（独立选项，不受互斥组约束）')

    args = parser.parse_args()

    if args.rebuild_cache:
        if _IN_STOCK_COMMON_DIR:
            from tdx_day_reader import rebuild_all_caches
        else:
            from stock_common.tdx_day_reader import rebuild_all_caches
        rebuild_all_caches()
        return

    # 必须指定一种模式
    if not args.code and not args.file and not args.rank:
        print("❌ 必须指定 --code / --file / --rank 之一")
        sys.exit(1)

    if args.rank:
        # 涨幅排序模式
        if not args.date:
            print("❌ --rank 模式必须指定 --date")
            sys.exit(1)
        rank_stocks(args.date, args.gain, args.top, args.output, args.workers,
                    args.trend_volume, args.breakout, args.turnover)
        return

    # 常规模式：单码 / 批量
    if args.code:
        codes = [args.code]
    else:
        file_path = args.file or DEFAULT_CODE_FILE
        codes = load_codes_from_file(file_path)
        print(f"📂 从文件加载了 {len(codes)} 个股票代码")

    print(f"📊 日期: {args.date or '今天'}  |  天数: {args.days}")

    for i, code in enumerate(codes, 1):
        print(f"\n[{i}/{len(codes)}]", end="")
        if args.detail and len(codes) == 1:
            show_detail(code.strip(), args.date)
        else:
            show_single(code.strip(), args.date, args.days)


if __name__ == "__main__":
    main()
