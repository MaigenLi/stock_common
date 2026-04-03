#!/usr/bin/env python3
"""
全市场股票股本信息抓取与缓存
=====================================
数据来源：腾讯财经 qt.gtimg.cn（实时/收盘数据）

抓取字段：
  - name:           股票名称
  - close:          收盘价/最新价
  - prev_close:      昨收
  - open:            开盘价
  - volume:          成交量（手）
  - amount:          成交额（万元）
  - turnover:        换手率（%）
  - pe:              市盈率
  - pb:              市净率
  - total_market_cap:  总市值（亿元）
  - flow_market_cap:   流通市值（亿元）
  - high:            最高价
  - low:             最低价
  - date:            日期时间戳

缓存路径：~/.stock_cache/shares/shares_cache.json
缓存有效期：24小时（收盘后更新一次足够）

用法：
  # 全量抓取（约5-10分钟）
  python fetch_share_base.py --full

  # 增量更新（仅更新缓存中时间戳超过1小时的）
  python fetch_share_base.py --update

  # 单股票查询
  python fetch_share_base.py --code sh600036

  # 定时任务（每日收盘后运行）
  python fetch_share_base.py --full --report
"""

import argparse
import json
import os
import re
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests

# 路径配置
CACHE_DIR = Path.home() / ".stock_cache" / "shares"
CACHE_FILE = CACHE_DIR / "shares_cache.json"
LOG_FILE = CACHE_DIR / "fetch.log"

STOCK_CODES_FILE = Path.home() / "stock_code" / "results" / "stock_codes.txt"
BATCH_SIZE = 100
API_TIMEOUT = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def load_stock_codes(filepath: str = None) -> List[str]:
    """加载股票代码列表"""
    path = Path(filepath) if filepath else STOCK_CODES_FILE
    if not path.exists():
        log.error(f"股票代码文件不存在: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        codes = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return codes


def _normalize_code(code: str) -> tuple:
    """标准化股票代码为腾讯API格式"""
    code = code.strip().lower()
    if code.startswith(("sh", "sz", "bj")):
        return code[:2], code[2:]
    if len(code) == 6:
        first = code[0]
        if first in ("0", "3"):
            return ("sz", code)
        elif first in ("4", "8", "9"):
            return ("bj", code)
        else:
            return ("sh", code)
    return None, None


def _fetch_batch(codes: List[str]) -> Dict[str, dict]:
    """
    批量从腾讯API获取股票数据。
    返回 {code: data_dict}
    """
    if not codes:
        return {}

    # 转换为腾讯API格式
    api_codes = ",".join([f"{m}{p}" for m, p in codes])
    result = {}

    try:
        resp = requests.get(
            f"http://qt.gtimg.cn/q={api_codes}",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "http://finance.qq.com/",
            },
            timeout=API_TIMEOUT,
        )
    except Exception as e:
        log.warning(f"请求失败 {codes[0]}: {e}")
        return {}

    if resp.status_code != 200:
        return {}

    # 解析 v_code="..." 格式
    for m, p in codes:
        code = f"{m}{p}"
        pattern = rf'v_{code}="([^"]+)"'
        m = re.search(pattern, resp.text)
        if not m:
            continue

        fields = m.group(1).split("~")
        if len(fields) < 50:
            continue

        try:
            prev_close = float(fields[4]) if fields[4] else None
            turnover = float(fields[38]) if fields[38] else None
            total_mc = float(fields[45]) if fields[45] else None
            flow_mc = float(fields[44]) if fields[44] else None

            # 换手率校验：腾讯的 turnover 直接是 %，需要验证
            # 计算验证：成交额(万元) / 流通市值(万元) × 100 ≈ turnover
            amount_wan = float(fields[37]) if fields[37] else None
            if amount_wan and flow_mc and flow_mc > 0:
                computed_turnover = amount_wan / (flow_mc * 1e4) * 100
                # 如果腾讯值和计算值差距大，用计算值
                if turnover is not None and abs(computed_turnover - turnover) > 5:
                    turnover = round(computed_turnover, 2)

            result[code] = {
                "name": fields[1] if fields[1] else "",
                "close": float(fields[3]) if fields[3] else None,
                "prev_close": prev_close,
                "open": float(fields[5]) if fields[5] else None,
                "volume": int(fields[36]) if fields[36] else 0,          # 成交量（手）
                "amount_wan": amount_wan,                                 # 成交额（万元）
                "turnover": turnover,                                      # 换手率（%）
                "pe": float(fields[39]) if fields[39] and fields[39] not in ("", "-") else None,
                "pb": float(fields[46]) if fields[46] and fields[46] not in ("", "-") else None,
                "total_market_cap": total_mc,                              # 总市值（亿元）
                "flow_market_cap": flow_mc,                                # 流通市值（亿元）
                "high": float(fields[33]) if fields[33] else None,
                "low": float(fields[34]) if fields[34] else None,
                "updated_at": datetime.now().isoformat(),
            }
        except Exception as e:
            log.debug(f"解析失败 {code}: {e}")
            continue

    return result


def fetch_all_codes(codes: List[str], workers: int = 10, progress: bool = True) -> Dict[str, dict]:
    """
    多线程并发抓取全市场股票数据。

    Parameters
    ----------
    codes : list
        股票代码列表（纯数字或带 sh/sz/bj 前缀）
    workers : int
        并发线程数（腾讯API建议 ≤15）
    progress : bool
        是否打印进度

    Returns
    -------
    dict
        {code: data_dict}
    """
    # 预处理代码
    normalized = []
    for c in codes:
        m, p = _normalize_code(c)
        if m and p:
            normalized.append((m, p))

    total = len(normalized)
    results = {}
    done = 0
    t0 = time.time()

    # 分批
    batches = [normalized[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    def fetch_batch_wrapper(batch_codes):
        return _fetch_batch(batch_codes)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_batch_wrapper, batch): batch for batch in batches}
        for future in as_completed(futures):
            batch = futures[future]
            done += len(batch)
            try:
                batch_result = future.result()
                results.update(batch_result)
            except Exception as e:
                log.warning(f"批次失败: {e}")

            if progress and done % 500 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log.info(f"  进度: {done}/{total} ({done * 100 // total}%)  "
                         f"已用时{elapsed:.0f}s  剩余约{eta:.0f}s")

    elapsed = time.time() - t0
    return results


def load_cache() -> Dict[str, dict]:
    """加载本地缓存"""
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(data: Dict[str, dict], meta: dict = None):
    """保存到本地缓存"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_obj = {
        "_meta": meta or {},
        "_data": data,
    }
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache_obj, f, ensure_ascii=False, indent=1)


def incremental_update(existing: Dict[str, dict], workers: int = 10) -> tuple:
    """
    增量更新：只更新缓存中时间超过1小时的股票。
    返回 (new_data, updated_count)
    """
    now = datetime.now()
    stale_codes = []
    for code, info in existing.items():
        updated_at = info.get("updated_at", "")
        if not updated_at:
            stale_codes.append(code)
            continue
        try:
            updated_time = datetime.fromisoformat(updated_at)
            if (now - updated_time) > timedelta(hours=1):
                stale_codes.append(code)
        except Exception:
            stale_codes.append(code)

    if not stale_codes:
        log.info("无需增量更新，所有数据都在1小时内")
        return {}, 0

    log.info(f"增量更新 {len(stale_codes)} 只股票...")
    new_data = fetch_all_codes(stale_codes, workers=workers, progress=True)

    # 合并到现有缓存
    updated = 0
    for code, info in new_data.items():
        if code in new_data:
            existing[code] = info
            updated += 1

    return existing, updated


def get_share_info(code: str) -> Optional[dict]:
    """
    查询单只股票的股本信息（优先从缓存，缓存失效则抓取）

    Returns
    -------
    dict or None
        包含 name, close, volume, amount_wan, turnover,
        total_market_cap, flow_market_cap 等字段
    """
    cache = load_cache()
    now = datetime.now()

    # 命中缓存且未过期
    if code in cache:
        updated_at = cache[code].get("updated_at", "")
        if updated_at:
            try:
                updated_time = datetime.fromisoformat(updated_at)
                if (now - updated_time) < timedelta(hours=1):
                    return cache[code]
            except Exception:
                pass

    # 缓存失效，单独抓取
    m, p = _normalize_code(code)
    if not m:
        return None
    result = _fetch_batch([(m, p)])
    if result:
        cache[code] = result[code]
        save_cache(cache)
        return result[code]
    return None


def print_share_info(code: str, info: dict):
    """格式化打印股票股本信息"""
    if not info:
        print(f"❌ 未找到 {code} 的数据")
        return

    updated = info.get("updated_at", "未知")
    turnover = info.get("turnover")
    turnover_str = f"{turnover:.2f}%" if turnover is not None else "N/A"
    pe = info.get("pe")
    pe_str = f"{pe:.2f}" if pe is not None else "N/A"
    pb = info.get("pb")
    pb_str = f"{pb:.2f}" if pb is not None else "N/A"

    print(f"""
{'='*60}
  {info.get('name', code)} ({code})
{'='*60}
  行情数据（实时）
  最新价:      {info.get('close'):>12.2f}    昨收:  {info.get('prev_close'):>12.2f}
  开盘:        {info.get('open', 0):>12.2f}    最高:  {info.get('high'):>12.2f}
  最低:        {info.get('low', 0):>12.2f}

  成交数据
  成交量:      {info.get('volume', 0):>12,} 手
  成交额:      {info.get('amount_wan', 0):>12,.2f} 万元
  换手率:      {turnover_str:>12}

  市值数据
  总市值:      {info.get('total_market_cap', 0):>12.2f} 亿元
  流通市值:    {info.get('flow_market_cap', 0):>12.2f} 亿元

  估值指标
  市盈率(PE):  {pe_str:>12}    市净率(PB): {pb_str:>12}

  更新时间:    {updated}
{'='*60}
""")


def main():
    parser = argparse.ArgumentParser(description="全市场股票股本信息抓取与缓存")
    parser.add_argument("--full", action="store_true", help="全量抓取（覆盖缓存）")
    parser.add_argument("--update", action="store_true", help="增量更新（只更新时间超过1小时的）")
    parser.add_argument("--code", "-c", type=str, help="查询单只股票")
    parser.add_argument("--report", action="store_true", help="完成后打印统计报告")
    parser.add_argument("--workers", "-w", type=int, default=10, help="并发数（默认10）")
    parser.add_argument("--codes-file", type=str, help="股票代码文件路径")
    parser.add_argument("--show", action="store_true", help="显示缓存中的股票数量后退出")
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 单股票查询
    if args.code:
        info = get_share_info(args.code)
        print_share_info(args.code, info)
        return

    # 显示缓存状态
    if args.show:
        cache = load_cache()
        meta = cache.get("_meta", {})
        data = cache.get("_data", {})
        print(f"缓存股票数: {len(data)}")
        if "_meta" in cache:
            print(f"最后更新: {meta.get('updated_at', '未知')}")
        return

    # 默认：增量更新
    if not args.full and not args.update:
        args.update = True

    t0 = time.time()

    if args.full:
        log.info("🚀 开始全量抓取全市场股票股本数据...")
        codes = load_stock_codes(args.codes_file)
        log.info(f"待抓取股票数: {len(codes)}")
        results = fetch_all_codes(codes, workers=args.workers, progress=True)

        meta = {
            "mode": "full",
            "updated_at": datetime.now().isoformat(),
            "total": len(codes),
            "fetched": len(results),
            "elapsed_seconds": round(time.time() - t0, 1),
        }
        save_cache(results, meta)
        log.info(f"✅ 全量抓取完成！共获取 {len(results)} 只股票，耗时 {meta['elapsed_seconds']}s")

    elif args.update:
        existing = load_cache()
        data = existing.get("_data", {})
        if not data:
            log.info("缓存为空，自动切换为全量抓取模式")
            args.full = True
            args.update = False
            main()
            return

        # 清除过期条目（超过7天的）
        now = datetime.now()
        expired = []
        for code, info in data.items():
            if code == "_meta":
                continue
            updated_at = info.get("updated_at", "")
            if not updated_at:
                continue
            try:
                age = now - datetime.fromisoformat(updated_at)
                if age > timedelta(days=7):
                    expired.append(code)
            except Exception:
                continue
        for code in expired:
            del data[code]
        log.info(f"已清除 {len(expired)} 条超过7天的旧缓存")

        data, updated = incremental_update(data, workers=args.workers)
        meta = {
            "mode": "incremental",
            "updated_at": datetime.now().isoformat(),
            "total_stocks": len(data),
            "updated_count": updated,
            "elapsed_seconds": round(time.time() - t0, 1),
        }
        save_cache(data, meta)
        log.info(f"✅ 增量更新完成！更新 {updated} 只，总缓存 {len(data)} 只，耗时 {meta['elapsed_seconds']}s")

    if args.report:
        cache = load_cache()
        data = cache.get("_data", {})
        total = len(data)
        with_turnover = sum(1 for v in data.values() if v.get("turnover"))
        with_pe = sum(1 for v in data.values() if v.get("pe"))
        with_cap = sum(1 for v in data.values() if v.get("total_market_cap"))

        meta = cache.get("_meta", {})
        print(f"""
{'='*60}
  抓取报告
{'='*60}
  模式:       {meta.get('mode', '?')}
  更新时间:   {meta.get('updated_at', '?')}
  总耗时:     {meta.get('elapsed_seconds', '?')}s
  ─────────────────────────────────
  股票总数:   {total}
  含换手率:   {with_turnover} ({with_turnover*100//max(total,1)}%)
  含PE:       {with_pe} ({with_pe*100//max(total,1)}%)
  含市值:     {with_cap} ({with_cap*100//max(total,1)}%)
{'='*60}
""")


if __name__ == "__main__":
    main()
