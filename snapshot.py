#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票统一快照接口。

目标：
1. 统一获取股票名称
2. 统一获取板块信息
3. 统一获取最新完整日K / 完整K线
4. 为策略、筛选器、报告系统提供更高层的数据入口
"""

from __future__ import annotations

import importlib.util
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Iterable, Optional

from .complete_kline import (
    CompleteKlineResult,
    LatestKlineResult,
    get_complete_kline,
    get_latest_kline,
    normalize_stock_code,
)

LOCAL_STOCK_NAMES_FILE = os.path.expanduser("~/stock_code/results/all_stock_names_final.json")
WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
STAGE_TREND_SECTOR_FILE = os.path.join(WORKSPACE_ROOT, 'stock_trend', 'core', 'stock_sector.py')


@dataclass
class StockSnapshot:
    code: str
    name: str
    sector_info: Dict
    latest_kline: Optional[Dict]
    source: str
    is_complete: bool
    used_realtime_patch: bool
    reason: str
    latest_result: LatestKlineResult
    full_kline_result: Optional[CompleteKlineResult] = None


@lru_cache(maxsize=1)
def _load_local_stock_names() -> Dict[str, str]:
    if not os.path.exists(LOCAL_STOCK_NAMES_FILE):
        return {}

    try:
        with open(LOCAL_STOCK_NAMES_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return {}

    stock_names: Dict[str, str] = {}
    stocks_data = data.get('stocks', {}) if isinstance(data, dict) else {}
    for stock_key, stock_info in stocks_data.items():
        if not isinstance(stock_info, dict):
            continue
        code = str(stock_info.get('code', '')).strip().lower()
        name = str(stock_info.get('name', '')).strip()
        full_code = str(stock_key).strip().lower()
        if code and name:
            stock_names[code] = name
        if full_code and name:
            stock_names[full_code] = name
    return stock_names


def get_local_stock_name(code: str) -> str:
    normalized = normalize_stock_code(code)
    code_num = normalized[2:]
    stock_names = _load_local_stock_names()
    return stock_names.get(normalized) or stock_names.get(code_num) or '未知'


@lru_cache(maxsize=1)
def _load_sector_module():
    if not os.path.exists(STAGE_TREND_SECTOR_FILE):
        return None

    spec = importlib.util.spec_from_file_location('stock_common_stage_sector', STAGE_TREND_SECTOR_FILE)
    if spec is None or spec.loader is None:
        return None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def _get_sector_fetcher():
    module = _load_sector_module()
    if module is None or not hasattr(module, 'get_sector_info'):
        return None
    try:
        return module.get_sector_info()
    except Exception:
        return None


def get_stock_sector_info(code: str, allow_online: bool = True) -> Dict:
    """
    获取单只股票的板块信息及板块热点。

    参数：
        code        股票代码（如 sh600036、600036、sz000858）
        allow_online  是否允许联网获取（默认 True）

    返回字典：
        code              标准化代码
        name              股票名称
        sectors           所属板块列表
        main_sector       主板块名称
        sector_category   板块分类（科技/新能源/医药/…）
        sector_hotness    板块热度 0~100
        sector_popularity 板块人气 0~100
        source            数据来源

    示例：
        >>> info = get_stock_sector_info('sh600036')
        >>> print(info['main_sector'], info['sector_hotness'])
    """
    normalized = normalize_stock_code(code)
    name = get_local_stock_name(normalized)
    snap = get_sector_snapshot(normalized, name=name, allow_online=allow_online)
    return snap


def get_sector_snapshot(code: str, name: str = '', allow_online: bool = True) -> Dict:
    normalized = normalize_stock_code(code)
    fetcher = _get_sector_fetcher()
    if fetcher is None:
        return {
            'code': normalized,
            'name': name,
            'sectors': [],
            'main_sector': '未知',
            'sector_hotness': 40,
            'sector_popularity': 30,
            'sector_category': '其他',
            'source': 'snapshot_default',
        }

    try:
        return fetcher.get_stock_sector_info(normalized, name=name, allow_online=allow_online)
    except TypeError:
        try:
            return fetcher.get_stock_sector_info(normalized, name=name)
        except Exception:
            pass
    except Exception:
        pass

    return {
        'code': normalized,
        'name': name,
        'sectors': [],
        'main_sector': '未知',
        'sector_hotness': 40,
        'sector_popularity': 30,
        'sector_category': '其他',
        'source': 'snapshot_default',
    }


def get_stock_snapshot(
    code: str,
    include_full_kline: bool = False,
    allow_sector_online: bool = True,
    data_dir: Optional[str] = None,
    now=None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
) -> StockSnapshot:
    """获取单只股票统一快照。"""
    normalized = normalize_stock_code(code)

    latest_result = get_latest_kline(
        code=normalized,
        data_dir=data_dir,
        now=now,
        timeout=timeout,
        allow_realtime_patch=allow_realtime_patch,
        keep_zero_volume=keep_zero_volume,
    )

    full_result = None
    if include_full_kline:
        full_result = get_complete_kline(
            code=normalized,
            data_dir=data_dir,
            now=now,
            timeout=timeout,
            allow_realtime_patch=allow_realtime_patch,
            keep_zero_volume=keep_zero_volume,
        )

    name = get_local_stock_name(normalized)
    if name == '未知' and latest_result.full_result.realtime_snapshot:
        name = latest_result.full_result.realtime_snapshot.get('name') or '未知'

    sector_info = get_sector_snapshot(
        normalized,
        name=name,
        allow_online=allow_sector_online,
    )

    return StockSnapshot(
        code=normalized,
        name=name,
        sector_info=sector_info,
        latest_kline=latest_result.kline,
        source=latest_result.source,
        is_complete=latest_result.is_complete,
        used_realtime_patch=latest_result.used_realtime_patch,
        reason=latest_result.reason,
        latest_result=latest_result,
        full_kline_result=full_result,
    )


def get_stock_snapshot_batch(
    codes: Iterable[str],
    include_full_kline: bool = False,
    allow_sector_online: bool = True,
    data_dir: Optional[str] = None,
    now=None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
    max_workers: int = 8,
) -> Dict[str, StockSnapshot]:
    """批量获取股票统一快照。"""
    normalized_codes = [normalize_stock_code(code) for code in codes]
    if not normalized_codes:
        return {}

    results: Dict[str, StockSnapshot] = {}
    worker_count = max(1, min(max_workers, len(normalized_codes)))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                get_stock_snapshot,
                code,
                include_full_kline,
                allow_sector_online,
                data_dir,
                now,
                timeout,
                allow_realtime_patch,
                keep_zero_volume,
            ): code
            for code in normalized_codes
        }

        for future in as_completed(future_map):
            code = future_map[future]
            try:
                results[code] = future.result()
            except Exception as exc:
                fallback_latest = get_latest_kline(
                    code=code,
                    data_dir=data_dir,
                    now=now,
                    timeout=timeout,
                    allow_realtime_patch=allow_realtime_patch,
                    keep_zero_volume=keep_zero_volume,
                )
                results[code] = StockSnapshot(
                    code=code,
                    name=get_local_stock_name(code),
                    sector_info={
                        'code': code,
                        'name': get_local_stock_name(code),
                        'sectors': [],
                        'main_sector': '未知',
                        'sector_hotness': 40,
                        'sector_popularity': 30,
                        'sector_category': '其他',
                        'source': f'batch_snapshot_error: {exc}',
                    },
                    latest_kline=fallback_latest.kline,
                    source=fallback_latest.source,
                    is_complete=fallback_latest.is_complete,
                    used_realtime_patch=fallback_latest.used_realtime_patch,
                    reason=f'批量快照异常: {exc}',
                    latest_result=fallback_latest,
                    full_kline_result=None,
                )

    return results


def get_stock_snapshot_usage_rules():
    return [
        '默认优先使用 get_stock_snapshot(code) 作为策略层和报告层入口。',
        '批量扫描、批量报告、批量打分优先使用 get_stock_snapshot_batch(codes)。',
        '如果只关心K线，不关心名称和板块，才退回 get_complete_kline / get_latest_kline。',
        '如果只做纯本地历史研究，才允许直接用 read_local_tdx_daily(code)。',
        '交易日15:00前需要完整K线时，不要关闭 allow_realtime_patch。',
        '如果 snapshot.is_complete=False，不能把 snapshot.latest_kline 当成完整K线用于正式分析。',
        '名称默认以本地名称库为准，板块默认允许在线获取并带缓存回退。',
    ]
