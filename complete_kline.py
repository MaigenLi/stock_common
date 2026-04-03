#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的完整日 K 线获取接口。

规则固定如下：
1. 非交易日：本地目录就是完整数据
2. 交易日 15:00 后：本地目录就是完整数据
3. 交易日 15:00 前：本地目录 + 联网获取的当天数据 = 完整数据

说明：
- 历史部分始终以通达信本地离线数据为准
- 交易日 15:00 前，如果本地目录还没有今天这一根 K 线，就尝试用腾讯行情补当天数据
- 返回结果会附带元信息，明确是否用了实时补丁、是否判定为完整数据
"""

from __future__ import annotations

import os
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_TDX_DIR_CANDIDATES = (
    os.path.expanduser("~/stock_data/vipdoc/"),
    "/mnt/d/new_tdx/vipdoc/",
)


@dataclass
class CompleteKlineResult:
    code: str
    data: pd.DataFrame
    source: str
    local_latest_date: Optional[pd.Timestamp]
    realtime_date: Optional[pd.Timestamp]
    used_realtime_patch: bool
    expected_realtime_patch: bool
    is_complete: bool
    reason: str
    realtime_snapshot: Optional[Dict] = None


@dataclass
class LatestKlineResult:
    code: str
    kline: Optional[Dict]
    source: str
    is_complete: bool
    used_realtime_patch: bool
    reason: str
    full_result: CompleteKlineResult


def get_interface_usage_rules() -> List[str]:
    """返回统一接口的使用规则。"""
    return [
        "默认入口：get_complete_kline(code)；需要元信息时用它。",
        "只要你要的是‘完整K线’，不要自己直接拼本地.day和实时数据，统一走 get_complete_kline / get_complete_kline_df。",
        "批量扫描、全市场筛选统一走 get_complete_kline_batch(codes)。",
        "只需要最后一根完整日K时，走 get_latest_kline(code)。",
        "纯本地历史研究才允许直接用 read_local_tdx_daily(code)，它不负责交易日盘中补当天K线。",
        "交易日15:00前的策略、报告、筛选器，如果需要完整K线，必须允许实时补丁（allow_realtime_patch=True）。",
        "如果返回 is_complete=False，说明按规则应该补当天数据但补丁失败，这时不能把结果当成完整K线。",
    ]


def get_default_tdx_dir() -> str:
    """返回可用的通达信数据目录。优先使用用户指定的 ~/stock_data/vipdoc/。"""
    for path in DEFAULT_TDX_DIR_CANDIDATES:
        if os.path.exists(path):
            return path
    return DEFAULT_TDX_DIR_CANDIDATES[0]


def normalize_stock_code(code: str) -> str:
    """归一化股票代码为 sh600000 / sz000001 / bj430047 形式。"""
    raw = (code or "").strip().lower()
    if not raw:
        raise ValueError("股票代码不能为空")

    if raw.startswith(("sh", "sz", "bj")) and len(raw) > 2:
        return raw

    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) != 6:
        raise ValueError(f"无法识别股票代码: {code}")

    if digits.startswith(("60", "68", "90")):
        return f"sh{digits}"
    if digits.startswith(("00", "30", "20")):
        return f"sz{digits}"
    if digits.startswith(("43", "83", "87", "92")):
        return f"bj{digits}"

    # 默认仍按 A 股主板/中小创逻辑推断
    return f"sh{digits}" if digits.startswith("6") else f"sz{digits}"


def get_tdx_day_path(code: str, data_dir: Optional[str] = None) -> str:
    normalized = normalize_stock_code(code)
    market, code_num = normalized[:2], normalized[2:]
    base_dir = data_dir or get_default_tdx_dir()
    return os.path.join(base_dir, market, "lday", f"{market}{code_num}.day")


def read_local_tdx_daily(
    code: str,
    data_dir: Optional[str] = None,
    keep_zero_volume: bool = True,
) -> Optional[pd.DataFrame]:
    """读取本地通达信日线。

    字段约定：
    - price: 元
    - amount: 元（按 IEEE754 单精度浮点数解析）
    - volume: 保持与通达信原始字段一致（手 × 100），便于和既有项目兼容
    """
    path = get_tdx_day_path(code, data_dir=data_dir)
    if not os.path.exists(path):
        return None

    try:
        with open(path, "rb") as f:
            binary = f.read()

        rows = []
        for i in range(0, len(binary), 32):
            chunk = binary[i:i + 32]
            if len(chunk) < 32:
                continue

            date_int = struct.unpack("I", chunk[0:4])[0]
            open_price = struct.unpack("I", chunk[4:8])[0] / 100.0
            high = struct.unpack("I", chunk[8:12])[0] / 100.0
            low = struct.unpack("I", chunk[12:16])[0] / 100.0
            close = struct.unpack("I", chunk[16:20])[0] / 100.0
            amount = float(struct.unpack("f", chunk[20:24])[0])
            volume = struct.unpack("I", chunk[24:28])[0]

            rows.append([
                date_int,
                open_price,
                high,
                low,
                close,
                amount,
                volume,
            ])

        if not rows:
            return None

        df = pd.DataFrame(
            rows,
            columns=["date_int", "open", "high", "low", "close", "amount", "volume"],
        )
        df["date"] = pd.to_datetime(df["date_int"].astype(str), format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"])
        df = df[df["close"] > 0].sort_values("date").reset_index(drop=True)

        if not keep_zero_volume:
            df = df[df["volume"] > 0].reset_index(drop=True)

        return df if not df.empty else None
    except Exception:
        return None


def _safe_float(value: str, default: float = 0.0) -> float:
    try:
        value = (value or "").strip()
        if not value:
            return default
        return float(value)
    except Exception:
        return default


def _parse_tencent_timestamp(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI_TZ)
    except Exception:
        return None


def fetch_tencent_realtime_bar(code: str, timeout: float = 3.0) -> Optional[Dict]:
    """获取腾讯实时行情，并转换成可拼接的日 K 行数据。"""
    normalized = normalize_stock_code(code)
    api_code = normalized

    try:
        response = requests.get(
            f"http://qt.gtimg.cn/q={api_code}",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "http://finance.qq.com/",
                "Accept": "*/*",
            },
            timeout=timeout,
        )
    except Exception:
        return None

    if response.status_code != 200 or not response.text:
        return None

    text = response.text.strip()
    if '="' not in text:
        return None

    try:
        payload = text.split('="', 1)[1].rstrip('";')
        fields = payload.split('~')
        if len(fields) < 35:
            return None

        timestamp = _parse_tencent_timestamp(fields[30] if len(fields) > 30 else "")
        trade_date = pd.Timestamp(timestamp.date()) if timestamp else None

        name = fields[1] if len(fields) > 1 and fields[1] else "未知"
        current_price = _safe_float(fields[3])
        prev_close = _safe_float(fields[4])
        open_price = _safe_float(fields[5], default=prev_close)
        volume_hand = _safe_float(fields[6]) or _safe_float(fields[36] if len(fields) > 36 else "")
        high = _safe_float(fields[33], default=max(current_price, open_price, prev_close))
        low = _safe_float(fields[34], default=min(v for v in [current_price, open_price, prev_close] if v > 0) if any(v > 0 for v in [current_price, open_price, prev_close]) else 0.0)

        amount_yuan = 0.0
        summary_field = fields[35] if len(fields) > 35 else ""
        if summary_field and "/" in summary_field:
            summary_parts = summary_field.split("/")
            if len(summary_parts) >= 3:
                amount_yuan = _safe_float(summary_parts[2])
        if amount_yuan <= 0:
            amount_wan = _safe_float(fields[57] if len(fields) > 57 else "")
            if amount_wan <= 0:
                amount_wan = _safe_float(fields[37] if len(fields) > 37 else "")
            amount_yuan = amount_wan * 10000

        volume_raw = volume_hand * 100
        valid_bar = bool(trade_date is not None and current_price > 0 and open_price > 0 and high > 0 and low > 0)

        return {
            "code": normalized,
            "name": name,
            "trade_date": trade_date,
            "quote_timestamp": timestamp,
            "open": open_price,
            "high": high,
            "low": low,
            "close": current_price,
            "prev_close": prev_close,
            "amount": amount_yuan,
            "volume": volume_raw,
            "source": "tencent_realtime",
            "valid_bar": valid_bar,
            "row": {
                "date_int": int(trade_date.strftime("%Y%m%d")) if trade_date is not None else None,
                "date": trade_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": current_price,
                "amount": amount_yuan,
                "volume": volume_raw,
            },
        }
    except Exception:
        return None


def _should_expect_realtime_patch(local_df: Optional[pd.DataFrame], now: datetime) -> bool:
    """是否应该按“交易日 15:00 前需要补今天 K 线”的规则尝试联网补丁。"""
    local_now = now.astimezone(SHANGHAI_TZ) if now.tzinfo else now.replace(tzinfo=SHANGHAI_TZ)

    if local_now.weekday() >= 5:
        return False
    if local_now.time() >= time(15, 0):
        return False

    # 用户定义是严格规则：只要是工作日且未到15:00，就按“本地 + 当天实时”处理。
    # 即使本地目录里已经出现今天这行，也优先用实时数据覆盖今天这一根K线。
    return True


def _merge_realtime_row(local_df: Optional[pd.DataFrame], realtime_bar: Dict) -> pd.DataFrame:
    row_df = pd.DataFrame([realtime_bar["row"]])
    if local_df is None or local_df.empty:
        return row_df.sort_values("date").reset_index(drop=True)

    trade_date = realtime_bar["trade_date"]
    merged = local_df[local_df["date"].dt.normalize() != trade_date].copy()
    merged = pd.concat([merged, row_df], ignore_index=True)
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


def get_complete_kline(
    code: str,
    data_dir: Optional[str] = None,
    now: Optional[datetime] = None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
) -> CompleteKlineResult:
    """获取“完整 K 线数据”。"""
    normalized = normalize_stock_code(code)
    current_time = now or datetime.now(SHANGHAI_TZ)
    local_df = read_local_tdx_daily(
        normalized,
        data_dir=data_dir,
        keep_zero_volume=keep_zero_volume,
    )

    if local_df is None:
        local_df = pd.DataFrame(columns=["date_int", "open", "high", "low", "close", "amount", "volume", "date"])

    local_latest_date = None
    if not local_df.empty:
        local_latest_date = pd.Timestamp(local_df["date"].iloc[-1]).normalize()

    expected_patch = allow_realtime_patch and _should_expect_realtime_patch(local_df, current_time)

    if not expected_patch:
        reason = "非交易日或已过15:00，本地离线数据视为完整数据"
        if local_latest_date is not None:
            today = pd.Timestamp((current_time.astimezone(SHANGHAI_TZ) if current_time.tzinfo else current_time).date())
            if local_latest_date >= today:
                reason = "本地目录已经包含今天数据，无需联网补丁"
        return CompleteKlineResult(
            code=normalized,
            data=local_df,
            source="local_only",
            local_latest_date=local_latest_date,
            realtime_date=None,
            used_realtime_patch=False,
            expected_realtime_patch=False,
            is_complete=True,
            reason=reason,
            realtime_snapshot=None,
        )

    realtime_bar = fetch_tencent_realtime_bar(normalized, timeout=timeout)
    today = pd.Timestamp((current_time.astimezone(SHANGHAI_TZ) if current_time.tzinfo else current_time.replace(tzinfo=SHANGHAI_TZ)).date())

    if realtime_bar:
        realtime_date = realtime_bar.get("trade_date")
        if realtime_date == today and realtime_bar.get("valid_bar"):
            merged_df = _merge_realtime_row(local_df, realtime_bar)
            return CompleteKlineResult(
                code=normalized,
                data=merged_df,
                source="local+realtime",
                local_latest_date=local_latest_date,
                realtime_date=realtime_date,
                used_realtime_patch=True,
                expected_realtime_patch=True,
                is_complete=True,
                reason="交易日15:00前，已用实时行情补上今天这根K线",
                realtime_snapshot=realtime_bar,
            )

        if realtime_date is not None and realtime_date != today:
            return CompleteKlineResult(
                code=normalized,
                data=local_df,
                source="local_only",
                local_latest_date=local_latest_date,
                realtime_date=realtime_date,
                used_realtime_patch=False,
                expected_realtime_patch=True,
                is_complete=True,
                reason="实时源没有返回今天行情，按非交易日/休市处理，本地数据视为完整",
                realtime_snapshot=realtime_bar,
            )

    return CompleteKlineResult(
        code=normalized,
        data=local_df,
        source="local_only_incomplete",
        local_latest_date=local_latest_date,
        realtime_date=realtime_bar.get("trade_date") if realtime_bar else None,
        used_realtime_patch=False,
        expected_realtime_patch=True,
        is_complete=False,
        reason="按规则此时需要联网补当天数据，但实时补丁获取失败",
        realtime_snapshot=realtime_bar,
    )


def get_complete_kline_df(
    code: str,
    data_dir: Optional[str] = None,
    now: Optional[datetime] = None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
) -> Optional[pd.DataFrame]:
    result = get_complete_kline(
        code=code,
        data_dir=data_dir,
        now=now,
        timeout=timeout,
        allow_realtime_patch=allow_realtime_patch,
        keep_zero_volume=keep_zero_volume,
    )
    return result.data if result.data is not None and not result.data.empty else None


def get_latest_kline(
    code: str,
    data_dir: Optional[str] = None,
    now: Optional[datetime] = None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
) -> LatestKlineResult:
    """获取最后一根完整日K。

    适合报告系统、策略打分、信号判断这类只关心最新一根K线的场景。
    """
    full_result = get_complete_kline(
        code=code,
        data_dir=data_dir,
        now=now,
        timeout=timeout,
        allow_realtime_patch=allow_realtime_patch,
        keep_zero_volume=keep_zero_volume,
    )

    latest_row = None
    df = full_result.data
    if df is not None and not df.empty:
        latest = df.iloc[-1]
        latest_row = {
            "code": full_result.code,
            "date_int": int(latest["date_int"]) if pd.notna(latest.get("date_int")) else None,
            "date": pd.Timestamp(latest["date"]).normalize() if pd.notna(latest.get("date")) else None,
            "open": float(latest["open"]),
            "high": float(latest["high"]),
            "low": float(latest["low"]),
            "close": float(latest["close"]),
            "amount": float(latest["amount"]) if "amount" in latest and pd.notna(latest["amount"]) else 0.0,
            "volume": float(latest["volume"]) if "volume" in latest and pd.notna(latest["volume"]) else 0.0,
            "source": full_result.source,
            "is_complete": full_result.is_complete,
            "used_realtime_patch": full_result.used_realtime_patch,
        }

    return LatestKlineResult(
        code=full_result.code,
        kline=latest_row,
        source=full_result.source,
        is_complete=full_result.is_complete,
        used_realtime_patch=full_result.used_realtime_patch,
        reason=full_result.reason,
        full_result=full_result,
    )


def get_complete_kline_batch(
    codes: Iterable[str],
    data_dir: Optional[str] = None,
    now: Optional[datetime] = None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
    max_workers: int = 8,
) -> Dict[str, CompleteKlineResult]:
    """批量获取完整K线结果。返回 {标准代码: CompleteKlineResult}。"""
    normalized_codes = [normalize_stock_code(code) for code in codes]
    if not normalized_codes:
        return {}

    results: Dict[str, CompleteKlineResult] = {}
    worker_count = max(1, min(max_workers, len(normalized_codes)))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                get_complete_kline,
                code,
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
                results[code] = CompleteKlineResult(
                    code=code,
                    data=pd.DataFrame(columns=["date_int", "open", "high", "low", "close", "amount", "volume", "date"]),
                    source="batch_error",
                    local_latest_date=None,
                    realtime_date=None,
                    used_realtime_patch=False,
                    expected_realtime_patch=False,
                    is_complete=False,
                    reason=f"批量获取异常: {exc}",
                    realtime_snapshot=None,
                )

    return results


def get_latest_kline_batch(
    codes: Iterable[str],
    data_dir: Optional[str] = None,
    now: Optional[datetime] = None,
    timeout: float = 3.0,
    allow_realtime_patch: bool = True,
    keep_zero_volume: bool = True,
    max_workers: int = 8,
) -> Dict[str, LatestKlineResult]:
    """批量获取最后一根完整日K。"""
    full_results = get_complete_kline_batch(
        codes=codes,
        data_dir=data_dir,
        now=now,
        timeout=timeout,
        allow_realtime_patch=allow_realtime_patch,
        keep_zero_volume=keep_zero_volume,
        max_workers=max_workers,
    )

    latest_results: Dict[str, LatestKlineResult] = {}
    for code, full_result in full_results.items():
        latest_row = None
        df = full_result.data
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            latest_row = {
                "code": full_result.code,
                "date_int": int(latest["date_int"]) if pd.notna(latest.get("date_int")) else None,
                "date": pd.Timestamp(latest["date"]).normalize() if pd.notna(latest.get("date")) else None,
                "open": float(latest["open"]),
                "high": float(latest["high"]),
                "low": float(latest["low"]),
                "close": float(latest["close"]),
                "amount": float(latest["amount"]) if "amount" in latest and pd.notna(latest["amount"]) else 0.0,
                "volume": float(latest["volume"]) if "volume" in latest and pd.notna(latest["volume"]) else 0.0,
                "source": full_result.source,
                "is_complete": full_result.is_complete,
                "used_realtime_patch": full_result.used_realtime_patch,
            }

        latest_results[code] = LatestKlineResult(
            code=full_result.code,
            kline=latest_row,
            source=full_result.source,
            is_complete=full_result.is_complete,
            used_realtime_patch=full_result.used_realtime_patch,
            reason=full_result.reason,
            full_result=full_result,
        )

    return latest_results
