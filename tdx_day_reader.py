"""
通达信 .day 文件解析接口
=========================
从本地离线目录读取股票日K线数据

数据目录: ~/stock_data/vipdoc/
目录结构:
  sh/lday/sh{code}.day   - 上海市场
  sz/lday/sz{code}.day   - 深圳市场
  bj/lday/bj{code}.day   - 北京市场

Usage:
  from tdx_day_reader import read_tdx_kline

  # 获取最近10天数据
  df = read_tdx_kline("600862", days=10)

  # 获取指定截止日期之前的数据
  df = read_tdx_kline("600862", end_date="2026-04-02", days=20)

  # 获取指定代码（支持带前缀/不带前缀）
  df = read_tdx_kline("sh600862", days=10)
"""

import struct
import datetime
import os
from typing import Literal, Optional
from pathlib import Path

# 默认数据目录
TDX_DATA_DIR = Path(os.path.expanduser("~/stock_data/vipdoc"))

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


def _normalize_code(code: str) -> tuple[str, str]:
    """
    标准化股票代码，返回 (市场前缀, 纯代码)
    例如:
      "600862"  -> ("sh", "600862")
      "sh600862" -> ("sh", "600862")
      "000001"  -> ("sz", "000001")
      "sz000001" -> ("sz", "000001")
      "bj430001" -> ("bj", "430001")
    """
    code = code.strip().lower()
    if code.startswith("sh"):
        return ("sh", code[2:])
    elif code.startswith("sz"):
        return ("sz", code[2:])
    elif code.startswith("bj"):
        return ("bj", code[4:])
    elif len(code) == 6:
        # 根据代码范围判断市场
        first = code[0]
        if first in ("0", "3"):
            return ("sz", code)  # 深圳
        elif first in ("4", "8", "9"):
            return ("bj", code)  # 北京
        else:
            return ("sh", code)  # 上海
    else:
        raise ValueError(f"无法识别的股票代码: {code}")


def _parse_record(chunk: bytes) -> dict:
    """解析单条32字节记录"""
    date, open_, high, low, close, amount, volume, _ = struct.unpack('<IIIIIfII', chunk)
    return {
        'date':   datetime.datetime.strptime(str(date), '%Y%m%d'),
        'open':   open_ / 100.0,
        'high':   high / 100.0,
        'low':    low / 100.0,
        'close':  close / 100.0,
        'amount': float(amount),
        'volume': volume / 100.0,  # 成交量（手）
    }


def _find_file(code: str) -> Path:
    """查找股票.day文件路径"""
    market, pure = _normalize_code(code)
    file_path = TDX_DATA_DIR / market / "lday" / f"{market}{pure}.day"
    if not file_path.exists():
        raise FileNotFoundError(f"找不到数据文件: {file_path}")
    return file_path


def _load_all_records(file_path: Path) -> list[dict]:
    """加载文件所有记录"""
    with open(file_path, 'rb') as f:
        data = f.read()

    records = []
    for i in range(0, len(data), RECORD_SIZE):
        chunk = data[i:i+RECORD_SIZE]
        if len(chunk) < RECORD_SIZE:
            break
        records.append(_parse_record(chunk))
    return records


def read_tdx_kline(
    code: str,
    days: Optional[int] = None,
    end_date: Optional[Literal["today"] | str | datetime.date] = None,
    include_full: bool = False,
) -> list[dict]:
    """
    读取通达信日K线数据

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
        - "today": 今天
        - "YYYY-MM-DD" 格式字符串
        - datetime.date 对象

    Returns
    -------
    list[dict]
        K线数据列表，每条包含:
        date (datetime), open, high, low, close, amount, volume
        按日期升序排列

    Examples
    --------
    >>> df = read_tdx_kline("600862", days=10)
    >>> df = read_tdx_kline("sh600862", end_date="2026-04-02", days=5)
    >>> df = read_tdx_kline("000001", days=20)
    """
    file_path = _find_file(code)
    all_records = _load_all_records(file_path)

    # 处理截止日期
    if end_date is None or end_date == "today":
        end_dt = datetime.datetime.now()
    elif isinstance(end_date, str):
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    elif isinstance(end_date, datetime.date):
        end_dt = datetime.datetime.combine(end_date, datetime.time())
    else:
        raise TypeError(f"end_date 类型不支持: {type(end_date)}")

    # 过滤截止日期之前的数据
    filtered = [r for r in all_records if r['date'] <= end_dt]

    # 取最近 days 条
    if days is not None:
        filtered = filtered[-days:]

    # 标准化输出格式
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
    """
    打印K线数据的便捷函数

    Returns
    -------
    list[dict]: K线数据列表
    """
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


if __name__ == "__main__":
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "600862"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    print_kline(code, days=days)
