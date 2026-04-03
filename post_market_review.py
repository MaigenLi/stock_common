#!/usr/bin/env python3
"""
盘后复盘报告生成脚本
=========================
每日 17:00 自动执行，生成复盘报告并发送至邮箱

用法（手动）：
  python post_market_review.py

依赖：
  stock_common（数据接口）
  通达信离线数据
  ~/.stock_cache/shares/shares_cache.json（股本缓存）
  ~/.stock_cache/{sh,sz}/（numpy 缓存）

邮件配置（环境变量）：
  QQ_EMAIL=maigenmuzi@qq.com
  QQ_PASS=xxx（QQ邮箱授权码）
"""

import argparse
import json
import os
import smtplib
import ssl
import sys
import time
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from pathlib import Path
from threading import Lock

# ── 邮件配置 ──────────────────────────────────────────────
# 优先读取环境变量，未设置则从 .env 文件加载
_QQ_EMAIL = os.getenv("QQ_EMAIL")
_QQ_PASS = os.getenv("QQ_PASS")
if not _QQ_EMAIL or not _QQ_PASS:
    _env_file = Path.home() / ".openclaw" / ".env"
    if _env_file.exists():
        for line in _env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or not line:
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                if k == "QQ_EMAIL":
                    _QQ_EMAIL = v.strip()
                elif k == "QQ_PASS":
                    _QQ_PASS = v.strip()
QQ_EMAIL = _QQ_EMAIL or "920662304@qq.com"
QQ_PASS = _QQ_PASS or ""
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 587

# ── 路径配置 ──────────────────────────────────────────────
WORKSPACE = Path(__file__).parent.parent.resolve()
STOCK_CODES_FILE = Path.home() / "stock_code" / "results" / "stock_codes.txt"
REPORT_DIR = WORKSPACE / "stock_reports"
REPORT_DIR.mkdir(exist_ok=True)

# ── 指数列表 ──────────────────────────────────────────────
INDICES = [
    ("sh000001", "上证指数"),
    ("sh000300", "沪深300"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
    ("sh000016", "上证50"),
    ("sh000688", "科创50"),
]

# ── 全市场股票代码（缓存） ──────────────────────────────────
_stock_codes: list = None
_stock_codes_lock = Lock()


def get_stock_codes() -> list:
    global _stock_codes
    with _stock_codes_lock:
        if _stock_codes is not None:
            return _stock_codes
        if not STOCK_CODES_FILE.exists():
            _stock_codes = []
            return _stock_codes
        with open(STOCK_CODES_FILE, "r", encoding="utf-8") as f:
            _stock_codes = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        return _stock_codes


# ── 邮件发送 ──────────────────────────────────────────────

def send_email(subject: str, html_body: str, to_email: str = None) -> bool:
    """发送 HTML 邮件（QQ 邮箱 SMTP）"""
    to_email = to_email or QQ_EMAIL
    if not QQ_PASS:
        print("⚠️  未配置 QQ_PASS，跳过邮件发送")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = Header(subject, "utf-8").encode()
    msg["From"] = QQ_EMAIL
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # QQ 邮箱：优先 SSL 465，次选 STARTTLS 587
    errors = []
    # 方式1：SSL 直接连接（465）
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.qq.com", 465, context=context, timeout=15) as server:
            server.login(QQ_EMAIL, QQ_PASS)
            server.sendmail(QQ_EMAIL, [to_email], msg.as_string())
        print(f"✅ 邮件已发送至 {to_email}（SSL）")
        return True
    except Exception as e:
        errors.append(f"SSL(465): {e}")

    # 方式2：STARTTLS（587）
    try:
        with smtplib.SMTP("smtp.qq.com", 587, timeout=15) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(QQ_EMAIL, QQ_PASS)
            server.sendmail(QQ_EMAIL, [to_email], msg.as_string())
        print(f"✅ 邮件已发送至 {to_email}（STARTTLS）")
        return True
    except Exception as e:
        errors.append(f"STARTTLS(587): {e}")

    print(f"❌ 邮件发送失败: {'; '.join(errors)}")
    return False


# ── 指数数据 ──────────────────────────────────────────────

def get_index_data() -> list:
    """
    获取主要指数今日和近期数据。
    返回 [{code, name, prev_close, close, change_pct, volume, amount}, ...]
    """
    sys.path.insert(0, str(WORKSPACE))
    from stock_common.complete_kline import get_complete_kline

    results = []
    for code, name in INDICES:
        try:
            r = get_complete_kline(code)
            df = r.data
            if df is None or len(df) < 2:
                continue
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            change = (float(latest["close"]) - float(prev["close"])) / float(prev["close"]) * 100
            vol = float(latest.get("volume", 0))
            amount = float(latest.get("amount", 0))
            results.append({
                "code": code,
                "name": name,
                "prev_close": round(float(prev["close"]), 2),
                "close": round(float(latest["close"]), 2),
                "change_pct": round(change, 2),
                "volume": vol,
                "amount": amount,
            })
        except Exception as e:
            print(f"  ⚠️  指数 {code} 获取失败: {e}")
    return results


# ── 全市场股本快照（今日） ─────────────────────────────────

def load_today_shares() -> dict:
    """
    从本地缓存加载全市场股本数据。

    兼容两种保存格式：
    - fetch_share_base 批量保存：{"_meta":..., "_data":{...}}
    - get_share_info 单股票追加保存：{code: info, ...}
    """
    cache_path = Path.home() / ".stock_cache" / "shares" / "shares_cache.json"
    if not cache_path.exists():
        return {}
    with open(cache_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    # 批量格式
    if isinstance(obj, dict) and "_data" in obj:
        return obj["_data"]
    # 单股票追加格式（直接就是 {code: info}）
    return obj


# ── 涨跌幅排序 ────────────────────────────────────────────

def get_top_movers(codes: list, shares_cache: dict, top_n: int = 30) -> dict:
    """
    从本地 K 线数据计算全市场涨跌幅。
    返回 {code: {name, close, prev_close, change_pct, turnover, volume}}
    """
    sys.path.insert(0, str(WORKSPACE))
    from stock_common.tdx_day_reader import read_tdx_kline

    movers = {}
    lock = Lock()

    def process(code: str):
        try:
            records = read_tdx_kline(code, days=2)
            if not records or len(records) < 2:
                return
            today = records[-1]
            yesterday = records[-2]
            close = float(today["close"])
            prev_close = float(yesterday["close"])
            if prev_close <= 0:
                return
            change = (close - prev_close) / prev_close * 100

            info = shares_cache.get(code, {})
            name = info.get("name", "")
            turnover = info.get("turnover")
            volume = today.get("volume", 0)

            with lock:
                movers[code] = {
                    "name": name,
                    "close": close,
                    "prev_close": prev_close,
                    "change_pct": round(change, 2),
                    "turnover": turnover,
                    "volume": volume,
                }
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=30) as ex:
        futures = {ex.submit(process, c): c for c in codes}
        for future in as_completed(futures):
            pass  # 等待全部完成

    return movers


# ── 趋势放量筛选 ──────────────────────────────────────────

def screen_trend_volume(codes: list, shares_cache: dict) -> list:
    """
    使用 trend_volume 模块筛选趋势放量股票。
    返回符合条件的股票列表（最多50只）。
    """
    sys.path.insert(0, str(WORKSPACE))
    from stock_common.trend_volume import preload_all_klines, analyze_from_cache

    # 预加载 K 线（使用 numpy 缓存，零磁盘重复 IO）
    data_map = preload_all_klines(codes, days=80, workers=20, progress=False)

    candidates = []

    def analyze(code: str):
        try:
            result = analyze_from_cache(code, data_map, vol_ratio_threshold=1.5)
            if not result:
                return
            is_tv = result.get("is_trend_volume", False)
            score = result.get("score") or 0
            turnover = result.get("turnover_rate")
            change = result.get("change_pct", 0)
            vol_ratio = result.get("volume_ratio", 0)

            info = shares_cache.get(code, {})
            name = info.get("name", "")

            if is_tv or (vol_ratio and vol_ratio >= 2.0 and change >= 3.0):
                candidates.append({
                    "code": code,
                    "name": name,
                    "close": data_map.get(code, [{}])[-1].get("close", 0),
                    "change_pct": round(change, 2),
                    "vol_ratio": round(float(vol_ratio), 2) if vol_ratio else None,
                    "turnover": turnover,
                    "score": round(score, 1) if score else None,
                    "trend": "✅" if is_tv else "⬆️",
                })
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(analyze, c): c for c in codes}
        for future in as_completed(futures):
            pass

    # 按涨幅排序
    candidates.sort(key=lambda x: x["change_pct"], reverse=True)
    return candidates[:50]


# ── 板块热点 ─────────────────────────────────────────────

def get_market_sector_hotspots(top_n: int = 15) -> dict:
    """
    获取市场板块热点数据（东方财富 API）。
    返回 dict:
      - rise: 涨幅前N板块 [{name, change_pct}]
      - fall: 跌幅前N板块
      - amount: 成交额前N板块 [{name, change_pct, amount}]
    """
    import requests
    import warnings
    warnings.filterwarnings('ignore')

    result = {"rise": [], "fall": [], "amount": []}

    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/",
        }

        # 涨幅榜
        rise_params = {
            "cb": "jQuery",
            "pn": 1,
            "pz": 200,          # 多取一些，确保包含跌幅榜
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:90 t:2 f:!50",
            "fields": "f3,f6,f8,f12,f14",
            "_": int(time.time() * 1000),
        }
        resp = requests.get(url, params=rise_params, headers=headers, timeout=8)
        text = resp.text[resp.text.index("(") + 1 : resp.text.rindex(")")]
        data = json.loads(text)
        items = (data.get("data") or {}).get("diff") or []
        result["rise"] = [
            {"name": itm["f14"], "change_pct": round(float(itm["f3"] or 0), 2)}
            for itm in items if itm.get("f14")
        ][:top_n]

        # 跌幅榜（按涨幅升序排列，取前top_n个负值板块）
        fall_items = sorted(
            [itm for itm in items if itm.get("f14")],
            key=lambda x: float(x.get("f3") or 0)
        )
        result["fall"] = [
            {"name": itm["f14"], "change_pct": round(float(itm["f3"] or 0), 2)}
            for itm in fall_items if float(itm.get("f3") or 0) < 0
        ][:top_n]

        # 成交额榜
        amt_params = dict(rise_params)
        amt_params["fid"] = "f6"   # f6=成交额（元）
        amt_params["pz"] = 100     # 取成交额前100
        amt_resp = requests.get(url, params=amt_params, headers=headers, timeout=8)
        amt_text = amt_resp.text[amt_resp.text.index("(") + 1 : amt_resp.text.rindex(")")]
        amt_data = json.loads(amt_text)
        amt_items = (amt_data.get("data") or {}).get("diff") or []
        result["amount"] = [
            {
                "name": itm["f14"],
                "change_pct": round(float(itm["f3"] or 0), 2),
                "amount": float(itm.get("f6") or 0),
            }
            for itm in amt_items if itm.get("f14") and float(itm.get("f6") or 0) > 0
        ][:top_n]

    except Exception as e:
        print(f"  ⚠️  板块热点获取失败: {e}")

    return result


# ── 涨停股 ────────────────────────────────────────────────

def get_limit_up_stocks(movers: dict) -> list:
    """从 movers 中筛选涨停股（涨幅 >= 9.9%）"""
    limit_up = []
    for code, info in movers.items():
        if info["change_pct"] >= 9.9:
            limit_up.append({**info, "code": code})
    limit_up.sort(key=lambda x: x["change_pct"], reverse=True)
    return limit_up


# ── 报告生成 ──────────────────────────────────────────────

def format_amount(amount: float) -> str:
    """格式化成交额（亿元）"""
    if amount >= 1e8:
        return f"{amount / 1e8:.2f}亿"
    elif amount >= 1e4:
        return f"{amount / 1e4:.2f}万"
    return f"{amount:.2f}"


def format_volume(vol: float) -> str:
    """格式化成交量（万手）"""
    if vol >= 1e4:
        return f"{vol / 1e4:.2f}万手"
    return f"{vol:.0f}手"


def generate_html_report(date_str: str, index_data: list, movers: dict,
                         limit_up: list, trend_stocks: list,
                         sector_hotspots: dict = None) -> str:
    """生成 HTML 格式复盘报告"""

    # 涨跌统计
    risers = [v for v in movers.values() if v["change_pct"] > 0]
    fallers = [v for v in movers.values() if v["change_pct"] < 0]
    flaters = [v for v in movers.values() if v["change_pct"] == 0]

    # 成交额前20
    by_amount = sorted(movers.values(), key=lambda x: x.get("amount") or 0, reverse=True)[:20]

    def arrow(pct):
        if pct > 0:
            return f'<span style="color:red">▲ {pct:+.2f}%</span>'
        elif pct < 0:
            return f'<span style="color:green">▼ {pct:+.2f}%</span>'
        return f'<span>─ {pct:+.2f}%</span>'

    html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif;
         max-width: 900px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
  h1 {{ color: #1a1a1a; border-bottom: 3px solid #1a73e8; padding-bottom: 10px; }}
  h2 {{ color: #333; margin-top: 30px; border-left: 4px solid #1a73e8; padding-left: 10px; }}
  h3 {{ color: #555; margin-top: 20px; }}
  .date {{ color: #888; font-size: 14px; }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }}
  th {{ background: #1a73e8; color: white; padding: 8px 12px; text-align: right; }}
  td {{ padding: 7px 12px; text-align: right; border-bottom: 1px solid #eee; }}
  th:first-child, td:first-child {{ text-align: left; }}
  tr:hover {{ background: #f0f7ff; }}
  .up {{ color: #e53935; }}
  .down {{ color: #43a047; }}
  .flat {{ color: #888; }}
  .stat-box {{ display: flex; gap: 20px; margin: 15px 0; }}
  .stat {{ background: white; border-radius: 8px; padding: 15px 25px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); }}
  .stat .num {{ font-size: 28px; font-weight: bold; }}
  .stat .label {{ color: #888; font-size: 13px; margin-top: 5px; }}
  .tag {{ display: inline-block; background: #e8f0fe; color: #1a73e8; border-radius: 4px;
          padding: 2px 8px; font-size: 12px; margin-left: 5px; }}
  .footer {{ margin-top: 40px; color: #aaa; font-size: 12px; text-align: center; }}
  .index-card {{ background: white; border-radius: 8px; padding: 15px 20px; margin: 10px 0;
                 box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
  .index-card .name {{ font-size: 16px; font-weight: bold; }}
  .index-card .price {{ font-size: 22px; font-weight: bold; margin: 5px 0; }}
</style>
</head>
<body>
<h1>📊 盘后复盘报告 <span class="date">{date_str}</span></h1>

<!-- 指数 -->
<h2>一、主要指数</h2>
<div style="display:grid; grid-template-columns: repeat(3, 1fr); gap: 12px;">
"""
    for idx in index_data:
        color = "red" if idx["change_pct"] > 0 else "green" if idx["change_pct"] < 0 else "#888"
        arrow_sym = "▲" if idx["change_pct"] > 0 else "▼" if idx["change_pct"] < 0 else "─"
        html += f"""
  <div class="index-card">
    <div class="name">{idx['name']}</div>
    <div class="price" style="color:{color}">{idx['close']}</div>
    <div style="color:{color}">{arrow_sym} {idx['change_pct']:+.2f}%</div>
    <div style="color:#888;font-size:12px">昨收 {idx['prev_close']}</div>
  </div>"""
    html += "</div>"

    # 市场概况
    html += f"""
<h2>二、市场概况</h2>
<div class="stat-box">
  <div class="stat"><div class="num" style="color:#e53935">{len(risers)}</div>
    <div class="label">上涨</div></div>
  <div class="stat"><div class="num" style="color:#888">{len(fallers)}</div>
    <div class="label">下跌</div></div>
  <div class="stat"><div class="num" style="color:#43a047">{len(flaters)}</div>
    <div class="label">平盘</div></div>
  <div class="stat"><div class="num">{len(movers)}</div>
    <div class="label">统计股票</div></div>
</div>"""

    # 板块热点
    top_n_sectors = 10
    if sector_hotspots:
        # 涨幅榜
        rise_sectors = sector_hotspots.get("rise", [])
        # 跌幅榜
        fall_sectors = sector_hotspots.get("fall", [])
        # 成交额榜
        amount_sectors = sector_hotspots.get("amount", [])

        html += """
<h2>三、市场板块热点 <span class="tag">实时数据</span></h2>"""

        # 涨幅榜
        if rise_sectors:
            html += f"""
<h3>🔥 涨幅榜（前 {len(rise_sectors)} 板块）</h3>
<div style="display:flex; flex-wrap:wrap; gap:8px; margin:10px 0;">"""
            for s in rise_sectors[:top_n_sectors]:
                color = "#e53935" if s["change_pct"] >= 5 else "#ff7043" if s["change_pct"] >= 3 else "#ef5350"
                html += f"""<div style="background:#fff0f0; border-left:3px solid {color}; border-radius:4px;
                             padding:6px 12px; min-width:120px;">
  <div style="font-weight:bold; color:#333;">{s['name']}</div>
  <div style="color:{color}; font-size:15px;">▲ {s['change_pct']:+.2f}%</div>
</div>"""
            html += "</div>"

        # 跌幅榜
        if fall_sectors:
            html += f"""
<h3>❄️ 跌幅榜（前 {len(fall_sectors)} 板块）</h3>
<div style="display:flex; flex-wrap:wrap; gap:8px; margin:10px 0;">"""
            for s in fall_sectors[:top_n_sectors]:
                color = "#43a047" if s["change_pct"] <= -3 else "#66bb6a" if s["change_pct"] <= -1 else "#81c784"
                html += f"""<div style="background:#f0f8f0; border-left:3px solid {color}; border-radius:4px;
                             padding:6px 12px; min-width:120px;">
  <div style="font-weight:bold; color:#333;">{s['name']}</div>
  <div style="color:{color}; font-size:15px;">▼ {s['change_pct']:+.2f}%</div>
</div>"""
            html += "</div>"

        # 成交额榜
        if amount_sectors:
            html += f"""
<h3>💰 成交额榜（主力板块）</h3>
<table><tr><th>板块名称</th><th>涨跌幅</th><th>成交额</th></tr>"""
            for s in amount_sectors:
                pct = s["change_pct"]
                color = "#e53935" if pct > 0 else "#43a047" if pct < 0 else "#888"
                arrow = "▲" if pct > 0 else "▼" if pct < 0 else "─"
                amt_str = format_amount(s.get("amount") or 0)
                html += f"<tr><td style='text-align:left;font-weight:bold;'>{s['name']}</td>"
                html += f"<td style='color:{color}'>{arrow} {pct:+.2f}%</td>"
                html += f"<td>{amt_str}</td></tr>"
            html += "</table>"

    html += f"""
<h2>四、涨停股 <span class="tag">共 {len(limit_up)} 只</span></h2>"""
    if limit_up:
        html += "<table><tr><th>代码</th><th>名称</th><th>收盘价</th><th>涨幅</th><th>换手率</th></tr>"
        for s in limit_up[:30]:
            turnover_str = f"{s['turnover']:.2f}%" if s.get("turnover") else "—"
            html += f"<tr><td>{s['code']}</td><td>{s['name']}</td>"
            html += f"<td>{s['close']:.2f}</td>"
            html += f"<td class='up'>▲ {s['change_pct']:.2f}%</td>"
            html += f"<td>{turnover_str}</td></tr>"
        html += "</table>"
    else:
        html += "<p>今日无涨停股</p>"

    # 成交额TOP20
    html += f"""
<h2>五、成交额TOP20 <span class="tag">单位：元</span></h2>
<table><tr><th>代码</th><th>名称</th><th>收盘价</th><th>涨跌幅</th><th>成交额</th><th>换手率</th></tr>"""
    for s in by_amount:
        code = None
        for c, v in movers.items():
            if v == s:
                code = c
                break
        if not code:
            continue
        pct = s["change_pct"]
        color_cls = "up" if pct > 0 else "down" if pct < 0 else "flat"
        arrow_sym = "▲" if pct > 0 else "▼" if pct < 0 else "─"
        turnover_str = f"{s.get('turnover', '—'):.2f}%" if s.get("turnover") else "—"
        html += f"<tr><td>{code}</td><td>{s['name']}</td><td>{s['close']:.2f}</td>"
        html += f"<td class='{color_cls}'>{arrow_sym} {pct:+.2f}%</td>"
        html += f"<td>{format_amount(s.get('amount') or 0)}</td><td>{turnover_str}</td></tr>"
    html += "</table>"

    # 趋势放量候选
    html += f"""
<h2>六、趋势放量候选 <span class="tag">共 {len(trend_stocks)} 只</span></h2>"""
    if trend_stocks:
        html += "<table><tr><th>代码</th><th>名称</th><th>收盘价</th><th>涨幅</th><th>量比</th><th>换手率</th><th>状态</th></tr>"
        for s in trend_stocks[:30]:
            pct = s["change_pct"]
            color_cls = "up" if pct > 0 else "down" if pct < 0 else "flat"
            arrow_sym = "▲" if pct > 0 else "▼" if pct < 0 else "─"
            vol_str = f"{s['vol_ratio']:.2f}" if s.get("vol_ratio") else "—"
            turnover_str = f"{s['turnover']:.2f}%" if s.get("turnover") else "—"
            html += f"<tr><td>{s['code']}</td><td>{s['name']}</td><td>{s['close']:.2f}</td>"
            html += f"<td class='{color_cls}'>{arrow_sym} {pct:+.2f}%</td>"
            html += f"<td>{vol_str}</td><td>{turnover_str}</td><td>{s['trend']}</td></tr>"
        html += "</table>"
    else:
        html += "<p>今日无符合趋势放量条件的股票</p>"

    html += f"""
<div class="footer">
  <p>报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
  <p>数据来源: 通达信离线数据 + 腾讯财经实时数据 | 换手率基于本地股本缓存计算</p>
</div>
</body></html>"""
    return html


def save_report(date_str: str, html: str) -> Path:
    """保存报告到本地文件"""
    report_file = REPORT_DIR / f"复盘报告_{date_str}.html"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"📄 报告已保存: {report_file}")
    return report_file


# ── 主流程 ────────────────────────────────────────────────

def run_review(date_str: str = None):
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"📊 盘后复盘报告生成中... {date_str}")
    print(f"{'='*60}\n")

    # 1. 指数数据
    print("▶️  获取主要指数数据...")
    t0 = time.time()
    index_data = get_index_data()
    print(f"  完成 ({time.time()-t0:.1f}s)")

    # 2. 股票代码列表
    print("▶️  加载股票列表...")
    t1 = time.time()
    codes = get_stock_codes()
    print(f"  共 {len(codes)} 只股票 ({time.time()-t1:.1f}s)")

    # 3. 股本缓存
    print("▶️  加载股本缓存...")
    t2 = time.time()
    shares_cache = load_today_shares()
    print(f"  股本缓存 {len(shares_cache)} 只 ({time.time()-t2:.1f}s)")

    # 4. 全市场涨跌幅
    print("▶️  计算全市场涨跌幅...")
    t3 = time.time()
    movers = get_top_movers(codes, shares_cache)
    print(f"  完成 {len(movers)} 只 ({time.time()-t3:.1f}s)")

    # 5. 涨停股
    print("▶️  筛选涨停股...")
    limit_up = get_limit_up_stocks(movers)
    print(f"  涨停 {len(limit_up)} 只")

    # 5b. 板块热点
    print("▶️  获取板块热点数据...")
    t5 = time.time()
    sector_hotspots = get_market_sector_hotspots(top_n=15)
    print(f"  涨幅板块 {len(sector_hotspots.get('rise', []))} 个，"
          f"跌幅板块 {len(sector_hotspots.get('fall', []))} 个，"
          f"成交额板块 {len(sector_hotspots.get('amount', []))} 个 ({time.time()-t5:.1f}s)")

    # 6. 趋势放量
    print("▶️  趋势放量筛选...")
    t4 = time.time()
    trend_stocks = screen_trend_volume(codes, shares_cache)
    print(f"  趋势放量候选 {len(trend_stocks)} 只 ({time.time()-t4:.1f}s)")

    # 7. 生成报告
    print("▶️  生成报告...")
    html = generate_html_report(date_str, index_data, movers, limit_up, trend_stocks, sector_hotspots)

    # 8. 保存
    report_file = save_report(date_str, html)

    # 9. 发送邮件
    subject = f"📊 盘后复盘 {date_str} | 涨停{len(limit_up)}只"
    print("\n▶️  发送邮件...")
    sent = send_email(subject, html)

    print(f"\n{'='*60}")
    print(f"✅ 复盘报告生成完成！总耗时 {time.time()-t0:.1f}s")
    print(f"   涨停: {len(limit_up)} 只")
    print(f"   趋势放量候选: {len(trend_stocks)} 只")
    if sent:
        print(f"   邮件: 已发送 ✅")
    else:
        print(f"   邮件: 跳过 ⚠️")
    print(f"{'='*60}")
    return html


# ── CLI ───────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="盘后复盘报告生成")
    parser.add_argument("--date", "-d", type=str, help="指定日期 (YYYY-MM-DD)，默认今天")
    parser.add_argument("--no-email", action="store_true", help="不发送邮件，只生成报告")
    parser.add_argument("--save-only", action="store_true", help="只保存文件，不发送邮件")
    args = parser.parse_args()

    # 临时禁用邮件
    if args.no_email or args.save_only:
        import stock_common.fetch_share_base as _fb
        _orig = None
        def _no_email(*a, **k): print("📧 邮件发送已禁用（--no-email）"); return False
        # 不修改模块，只跳过
        pass

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run_review(date_str)
