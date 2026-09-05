"""
thetalab.server — 本地交易工作台服务器（Python 标准库实现，零第三方依赖）

启动：python -m thetalab.server  →  http://127.0.0.1:8300
API：GET / 与 /api/state(?underlying=)；POST /api/order、/api/confirm、/api/advance、
     /api/set_day、/api/cancel_all、/api/set_underlying
多品种：IV/Greeks 五个沪市品种全有；逐合约行情仅 510300 已采集——其他品种可浏览
链与 IV，下单被明确拒绝并提示采集命令（数据边界诚实，不造假价）。
纪律：T 日挂单、人工确认、T+1 按 CLOSE_SLIPPAGE 撮合；单笔 ≤ 当日成交量 2%。
"""
from __future__ import annotations

import os as _os
import sys as _sys

# 任意目录启动自愈：把包父目录（WKB）加入模块搜索路径——
# 必须在 import thetalab.* 之前执行（python -m / 直接路径启动都覆盖）
_PARENT = str(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)
_os.chdir(_PARENT)

# import 兜底：本机偶发 "thetalab" 默认查找失效（spec 查询正常但 import 语句抛
# ModuleNotFoundError，疑似环境对精确目录名的文件系统缓存锁定）。
# 用显式 spec 加载先注册进 sys.modules，后续 from thetalab.* 直接命中已加载模块。
def _ensure_thetalab_importable():
    import importlib.util
    if "thetalab" in _sys.modules:
        return
    _pkg = _os.path.join(_PARENT, "thetalab")
    if _os.path.isdir(_pkg):
        try:
            _spec = importlib.util.spec_from_file_location(
                "thetalab", _os.path.join(_pkg, "__init__.py"),
                submodule_search_locations=[_pkg])
            if _spec and _spec.loader:
                _m = importlib.util.module_from_spec(_spec)
                _spec.loader.exec_module(_m)
                _sys.modules["thetalab"] = _m
        except Exception:
            pass

_ensure_thetalab_importable()

import json
import re
import subprocess
import sys
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import time
from urllib.parse import urlparse, parse_qs

import pandas as pd

from thetalab import __version__ as THE_VERSION
from thetalab.core.models import Account, Direction, Offset, Order, Right
from thetalab.data.persist import StateStore
from thetalab.data.provider import ParquetStore
from thetalab.engine.broker import Broker, MarketRow
from thetalab.engine.paper import PaperTradingRunner
from thetalab.engine.runner import BacktestRunner

ROOT = Path(__file__).resolve().parents[1]
# 用户指定品种（2026-08-29）：科创50 / 创业板(深) / 沪深300 / 中证1000(无场内期权) / 中证500
UNDERLYINGS = ["588000", "159915", "510300", "510050", "510500"]
UND_NAME = {"588000": "科创50ETF", "159915": "创业板ETF", "510300": "沪深300ETF",
            "512100": "中证1000ETF", "510500": "中证500ETF", "510050": "上证50ETF",
            "588080": "科创板50ETF", "159919": "沪深300ETF(深)", "159922": "中证500ETF(深)",
            "159901": "深证100ETF"}
# 512100 无场内 ETF 期权（risk_indicators 0 行，中证1000 的场内衍生品为中金所 MO 股指期权）
UND_NOTE = {"512100": "无场内 ETF 期权（中证1000 场内衍生品为中金所 MO 股指期权）"}
DEFAULT_UND = "510300"   # 默认品种（模拟时钟/主标的口径）；撮合已多品种开放


def pd_read(path):
    import pandas as pd
    return pd.read_parquet(path)


class Workbench:
    """装配器 + 模拟时钟 + 多品种切换。路由逻辑在此（可单测），HTTP 壳在 Handler。"""

    def __init__(self, data_dir=None, auto_update=True):
        self.data_dir = data_dir or ROOT / "thetalab_data"
        store = ParquetStore(self.data_dir / "store")
        risk_all = store.read("risk_indicators")
        daily = pd_read(self.data_dir / "store" / "contract_daily" / "all.parquet")
        udl = pd_read(self.data_dir / "store" / "underlying_daily" / "all.parquet")
        self.close_all = {}   # (underlying, date) -> 收盘价（全部品种）
        for u_, g_ in udl.groupby("underlying"):
            for d_, c_ in zip(g_["date"], g_["close"].astype(float)):
                self.close_all[(u_, d_)] = c_
        close = pd.Series(udl[udl["underlying"] == DEFAULT_UND]["close"].astype(float).values,
                          index=udl[udl["underlying"] == DEFAULT_UND]["date"].values)
        self.feed = BacktestRunner(risk_all, daily, close)   # 全品种 risk；撮合数据仅 510300
        self.daily_unds = set(daily["contract_id"].astype(str).str[:6].unique())
        self.days = self.feed._days
        self.underlying = DEFAULT_UND   # 默认模拟时钟品种
        self.broker = Broker()
        self._oi = {}
        for f in (self.data_dir / "store" / "oi_snapshots").glob("*.parquet"):
            df = pd_read(f)
            for d, g in df.groupby("trade_date"):
                self._oi[d] = g.set_index("security_id")
        # 多品种标的价贯穿（撮合/盯市/结算/Greeks 全部经此取价）
        self.feed.spot_fn = lambda u, d: self.close_all.get((u, d), float("nan"))
        self.store = StateStore(self.data_dir / "paper.db")
        self.paper = PaperTradingRunner(self.feed, self.store, data_dir=self.data_dir,
                                        extra_rows_fn=self._szse_market_rows)
        self.cursor = self.days[-1]   # 默认最近交易日
        self.update_status = {"running": False, "tail": [], "done": False}
        from datetime import datetime as _dt
        self.server_started = _dt.now().strftime("%Y-%m-%d %H:%M")
        # 实时行情缓存：{symbol: {"last":..,"volume":..,"ts":epoch}}（sina 快照轮询写入）
        self.live_cache = {}
        self.live_thread = None
        self.live_target = None            # (underlying, expiry) 或 None
        self._last_auto_date = None        # 调度器去重
        self._last_probe = None            # 晚间探测节流
        self._probe_provider = None        # 惰性创建（避免拖慢启动与测试）
        if auto_update:
            threading.Thread(target=self._auto_update_loop, daemon=True).start()
        self.collect_status = {"running": False, "code": None, "started": None,
                               "tail": [], "done": False}

    # ------------------------------------------------------------ 品种
    def set_underlying(self, code):
        if code not in UNDERLYINGS:
            return {"ok": False, "msg": f"未知品种 {code}"}
        self.underlying = code
        collected = code in self.daily_unds or code == "159915"
        has_daily_bars = code in self.daily_unds   # 有逐合约日线即可撮合（多品种已开放）
        if has_daily_bars:
            msg = None
        elif collected and code == "159915":
            msg = "159915 已切换：深市快照口径（价格=前结算价，可下单，成交量闸门以 OI 代理）"
        elif collected:
            msg = f"{code} 行情已采集，多品种可交易"
        else:
            msg = f"{code} 未采集逐合约行情。采集：python -m thetalab.scripts.collect_contract_history {code}"
        return {"ok": True, "underlying": code, "name": UND_NAME.get(code, code),
                "has_daily_bars": has_daily_bars, "collected": collected, "msg": msg}

    def _fresh_account(self):
        return Account(initial_cash=1_000_000.0, cash=1_000_000.0)

    # ------------------------------------------------------------ 挂单/确认/推进
    def _full_chain(self, day):
        """沪市日线链 + 深市快照链合并（多品种撮合的统一行情面）"""
        chain = self.feed._build_chain(day, self.feed.risk_by_day[day])
        chain.update(self._szse_market_rows(day, "159915"))
        return chain

    def place_order(self, symbol, direction, offset, qty):
        if direction not in ("BUY", "SELL") or offset not in ("OPEN", "CLOSE"):
            return {"ok": False, "msg": "非法方向/开平"}
        qty = int(qty)
        if qty < 1:
            return {"ok": False, "msg": "数量至少 1 张"}
        day = self.cursor
        account = self.store.load_account() or self._fresh_account()
        if self.underlying == "159915" and self.szse_days() < self.SZSE_MIN_DAYS:
            return {"ok": False,
                    "msg": f"159915 快照积累不足：{self.szse_days()}/{self.SZSE_MIN_DAYS} 个交易日"}
        chain = self._full_chain(day)
        row = chain.get(symbol)
        if row is None:
            return {"ok": False, "msg": "当日无该合约行情（可能停牌/无量）"}
        inst = row.instrument
        pos = account.positions.get(symbol)
        if offset == "CLOSE":
            if pos is None:
                return {"ok": False, "msg": "无持仓可平"}
            if (direction == "BUY") != (pos.net_qty < 0):
                return {"ok": False, "msg": "平仓方向与持仓不符"}
            qty = min(qty, abs(int(pos.net_qty)))
        order = Order(instrument=inst, direction=Direction[direction],
                      offset=Offset[offset], qty=qty, strategy_id="manual",
                      reason="界面手动挂单")
        reject = self.paper.broker._preflight(order, row, account)
        if reject:
            return {"ok": False, "msg": f"拒单：{reject}"}
        key = f"{day}|{symbol}|{direction}|{offset}"
        self.store.put_pending(
            order_key=key, decision_day=str(day), symbol=symbol,
            direction=direction, offset=offset, qty=qty,
            reason="[手动] 界面挂单", instrument={"symbol": symbol})
        return {"ok": True, "key": key,
                "msg": "已挂单。请确认后推进交易日，将按下一日收盘价±价差成交（T+1 纪律）"}

    def confirm(self, keys=None):
        n = 0
        for p in self.store.pending():
            if keys and p["order_key"] not in keys:
                continue
            self.store.set_order_status(p["order_key"], "CONFIRMED")
            n += 1
        return {"ok": True, "confirmed": n}

    def _auto_jump_cursor(self) -> bool:
        """更新成功后时钟自动跳到最新交易日（纯浏览动作，有新数据才可见变化）。
        有未撮合挂单（PENDING/CONFIRMED）时不跳：跳到最新日后「推进」无下一日可推进，
        挂单会被跳过结算——此时提示用户先推进撮合。"""
        new_cursor = max(self.days)
        if new_cursor <= self.cursor:
            return False
        if self.store.pending("PENDING") or self.store.pending("CONFIRMED"):
            self.update_status["tail"].append(
                "有未撮合挂单，时钟未自动跳转：请先「推进下一交易日」完成撮合，再看新数据")
            return False
        self.cursor = new_cursor
        self.update_status["tail"].append(f"CURSOR→{new_cursor}")
        return True

    def start_update(self) -> dict:
        """页面一键更新当日数据：collect_daily（风险指标/标的日线/持仓快照/深市快照）→ reload"""
        if self.update_status["running"]:
            return {"ok": False, "msg": "数据更新进行中，请稍候"}
        self.update_status = {"running": True, "tail": [], "done": False}

        def run():
            try:
                proc = subprocess.Popen(
                    [sys.executable, str(ROOT / "thetalab" / "scripts" / "collect_daily.py")],
                    cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, encoding="utf-8", errors="ignore")
                for line in proc.stdout:
                    self.update_status["tail"].append(line.strip())
                    self.update_status["tail"] = self.update_status["tail"][-6:]
                proc.wait()
                self.update_status["done"] = proc.returncode == 0
                if proc.returncode == 0:
                    self.reload_data()
                    self._auto_jump_cursor()
                    self.update_status["tail"].append("DONE (reloaded)")
                else:
                    self.update_status["tail"].append(f"FAILED rc={proc.returncode}")
            except Exception as e:
                self.update_status["tail"].append(f"{type(e).__name__}: {e}")
            finally:
                self.update_status["running"] = False

        threading.Thread(target=run, daemon=True).start()
        return {"ok": True, "msg": "当日数据更新已在后台启动（约 4~6 分钟），完成后自动生效"}

    def update_status_now(self) -> dict:
        cs = dict(self.update_status)
        tail = cs.get("tail") or []
        cs["last"] = tail[-1] if tail else None
        return cs

    # ------------------------------------------------------------ 自动更新调度（晚间盯发布）
    AUTO_WINDOW_START = (19, 0)    # 收盘后开始盯发布（风险指标实测 19:30~21:00+ 才出，15:30 必空）
    AUTO_WINDOW_END = (23, 0)      # 截止时间
    AUTO_PROBE_INTERVAL = 600      # 探测间隔 10 分钟（轻探测：仅拉一次当日风险指标）

    def _auto_update_step(self, now) -> float:
        """自动调度的单步决策（可单测）：晚间窗口内每 10 分钟轻探测一次交易所风险指标，
        发布即触发采集+热更新+跳日。返回建议睡眠秒数。"""
        if self.update_status["running"] or now.weekday() >= 5:
            return 60.0
        today = now.date()
        if self._last_auto_date == str(today):
            return 60.0   # 今天已自动采集过
        start = now.replace(hour=self.AUTO_WINDOW_START[0], minute=self.AUTO_WINDOW_START[1],
                            second=0, microsecond=0)
        end = now.replace(hour=self.AUTO_WINDOW_END[0], minute=self.AUTO_WINDOW_END[1],
                          second=0, microsecond=0)
        if not (start <= now <= end):
            return 60.0
        if self._last_probe is not None and (now - self._last_probe).total_seconds() < self.AUTO_PROBE_INTERVAL:
            return 60.0
        self._last_probe = now
        if self._probe_provider is None:
            from thetalab.data.provider import SseOptionProvider
            self._probe_provider = SseOptionProvider(min_interval=0.3)
        try:
            published = not self._probe_provider.risk_indicators(today).empty
        except Exception:
            published = False   # 未发布（接口返回空表头时 akshare 会抛错）
        if not published:
            return 60.0
        rc = self._run_auto_collect(today)
        if rc == 0:
            self._last_auto_date = str(today)
        return 60.0

    def _run_auto_collect(self, day) -> int:
        """跑一次 collect_daily 并热更新；返回进程码。当日数据真正落库才算成功（否则下轮探测重试）。"""
        self.update_status = {"running": True, "tail": [], "done": False}
        try:
            proc = subprocess.Popen(
                [sys.executable, str(ROOT / "thetalab" / "scripts" / "collect_daily.py"), str(day)],
                cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="ignore")
            for line in proc.stdout:
                self.update_status["tail"].append(line.strip())
                self.update_status["tail"] = self.update_status["tail"][-6:]
            proc.wait()
            if proc.returncode != 0:
                self.update_status["tail"].append(f"FAILED rc={proc.returncode}")
                return proc.returncode
            self.reload_data()
            if day not in self.days:
                self.update_status["tail"].append(f"采集完成但 {day} 未落库，10 分钟后自动重试")
                return 1
            self._auto_jump_cursor()   # 有未撮合挂单则不跳（见方法注释）
            self.update_status["done"] = True
            self.update_status["tail"].append("DONE (auto)")
            return 0
        finally:
            self.update_status["running"] = False

    def _auto_update_loop(self):
        """服务器常开即自动：晚间窗口内盯交易所发布，发布即采集。
        替代原 15:30 定时——那时核心风险指标尚未发布，且 today 未落库使旧条件永不触发。"""
        from datetime import datetime as dt
        while True:
            try:
                self._auto_update_step(dt.now())
            except Exception:
                pass
            time.sleep(60)

    # ------------------------------------------------------------ 实时行情（盘中轮询 sina 快照）
    def _spot_timeout(self, p, u, timeout=5.0):
        """underlying_spot 带超时保护：东财全量快照偶发网络挂起会卡死整轮轮询（线程存活但缓存空）"""
        box = {}
        def _f():
            try:
                box["v"] = float(p.underlying_spot(u)["last"])
            except Exception:
                pass
        t = threading.Thread(target=_f, daemon=True)
        t.start()
        t.join(timeout)
        return box.get("v", float("nan"))

    def live_start(self, underlying: str, expiry: str = None) -> dict:
        """开启实时轮询：仅采集当前查看的到期月链（ATM±6 档），一轮约 40~90s（逐合约 sina 限流）。
        盘中语义：价格=新浪实时快照（当日），IV/Greeks=最近收盘日官方口径（cursor 日）。
        ATM 档按标的实时价定位（cursor 日无该品种风险表时退化为全量前 13 档）。"""
        if underlying not in UNDERLYINGS:
            return {"ok": False, "msg": "未知品种"}
        if underlying == "159915":
            return {"ok": False, "msg": "深市快照口径暂无实时源（sina 仅覆盖沪市期权）"}
        self.live_target = (underlying, expiry)
        if self.live_thread and self.live_thread.is_alive():
            return {"ok": True, "msg": "实时行情已在运行"}
        def run():
            from thetalab.data.provider import SseOptionProvider
            p = SseOptionProvider(min_interval=0.18)
            import time as _t
            while (self.live_target and
                   self.live_thread is not None):
                u, exp = self.live_target
                try:
                    day = self.cursor
                    day_risk = self.feed.risk_by_day.get(day)
                    g = (day_risk[day_risk["underlying"] == u]
                         if day_risk is not None else None)
                    spot = None
                    try:   # 盘中 ATM 按标的实时价定位（与收盘价无关；带超时防挂起）
                        spot = self._spot_timeout(p, u)
                    except Exception:
                        spot = float("nan")
                    if spot != spot:
                        spot = self.close_all.get((u, day), float("nan"))
                    if exp and g is not None:
                        g = g[g["expiry"].astype(str) == exp]
                    if g is not None and len(g) and spot == spot:
                        strikes = sorted(g["strike"].unique(),
                                         key=lambda k: abs(k - spot))[:13]
                        g = g[g["strike"].isin(strikes)]
                    for r in (g.itertuples(index=False) if g is not None else []):
                        try:
                            s_ = p.contract_spot(r.security_id)
                            self.live_cache[r.contract_id] = {
                                "last": s_["last"], "volume": s_["volume"],
                                "bid": s_["bid"], "ask": s_["ask"],
                                "open_interest": s_["open_interest"],
                                "ts": _t.time()}
                        except Exception:
                            pass
                except Exception:
                    pass
                _t.sleep(4)
        self.live_thread = threading.Thread(target=run, daemon=True)
        self.live_thread.start()
        return {"ok": True, "msg": "实时行情已开启（sina 快照，约 1~2 分钟级刷新）"}

    def live_stop(self) -> dict:
        self.live_target = None
        self.live_thread = None
        return {"ok": True, "msg": "实时行情已关闭"}

    def cancel_all(self):
        n = 0
        for p in self.store.pending(status="PENDING"):
            self.store.set_order_status(p["order_key"], "CANCELLED")
            n += 1
        return {"ok": True, "cancelled": n}

    def advance(self):
        i = self.days.index(self.cursor) if self.cursor in self.days else 0
        if i + 1 >= len(self.days):
            # 数据尽头（收盘后、明日数据未采）：同日本日撮合——
            # 用当天收盘价把 PENDING/CONFIRMED 挂单撮合掉，不阻塞收盘后的模拟下单。
            if self.store.pending("CONFIRMED") or self.store.pending("PENDING"):
                day = self.cursor
                report = self.paper.daily_update(day, include_pending=True)
                return {"ok": True, "day": str(day), "equity": report.equity,
                        "fills": report.fills, "notes": report.notes,
                        "signals": len(report.signals), "same_day": True}
            return {"ok": False, "msg": "已到数据尽头（当前为最新交易日）。可在此日挂单后再次点击「推进」按当日收盘价撮合"}
        self.cursor = self.days[i + 1]
        report = self.paper.daily_update(self.cursor)
        return {"ok": True, "day": str(self.cursor), "equity": report.equity,
                "fills": report.fills, "notes": report.notes, "signals": len(report.signals)}

    def set_day(self, day):
        d = date.fromisoformat(day)
        if d not in self.days:
            return {"ok": False, "msg": "该日无数据"}
        self.cursor = d
        return {"ok": True, "day": day}

    # ------------------------------------------------------------ 后台采集
    def start_collect(self, code: str) -> dict:
        if code not in UNDERLYINGS:
            return {"ok": False, "msg": f"未知品种 {code}"}
        if code == DEFAULT_UND:
            return {"ok": True, "msg": f"{DEFAULT_UND} 已有完整行情，无需采集"}
        if self.collect_status["running"]:
            return {"ok": False, "msg": f"采集进行中（{self.collect_status['code']}），请稍候"}
        self.collect_status = {"running": True, "code": code,
                               "started": str(date.today()), "tail": [], "done": False}

        def run():
            try:
                proc = subprocess.Popen(
                    [sys.executable, str(ROOT / "thetalab" / "scripts" / "collect_contract_history.py"), code],
                    cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, encoding="utf-8", errors="ignore")
                for line in proc.stdout:
                    self.collect_status["tail"].append(line.strip())
                    self.collect_status["tail"] = self.collect_status["tail"][-8:]
                proc.wait()
                self.collect_status["done"] = proc.returncode == 0
                self.collect_status["tail"].append(
                    "DONE" if proc.returncode == 0 else f"FAILED rc={proc.returncode}")
            except Exception as e:
                self.collect_status["tail"].append(f"{type(e).__name__}: {e}")
            finally:
                self.collect_status["running"] = False

        threading.Thread(target=run, daemon=True).start()
        return {"ok": True, "msg": f"{code} 行情采集已在后台启动（约 1~2 分钟），"
                                   f"完成后自动生效，无需重启"}

    def collect_status_now(self) -> dict:
        cs = dict(self.collect_status)
        tail = cs.get("tail") or []
        cs["progress"] = next((t for t in reversed(tail) if t.startswith("  进度")), None)
        return cs

    def reload_data(self):
        """采集完成后重建行情索引（无需重启服务器）"""
        store = ParquetStore(self.data_dir / "store")
        risk_all = store.read("risk_indicators")
        daily = pd_read(self.data_dir / "store" / "contract_daily" / "all.parquet")
        udl = pd_read(self.data_dir / "store" / "underlying_daily" / "all.parquet")
        udl = udl[udl["underlying"] == DEFAULT_UND]
        close = pd.Series(udl["close"].astype(float).values, index=udl["date"].values)
        self.feed = BacktestRunner(risk_all, daily, close)
        self.days = self.feed._days
        self.paper.feed = self.feed
        if self.cursor not in self.days:
            self.cursor = self.days[-1]

    # ------------------------------------------------------------ 行情装配
    def _quote(self, day, contract_id, security_id, underlying):
        """逐合约价格源：日线库优先（已采集品种全历史）；OI 快照兜底（仅快照日）"""
        d = self.feed._daily_ix.get((contract_id, day)) \
            or self.feed._daily_ix_sid.get((security_id, day))
        if d is not None and d.close > 0:
            return {"last": round(float(d.close), 4),
                    "volume": float(getattr(d, "volume_lots", 0) or 0)}
        snap = self._oi.get(day)
        if snap is not None and security_id in snap.index:
            r = snap.loc[security_id]
            return {"last": round(float(r["last"]), 4), "volume": float(r["volume"])}
        return None

    SZSE_MIN_DAYS = 1    # 深市快照门槛：链路验证完备（前结算/OI 均真实数据），日更积累中
                         # 成交基准=前结算价、成交量闸门=OI 代理（2%），如需更严可调高

    def szse_days(self) -> int:
        """深市快照已积累的交易日数"""
        n = set()
        for f in (self.data_dir / "store" / "snapshots_szse").glob("*.parquet"):
            df = pd_read(f)
            n.update(df["trade_date"].astype(str).unique())
        return len(n)

    def _szse_market_rows(self, day, underlying: str = "159915") -> dict:
        """深市快照 → MarketRow（成交量用 OI 代理，撮合口径保守）"""
        snap, snap_day = self._szse_latest(underlying)
        if snap is None:
            return {}
        rows = {}
        asof = min(day, date.fromisoformat(snap_day)) if snap_day else day
        import dataclasses
        for r in snap.itertuples(index=False):
            inst = self.feed.spec.option(underlying, Right[r.right], r.expiry,
                                         float(r.strike))
            # 深交所行权价段 6 位（上交所 5 位），build_option_symbol 与之不一致——
            # 合约 symbol 统一用快照原始 contract_id（键/持仓/挂单全链一致）
            inst = dataclasses.replace(inst, symbol=r.contract_id)
            rows[r.contract_id] = MarketRow(
                instrument=inst, trade_date=day, close=float(r.pre_settle),
                volume=float(r.oi or 0), open_interest=float(r.oi or 0),
                pre_close=float(r.pre_settle), pre_settle=float(r.pre_settle),
                spot_close=self.close_all.get((underlying, day), float("nan")),
                spot_prev_close=self.close_all.get(
                    (underlying, self.days[max(self.days.index(asof) - 1, 0)])
                    if asof in self.days else float("nan")))
        return rows

    def _szse_latest(self, underlying: str):
        """深市最新静态快照（含前结算价/OI/涨跌停/合约调整）"""
        for f in sorted((self.data_dir / "store" / "snapshots_szse").glob("*.parquet"),
                        reverse=True):
            df = pd_read(f)
            g = df[df["underlying"] == underlying]
            if len(g):
                return g, str(g["trade_date"].iloc[0])
        return None, None

    @staticmethod
    def expiries_of(rows):
        return sorted({r["expiry"] for r in rows})

    def _szse_rows(self, und, day, spot):
        """深市链装配：前结算价反推 IV（交易所同口径）+ 自算 Greeks + 交易所 OI/涨跌停"""
        from thetalab.core.pricing import bs_greeks, implied_vol
        snap, snap_day = self._szse_latest(und)
        if snap is None:
            return [], None, None
        asof = min(day, date.fromisoformat(snap_day)) if snap_day else day
        rows = []
        for r in snap.itertuples(index=False):
            T = max((r.expiry - asof).days, 0) / 365.0
            iv = implied_vol(float(r.pre_settle), spot, float(r.strike), T, 0.03,
                             Right[r.right]) if (spot == spot and T > 0) else float("nan")
            if iv == iv:
                g = bs_greeks(spot, float(r.strike), T, 0.03, iv, Right[r.right])
                gamma = round(g.gamma, 4)
                vega = round(g.vega, 5)
                theta = round(g.theta, 5)
                delta = round(g.delta, 3)
            else:
                gamma = vega = theta = delta = None
            inst = self.feed.spec.option(und, Right[r.right], r.expiry, float(r.strike))
            mrow = MarketRow(instrument=inst, trade_date=day, close=float(r.pre_settle),
                             volume=float(r.oi or 0), spot_close=spot, spot_prev_close=spot)
            mp = round(self.broker._margin_per_lot(inst, mrow), 0) if spot == spot else None
            rows.append({
                "contract_id": r.contract_id, "strike": float(r.strike),
                "right": r.right, "expiry": str(r.expiry), "dte": (r.expiry - asof).days,
                "last": round(float(r.pre_settle), 4), "volume": float(r.oi or 0),
                "iv": round(float(iv), 4) if iv == iv else None,
                "delta": delta, "gamma": gamma, "vega": vega, "theta": theta,
                "margin_per_lot": mp,
            })
        note = (f"深市快照口径（{snap_day}）：价格=前结算价（可交易），"
                f"IV 自算（前结算反推，交易所同法），OI/涨跌停为交易所公布，"
                f"成交量闸门以 OI 代理")
        return rows, self.expiries_of(rows), note

    def state(self, underlying=None):
        und = underlying or self.underlying
        if und not in UNDERLYINGS:
            und = DEFAULT_UND
        day = self.cursor
        account = self.store.load_account()
        note = UND_NOTE.get(und)

        if und == "159915":
            # 深市：快照链（IV/Greeks 自算自前结算，OI/涨跌停为交易所数据），撮合未开放
            spot = self.close_all.get((und, day), float("nan"))
            rows, expiries, sz_note = self._szse_rows(und, day, spot)
            rows.sort(key=lambda r: -r["strike"])
            note = sz_note or note
            has_daily_bars, collected = False, True
            pending = self.store.pending(status="PENDING") + \
                self.store.pending(status="CONFIRMED")
            eq = self.store.equity_curve()
            return self._state_payload(und, day, spot, account, rows, expiries,
                                       pending, eq, has_daily_bars, collected, note)

        account = self.store.load_account()
        day_risk = self.feed.risk_by_day[day]
        g = day_risk[day_risk["underlying"] == und]
        spot = self.close_all.get((und, day), float("nan"))
        gmap = self.feed._greeks_map(day_risk, spot=spot if spot == spot else 1.0, day=day)
        rows = []
        for r in g.itertuples(index=False):
            q = self._quote(day, r.contract_id, r.security_id, und)
            mp = None
            if q:
                inst = self.feed.spec.option(r.underlying, Right[r.right], r.expiry,
                                             float(r.strike))
                sc = spot if spot == spot else q["last"]
                mrow = MarketRow(instrument=inst, trade_date=day, close=q["last"],
                                 volume=q["volume"], spot_close=sc, spot_prev_close=sc)
                mp = round(self.broker._margin_per_lot(inst, mrow), 0)
            live = self.live_cache.get(r.contract_id)
            live_fresh = live and (time.time() - live["ts"]) < 120   # 一轮 40~90s,窗口须覆盖轮时长
            if live_fresh:
                q = {"last": live["last"], "volume": live["volume"]}
            gk = gmap.get(r.contract_id)
            rows.append({
                "contract_id": r.contract_id, "strike": float(r.strike),
                "right": r.right, "expiry": str(r.expiry),
                "dte": (r.expiry - day).days,
                "last": q["last"] if q else None,
                "volume": q["volume"] if q else 0,
                "live": bool(live_fresh),
                "iv": round(float(r.iv), 4) if r.iv == r.iv else None,
                "delta": round(float(gk.delta), 3) if gk else None,
                "gamma": round(float(gk.gamma), 4) if gk else None,
                "vega": round(float(gk.vega), 5) if gk else None,
                "theta": round(float(gk.theta), 5) if gk else None,
                "margin_per_lot": mp,
            })
        expiries = sorted({str(e) for e in g["expiry"].unique()})
        pending = self.store.pending(status="PENDING") + \
            self.store.pending(status="CONFIRMED")
        eq = self.store.equity_curve()
        has_daily_bars = und in self.daily_unds
        collected = und in self.daily_unds
        return self._state_payload(und, day, spot, account, rows, expiries,
                                   pending, eq, has_daily_bars, collected, note)

    def _state_payload(self, und, day, spot, account, rows, expiries, pending, eq,
                       has_daily_bars, collected, note):
        import datetime as _dt
        live_on = bool(self.live_thread and self.live_thread.is_alive())
        now = _dt.datetime.now()
        # 实时行情的日期歧义防护：盘中(当日有交易且已收盘前)实时价属于"今天"，
        # 而 IV/Greeks/撮合数据属于 cursor 日——前端据此显示双时间基准
        live_today = live_on and now.date() not in self.days   # 今天尚无收盘数据=盘中
        return {
            "ok": True, "cursor": str(day), "underlying": und,
            "live": live_on,
            "live_today": live_today,
            "now_clock": now.strftime("%Y-%m-%d %H:%M"),
            "server_started": self.server_started,
            "underlying_name": UND_NAME.get(und, und),
            "has_daily_bars": has_daily_bars, "collected": collected, "note": note,
            "underlyings": [{"code": u, "name": UND_NAME.get(u, u),
                             "collected": u in self.daily_unds or u == "159915",
                             "has_daily_bars": u in self.daily_unds,
                             "note": UND_NOTE.get(u)} for u in UNDERLYINGS],
            "spot": spot if spot == spot else None,
            "account": None if account is None else {
                "cash": round(account.cash, 2), "margin_used": round(account.margin_used, 2),
                "equity": round(account.equity, 2),
                "positions": [
                    {"symbol": s, "net_qty": p.net_qty, "avg_open": p.avg_open_price,
                     "last": p.last_price, "margin": p.margin, "pnl": round(p.total_pnl, 2)}
                    for s, p in account.positions.items()]},
            "pending": pending,
            "chain": rows, "expiries": expiries,
            "days": [str(d) for d in self.days],
            "equity_curve": eq,
        }

    def page(self):
        f = self.data_dir / "dashboard.html"
        if f.exists():
            return f.read_bytes()
        return "<h1>dashboard.html 不存在，请先运行 python -m thetalab.scripts.build_dashboard</h1>".encode("utf-8")


class Server(ThreadingHTTPServer):
    # Windows 上 allow_reuse_address=True 会让第二个进程"成功"绑定同一端口
    # （服务实际不响应）——必须关闭，端口冲突时才能触发 main() 的重试逻辑
    allow_reuse_address = False


class Handler(BaseHTTPRequestHandler):
    wb = None

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self._send(200, self.wb.page(), "text/html; charset=utf-8")
        elif path == "/api/state":
            q = parse_qs(urlparse(self.path).query)
            self._json(self.wb.state((q.get("underlying") or [None])[0]))
        elif path == "/api/collect_status":
            self._json({"ok": True, **self.wb.collect_status_now()})
        elif path == "/api/update_status":
            self._json({"ok": True, **self.wb.update_status_now()})
        else:
            self._json({"ok": False, "msg": "not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            body = {}
            n = int(self.headers.get("Content-Length") or 0)
            if n:
                body = json.loads(self.rfile.read(n).decode("utf-8"))
            if path == "/api/order":
                self._json(self.wb.place_order(
                    body.get("symbol", ""), body.get("direction", ""),
                    body.get("offset", ""), int(body.get("qty", 0))))
            elif path == "/api/confirm":
                self._json(self.wb.confirm(body.get("keys")))
            elif path == "/api/advance":
                self._json(self.wb.advance())
            elif path == "/api/set_day":
                self._json(self.wb.set_day(body.get("day", "")))
            elif path == "/api/cancel_all":
                self._json(self.wb.cancel_all())
            elif path == "/api/update_daily":
                self._json(self.wb.start_update())
            elif path == "/api/update_status":
                self._json({"ok": True, **self.wb.update_status_now()})
            elif path == "/api/set_underlying":
                self._json(self.wb.set_underlying(body.get("code", "")))
            elif path == "/api/live_start":
                self._json(self.wb.live_start(body.get("underlying", ""),
                                              body.get("expiry")))
            elif path == "/api/live_stop":
                self._json(self.wb.live_stop())
            elif path == "/api/collect":
                self._json(self.wb.start_collect(body.get("code", "")))
            elif path == "/api/collect_status":
                self._json({"ok": True, **self.wb.collect_status_now()})
            elif path == "/api/reload":
                self.wb.reload_data()
                self._json({"ok": True, "days": len(self.wb.days)})
            else:
                self._json({"ok": False, "msg": "not found"}, 404)
        except Exception as e:
            self._json({"ok": False, "msg": f"{type(e).__name__}: {e}"})

    def log_message(self, *args):
        pass


def main(host="127.0.0.1", port=8300, open_browser=True, retries=5):
    # 目录自愈：从任意目录启动都能找到 thetalab 包（ModuleNotFoundError 防护）
    import os as _os
    _root = str(ROOT)
    if _root not in _os.getcwd():
        _os.chdir(_root)
        sys.path.insert(0, _root)
    Handler.wb = Workbench()
    srv = None
    for p in range(port, port + retries):
        try:
            srv = Server((host, p), Handler)
            port = p
            break
        except OSError:
            print(f"端口 {p} 被占用，尝试 {p + 1} …")
    if srv is None:
        print(f"无法绑定端口 {port}~{port + retries - 1}，请手动指定端口")
        sys.exit(1)
    url = f"http://{host}:{port}"
    print(f"thetalab {THE_VERSION} — ETF 期权模拟交易工作台: {url}")
    print(f"品种: {','.join(UNDERLYINGS)}（仅 510300 已开放模拟撮合，其余可浏览）")
    print(f"模拟时钟: {Handler.wb.cursor} | 数据: {Handler.wb.days[0]} ~ {Handler.wb.days[-1]}")
    print("关闭服务：在本窗口按 Ctrl+C")
    if open_browser:
        import threading
        import webbrowser
        threading.Timer(1.8, lambda: webbrowser.open(url)).start()
    srv.serve_forever()


if __name__ == "__main__":
    import sys
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    main(port=int(args[0]) if args else 8300,
         open_browser="--no-browser" not in sys.argv)
