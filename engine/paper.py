"""
thetalab.engine.paper — 模拟盘 Runner（日频批处理，设计方案 §7.3）

流程（每交易日收盘后 15:30 跑一次 daily_update）：
    恢复状态 → 数据闸门 → 到期结算 → 盯市/风控 → 强制单/策略建议(PENDING)
    → 次日人工 confirm 后按 CLOSE_SLIPPAGE 撮合（T 日决策 T+1 执行纪律）
    → 信号/推荐/报告 JSON → 持久化

dry_run=True：新策略观察期，信号照常生成但不生成可确认订单（§7.3 纪律3）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from ..core.indicators import build_indicators, iv_rank_of
from ..core.models import Account, Direction, Offset, Order
from ..data.persist import StateStore
from ..strategy.advisor import Advisor
from ..strategy.signals import SignalEngine
from .broker import Broker, FillMode, MarketRow, RiskLimits
from .portfolio import Portfolio, pnl_attribution
from .runner import BacktestRunner  # 复用数据装配（_build_chain/_greeks_map/_atm_iv）

__all__ = ["DailyReport", "PaperTradingRunner"]


@dataclass
class DailyReport:
    day: date
    equity: float
    margin_ratio: float
    fills: List[dict] = field(default_factory=list)
    forced_orders: List[dict] = field(default_factory=list)   # 风控强制单（待确认）
    strategy_orders: List[dict] = field(default_factory=list) # 策略建议单（待确认）
    signals: List[dict] = field(default_factory=list)
    recommendations: List[dict] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


class PaperTradingRunner:
    """
    用法（每日收盘后）：
        runner = PaperTradingRunner(feed, store)
        report = runner.daily_update(day)          # 生成建议/强制单 + 报告
        runner.confirm_orders(report)              # 次日人工确认 → 撮合
    """

    def __init__(self, feed: BacktestRunner, store: StateStore,
                 data_dir: str | Path = "thetalab_data",
                 strategy=None, advisor: Optional[Advisor] = None,
                 risk_limits: Optional[RiskLimits] = None,
                 dry_run: bool = False,
                 extra_rows_fn=None):
        self.feed = feed                      # BacktestRunner 实例仅作数据装配器
        self.store = store
        self.data_dir = Path(data_dir)
        self.strategy = strategy
        self.advisor = advisor or Advisor()
        self.limits = risk_limits or RiskLimits()
        self.dry_run = dry_run
        self.extra_rows_fn = extra_rows_fn
        self.broker = Broker(fill_mode=FillMode.CLOSE_SLIPPAGE, limits=self.limits)
        self.signal_engine = SignalEngine(cooldown_days=1)
        self.account: Optional[Account] = None
        self.pf: Optional[Portfolio] = None

    # ------------------------------------------------------------ 主入口
    def daily_update(self, day: date, include_pending: bool = False,
                     match_orders: bool = True, chain_fn=None) -> DailyReport:
        report = DailyReport(day=day, equity=0.0, margin_ratio=0.0)
        # 1) 恢复状态
        self.account = self.store.load_account() or Account(initial_cash=1_000_000.0,
                                                            cash=1_000_000.0)
        self.pf = Portfolio(self.account)
        if day not in self.feed.risk_by_day:
            report.notes.append(f"{day} 非交易日或无数据，跳过")
            return report
        day_risk = self.feed.risk_by_day[day]
        spot = float(self.feed._spot("510300", day))   # 主标的收盘（结算/快照口径）
        if spot != spot:
            report.notes.append(f"{day} 标的收盘缺失，跳过")
            return report

        # 2) 数据装配（+深市快照行注入，多品种撮合）+ 八道闸门
        chain = chain_fn(day) if chain_fn else self.feed._build_chain(day, day_risk)
        if self.extra_rows_fn:
            for sym, mrow in (self.extra_rows_fn(day) or {}).items():
                if sym not in chain:
                    chain[sym] = mrow
        gmap = self.feed._greeks_map(day_risk, spot=spot, day=day,
                                    spot_of=lambda u: self.feed._spot(u, day))

        # 3) 到期结算
        fills = self.broker.settle_expiry(
            self.account, spot=lambda inst: self.feed._spot(inst.underlying, day), on=day)
        if fills:
            self.store.append_trades(fills)
            report.fills += [{"symbol": t.instrument.symbol, "type": "到期结算",
                              "pnl": t.realized_pnl} for t in fills]

        # 4) 人工确认单（昨日 PENDING → 今日按 CLOSE_SLIPPAGE 撮合）
        if match_orders:   # 盯市重估(match_orders=False)不替用户成交
            report.fills += self._match_confirmed(day, chain, include_pending=include_pending)

        # 5) 盯市 + 维持保证金（多品种：各合约用其标的当日收盘）+ Greeks 快照
        self.pf.update_mark({s: r.close for s, r in chain.items() if r.close > 0})
        self.pf.margin_refresh(
            {s: r.close for s, r in chain.items() if r.close > 0},
            spot_close=lambda inst: self.feed._spot(inst.underlying, day))
        greeks_map = {sym: gmap[sym] for sym in self.account.positions if sym in gmap}
        legs = {}
        for sym, pos in self.account.positions.items():
            g = gmap.get(sym)
            if g is None:
                continue
            legs[sym] = {"delta": g.delta, "gamma": g.gamma, "vega": g.vega,
                         "theta": g.theta, "iv": g.iv,
                         "qm": pos.net_qty * pos.instrument.multiplier,
                         "price": pos.last_price, "avg_open": pos.avg_open_price}
        atm_iv = self.feed._atm_iv(day_risk, spot)
        state = self.pf.snapshot(day, spot=spot, atm_iv=atm_iv,
                                 greeks_map=greeks_map, legs=legs)
        self.store.append_equity(str(day), state)
        report.equity = state.equity
        report.margin_ratio = self.account.margin_ratio

        # 6) 指标 + 信号 + 推荐
        ind = self._indicators(day, spot, atm_iv)
        ind.update(iv_rank_of(self._iv_series(), atm_iv))
        sigs = self.signal_engine.generate(
            ind, positions=self.account.positions, today=day,
            next_expiry=self.feed._next_expiry(day))
        self.store.append_signals(sigs, day)
        report.signals = [s.__dict__ | {"level": s.level} for s in sigs]
        chain_df = self.feed._chain_df(chain, day_risk)
        recs = self.advisor.recommend(
            ind, chain_oi=float(day_risk["oi"].sum()) if "oi" in day_risk else 0.0,
            chain_volume=float(day_risk["volume"].sum()) if "volume" in day_risk else 0.0,
            dte_choices=sorted({(e - day).days for e in day_risk["expiry"].unique()}))
        report.recommendations = [
            {"id": r.template_id, "name": r.name, "score": r.score,
             "reasons": r.reasons, "risks": r.risks, "exit_plan": r.exit_plan,
             "needs_permission_note": r.needs_permission_note} for r in recs]

        # 7) 风控强制单（保证金占用率超限 → 平最近月义务仓，待确认）
        if self.account.margin_ratio > 0.85:
            victim = self._nearest_short(day)
            if victim:
                self._queue_order(report, day, victim, "FORCED",
                                  f"保证金占用率 {self.account.margin_ratio:.0%}>85% 强制减仓",
                                  report.forced_orders)

        # 8) 策略建议（dry_run 时不挂单）
        if self.strategy and not self.dry_run:
            self.account._month_first = self._is_month_first(day)
            for o in self.strategy.on_day(day, chain_df, spot, self.account, state.equity):
                self._queue_order(report, day, o, "STRATEGY", o.reason,
                                  report.strategy_orders)

        # 9) 报告 + 持久化
        self.store.save_account(self.account)
        self._write_report(report, ind, state, chain_df)
        return report

    # ------------------------------------------------------------ 确认撮合
    def confirm_orders(self, report_day_keys: Optional[List[str]] = None) -> List[dict]:
        """人工确认（默认全部 PENDING）→ 标记 CONFIRMED，次日 daily_update 时撮合。
        本方法只做标记；撮合发生在下一次 daily_update（T+1 纪律）。"""
        n = 0
        for p in self.store.pending():
            if report_day_keys and p["order_key"] not in report_day_keys:
                continue
            self.store.set_order_status(p["order_key"], "CONFIRMED")
            n += 1
        return [{"confirmed": n, "note": "确认后将于下一交易日按 CLOSE_SLIPPAGE 撮合"}]

    def _match_confirmed(self, day: date, chain: Dict[str, MarketRow],
                         include_pending: bool = False) -> List[dict]:
        """撮合已人工确认（CONFIRMED）的订单——T+1 执行纪律的落地。
        include_pending=True 时同时撮合 PENDING 单（当日撮合：收盘后挂单→推进→当天成交）。"""
        fills = []
        pend = self.store.pending(status="CONFIRMED")
        if include_pending:
            pend += self.store.pending(status="PENDING")
        for p in pend:
            inst_d = p["instrument"]
            inst = next((r.instrument for r in chain.values()
                         if r.instrument.symbol == inst_d["symbol"]), None)
            if inst is None:
                self.store.set_order_status(p["order_key"], "EXPIRED")
                fills.append({"symbol": inst_d["symbol"], "type": "拒单",
                              "pnl": 0.0, "note": "当日无该合约行情"})
                continue
            row = chain[inst.symbol]
            o = Order(instrument=inst, direction=Direction[p["direction"]],
                      offset=Offset[p["offset"]], qty=p["qty"],
                      strategy_id="paper", reason=p["reason"])
            trades, reject = self.broker.match(o, row, self.account)
            if trades:
                self.store.append_trades(trades)
                fills += [{"symbol": t.instrument.symbol, "type": "成交",
                           "price": t.price, "qty": t.qty, "pnl": t.realized_pnl}
                          for t in trades]
            if reject:
                fills.append({"symbol": inst.symbol, "type": "拒单", "pnl": 0.0,
                              "note": reject})
            self.store.set_order_status(p["order_key"],
                                        "FILLED" if trades else "REJECTED")
        return fills

    # ------------------------------------------------------------ 工具
    def _queue_order(self, report, day, o: Order, source: str, reason: str, bucket: List[dict]):
        key = f"{day}|{o.instrument.symbol}|{o.direction.value}|{o.offset.value}"
        self.store.put_pending(
            order_key=key, decision_day=str(day), symbol=o.instrument.symbol,
            direction=o.direction.value, offset=o.offset.value, qty=o.qty,
            reason=f"[{source}] {reason}",
            instrument={"symbol": o.instrument.symbol})
        bucket.append({"key": key, "symbol": o.instrument.symbol,
                       "action": f"{o.direction.value}/{o.offset.value}",
                       "qty": o.qty, "reason": reason})

    def _nearest_short(self, day: date):
        shorts = [(p.instrument.expiry, sym) for sym, p in self.account.positions.items()
                  if p.net_qty < 0 and p.instrument.expiry >= day]
        if not shorts:
            return None
        sym = min(shorts)[1]
        pos = self.account.positions[sym]
        return Order(instrument=pos.instrument, direction=Direction.BUY,
                     offset=Offset.CLOSE, qty=abs(pos.net_qty),
                     strategy_id="risk", reason="强制减仓")

    def _is_month_first(self, day: date) -> bool:
        prev = [d for d, _ in self.store.equity_curve() if d != str(day)]
        return not prev or pd.Timestamp(prev[-1]).month != day.month

    def _indicators(self, day: date, spot: float, atm_iv: float) -> dict:
        udl = self.feed.underlying_close
        days = [d for d in sorted(udl.index) if d <= day][-90:]
        closes = [float(udl[d]) for d in days]
        df = pd.DataFrame({
            "date": days, "close": closes,
            "high": [c * 1.004 for c in closes], "low": [c * 0.996 for c in closes]})
        ind = build_indicators(df)
        ind["iv_atm"] = atm_iv
        ind["iv_rank"] = self.feed._iv_rank_cache.get(day, float("nan")) \
            if hasattr(self.feed, "_iv_rank_cache") else float("nan")
        return ind

    def _iv_series(self) -> pd.Series:
        if not hasattr(self, "_iv_cache"):
            f = self.data_dir / "atm_iv_series.csv"
            if f.exists():
                ivs = pd.read_csv(f, parse_dates=["date"])
                self._iv_cache = pd.Series(ivs["atm_iv"].values,
                                           index=ivs["date"].dt.date)
            else:
                self._iv_cache = pd.Series(dtype=float)   # 无历史 → iv_rank=NaN
        return self._iv_cache

    def _write_report(self, report: DailyReport, ind: dict, state, chain_df: pd.DataFrame):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "day": str(report.day),
            "equity": report.equity, "margin_ratio": report.margin_ratio,
            "cash": self.account.cash, "margin_used": self.account.margin_used,
            "indicators": {k: (v if v == v else None) for k, v in ind.items()
                           if isinstance(v, (int, float, str))},
            "positions": [
                {"symbol": sym, "net_qty": p.net_qty, "avg_open": p.avg_open_price,
                 "last": p.last_price, "margin": p.margin,
                 "market_value": p.market_value, "pnl": p.total_pnl}
                for sym, p in self.account.positions.items()],
            "signals": report.signals, "recommendations": report.recommendations,
            "fills": report.fills,
            "pending": report.forced_orders + report.strategy_orders,
            "equity_curve": self.store.equity_curve(),
            "chain": chain_df[["contract_id", "strike", "right", "expiry", "close",
                               "iv", "delta", "volume"]].where(
                pd.notna(chain_df[["contract_id", "strike", "right", "expiry", "close",
                                   "iv", "delta", "volume"]]), None
            ).to_dict(orient="records"),
        }
        out = self.data_dir / "report.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=1),
                       encoding="utf-8")
