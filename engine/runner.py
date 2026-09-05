"""
thetalab.engine.runner — 回测 Runner（T日决策、T+1执行）+ 内置策略示例（§7.1/§7.3）

关键纪律（§7.3，不可省略）：
    1. T 日决策、T+1 日执行——用当日收盘价撮合当日决策属于作弊
    2. 风控检查先于策略信号（先止损、后开仓）
    3. 单日亏损 > daily_loss_break（默认 3%）→ 当日禁止开仓
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd

from ..core.models import (
    Account, Direction, FeeRule, Greeks, Instrument, Offset, Order, OrderStatus, Right, Trade,
)
from ..core.pricing import bs_greeks
from ..core.spec import OptionSpecRegistry
from .broker import Broker, FillMode, MarketRow, RiskLimits
from .metrics import fee_share, performance_metrics, trade_stats
from .portfolio import Portfolio, pnl_attribution

__all__ = ["BacktestResult", "SellStrangleStrategy", "BacktestRunner"]


@dataclass
class BacktestResult:
    metrics: Dict = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[Tuple] = field(default_factory=list)
    states: List = field(default_factory=list)          # 每日 PortfolioState（Greeks 暴露用）
    attribution: pd.DataFrame = field(default_factory=pd.DataFrame)
    rejects: List[Tuple[str, str]] = field(default_factory=list)
    orders_log: List[Dict] = field(default_factory=list)


# ---------------------------------------------------------------- 策略

class SellStrangleStrategy:
    """
    月度卖出虚值宽跨式（delta 目标 ±0.20），DTE<=exit_dte 平仓，单腿 2 倍权利金止损。
    持仓未清时不重复建仓；决策产生于 T 日，订单 T+1 由 Runner 撮合。
    """
    def __init__(self, delta_target: float = 0.20, entry_dte_min: int = 25,
                 exit_dte: int = 7, stop_multiple: float = 2.0,
                 lots_per_side: int = 10):
        self.delta_target = delta_target
        self.entry_dte_min = entry_dte_min
        self.exit_dte = exit_dte
        self.stop_multiple = stop_multiple
        self.lots_per_side = lots_per_side
        self.entry_open: Dict[str, float] = {}   # symbol -> 开仓权利金

    def on_day(self, day: date, chain: pd.DataFrame, spot: float,
               account: Account, equity: float) -> List[Order]:
        """chain 列: contract_id/strike/right/expiry/delta/close/volume/_instrument"""
        orders: List[Order] = []
        if chain.empty:
            return orders

        # 1) 持仓管理（先于开仓，§7.1 顺序原则）：DTE 退出 + 止损
        for sym, pos in list(account.positions.items()):
            dte = (pos.instrument.expiry - day).days
            if dte <= self.exit_dte:
                orders.append(Order(instrument=pos.instrument,
                                    direction=Direction.BUY if pos.net_qty < 0 else Direction.SELL,
                                    offset=Offset.CLOSE, qty=abs(pos.net_qty),
                                    strategy_id="strangle",
                                    reason=f"DTE={dte}<={self.exit_dte} 平仓"))
                continue
            entry_px = self.entry_open.get(sym)
            if entry_px and pos.last_price == pos.last_price \
                    and pos.last_price >= entry_px * self.stop_multiple:
                orders.append(Order(instrument=pos.instrument,
                                    direction=Direction.BUY if pos.net_qty < 0 else Direction.SELL,
                                    offset=Offset.CLOSE, qty=abs(pos.net_qty),
                                    strategy_id="strangle",
                                    reason=f"止损: {pos.last_price:.4f}>={self.stop_multiple}x{entry_px:.4f}"))

        if account.positions:          # 有持仓：只管理不加仓
            return orders

        # 2) 建仓：仅当月首个交易日
        if not getattr(account, "_month_first", False):
            return orders
        exps = sorted(e for e in chain["expiry"].unique() if (e - day).days >= self.entry_dte_min)
        if not exps:
            return orders
        exp = exps[0]
        g = chain[(chain["expiry"] == exp) & chain["delta"].notna() & (chain["close"] > 0)]
        picked: List[Order] = []
        for right, want in ((Right.CALL, self.delta_target), (Right.PUT, -self.delta_target)):
            gg = g[g["right"] == right]
            if gg.empty:
                continue
            row = gg.loc[(gg["delta"] - want).abs().idxmin()]
            if row["_instrument"] is None or row["volume"] < 100:
                continue
            picked.append(Order(instrument=row["_instrument"], direction=Direction.SELL,
                                offset=Offset.OPEN, qty=self.lots_per_side,
                                strategy_id="strangle",
                                reason=f"月度卖{right.cn}: Δ={row['delta']:.2f} "
                                       f"K={row['strike']} DTE={(exp - day).days}"))
        return picked

    def remember_entries(self, trades: List[Trade]):
        for t in trades:
            if t.offset is Offset.OPEN and t.direction is Direction.SELL:
                self.entry_open[t.instrument.symbol] = t.price


# ---------------------------------------------------------------- Runner

class BacktestRunner:
    """
    数据装配：risk_indicators(IV/Greeks/合约宇宙) + contract_daily(成交价/量) + 标的收盘。
    撮合基准：收盘价（交易所结算价无免费接口，pre_settle 用收盘近似——报告须标注）。
    """

    def __init__(self, risk_df: pd.DataFrame, daily_df: pd.DataFrame,
                 underlying_close: pd.Series, spec: Optional[OptionSpecRegistry] = None):
        self.risk = risk_df[~risk_df["adjusted"]] if "adjusted" in risk_df.columns else risk_df
        self.daily = daily_df
        self.underlying_close = underlying_close
        self.spec = spec or OptionSpecRegistry()
        # 主键 contract_id（2026-08-29 检验：security_id 存在 19% ID 复用，不可作时间序列主键）
        # 多品种标的价回调：spot_fn(underlying, day) -> float。缺省=单标的(underlying_close)
        self.spot_fn = None
        self._daily_ix, self._daily_ix_sid = {}, {}
        for r in self.daily.itertuples(index=False):
            cid = getattr(r, "contract_id", None)
            if cid:
                self._daily_ix[(cid, r.trade_date)] = r
            self._daily_ix_sid[(r.security_id, r.trade_date)] = r
        self.risk_by_day = {d: g.reset_index(drop=True)
                            for d, g in self.risk.groupby("trade_date")}
        self._days = sorted(self.risk_by_day)

    def run(self, strategy: SellStrangleStrategy, start: date, end: date,
            cash: float = 1_000_000.0, daily_loss_break: float = 0.03,
            risk_limits: Optional[RiskLimits] = None,
            fee_rule: Optional[FeeRule] = None) -> BacktestResult:
        broker = Broker(fill_mode=FillMode.CLOSE_SLIPPAGE,
                        limits=risk_limits or RiskLimits(), fee_rule=fee_rule)
        account = Account(initial_cash=cash, cash=cash)
        pf = Portfolio(account)
        result = BacktestResult()
        pending: List[Order] = []
        prev_state: Optional[object] = None
        month_first_seen = set()

        for day in [d for d in self._days if start <= d <= end]:
            spot = float(self.underlying_close.get(day, float("nan")))
            if spot != spot:
                continue
            day_risk = self.risk_by_day[day]

            # 1) 到期处理
            for t in broker.settle_expiry(account, spot=spot, on=day):
                result.trades.append(t)

            # 2) 盯市 + 维持保证金 + 希腊聚合 + 快照
            chain = self._build_chain(day, day_risk)
            pf.update_mark({s: r.close for s, r in chain.items() if r.close > 0})
            pf.margin_refresh({s: r.close for s, r in chain.items() if r.close > 0}, spot)
            gmap = self._greeks_map(day_risk, spot=spot, day=day)
            greeks_map = {sym: gmap[sym] for sym in account.positions if sym in gmap}
            legs = {}
            for sym, pos in account.positions.items():
                g = gmap.get(sym)
                if g is None:
                    continue
                legs[sym] = {"delta": g.delta, "gamma": g.gamma, "vega": g.vega,
                             "theta": g.theta, "iv": g.iv,
                             "qm": pos.net_qty * pos.instrument.multiplier,
                             "price": pos.last_price, "avg_open": pos.avg_open_price}
            atm_iv = self._atm_iv(day_risk, spot)
            state = pf.snapshot(day, spot=spot, atm_iv=atm_iv,
                                greeks_map=greeks_map, legs=legs)

            # 3) 逐日损益归因
            if prev_state is not None:
                result.attribution = pd.concat(
                    [result.attribution,
                     pd.DataFrame([{"day": day, **pnl_attribution(prev_state, state)}])],
                    ignore_index=True)
            prev_state = state

            # 4) 执行 T-1 日决策（T+1 纪律）
            for o in pending:
                row = chain.get(o.instrument.symbol)
                if row is None:
                    o.status = OrderStatus.REJECTED
                    o.reject_msg = "当日无该合约行情"
                    result.rejects.append((o.instrument.symbol, o.reject_msg))
                    continue
                trades, reject = broker.match(o, row, account)
                result.trades.extend(trades)
                if reject:
                    result.rejects.append((o.instrument.symbol, reject))
            strategy.remember_entries(result.trades)
            pending = []

            # 5) 熔断：单日亏损超限 → 当日只平不开
            allow_open = True
            if not result.attribution.empty:
                day_total = result.attribution.iloc[-1]["total"]
                if state.equity > 0 and day_total < -daily_loss_break * state.equity:
                    allow_open = False

            # 6) T 日决策
            account._month_first = day.month not in month_first_seen
            if account._month_first:
                month_first_seen.add(day.month)
            for o in strategy.on_day(day, self._chain_df(chain, day_risk), spot,
                                     account, state.equity):
                if not allow_open and o.offset is Offset.OPEN:
                    result.rejects.append((o.instrument.symbol, "单日亏损熔断，禁止开仓"))
                    continue
                pending.append(o)
                result.orders_log.append({
                    "decision_day": str(day), "symbol": o.instrument.symbol,
                    "action": f"{o.direction.value}/{o.offset.value}",
                    "qty": o.qty, "reason": o.reason})

        result.equity_curve = pf.equity_curve()
        result.states = pf.states
        result.metrics = performance_metrics(result.equity_curve)
        result.metrics.update(trade_stats(result.trades))
        total_pnl = sum(t.realized_pnl for t in result.trades)
        result.metrics["fee_share"] = fee_share(result.trades, total_pnl)
        return result

    # ------------------------------------------------------------ 数据装配
    def _spot(self, underlying: str, day: date) -> float:
        if self.spot_fn is not None:
            v = self.spot_fn(underlying, day)
            return float(v) if v == v else float("nan")
        return float(self.underlying_close.get(day, float("nan")))

    def _build_chain(self, day: date, day_risk: pd.DataFrame) -> Dict[str, MarketRow]:
        rows: Dict[str, MarketRow] = {}
        i = self._days.index(day)
        prev_day = self._days[i - 1] if i > 0 else None
        for r in day_risk.itertuples(index=False):
            d = self._daily_ix.get((r.contract_id, day))                 or self._daily_ix_sid.get((r.security_id, day))
            if d is None or not d.close > 0:
                continue
            inst = self.spec.option(r.underlying, Right[r.right], r.expiry, float(r.strike))
            # 标的收盘按各合约自己的品种取（多品种：510300 的虚值度不能用 588000 的价格算）
            spot_now = self._spot(r.underlying, day)
            spot_prev = self._spot(r.underlying, prev_day) if prev_day else float("nan")
            rows[inst.symbol] = MarketRow(
                instrument=inst, trade_date=day, close=float(d.close),
                volume=float(getattr(d, "volume_lots", 0.0) or 0.0),
                pre_close=float(d.close), pre_settle=float(d.close),  # 结算价缺口：收盘近似
                spot_close=spot_now, spot_prev_close=spot_prev)
        return rows

    def _chain_df(self, chain: Dict[str, MarketRow], day_risk: pd.DataFrame) -> pd.DataFrame:
        df = day_risk.copy()
        df["_instrument"] = [chain[r.contract_id].instrument if r.contract_id in chain else None
                             for r in day_risk.itertuples(index=False)]
        df["close"] = [chain[r.contract_id].close if r.contract_id in chain else float("nan")
                       for r in day_risk.itertuples(index=False)]
        df["volume"] = [chain[r.contract_id].volume if r.contract_id in chain else 0.0
                        for r in day_risk.itertuples(index=False)]
        return df

    def _next_expiry(self, day: date) -> Optional[date]:
        exps = sorted(e for e in self.risk["expiry"].unique() if e >= day)
        return exps[0] if exps else None

    def _atm_iv(self, day_risk: pd.DataFrame, spot: float) -> float:
        """当日 ATM IV（最近月、距标的价格最近的行权价，认购认沽均值）"""
        g = day_risk[day_risk["iv"].notna()]
        if g.empty:
            return float("nan")
        g = g[g["expiry"] == sorted(g["expiry"].unique())[0]]
        if g.empty:
            return float("nan")
        k = g["strike"].iloc[int((g["strike"] - spot).abs().values.argmin())]
        return float(g[g["strike"] == k]["iv"].mean())

    def _greeks_map(self, day_risk: pd.DataFrame, spot: float, day: date,
                    spot_of=None) -> Dict[str, Greeks]:
        """
        Greeks 口径决策（2026-08-29 复检后重写）：

        交易所风险指标基于**结算价**，而本系统盯市/成交价基于**收盘价**——两个基准
        每日错位，曾导致逐日归因残差占比中位 75%+。修复：Greeks 一律用自有 BS 模型
        按「交易所 IV + 标的收盘价」现算，与盯市口径完全一致（模型内归因）；
        归因残差从此只包含模型外效应（微笑动态/供需噪声），语义清晰。

        IV 仍用交易所公布值（市场共识，深度档缺失时该腿不入聚合）。
        """
        out = {}
        for r in day_risk.itertuples(index=False):
            if not pd.notna(r.iv) or not pd.notna(r.strike) or r.expiry is None:
                continue
            s_ = spot_of(r.underlying) if spot_of else spot
            if s_ != s_ or s_ <= 0:
                continue
            T = max((r.expiry - day).days, 0) / 365.0
            g = bs_greeks(s_, float(r.strike), T, 0.03, float(r.iv), Right[r.right])
            out[r.contract_id] = g
        return out


def spot_nan(day: date) -> float:
    return float("nan")
