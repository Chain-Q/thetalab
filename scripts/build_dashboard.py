"""
thetalab.scripts.build_dashboard — 单文件 HTML 工作台（设计方案 P4 / §3 L5）

零依赖：无 CDN、无构建，双击即开。数据由本脚本从本地库装配后内联。
运行：python -m thetalab.scripts.build_dashboard  →  thetalab_data/dashboard.html
"""
import json
import re
import math
import sys
import warnings
from datetime import date, datetime
from pathlib import Path

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import numpy as np

# import 兜底（同 server.py）：本机偶发 thetalab 默认查找失效
def _ensure_thetalab_importable():
    import importlib.util as _iu
    import sys as _s
    if "thetalab" in _s.modules:
        return
    _pkg = str(Path(__file__).resolve().parents[2]) + "/thetalab"
    if Path(_pkg).is_dir():
        try:
            _spec = _iu.spec_from_file_location(
                "thetalab", Path(_pkg) / "__init__.py", submodule_search_locations=[_pkg])
            if _spec and _spec.loader:
                _m = _iu.module_from_spec(_spec); _spec.loader.exec_module(_m)
                _s.modules["thetalab"] = _m
        except Exception:
            pass
_ensure_thetalab_importable()

from thetalab.core.indicators import build_indicators, iv_rank_of
from thetalab.data.persist import StateStore
from thetalab.data.provider import ParquetStore, SseOptionProvider
from thetalab.engine.runner import BacktestRunner
from thetalab.core.pricing import bs_greeks
from thetalab.strategy.advisor import Advisor
from thetalab.strategy.payoff import PayoffLeg, build_curves
from thetalab.strategy.signals import SignalEngine
from thetalab.strategy.spec import resolve_legs

UNDERLYING = "510300"
DATA = Path("thetalab_data")


def assemble():
    store = ParquetStore(DATA / "store")
    risk_all = store.read("risk_indicators")
    risk = risk_all[risk_all["underlying"] == UNDERLYING]
    day = max(risk["trade_date"])
    day_risk = risk[risk["trade_date"] == day].reset_index(drop=True)
    daily = pd.read_parquet(store.root / "contract_daily" / "all.parquet")
    provider = SseOptionProvider(min_interval=0.3)
    udl = provider.underlying_daily(UNDERLYING, day - pd.Timedelta(days=200).to_pytimedelta(), day)
    close = pd.Series(udl["close"].astype(float).values, index=udl["date"].values)
    feed = BacktestRunner(risk, daily, close)
    chain_df = feed._chain_df(feed._build_chain(day, day_risk), day_risk)
    spot = float(close.get(day, float("nan")))

    # 指标 + IV rank（截至当日）
    ind = build_indicators(udl.rename(columns={}))
    ind["iv_atm"] = feed._atm_iv(day_risk, spot)
    ivs = pd.read_csv(DATA / "atm_iv_series.csv", parse_dates=["date"])
    ser = pd.Series(ivs["atm_iv"].values, index=ivs["date"].dt.date)
    ind.update(iv_rank_of(ser[ser.index <= day], ind["iv_atm"]))

    # 信号 + 推荐（偏斜突变用当日 vs 5 日前链，全部真实数据）
    days_all = sorted(risk["trade_date"].unique())
    i = days_all.index(day)
    prev_day = days_all[i - 5] if i >= 5 else None
    chain_prev = (risk[risk["trade_date"] == prev_day] if prev_day else None)
    sigs = SignalEngine(cooldown_days=1).generate(
        ind, chain_today=day_risk, chain_prev=chain_prev, positions={},
        today=day, next_expiry=feed._next_expiry(day))
    recs = Advisor().recommend(
        ind, float(day_risk["open_interest"].sum()) if "open_interest" in day_risk else 2e5,
        float(day_risk["volume"].sum()) if "volume" in day_risk else 1e5,
        dte_choices=sorted({(e - day).days for e in day_risk["expiry"].unique()}))

    # 盈亏结构：对 Top1 推荐按当前链解析
    payoff = None
    if recs and recs[0].spec:
        orders, _ = resolve_legs(recs[0].spec, chain_df, day, spot, 1_000_000.0,
                                 margin_of=lambda row, s, e, t: 3000.0)
        # 标的腿（kind=UNDERLYING）resolve 出的 Instrument 既无 expiry 也无数量
        # （qty=0，尺寸留给上层补），对期权盈亏曲线零贡献 → 直接跳过，
        # 否则 expiry=None 参与日期减法会 TypeError 让整张静态页生成失败
        orders = [x for x in orders if x.qty and x.instrument.expiry]
        if orders:
            # 取价键用合约对象而非 contract_id 字符串：分红调整(A)合约的交易所代码与
            # spec.option() 生成的 symbol 不同，用 contract_id 查会查空 → iloc[0] 直接
            # IndexError，整张静态页生成失败（2026-09-12 持仓/推荐含该档合约时复现）
            have = chain_df["_instrument"].notna()
            px = {i.symbol: c for i, c in
                  zip(chain_df.loc[have, "_instrument"], chain_df.loc[have, "close"])}
            legs = []
            for o in orders:
                e = px.get(o.instrument.symbol)
                if e is None or e != e or e <= 0:
                    legs = []      # 该推荐腿当日无行情：宁可不画也不用错价
                    break
                legs.append(PayoffLeg(right=o.instrument.right, strike=o.instrument.strike,
                                      expiry=o.instrument.expiry,
                                      qty=o.qty * (1 if o.direction.value == "BUY" else -1),
                                      multiplier=o.instrument.multiplier,
                                      entry_price=float(e), iv=0.16))
            c = build_curves(legs, spot=spot, asof=day)
            payoff = {"spots": [round(x, 4) for x in c.spots],
                      "at_expiry": [round(x) for x in c.at_expiry],
                      "t0": [round(x) for x in c.t0],
                      "breakevens": c.breakevens,
                      "name": recs[0].name}

    # 账户（模拟盘若已初始化）
    db = DATA / "paper.db"
    account = None
    paper_curve = []
    if db.exists():
        st = StateStore(db)
        a = st.load_account()
        if a:
            account = {"cash": a.cash, "margin_used": a.margin_used,
                       "equity": a.equity,
                       "positions": [
                           {"symbol": s, "net_qty": p.net_qty, "avg_open": p.avg_open_price,
                            "last": p.last_price, "margin": p.margin, "pnl": p.total_pnl}
                           for s, p in a.positions.items()]}
        paper_curve = st.equity_curve()
    # 概览净值固定用回测曲线（模拟盘净值在 KPI 权益中体现）
    equity_curve = []
    eq_csv = DATA / "strangle_equity_curve.csv"
    if eq_csv.exists():
        eq = pd.read_csv(eq_csv)
        equity_curve = [(str(d), round(float(v), 0))
                        for d, v in zip(eq["date"], eq["equity"])]
    paper_curve_out = paper_curve

    # ---- IV 曲面：当日全链 (到期, 行权价, IV) —— 虚值度归一
    surf = []
    g_all = chain_df[chain_df["iv"].notna()]
    for exp, g in g_all.groupby("expiry"):
        for r in g.itertuples():
            surf.append({"dte": (exp - day).days, "strike": float(r.strike),
                         "iv": round(float(r.iv), 4),
                         "mny": round(float(r.strike) / spot - 1, 4)})

    # ---- 归因逐日分项 + 累计（回测产出）
    attr = None
    a_csv = DATA / "strangle_attribution.csv"
    if a_csv.exists():
        a = pd.read_csv(a_csv)
        for c in ("delta", "gamma", "vega", "theta", "trade", "residual"):
            if c not in a.columns:
                a[c] = 0.0
        a["cum"] = a["total"].cumsum()
        attr = {"days": [str(x) for x in a["day"]],
                "series": {c: [round(float(x)) for x in a[c]]
                           for c in ("delta", "gamma", "vega", "theta", "residual")},
                "cum": [round(float(x)) for x in a["cum"]]}

    # ---- 压力矩阵 + POP：对 Top1 推荐的解析腿现算（无持仓时的示意口径）
    stress = pop = None
    if recs and recs[0].spec:
        orders, _ = resolve_legs(recs[0].spec, chain_df, day, spot, 1_000_000.0,
                                 margin_of=lambda row, s_, e_, t_: 3000.0)
        orders = [x for x in orders if x.qty and x.instrument.expiry]
        if orders:
            legs = []
            for o in orders:
                px_row = chain_df.loc[chain_df["contract_id"] == o.instrument.symbol]
                px0 = float(px_row["close"].iloc[0]) if not px_row.empty else 0.01
                iv0 = float(px_row["iv"].iloc[0]) if not px_row.empty and px_row["iv"].notna().iloc[0] else 0.16
                qm = o.qty * o.instrument.multiplier * (1 if o.direction.value == "BUY" else -1)
                T = max((o.instrument.expiry - day).days, 0) / 365.0
                g = bs_greeks(spot, o.instrument.strike, T, 0.03, iv0, o.instrument.right)
                legs.append({"strike": o.instrument.strike, "right": o.instrument.right,
                             "qm": qm, "iv": iv0, "entry": px0, "T": T,
                             "delta": g.delta, "gamma": g.gamma, "vega": g.vega, "theta": g.theta})
            # 压力矩阵（Greeks 一阶+二阶近似）
            D = sum(l["delta"] * l["qm"] for l in legs)
            G = sum(l["gamma"] * l["qm"] for l in legs)
            V = sum(l["vega"] * l["qm"] for l in legs)
            Th = sum(l["theta"] * l["qm"] for l in legs)
            stress = {"rows": [], "note": f"{recs[0].name} 示意腿"}
            for pct in (-0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03):
                dS = spot * pct
                stress["rows"].append({"pct": f"{pct:+.0%}",
                                       **{f"{dv:+d}v": round(D*dS + 0.5*G*dS*dS + V*dv + Th, 0)
                                          for dv in (-5, -2, 0, 2, 5)}})
            # POP：到期对数正态 Monte Carlo（2000 次）
            rng = np.random.default_rng(7)
            ST = spot * np.exp((0.03 - 0.5 * ind.get("rv20", 0.16) ** 2)
                               * max((max(l["T"] for l in legs)), 1e-9)
                               + ind.get("rv20", 0.16)
                               * math.sqrt(max(max(l["T"] for l in legs), 1e-9))
                               * rng.standard_normal(4000))
            pnl = np.zeros(len(ST))
            for l in legs:
                px_T = np.maximum(0.0, (ST - l["strike"]) if l["right"].value == "CALL"
                                  else (l["strike"] - ST))
                pnl += (px_T - l["entry"]) * l["qm"] if l["qm"] != 0 else 0.0
            pop = {"prob": round(float((pnl > 0).mean()), 4),
                   "expect": round(float(pnl.mean())),
                   "p5": round(float(np.percentile(pnl, 5))),
                   "p95": round(float(np.percentile(pnl, 95)))}

    # ---- 参数平原（回测产出 36 组）
    plain = None
    sg = DATA / "sensitivity_grid.csv"
    if sg.exists():
        g = pd.read_csv(sg)
        plain = {"deltas": sorted(g["delta"].unique()),
                 "stops": sorted(g["stop"].unique()),
                 "cells": [{"d": float(r["delta"]), "s": float(r["stop"]),
                            "e": int(r["exit_dte"]), "ret": round(float(r["总收益%"]), 2),
                            "sharpe": None if pd.isna(r["夏普"]) else round(float(r["夏普"]), 2)}
                           for _, r in g.iterrows()]}

    # T 型报价（最近月，ATM ±6 档）
    near_exp = feed._next_expiry(day)
    tdf = chain_df[chain_df["expiry"] == near_exp].copy() if near_exp else chain_df
    tdf = tdf[tdf["iv"].notna()]
    if not tdf.empty:
        atm_k = tdf["strike"].iloc[int((tdf["strike"] - spot).abs().values.argmin())]
        strikes = sorted(tdf["strike"].unique())
        i0 = strikes.index(atm_k)
        tdf = tdf[tdf["strike"].isin(strikes[max(0, i0 - 6): i0 + 7])]
    t_rows = []
    if not tdf.empty:
        from thetalab.engine.broker import Broker as _B
        _bk = _B()
        for r in tdf.to_dict(orient="records"):
            row = feed._build_chain(day, day_risk).get(r["contract_id"])
            if row:
                r["margin_per_lot"] = round(_bk._margin_per_lot(row.instrument, row), 0)
            t_rows.append(r)
    t_type = t_rows

    return (day, spot, ind, sigs, recs, payoff, account, equity_curve, t_type,
            surf, attr, stress, pop, plain, paper_curve_out)


def main():
    (day, spot, ind, sigs, recs, payoff, account, equity_curve, t_type,
     surf, attr, stress, pop, plain, paper_curve_out) = assemble()
    payload = {
        "build_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "server_started": None,
        "version": __import__("thetalab").__version__,
        "day": str(day), "underlying": UNDERLYING, "spot": spot,
        "ind": {k: (round(v, 4) if isinstance(v, float) and v == v else v)
                for k, v in ind.items() if isinstance(v, (int, float, str))},
        "signals": [{"kind": s.kind, "name": s.name, "strength": s.strength,
                     "level": s.level, "reason": s.reason, "action": s.action}
                    for s in sigs],
        "recommendations": [{"id": r.template_id, "name": r.name, "score": r.score,
                             "reasons": r.reasons, "risks": r.risks,
                             "exit": r.exit_plan, "perm": r.needs_permission_note}
                            for r in recs],
        "payoff": payoff, "account": account,
        "equity": [[d, v] for d, v in equity_curve],
        "iv_series": [[str(d), round(float(v), 4)]
                      for d, v in pd.read_csv(DATA / "atm_iv_series.csv",
                                              parse_dates=["date"]).head(0).iterrows()] or None,
        "chain": t_type,
        "iv_surface": surf, "attribution": attr, "stress": stress,
        "pop": pop, "plain": plain, "paper_curve": paper_curve_out,
    }
    # IV 曲线（近一年，隔日采样）
    ivs = pd.read_csv(DATA / "atm_iv_series.csv", parse_dates=["date"])
    payload["iv_series"] = [[str(d.date()), round(float(v), 4)]
                            for d, v in zip(ivs["date"], ivs["atm_iv"])][::2]

    html = HTML.replace("__DATA__", json.dumps(payload, ensure_ascii=False, default=str))
    # 剥离模板防呆浮层（仅模板直接打开时显示）
    html = re.sub(r"<!--TPLGUARD-->.*?<!--/TPLGUARD-->", "", html, flags=re.S)
    out = DATA / "dashboard.html"
    out.write_text(html, encoding="utf-8")
    print(f"工作台已生成: {out.resolve()}  ({out.stat().st_size/1024:.0f} KB)")
    print(f"数据日: {day} | 标的 {UNDERLYING} 收盘 {spot} | 信号 {len(sigs)} 条 | "
          f"推荐 {len(recs)} 张 | 净值点 {len(equity_curve)}")


HTML = open(Path(__file__).parent / "dashboard_template.html", encoding="utf-8").read()


if __name__ == "__main__":
    main()
