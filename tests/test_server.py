"""
thetalab.tests.test_server — 本地交易服务器端到端闭环测试（Workbench 逻辑层，不起 HTTP）
运行：python -m thetalab.tests.test_server

流程：挂单 → 拒单校验 → 确认 → 推进交易日撮合 → 持仓出现 → 平仓挂单 → 确认 → 推进 → 平仓成交
"""
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from thetalab.core.models import Right
from thetalab.engine.paper import PaperTradingRunner
from thetalab.server import Workbench


def make_wb(tmp: str) -> Workbench:
    """真实 Workbench（与 server.main 同路径），仅 db 指向临时目录"""
    wb = Workbench(data_dir=Path("thetalab_data"), auto_update=False)  # 测试关调度线程（晚间窗口会真探测真采集）
    from thetalab.data.persist import StateStore
    wb.store = StateStore(Path(tmp) / "paper.db")
    wb.paper = PaperTradingRunner(wb.feed, wb.store, data_dir=tmp,
                                  extra_rows_fn=wb._szse_market_rows)
    wb.cursor = wb.days[-6]  # 回退到可推进的日期（交易闭环测试需要 advance 空间）
    return wb


def test_full_trading_cycle():
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    day = wb.cursor
    # 选一个流动性好的虚值合约（成交量最大的 put）
    st = wb.state()
    chain = [r for r in st["chain"] if r["right"] == "PUT" and r["last"] and r["volume"] > 500 and r["dte"] > 3]
    chain.sort(key=lambda r: -r["volume"])
    assert chain, "应有可交易合约"
    sym = chain[0]["contract_id"]
    per_lot_margin = chain[0]["margin_per_lot"]

    # 1) 挂单：卖出开仓 5 张
    r = wb.place_order(sym, "SELL", "OPEN", 5)
    assert r["ok"], r
    # 重复键覆盖：同 key 再挂会被 INSERT OR REPLACE
    # 2) 拒单：数量 0
    assert not wb.place_order(sym, "SELL", "OPEN", 0)["ok"]
    # 3) 拒单：无持仓平仓
    assert not wb.place_order(sym, "BUY", "CLOSE", 1)["ok"]
    # 4) 待确认单存在
    pend = wb.store.pending(status="PENDING")
    assert any(p["symbol"] == sym for p in pend)
    # 5) 确认
    r = wb.confirm()
    assert r["confirmed"] >= 1
    # 6) 推进交易日 → 成交
    r = wb.advance()
    assert r["ok"], r
    acct = wb.store.load_account()
    assert sym in acct.positions and acct.positions[sym].net_qty == -5
    # 保证金已冻结。注意：state 的 per_lot_margin 是开仓口径(8-21)，成交日盯市为
    # 维持保证金(8-24)——跨日跨口径，只做量级断言
    assert 0 < acct.margin_used < 200000, acct.margin_used
    # 7) 平仓：买入平仓 5 张 → 挂单 → 确认 → 推进
    r = wb.place_order(sym, "BUY", "CLOSE", 5)
    assert r["ok"], r
    wb.confirm()
    r = wb.advance()
    assert r["ok"], r
    acct = wb.store.load_account()
    assert sym not in acct.positions, "平仓后持仓应消失"
    assert wb.store.pending(status="PENDING") == []
    # 8) set_day 回放
    r = wb.set_day(str(wb.days[0]))
    assert r["ok"]


def test_multi_underlying():
    """品种切换：510300 可交易；其他品种浏览链、下单被明确拒绝；512100 无场内期权"""
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    r = wb.set_underlying("510500")
    assert r["ok"] and r["has_daily_bars"] is True and r["collected"] is True  # 多品种撮合已开放
    st = wb.state()
    assert st["underlying"] == "510500" and st["has_daily_bars"] is True
    assert st["chain"], "510500 应有 IV/Greeks 链"
    assert sum(1 for r in st["chain"] if r["iv"] is not None) > 20  # 深虚值端无IV属正常
    # 多品种撮合已开放：510500 有行情的合约可正常挂单（流动性闸门仍生效）
    liquid = [r0 for r0 in st["chain"] if r0["last"] and r0["volume"] > 500]
    assert liquid, "510500 应有流动性达标合约"
    r2 = wb.place_order(liquid[0]["contract_id"], "SELL", "OPEN", 1)
    assert r2["ok"], r2
    r3 = wb.set_underlying("510300")
    assert r3["has_daily_bars"] is True
    r4 = wb.set_underlying("512100")
    assert not r4["ok"]  # 512100 无场内期权且已从品种列表移除（用户指定换为 510050）
    r5 = wb.set_underlying("999999")
    assert not r5["ok"]


def test_cancel_all():
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    wb.set_underlying("510300")
    st = wb.state()
    cands = [r for r in st["chain"] if r["right"]=="PUT" and r["last"] and r["volume"]>500 and r["dte"]>3]
    cands.sort(key=lambda r:-r["volume"])
    sym = cands[0]["contract_id"]
    assert wb.place_order(sym,"SELL","OPEN",1)["ok"]
    wb.place_order(sym,"SELL","OPEN",1)  # 同 key 覆盖 → 仍 1 条
    assert len(wb.store.pending(status="PENDING")) >= 1
    r = wb.cancel_all()
    assert r["cancelled"] >= 1
    assert wb.store.pending(status="PENDING") == []


def test_multi_underlying_trading():
    """多品种撮合端到端：510050 卖开 → 确认 → 推进成交 → 盯市保证金按 510050 标的 → 平仓成交"""
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    wb.set_underlying("510050")
    st = wb.state()
    cands = [r for r in st["chain"] if r["right"]=="PUT" and r["last"] and r["volume"]>500 and r["dte"]>3]
    cands.sort(key=lambda r:-r["volume"])
    sym = cands[0]["contract_id"]
    mp_ref = cands[0]["margin_per_lot"]
    assert wb.place_order(sym, "SELL", "OPEN", 5)["ok"]
    wb.confirm()
    r = wb.advance(); assert r["ok"], r
    acct = wb.store.load_account()
    assert sym in acct.positions and acct.positions[sym].net_qty == -5
    # 盯市保证金按 510050 标的现算（非零，且与权益量级合理）
    assert 0 < acct.margin_used < acct.equity * 0.5, acct.margin_used
    # 平仓闭环
    assert wb.place_order(sym, "BUY", "CLOSE", 5)["ok"]
    wb.confirm(); r = wb.advance(); assert r["ok"], r
    acct = wb.store.load_account()
    assert sym not in acct.positions


def test_szse_159915_trading():
    """深市 159915 撮合闭环：快照链挂单 → 确认 → 推进成交 → 义务仓保证金"""
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    wb.set_underlying("159915")
    st = wb.state()
    cands = [x for x in st["chain"] if x["last"] and x["iv"] and x["right"] == "PUT"]
    assert cands, "159915 快照链应有可交易合约"
    cands.sort(key=lambda x: abs(x["delta"] + 0.5))
    sym = cands[0]["contract_id"]
    wb.SZSE_MIN_DAYS = 1
    assert wb.place_order(sym, "SELL", "OPEN", 2)["ok"]
    wb.confirm()
    r = wb.advance()
    fills = [f for f in r["fills"] if f.get("type") == "成交"]
    assert fills and fills[0]["symbol"] == sym, r["fills"]
    acct = wb.store.load_account()
    pos = acct.positions.get(sym)
    assert pos and pos.net_qty == -2 and pos.margin > 0


def test_state_shape():
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    st = wb.state()
    for k in ("cursor", "spot", "chain", "expiries", "days", "pending", "account"):
        assert k in st, k
    assert all("margin_per_lot" in r for r in st["chain"])
    assert len(st["chain"]) > 50


def _day_with_full_dailies(wb):
    """逐合约日线齐全的最近交易日（最近 1~2 日常因新浪发布滞后走快照兜底，会掩盖 spot 错误）"""
    for d in reversed(wb.days):
        if len(wb.feed._build_chain(d, wb.feed.risk_by_day[d])) > 300:
            return d
    raise AssertionError("找不到逐合约日线齐全的交易日")


def _sell_open_fills(wb, chain, n=6):
    """非默认品种若干合约的卖开成交价——价差模型对标的价失真极敏感，用作回归指纹"""
    from thetalab.core.models import Direction, Offset, Order
    syms = sorted(s for s, r in chain.items()
                  if r.instrument.underlying != "510300"
                  and r.close > 0.02 and r.volume > 300)
    assert syms, "应有非默认品种的可成交合约"
    out = {}
    for s in syms[:n]:
        r = chain[s]
        o = Order(instrument=r.instrument, direction=Direction.SELL, offset=Offset.OPEN,
                  qty=1, strategy_id="t", reason="t")
        out[s] = wb.paper.broker._fill_price(o, r)
    return out


def test_reload_preserves_market_state():
    """热更新必须与冷启动完全等价。reload_data 曾只重建 feed，漏挂
    spot_fn/close_all/_oi/daily_unds：所有品种的标的价退化成 DEFAULT_UND(510300) 的价，
    虚值度失真把 510050 卖开成交价从 -1.7% 打到 -8.0%（实测 482/743 行 spot 错）。"""
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    day = _day_with_full_dailies(wb)
    snap = (list(wb.days), len(wb.close_all), sorted(map(str, wb._oi)),
            sorted(wb.daily_unds))
    chain0 = wb._full_chain(day)
    spot0 = {s: r.spot_close for s, r in chain0.items()}
    fills0 = _sell_open_fills(wb, chain0)

    wb.reload_data()

    assert wb.feed.spot_fn is not None, "reload 后 spot_fn 丢失（多品种标的价将全部退化）"
    assert (list(wb.days), len(wb.close_all), sorted(map(str, wb._oi)),
            sorted(wb.daily_unds)) == snap, "reload 后行情面派生状态与冷启动不一致"
    chain1 = wb._full_chain(day)
    assert set(chain1) == set(chain0), "reload 后链的合约集合变化"
    wrong = [s for s, r in chain1.items()
             if r.spot_close != r.spot_close or abs(r.spot_close - spot0[s]) > 1e-9]
    assert not wrong, f"reload 后 {len(wrong)} 行 spot_close 漂移，例 {wrong[:3]}"
    assert _sell_open_fills(wb, chain1) == fills0, "reload 前后卖开成交价必须一致"
    for u in ("588000", "510050", "510500"):
        truth = wb.close_all.get((u, day))
        got = wb.feed._spot(u, day)
        assert truth == truth and abs(got - truth) < 1e-9, f"{u} spot={got} 应为 {truth}"


def test_snapshot_fallback_official_first():
    """OI 快照兜底：只注入日线缺失的合约（官方口径优先），且索引化结果与暴力扫描等价"""
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    day = wb.days[-1]
    snap = wb._oi.get(day)
    if snap is None:
        return                                   # 当日无快照库，跳过（数据依赖）
    official = wb.feed._build_chain(day, wb.feed.risk_by_day[day])
    rows = wb._sse_snapshot_fallback(day, official)
    assert not (set(rows) & set(official)), "兜底不得覆盖已有官方日线价"

    # 等价性：索引查表 vs 逐行 DataFrame 布尔扫描（旧实现）
    risk_day = wb.feed.risk_by_day[day]
    brute = {}
    for r in snap.reset_index().itertuples(index=False):
        g = risk_day[risk_day["security_id"] == getattr(r, "security_id", None)]
        if g.empty:
            continue
        g = g.iloc[0]
        inst = wb.feed.spec.option(g["underlying"], Right[g["right"]], g["expiry"],
                                   float(g["strike"]))
        if inst.symbol in official:
            continue
        last = float(getattr(r, "last", float("nan")))
        if last != last or last <= 0:
            continue
        brute[inst.symbol] = last
    assert set(brute) == set(rows), f"索引化漏/多合约：{len(brute)} vs {len(rows)}"
    assert all(abs(brute[s] - rows[s].close) < 1e-12 for s in brute), "兜底价格与旧实现不一致"

    # 当日无 risk 表 → 空返回（此前每行抛 TypeError 被静默吞掉）
    wb.feed.risk_by_day.pop(day, None)
    wb._idx_cache.clear()
    assert wb._sse_snapshot_fallback(day, {}) == {}


def test_state_honest_when_spot_missing():
    """标的收盘缺失时不得用 spot=1.0 兜底算 Greeks/保证金——那会展示量级全错的数字"""
    tmp = tempfile.mkdtemp()
    wb = make_wb(tmp)
    wb.set_underlying("510300")
    day = wb.cursor
    keep = wb.close_all.pop(("510300", day))
    try:
        st = wb.state("510300")
    finally:
        wb.close_all[("510300", day)] = keep
    assert st["spot"] is None, "标的收盘缺失应如实返回 None"
    assert st["chain"], "链本身仍应展示（IV 来自交易所风险指标）"
    assert all(r["delta"] is None for r in st["chain"]), "spot 缺失时 Delta 必须留空"
    assert all(r["margin_per_lot"] is None for r in st["chain"]), "spot 缺失时保证金必须留空"


def test_data_dir_accepts_str():
    """data_dir 传字符串路径也应可用（脚本/测试常这么传，此前直接 TypeError）"""
    wb = Workbench(data_dir="thetalab_data", auto_update=False)
    assert isinstance(wb.data_dir, Path) and len(wb.days) > 100


ALL_TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_")]

if __name__ == "__main__":
    failed = 0
    for fn in ALL_TESTS:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {fn.__name__}: {e}")
        except Exception as e:
            failed += 1
            import traceback; traceback.print_exc()
            print(f"ERROR {fn.__name__}: {type(e).__name__} {e}")
    print(f"\n{len(ALL_TESTS) - failed}/{len(ALL_TESTS)} passed")
    sys.exit(1 if failed else 0)
