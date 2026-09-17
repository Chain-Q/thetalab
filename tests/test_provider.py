"""
thetalab.tests.test_provider — 新浪快照解析与批量取数（离线：_sina_get 被打桩，不联网）

覆盖：期权字段序与 akshare 逐合约接口一致 ｜ 批量响应多行解析/空串跳过/分批
     ｜ 标的 ETF 单请求解析与沪深前缀选择 ｜ 3s 缓存生效
运行：python -m thetalab.tests.test_provider
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from thetalab.data import provider as P

Q = chr(34)

# 真实响应样本（2026-09-10 盘中，510300C2609M04000 / security_id=10011271，51 字段）
OPT_FIELDS = ("1,0.6098,0.6149,0.6130,2,1318,-3.47,4.0000,0.6320,0.5965,1.1007,0.1733,"
              "0.6290,1,0.6285,1,0.6197,1,0.6164,1,0.6130,2,0.6098,1,0.6074,1,0.5991,1,"
              "0.5986,1,0.5954,1,2026-09-10 14:28:15,0,E 00,EBS,510300,300ETF\u8d2d9\u67084000,"
              "6.25,0.6360,0.5965,120,739435.00,M,0.6370,C,2026-09-23,13,2,0.617,-0.0021")


def opt_line(sid, fields=OPT_FIELDS):
    return f"var hq_str_CON_OP_{sid}={Q}{fields}{Q};"


def etf_line(sym, name="\u6caa\u6df1300ETF", open_=4.617, pre=4.637, last=4.617,
             high=4.640, low=4.599, vol=438800000, amt=2026000000,
             d="2026-09-10", t="15:34:59"):
    f = [name, open_, pre, last, high, low, last - 0.001, last + 0.001, vol, amt]
    f += [100, last - 0.001, 200, last - 0.002, 300, last - 0.003,
          400, last - 0.004, 500, last - 0.005,
          100, last + 0.001, 200, last + 0.002, 300, last + 0.003,
          400, last + 0.004, 500, last + 0.005]
    f += [d, t, "00", "00"]
    return f"var hq_str_{sym}={Q}" + ",".join(str(x) for x in f) + f"{Q};"


def _stub(monkey_body):
    orig = P._sina_get
    calls = []

    def fake(symbols, timeout=10.0):
        calls.append(list(symbols))
        return monkey_body(symbols)

    P._sina_get = fake
    return orig, calls


def test_sina_option_field_mapping():
    """批量解析的字段序必须与 akshare option_sse_spot_price_sina 逐位一致"""
    kv = dict(zip(P._SINA_OPT_FIELDS, OPT_FIELDS.split(",")))
    d = P._sina_option_dict(kv, "10011271")
    assert len(P._SINA_OPT_FIELDS) == 43, "akshare 只映射前 43 位，改动须同步核对"
    assert d["security_id"] == "10011271"
    assert d["last"] == 0.6149 and d["bid"] == 0.6098 and d["ask"] == 0.6130
    assert d["bid_vol"] == 1.0 and d["ask_vol"] == 2.0
    assert d["open_interest"] == 1318.0 and d["volume"] == 120.0
    assert d["pre_close"] == 0.6320 and d["open"] == 0.5965
    assert d["high"] == 0.6360 and d["low"] == 0.5965
    assert d["limit_up"] == 1.1007 and d["limit_down"] == 0.1733
    assert d["underlying"] == "510300" and d["short_name"].endswith("4000")
    assert d["quote_time"] == "2026-09-10 14:28:15"
    # 成交量(张)×最新价×合约单位 ≈ 成交额(元)——单位口径自证
    assert abs(d["volume"] * d["last"] * 10000 - 739435.0) / 739435.0 < 0.02


def test_contract_spot_batch_parses_and_skips_empty():
    """多行响应逐条入库；空串（无效/已摘牌合约）跳过"""
    body = "\n".join([opt_line("10011271"),
                      "var hq_str_CON_OP_99999999=" + Q + Q + ";",
                      opt_line("10011272", OPT_FIELDS.replace("0.6149", "0.5218", 1)),
                      "", "garbage line"])
    orig, calls = _stub(lambda syms: body)
    try:
        p = P.SseOptionProvider(min_interval=0.0)
        out = p.contract_spot_batch(["10011271", "99999999", "10011272"])
    finally:
        P._sina_get = orig
    assert len(calls) == 1 and calls[0][0].startswith("CON_OP_")
    assert set(out) == {"10011271", "10011272"}, out.keys()
    assert out["10011271"]["last"] == 0.6149
    assert out["10011272"]["last"] == 0.5218


def test_contract_spot_batch_chunks():
    """超过 SINA_BATCH_CHUNK 分批请求，且每批都走一次限流"""
    sids = [str(10000000 + i) for i in range(137)]
    orig, calls = _stub(lambda syms: "\n".join(
        opt_line(s.split("CON_OP_")[-1]) for s in syms))
    try:
        p = P.SseOptionProvider(min_interval=0.0)
        out = p.contract_spot_batch(sids)
    finally:
        P._sina_get = orig
    step = P.SseOptionProvider.SINA_BATCH_CHUNK
    assert len(calls) == -(-len(sids) // step) == 3, calls
    assert all(len(c) <= step for c in calls)
    assert len(out) == len(sids)


def test_underlying_spot_sina_parses_and_prefixes():
    """标的快照：沪市 sh 前缀 / 深市(1 开头) sz 前缀；最新价取字段 3、昨收取字段 2"""
    seen = {}

    def body(syms):
        seen["syms"] = syms
        return etf_line(syms[0], last=7.742, pre=7.785)

    orig, calls = _stub(body)
    try:
        p = P.SseOptionProvider(min_interval=0.0)
        d = p.underlying_spot_sina("510500")
        assert calls[-1] == ["sh510500"], calls
        assert d["last"] == 7.742 and d["pre_close"] == 7.785
        assert d["quote_time"] == "2026-09-10 15:34:59"
        # 3s 缓存：同品种再取不产生新请求
        n = len(calls)
        p.underlying_spot_sina("510500")
        assert len(calls) == n, "3s 内应命中缓存"
        d2 = p.underlying_spot_sina("159915")
        assert calls[-1] == ["sz159915"], "深市品种须用 sz 前缀"
        assert d2["last"] == d2["last"]
    finally:
        P._sina_get = orig


def test_underlying_spot_sina_failure_is_nan():
    """响应异常/字段不足 → last=NaN（调用方退化用收盘价定 ATM，不得抛）"""
    orig, calls = _stub(lambda syms: "var hq_str_sh510300=" + Q + Q + ";")
    try:
        p = P.SseOptionProvider(min_interval=0.0)
        d = p.underlying_spot_sina("510300")
        assert d["last"] != d["last"], d
    finally:
        P._sina_get = orig


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
