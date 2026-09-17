"""
thetalab.tests.test_collect — 逐合约日线补缺落库逻辑（离线，不联网）

盯住 2026-09 连环定位的三个坑：
  1) contract_id 用 RangeIndex 构造却赋给保留原历史索引的切片 -> 整列 NaN ->
     被 drop_duplicates(["trade_date","contract_id"]) 折叠（NaN 互为相等）->
     补缺 600+ 合约只落 1 行且零报错（表现为「当日无该合约行情」/持仓不盯市）
  2) 上游限流时返回「空表」，与「该日尚未发布」被混成一类 -> 误判成发布滞后
  3) provider 的 _throttle 靠 self._last_call，跨线程共享实例时限流失效
运行：python -m thetalab.tests.test_collect
"""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from thetalab.scripts.collect_daily import (DAILY_INTERVAL, DAILY_WORKERS,
                                            DAY_BUDGET, RETRY_ROUNDS,
                                            RETRY_SLEEP, SOCKET_TIMEOUT,
                                            _thread_provider, build_daily_row)

TGT = date(2026, 9, 7)
TGT_S = str(TGT)


def fake_hist(n=118, end=TGT):
    """模拟 SseOptionProvider.contract_daily 的返回：全历史，末行日期=end"""
    dates = [end - timedelta(days=i) for i in range(n - 1, -1, -1)]
    return pd.DataFrame({"date": dates, "open": 1.0, "high": 1.0, "low": 1.0,
                         "close": 0.3686, "volume": 1000.0, "volume_lots": 0.1,
                         "volume_unit": "lots"})


def maps(sids):
    """日级映射 (trade_date, security_id)->contract_id ＋ security_id 全局众数兜底"""
    return ({(TGT, s): f"C_{s}" for s in sids},
            pd.Series({s: f"G_{s}" for s in sids}))


def test_contract_id_survives_offset_slice_index():
    """缺口日在历史中段（索引 != 0）时 contract_id 必须解析成功，不得为 NaN"""
    hist = fake_hist()
    assert hist[hist["date"].astype(str) == TGT_S].index.tolist() == [117]
    m_day, m_glob = maps(["10011255"])
    row = build_daily_row(hist, TGT_S, "10011255", "510050", m_day, m_glob)
    assert row is not None
    assert row["contract_id"].notna().all(), row["contract_id"].tolist()
    assert row["contract_id"].iloc[0] == "C_10011255"
    assert row["trade_date"].iloc[0] == TGT
    assert str(row["date"].iloc[0]) == TGT_S


def test_rows_not_collapsed_by_dedupe():
    """多合约补缺行并入库存后行数必须按合约数保留（NaN 主键会被折叠成 1 行）"""
    sids = [f"1001{i:04d}" for i in range(5)]
    m_day, m_glob = maps(sids)
    rows = [build_daily_row(fake_hist(), TGT_S, s, "510050", m_day, m_glob) for s in sids]
    assert all(r is not None for r in rows)
    inc = pd.concat(rows, ignore_index=True)
    assert inc["contract_id"].notna().all()
    store = pd.DataFrame({"trade_date": [TGT], "contract_id": ["OLD"], "close": [1.0]})
    merged = pd.concat([store, inc[["trade_date", "contract_id", "close"]]],
                       ignore_index=True).drop_duplicates(
        subset=["trade_date", "contract_id"], keep="last")
    assert len(merged) == 1 + len(sids), f"补缺行被折叠：{len(merged)}"


def test_missing_day_and_fallback_chain():
    """上游无该日 -> None；日级映射缺失退全局众数；两者皆缺退 security_id"""
    other = str(TGT + timedelta(days=1))
    m_day, m_glob = maps(["10011255", "99999999"])
    m_day.pop((TGT, "99999999"), None)          # 只留全局众数映射
    assert build_daily_row(fake_hist(), other, "10011255", "510050",
                           m_day, m_glob) is None
    assert build_daily_row(pd.DataFrame(), TGT_S, "10011255", "510050",
                           m_day, m_glob) is None
    assert build_daily_row(fake_hist(), TGT_S, "99999999", "510050",
                           m_day, m_glob)["contract_id"].iloc[0] == "G_99999999"
    r = build_daily_row(fake_hist(), TGT_S, "88888888", "510050", {},
                        pd.Series(dtype=object))
    assert r["contract_id"].iloc[0] == "88888888"


def test_thread_provider_is_per_thread():
    """限流靠 self._last_call：跨线程共享实例会竞态失效，必须每线程一个实例"""
    import threading
    res = {}

    def work(tag):
        first = _thread_provider(DAILY_INTERVAL)
        res[tag] = (first, first is _thread_provider(DAILY_INTERVAL))

    ts = [threading.Thread(target=work, args=(f"t{i}",)) for i in range(3)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(res) == 3 and all(same for _, same in res.values()), "同线程未复用实例"
    # 必须持有引用再比 id：线程结束实例被 GC，地址复用会造成假性相同
    assert len({id(v[0]) for v in res.values()}) == 3, "线程间共享了 provider"
    p = _thread_provider(DAILY_INTERVAL)
    assert p is _thread_provider(DAILY_INTERVAL)
    assert p is not _thread_provider(DAILY_INTERVAL + 0.1), "min_interval 变更须换新实例"


def test_upstream_hygiene_guards():
    """并发/退避/超时参数是踩出来的，改回去就会重现事故（限流封禁、永久挂起）"""
    assert DAILY_WORKERS / DAILY_INTERVAL <= 10, "聚合请求速率超上游阈值（曾触发 sina 拒连）"
    assert RETRY_ROUNDS >= 2, "上游空响应必须重试，否则会误报「该日未发布」"
    assert SOCKET_TIMEOUT > 0, "akshare 不带 timeout，必须靠进程级 socket 超时兜底"
    assert DAY_BUDGET > RETRY_SLEEP, "时间预算须大于单次退避，否则一轮也跑不完"


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