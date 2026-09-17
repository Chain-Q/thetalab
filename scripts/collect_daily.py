"""
thetalab.scripts.collect_daily — 每日收盘后统一采集入口（设计文档 §5.2 日频批处理）

用途：server 晚间调度自动调用，或手动运行（即「每日更新.bat」）。做五件事：
    1. 当日 risk_indicators（IV/Greeks，全市场一次调用）
    2. 标的日线增量（510300）
    3. 逐合约 OI/量 快照（自建历史 OI 库的唯一来源——sina/交易所均无历史逐合约 OI）
    4. 沪市逐合约日线日级补缺（新浪发布滞后；缺日会让撮合链为空 → 下单被拒/持仓不盯市）
    5. 逐合约 OI 快照落库后重算 ATM IV 序列
页面重建不在本脚本内：「每日更新.bat」第 2 步另行调用 build_dashboard.py。
运行：python -m thetalab.scripts.collect_daily [date=今天]
"""
import sys
import threading as _th
import time
import warnings
import socket
from datetime import date
from pathlib import Path

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# import 兜底（与 server.py 同因）：本机偶发 "thetalab" 默认查找失效，
# 显式 spec 加载注册进 sys.modules 后再 from thetalab.* 即可命中。
def _ensure_thetalab_importable():
    import importlib.util
    if "thetalab" in sys.modules:
        return
    _parent = str(Path(__file__).resolve().parents[2])
    _pkg = _parent + "/thetalab"
    if Path(_pkg).is_dir():
        try:
            _spec = importlib.util.spec_from_file_location(
                "thetalab", Path(_pkg) / "__init__.py",
                submodule_search_locations=[_pkg])
            if _spec and _spec.loader:
                _m = importlib.util.module_from_spec(_spec)
                _spec.loader.exec_module(_m)
                sys.modules["thetalab"] = _m
        except Exception:
            pass

_ensure_thetalab_importable()

import pandas as pd

from thetalab.data.provider import ParquetStore, SseOptionProvider
from thetalab.scripts.collect_contract_history import build_contract_id_map
from thetalab.scripts.collect_risk_history import UNDERLYING

DAILY_WORKERS = 3       # 逐合约请求并发度（5 线程实测 ≈14 请求/秒 → sina 拒连）
DAILY_INTERVAL = 0.5
DAY_BUDGET = 420.0
SOCKET_TIMEOUT = 20.0
RETRY_ROUNDS = 3      # 上游空响应/拒连时的重试轮数
RETRY_SLEEP = 75.0    # 轮间退避（秒）：sina 限流通常一两分钟解除    # 每线程限流：3 线程 ≈ 6 请求/秒


def self_recent_days(day: date, n: int):
    """含 day 在内的往前 n 个自然日（日级覆盖检查用）"""
    from datetime import timedelta
    return [day - timedelta(days=i) for i in range(n)]


def _thread_provider(min_interval: float = 0.3) -> SseOptionProvider:
    """当前线程专属 provider（各线程独立限流）。
    共享实例的 _throttle 靠 self._last_call，跨线程竞态会让限流失效；
    而"每次调用新建实例"同样等于不限流（_last_call 恒 0）——两者都不对。"""
    import threading
    tls = getattr(_thread_provider, "_tls", None)
    if tls is None:
        tls = _thread_provider._tls = threading.local()
    p = getattr(tls, "p", None)
    if p is None or p.min_interval != min_interval:
        p = SseOptionProvider(min_interval=min_interval)
        tls.p = p
    return p


def build_daily_row(hist, tgt: str, sid: str, und: str, m_day, m_glob):
    """从某合约的全历史里取出缺口日 tgt 那一行并补齐落库列（返回 None=该日无数据）。

    索引必须显式对齐：pd.Series([...]) 默认 RangeIndex(0..n-1)，而缺口日切片保留
    原历史索引（常为 100+），随后 day["contract_id"] = <Series> 按标签对齐会把整列
    写成 NaN；NaN 之间在 drop_duplicates(["trade_date","contract_id"]) 里视为相等，
    于是整天 600+ 行折叠成 1 行且不报任何错。2026-09-11 定位：9-07~9-10 补缺全丢，
    表现为「当日无该合约行情」下单被拒、持仓推进后不盯市（此前误判为新浪发布滞后）。
    """
    if hist is None or hist.empty:
        return None
    day = hist[hist["date"].astype(str).str[:10] == tgt]
    if day.empty:
        return None                          # 上游尚未发布该日
    day = day.copy()
    day["security_id"] = str(sid)
    day["underlying"] = str(und)
    day["trade_date"] = day["date"]
    key = list(zip(day["trade_date"], day["security_id"]))
    cid = pd.Series([m_day.get(k) for k in key], index=day.index)
    cid = cid.fillna(day["security_id"].map(m_glob))
    day["contract_id"] = cid.fillna(day["security_id"])
    if day["contract_id"].isna().any():
        return None                          # 宁可缺该行，也不写 NaN 主键（会被折叠）
    return day


def main(day: date = None):
    day = day or date.today()
    t0 = time.time()
    store = ParquetStore("thetalab_data/store")
    p = SseOptionProvider(min_interval=0.3)
    log = []

    # 1) 风险指标（收盘后交易所发布有延迟：重试 3 次每次隔 60s；实测发布时点约 19:30~21:00+，更稳的是让 server 晚间调度盯发布）
    df = pd.DataFrame()
    for attempt in range(3):
        try:
            df = p.risk_indicators(day)
            if not df.empty:
                break
        except Exception:
            pass
        if attempt < 2:
            log.append(f"risk_indicators 第{attempt+1}次无数据，60s 后重试（交易所发布延迟约 19:30~21:00+）")
            time.sleep(60)
    try:
        if df.empty:
            log.append(f"risk_indicators: {day} 数据未发布（非交易日或发布延迟），跳过")
        else:
            n = store.write("risk_indicators", df)
            log.append(f"risk_indicators: +{n} 行")
    except Exception as e:
        log.append(f"risk_indicators FAIL: {type(e).__name__} {str(e)[:50]}")

    # 2) 标的日线（全品种增量）
    try:
        daily_all = pd.read_parquet(store.root / "contract_daily" / "all.parquet")
        d_min = daily_all["trade_date"].min()
        ud_path = store.root / "underlying_daily" / "all.parquet"
        ud_old = pd.read_parquet(ud_path) if ud_path.exists() else pd.DataFrame()
        frames = [ud_old] if len(ud_old) else []
        for und in ("510300", "510050", "510500", "588000", "588080", "159915"):
            try:
                u = p.underlying_daily(und, d_min, day)
                u["underlying"] = und
                frames.append(u)
            except Exception as e:
                log.append(f"标的日线 {und} FAIL: {type(e).__name__}")
        ud = pd.concat(frames, ignore_index=True)             .drop_duplicates(subset=["underlying", "date"], keep="last")
        ud.to_parquet(ud_path, index=False)
        log.append(f"underlying_daily: {len(ud)} 行 / {ud['underlying'].nunique()} 品种")
    except Exception as e:
        log.append(f"underlying_daily FAIL: {type(e).__name__} {str(e)[:60]}")

    # 2b) 深市静态快照（159915 等：前结算/OI/涨跌停/合约调整）
    try:
        import akshare as _ak
        df = _ak.option_current_day_szse()
        ren = {"合约代码": "contract_id", "行权价": "strike", "合约单位": "multiplier",
               "前结算价": "pre_settle", "合约总持仓": "oi", "涨停价格": "limit_up",
               "跌停价格": "limit_down", "到期日": "expiry", "合约类型": "right_cn",
               "合约调整": "adjusted", "标的证券简称(代码)": "und_name"}
        g = df.rename(columns=ren)
        g["trade_date"] = g["交易日期"]
        g["underlying"] = g["contract_id"].astype(str).str[:6]
        g["adjusted"] = g["adjusted"].astype(str) == "是"
        g["right"] = g["right_cn"].map(lambda x: "CALL" if "购" in str(x) else "PUT")
        g["security_id"] = g["合约编码"]
        out = store.root / "snapshots_szse"
        out.mkdir(parents=True, exist_ok=True)
        snap_day = str(g["trade_date"].iloc[0]).replace("-", "")
        f = out / f"{snap_day[:6]}.parquet"
        g.to_parquet(f, index=False)
        log.append(f"深市快照: {len(g)} 行（{snap_day}）")
    except Exception as e:
        log.append(f"深市快照 FAIL: {type(e).__name__} {str(e)[:60]}")

    # 2c) 沪市逐合约日线日级补缺：新浪逐合约日线的发布滞后实测可达 4 个交易日，
    # 而 collect_contract_history 按 security_id 断点、采过即永久跳过 -> 缺口会永久
    # 留存，表现为「当日无该合约行情」下单被拒、持仓推进后不盯市。
    # 交易日基准 = risk 并 underlying_daily（服务器停机的夜晚 risk 整日缺失也不漏）；
    # 缺日先补 risk 再补逐合约日线。**每个合约只请求一次全历史**，一次切出所有缺口日
    # ——逐日各拉一遍会把同一合约重复请求 N 次，实测触发 sina 限流封禁。
    try:
        socket.setdefaulttimeout(SOCKET_TIMEOUT)   # akshare 的 requests 不带 timeout
        daily_all = pd.read_parquet(store.root / "contract_daily" / "all.parquet")
        daily_all["_d"] = daily_all["trade_date"].astype(str).str[:10]
        cnt = daily_all.groupby("_d").size().to_dict()
        risk_df = store.read("risk_indicators")
        risk_days = set(risk_df["trade_date"].astype(str).str[:10].unique())
        udl_days = set(pd.read_parquet(store.root / "underlying_daily" / "all.parquet")
                       ["date"].astype(str).str[:10].unique())
        sse = risk_df[risk_df["underlying"] != "159915"]
        rcnt = sse.groupby(sse["trade_date"].astype(str).str[:10]).size().to_dict()
        base = max(rcnt.values()) if rcnt else 600
        missing_days = [d for d in sorted(risk_days | udl_days)[-8:]
                        if d <= str(day) and cnt.get(d, 0) < 0.8 * rcnt.get(d, base)][:5]
        if not missing_days:
            log.append("逐合约日线日级覆盖完整（近 8 交易日）")
        else:
            log.append(f"沪市逐合约日线缺日: {missing_days}，按合约一次性补拉")
            for tgt in missing_days:            # risk 缺日先补（交易所历史可回拉）
                if tgt in risk_days:
                    continue
                try:
                    rd = p.risk_indicators(date.fromisoformat(tgt))
                    if not rd.empty:
                        store.write("risk_indicators", rd)
                        risk_df = pd.concat([risk_df, rd], ignore_index=True)
                        log.append(f"  补 {tgt} 风险指标: +{len(rd)} 行")
                except Exception as e:
                    log.append(f"  补 {tgt} 风险指标 FAIL: {type(e).__name__}")
            risk_df = risk_df.copy()
            risk_df["_d"] = risk_df["trade_date"].astype(str).str[:10]
            m_day, m_glob = build_contract_id_map(risk_df)
            have = {t: set(daily_all[daily_all["_d"] == t]["security_id"]
                           .astype(str).unique()) for t in missing_days}
            uni, plan = {}, {}
            for tgt in missing_days:
                g = risk_df[(risk_df["_d"] == tgt) & (risk_df["underlying"] != "159915")]
                if g.empty:                      # risk 拉不到：用最近一天的合约清单兜底
                    g = risk_df[(risk_df["_d"] == max(risk_days)) &
                                (risk_df["underlying"] != "159915")]
                uni[tgt] = int(g["security_id"].nunique())
                for sid, und in g.drop_duplicates("security_id")[
                        ["security_id", "underlying"]].itertuples(index=False, name=None):
                    sid = str(sid)
                    if sid in have[tgt]:
                        continue
                    ent = plan.setdefault(sid, {"und": und, "days": []})
                    if tgt not in ent["days"]:
                        ent["days"].append(tgt)
            log.append(f"  待请求合约 {len(plan)} 个（一次请求覆盖 {len(missing_days)} 个缺口日）")
            from concurrent.futures import ThreadPoolExecutor, as_completed
            rows = {t: [] for t in missing_days}
            ok2 = fail2 = empty2 = skip2 = nodata2 = 0
            abort = _th.Event()
            t0 = time.time()

            def _fetch(job, _m_day=m_day, _m_glob=m_glob, _ev=abort):
                """一个合约只请求一次全历史，一次切出它所有缺口日的行。

                被上游限流的表现是"返回空表"或"直接拒连"，与"该日尚未发布"是两件
                不同的事：前者退避重试就能拿到数据，后者重试纯属浪费——所以分开计数，
                不再笼统报"源无"（2026-09-12 实测 674 合约全报源无、稍后重跑全到手）。
                """
                sid, ent = job
                hist, last = None, "nodata"
                for rnd in range(RETRY_ROUNDS):
                    if _ev.is_set():
                        return "skip", None
                    try:
                        hist = _thread_provider(DAILY_INTERVAL).contract_daily(sid)
                        last = "nodata"
                    except Exception:
                        hist, last = None, "fail"   # 网络异常/被拒连
                    if hist is not None and not hist.empty:
                        break
                    if rnd + 1 < RETRY_ROUNDS:
                        _ev.wait(RETRY_SLEEP)       # 可被中止打断的退避
                        _thread_provider(DAILY_INTERVAL)._daily_cache.pop(sid, None)
                        if _ev.is_set():
                            return "skip", None
                if hist is None or hist.empty:
                    return last, None
                got = {}
                for tgt in ent["days"]:
                    r = build_daily_row(hist, tgt, sid, ent["und"], _m_day, _m_glob)
                    if r is not None:
                        got[tgt] = r
                return ("ok", got) if got else ("pending", None)

            if plan:
                with ThreadPoolExecutor(max_workers=DAILY_WORKERS) as ex:
                    futs = [ex.submit(_fetch, a) for a in plan.items()]
                    done = 0
                    for fu in as_completed(futs):
                        tag, got = fu.result()
                        if tag == "ok":
                            ok2 += 1
                            for tgt, r in got.items():
                                rows[tgt].append(r)
                        elif tag == "fail":
                            fail2 += 1
                        elif tag == "nodata":
                            nodata2 += 1
                        elif tag == "skip":
                            skip2 += 1
                        else:
                            empty2 += 1
                        done += 1
                        if abort.is_set():
                            continue
                        # 熔断：失败过半=上游限流；超预算=网络挂起（旧版曾卡死 1h40min）
                        if done >= 60 and (fail2 + nodata2) > done * 0.5:
                            abort.set()
                            log.append(f"  上游异常过半（失败 {fail2} + 空响应 {nodata2}"
                                       f"/{done}）-> 疑似限流，中止剩余请求，下轮重试")
                        elif time.time() - t0 > DAY_BUDGET:
                            abort.set()
                            log.append(f"  超出时间预算 {DAY_BUDGET:.0f}s（ok {ok2} / "
                                       f"fail {fail2}）-> 中止剩余请求，下轮重试")
            for tgt in missing_days:
                if not rows[tgt]:
                    log.append(f"  逐合约日线补缺 {tgt}: 无可用数据（该日未发布 {empty2}"
                               f" / 上游无数据 {nodata2} / 请求失败 {fail2}"
                               f" / 已中止 {skip2}）")
                    continue
                inc = pd.concat(rows[tgt], ignore_index=True)
                daily_all = pd.concat([daily_all, inc], ignore_index=True)
                daily_all = daily_all.drop_duplicates(
                    subset=["trade_date", "contract_id"], keep="last")
                daily_all["_d"] = daily_all["trade_date"].astype(str).str[:10]
                # 落库口径必须可对账：报该日真实覆盖合约数，不是"本次新增行数"
                log.append(f"  逐合约日线补缺 {tgt}: 新增 {len(inc)} 行，该日覆盖 "
                           f"{int((daily_all['_d'] == tgt).sum())}/{uni.get(tgt, 0)} 合约")
                daily_all.to_parquet(store.root / "contract_daily" / "all.parquet",
                                     index=False)
            log.append(f"  日线补缺耗时 {time.time() - t0:.0f}s")
    except Exception as e:
        log.append(f"逐合约日线补缺 FAIL: {type(e).__name__} {str(e)[:60]}")
    # 3) 逐合约 OI/量 快照（当日全合约，自建 OI 库）
    try:
        try:
            risks = p.risk_indicators(day)
        except Exception:
            risks = pd.DataFrame()   # 非交易日：交易所接口返回空表头 → KeyError
        if risks.empty:
            log.append(f"OI 快照跳过：{day} 非交易日（无行情）")
        else:
            sids = list(risks["security_id"].unique())
            # 并行拉取：每线程独立 provider（各自 0.35s 限流，N 并发≈N 倍吞吐，避免共享限流竞态）
            from concurrent.futures import ThreadPoolExecutor, as_completed
            rows = []
            def _fetch_one(sid):
                try:
                    s_ = _thread_provider(0.35).contract_spot(sid)
                    s_["trade_date"] = day
                    return s_
                except Exception:
                    return None
            with ThreadPoolExecutor(max_workers=DAILY_WORKERS) as ex:
                for fut in as_completed([ex.submit(_fetch_one, s) for s in sids]):
                    r = fut.result()
                    if r is not None:
                        rows.append(r)
            oi_df = pd.DataFrame(rows)
            out = store.root / "oi_snapshots"
            out.mkdir(parents=True, exist_ok=True)
            f = out / f"{day:%Y%m}.parquet"
            if f.exists():
                old = pd.read_parquet(f)
                oi_df = pd.concat([old, oi_df], ignore_index=True) \
                    .drop_duplicates(subset=["security_id", "trade_date"], keep="last")
            oi_df.to_parquet(f, index=False)
            log.append(f"OI 快照: {len(oi_df)} 行（含持仓量，累计建库）")
    except Exception as e:
        log.append(f"OI 快照 FAIL: {type(e).__name__} {str(e)[:60]}")

    # 4) ATM IV 序列重建
    try:
        from thetalab.scripts.collect_risk_history import main as _unused  # noqa
        hist = store.read("risk_indicators")
        und = hist[hist["underlying"] == UNDERLYING].copy()
        udl = pd.read_parquet(store.root / "underlying_daily" / "all.parquet")
        close_map = dict(zip(udl["date"], udl["close"].astype(float)))
        rows = []
        for d, g in und.groupby("trade_date"):
            if d not in close_map:
                continue
            S = close_map[d]
            exps = sorted(g["expiry"].unique())
            near = [e for e in exps if (e - d).days >= 7]
            if not near:
                continue
            gg = g[(g["expiry"] == near[0]) & g["iv"].notna()]
            if gg.empty:
                continue
            k = gg["strike"].iloc[int((gg["strike"] - S).abs().values.argmin())]
            row = {"date": d, "expiry": near[0], "atm_strike": float(k)}
            for right, key in (("CALL", "iv_call"), ("PUT", "iv_put")):
                r2 = gg[(gg["right"] == right) & (gg["strike"] == k)]
                row[key] = float(r2["iv"].iloc[0]) if len(r2) else float("nan")
            rows.append(row)
        s2 = pd.DataFrame(rows).dropna(subset=["iv_call", "iv_put"], how="any")
        s2["atm_iv"] = (s2["iv_call"] + s2["iv_put"]) / 2.0
        s2.to_csv(store.root.parent / "atm_iv_series.csv", index=False)
        log.append(f"ATM IV 序列: {len(s2)} 天")
    except Exception as e:
        log.append(f"ATM IV FAIL: {type(e).__name__} {str(e)[:60]}")

    for x in log:
        print(f"  [{x}]")
    print(f"耗时 {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None)
