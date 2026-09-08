"""
thetalab.scripts.collect_daily — 每日收盘后统一采集入口（设计文档 §5.2 日频批处理）

用途：server 晚间调度自动调用，或手动运行（即「每日更新.bat」）。做五件事：
    1. 当日 risk_indicators（IV/Greeks，全市场一次调用）
    2. 标的日线增量（510300）
    3. 逐合约 OI/量 快照（自建历史 OI 库的唯一来源——sina/交易所均无历史逐合约 OI）
    4. 重算 ATM IV 序列（追加）
    5. 重建 dashboard.html
运行：python -m thetalab.scripts.collect_daily [date=今天]
"""
import sys
import time
import warnings
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
from thetalab.scripts.collect_risk_history import UNDERLYING


def self_recent_days(day: date, n: int):
    """含 day 在内的往前 n 个自然日（日级覆盖检查用）"""
    from datetime import timedelta
    return [day - timedelta(days=i) for i in range(n)]


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

    # 2c) 沪市逐合约日线日级补缺：新浪日线发布滞后（实测 8-31 采于 20:57 全部只有 8-28），
    # 而 collect_contract_history 按 security_id 断点、采过即跳过，缺口会永久留存 →
    # 撮合链缺当日沪市合约，下单报"当日无该合约行情"。
    # 交易日基准 = risk ∪ underlying_daily（服务器停机的夜晚 risk 也可能整日缺失，
    # 但 sina 标的日线发布及时，可当交易日历）；缺日先补 risk 再补逐合约日线。
    try:
        daily_all = pd.read_parquet(store.root / "contract_daily" / "all.parquet")
        daily_all["_d"] = daily_all["trade_date"].astype(str).str[:10]
        cnt_by_day = daily_all[daily_all["_d"] <= str(day)].groupby("_d").size().to_dict()
        risk_df = store.read("risk_indicators")
        risk_days = set(risk_df["trade_date"].astype(str).str[:10].unique())
        udl_days = set(pd.read_parquet(store.root / "underlying_daily" / "all.parquet")
                       ["date"].astype(str).str[:10].unique())
        risk_cnt = risk_df[risk_df["underlying"] != "159915"].groupby(
            risk_df["trade_date"].astype(str).str[:10]).size().to_dict()
        expect_default = max(risk_cnt.values()) if risk_cnt else 600
        missing_days = []
        for d in sorted(risk_days | udl_days)[-5:]:
            if d > str(day):
                continue
            if cnt_by_day.get(d, 0) < 0.8 * risk_cnt.get(d, expect_default):
                missing_days.append(d)
        missing_days = missing_days[:2]   # 单次最多补 2 天（每天约 600 合约请求）
        if missing_days:
            log.append(f"沪市逐合约日线缺日: {missing_days}，增量补拉（仅缺失合约）")
            # 用显式 spec 加载绕过"thetalab 默认查找失效"的环境问题（与头部兜底同理）
            import importlib.util as _iu
            def _load_cch():
                _parent = str(Path(__file__).resolve().parents[2])
                _f = Path(_parent) / "thetalab" / "scripts" / "collect_contract_history.py"
                _spec = _iu.spec_from_file_location(
                    "thetalab.scripts.collect_contract_history", _f)
                _m = _iu.module_from_spec(_spec)
                sys.modules[_spec.name] = _m
                _spec.loader.exec_module(_m)
                return _m.build_contract_id_map
            try:
                from thetalab.scripts.collect_contract_history import build_contract_id_map
            except Exception:   # 默认查找失败 → 显式加载
                build_contract_id_map = _load_cch()
            for tgt in missing_days:
                # risk 也缺该日 → 先补 risk（交易所历史文件可回拉），否则用其合约清单
                if tgt not in risk_days:
                    try:
                        rd = p.risk_indicators(date.fromisoformat(tgt))
                        if not rd.empty:
                            store.write("risk_indicators", rd)
                            risk_df = pd.concat([risk_df, rd], ignore_index=True)
                            log.append(f"  补 {tgt} 风险指标: +{len(rd)} 行")
                    except Exception as e:
                        log.append(f"  补 {tgt} 风险指标 FAIL: {type(e).__name__}")
                risk_tgt = risk_df[risk_df["trade_date"].astype(str).str[:10] == tgt]
                if risk_tgt.empty:   # risk 拉不到：用最近一天的合约清单兜底
                    last_d = max(risk_days)
                    risk_tgt = risk_df[risk_df["trade_date"].astype(str).str[:10] == last_d]
                m_day, m_glob = build_contract_id_map(risk_df)
                have_tgt = set(daily_all[daily_all["_d"] == tgt]["security_id"].unique())
                todo = [(s, u) for s, u in
                        risk_tgt[risk_tgt["underlying"] != "159915"]
                        .drop_duplicates("security_id")[["security_id", "underlying"]]
                        .itertuples(index=False, name=None)
                        if s not in have_tgt]
                log.append(f"  目标日 {tgt} 合约 {risk_tgt['security_id'].nunique()} 个，缺失待补 {len(todo)}")
                ok2 = fail2 = 0
                new_rows = []
                for sid, und in todo:
                    try:
                        df = p.contract_daily(sid)
                        if df.empty:
                            continue
                        df = df[df["date"].astype(str).str[:10] == tgt]   # 只取缺口日行
                        if df.empty:
                            continue
                        df["security_id"] = sid
                        df["underlying"] = und
                        df["trade_date"] = df["date"]
                        key = list(zip(df["trade_date"], df["security_id"]))
                        cid = pd.Series([m_day.get(k) for k in key]) \
                            .fillna(df["security_id"].map(m_glob))
                        df["contract_id"] = cid.fillna(df["security_id"])
                        new_rows.append(df)
                        ok2 += 1
                    except Exception:
                        fail2 += 1
                if new_rows:
                    inc = pd.concat(new_rows, ignore_index=True)
                    daily_all = pd.concat([daily_all, inc], ignore_index=True) \
                        .drop_duplicates(subset=["trade_date", "contract_id"], keep="last")
                    daily_all.to_parquet(store.root / "contract_daily" / "all.parquet",
                                         index=False)
                    log.append(f"  逐合约日线补缺 {tgt}: +{len(inc)} 行（{ok2} 合约 / fail {fail2}）")
                else:
                    log.append(f"  逐合约日线补缺 {tgt}: 数据源仍无（发布滞后），下轮重试")
        else:
            log.append("逐合约日线日级覆盖完整（近 5 交易日）")
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
                    sp = SseOptionProvider(min_interval=0.35)
                    s_ = sp.contract_spot(sid)
                    s_["trade_date"] = day
                    return s_
                except Exception:
                    return None
            with ThreadPoolExecutor(max_workers=5) as ex:
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
