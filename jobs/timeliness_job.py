"""双口径时效作业：内部考核口径与客户感知口径并存计算、版本化治理与周期出具。

四个动作（第一个位置参数）：
  seed-calibers     按 pipeline.conf 幂等登记两套口径的初始版本 v2026.1
  compute           按完结日（SIGN 业务日）计算运单双口径事实，MERGE 幂等写入
  issue-period      出具考核周期报表：追加 report_version，旧版置 SUPERSEDED 并冻结排名
  register-version  口径切换：旧版本闭合保留，新增版本；绝不覆盖历史定义

口径定义（与 sql/09_timeliness.sql、meta.caliber_definition 一致）：
  INTERNAL 内部考核：时长 = 签收扫描(SIGN) - 揽收扫描(PICKUP)，不做任何扣减。
  CUSTOMER 客户感知：毛时长 = 实际送达(DELIVER，缺失兜底 SIGN) - 下单(ORDER，缺失兜底 PICKUP)
                     净时长 = 毛时长 - 商家备货时长(stockup_minutes)
                              - 改约次数 × 单次扣减分钟(reschedule_deduct_minutes)
                     净时长 > 阈值即 complaint_flag=true（客户投诉）。

版本治理纪律（强制）：
  * 口径切换只允许 register-version 新增版本行，历史定义行只闭合不删除/不改写；
  * 运单事实保存计算时的 caliber_version 与阈值快照，结论可回溯到当时定义；
  * 周期报表只追加：重出周期 -> report_version+1、旧版 SUPERSEDED、排名快照原样保留，
    禁止用新口径回写/重排历史排名（历史排名只能读 rank_snapshot）。
"""
from __future__ import annotations

import argparse
import json
from datetime import date

from pyspark.sql import functions as F

from common.spark_utils import (
    CALIBER_CUSTOMER, CALIBER_DEF, CALIBER_INTERNAL, DWD_EVENT, FUTURE_DATE,
    PERIOD_REPORT, TIMELINESS_FACT, build_spark, load_conf, new_batch_id,
)

FORMULA_INTERNAL = (
    "内部考核口径：时长=签收扫描时间-揽收扫描时间，不扣备货/改约；"
    "时长<=阈值即达标")
FORMULA_CUSTOMER = (
    "客户感知口径：毛时长=实际送达时间-下单时间，"
    "净时长=毛时长-商家备货时长-改约次数×单次扣减分钟；"
    "净时长<=阈值即达标，不达标计客户投诉")

FACT_COLUMNS = [
    "waybill_no", "biz_date", "customer_code", "sign_hub_code",
    "order_time", "order_time_fallback", "pickup_time", "stockup_ready_time",
    "deliver_time", "sign_time", "stockup_minutes", "reschedule_seq",
    "internal_caliber_version", "internal_duration_minutes",
    "internal_threshold_minutes", "internal_met",
    "customer_caliber_version", "customer_gross_minutes",
    "customer_deduct_stockup_minutes", "customer_deduct_reschedule_minutes",
    "customer_net_minutes", "customer_threshold_minutes", "customer_met",
    "complaint_flag", "diff_minutes", "diff_source", "diff_breakdown",
    "ingest_batch_id", "ingest_time",
]


def _minutes(start_col, end_col):
    """两时间戳相差的完整分钟数（end-start）；用 unix 秒差实现，跨 Spark 版本稳定。"""
    return ((F.unix_timestamp(end_col) - F.unix_timestamp(start_col)) / 60).cast("bigint")


# ============================ 口径版本台账 ============================
def seed_calibers(spark, conf: dict, batch_id: str) -> dict:
    """幂等登记两套口径初始版本（已存在则跳过）。"""
    seed_from = conf.get("timeliness.caliber.seed_from", "2026-11-01")
    internal_threshold = int(conf["timeliness.internal.threshold_minutes"])
    customer_threshold = int(conf["timeliness.customer.threshold_minutes"])
    rs_deduct = int(conf["timeliness.customer.reschedule_deduct_minutes"])
    eff_from = date.fromisoformat(seed_from)
    eff_to = date(9999, 12, 31)
    rows = [
        (CALIBER_INTERNAL, "v2026.1", "内部考核时效口径",
         "PICKUP", "SIGN", "NA", False, 0, internal_threshold,
         FORMULA_INTERNAL, eff_from, eff_to, True, "ACTIVE", "INITIAL"),
        (CALIBER_CUSTOMER, "v2026.1", "客户感知时效口径",
         "ORDER", "DELIVER", "SIGN", True, rs_deduct, customer_threshold,
         FORMULA_CUSTOMER, eff_from, eff_to, True, "ACTIVE", "INITIAL"),
    ]
    schema = (
        "caliber_code string, caliber_version string, caliber_name string, "
        "start_event string, end_event string, end_event_fallback string, "
        "stockup_deduct_flag boolean, reschedule_deduct_minutes bigint, "
        "threshold_minutes bigint, formula_desc string, effective_from date, "
        "effective_to date, is_current boolean, status string, change_reason string")
    spark.createDataFrame(rows, schema).createOrReplaceTempView("stg_caliber_seed")
    spark.sql(f"""
        MERGE INTO {CALIBER_DEF} t
        USING stg_caliber_seed s
        ON t.caliber_code = s.caliber_code AND t.caliber_version = s.caliber_version
        WHEN NOT MATCHED THEN INSERT
            (caliber_code, caliber_version, caliber_name, start_event, end_event,
             end_event_fallback, stockup_deduct_flag, reschedule_deduct_minutes,
             threshold_minutes, formula_desc, effective_from, effective_to,
             is_current, status, change_reason, ingest_batch_id, ingest_time)
        VALUES
            (s.caliber_code, s.caliber_version, s.caliber_name, s.start_event, s.end_event,
             s.end_event_fallback, s.stockup_deduct_flag, s.reschedule_deduct_minutes,
             s.threshold_minutes, s.formula_desc, s.effective_from, s.effective_to,
             s.is_current, s.status, s.change_reason, '{batch_id}', current_timestamp())
    """)
    seeded = spark.table(CALIBER_DEF).count()
    print(f"[timeliness] seed-calibers 完成，口径定义表共 {seeded} 行")
    return {"caliber_rows": seeded}


def register_version(spark, args, batch_id: str) -> dict:
    """口径切换：闭合当前版本 -> 新增版本。历史行只闭合，永不删除/改写结论。"""
    code = args.caliber
    current = (spark.table(CALIBER_DEF)
               .where(f"caliber_code = '{code}' AND is_current = true").collect())
    if not current:
        raise RuntimeError(f"口径 {code} 不存在当前版本，请先执行 seed-calibers")
    cur = current[0]
    exists = (spark.table(CALIBER_DEF)
              .where(f"caliber_code = '{code}' AND caliber_version = '{args.version}'")
              .count())
    if exists:
        raise ValueError(f"口径版本已存在，禁止重复登记: {code} {args.version}")
    if args.effective_from <= str(cur.effective_from):
        raise ValueError(
            f"新版本生效日 {args.effective_from} 必须晚于当前版本生效日 {cur.effective_from}")

    # 1) 旧版本闭合（只改闭合字段，公式/阈值等历史定义原样保留）
    spark.sql(f"""
        UPDATE {CALIBER_DEF}
        SET effective_to = DATE'{args.effective_from}',
            is_current = false, status = 'RETIRED'
        WHERE caliber_code = '{code}' AND caliber_version = '{cur.caliber_version}'
    """)
    # 2) 新增开放版本；未显式给的参数沿用上一版本，保证口径差异可逐项解释
    threshold = args.threshold if args.threshold is not None else cur.threshold_minutes
    rs_deduct = (args.reschedule_deduct if args.reschedule_deduct is not None
                 else cur.reschedule_deduct_minutes)
    stockup_flag = cur.stockup_deduct_flag
    spark.sql(f"""
        INSERT INTO {CALIBER_DEF}
        VALUES (
            '{code}', '{args.version}', '{cur.caliber_name}',
            '{cur.start_event}', '{cur.end_event}', '{cur.end_event_fallback}',
            {str(stockup_flag).lower()}, {rs_deduct}, {threshold},
            '{cur.formula_desc}', DATE'{args.effective_from}',
            DATE'{FUTURE_DATE}', true, 'ACTIVE', '{args.reason}',
            '{batch_id}', current_timestamp()
        )
    """)
    result = {"caliber": code, "new_version": args.version,
              "closed_version": cur.caliber_version,
              "effective_from": args.effective_from,
              "threshold_minutes": threshold,
              "reschedule_deduct_minutes": rs_deduct}
    print("[timeliness] register-version " + json.dumps(result, ensure_ascii=False))
    return result


# ============================ 运单双口径事实 ============================
def _anchors(spark, biz_date: str | None):
    """从 DWD 事件聚合每单的口径锚点时间与扣减基数。"""
    dwd = spark.table(DWD_EVENT)
    if biz_date:
        # 增量按运单完结日（SIGN 业务日）归属：先取当日签收运单，再 semi-join 回全量事件
        closed = (spark.table(DWD_EVENT)
                  .where((F.col("event_type") == "SIGN")
                         & (F.to_date("event_time") == biz_date))
                  .select("waybill_no").distinct())
        dwd = dwd.join(closed, "waybill_no", "left_semi")
    agg = (
        dwd.groupBy("waybill_no").agg(
            F.min("customer_code").alias("customer_code"),
            F.min(F.when(F.col("event_type") == "ORDER", F.col("event_time")))
             .alias("order_time"),
            F.min(F.when(F.col("event_type") == "PICKUP", F.col("event_time")))
             .alias("pickup_time"),
            F.min(F.when(F.col("event_type") == "DELIVER", F.col("event_time")))
             .alias("deliver_time"),
            F.min(F.when(F.col("event_type") == "SIGN", F.col("event_time")))
             .alias("sign_time"),
            F.max(F.when(F.col("event_type") == "ORDER", F.col("stockup_minutes")))
             .alias("stockup_minutes"),
            F.max(F.when(F.col("event_type") == "RESCHEDULE", F.col("reschedule_seq")))
             .alias("reschedule_cnt"),
        )
    )
    sign_hub = (
        spark.table(DWD_EVENT).where(F.col("event_type") == "SIGN")
        .select(F.col("waybill_no").alias("_wb"),
                F.col("hub_code").alias("sign_hub_code"))
        .dropDuplicates(["_wb"])
    )
    return (agg.join(sign_hub, agg.waybill_no == sign_hub._wb, "left")
               .drop("_wb")
               .where(F.col("sign_time").isNotNull()))


def compute_facts(spark, conf: dict, biz_date: str | None, batch_id: str):
    """计算双口径事实并 MERGE 入事实表（按 waybill_no 幂等）。"""
    anchors = _anchors(spark, biz_date)
    if anchors.limit(1).count() == 0:
        print(f"[timeliness] compute biz_date={biz_date} 无已签收运单，跳过")
        return None

    cal = spark.table(CALIBER_DEF)
    cal_i = (cal.where(F.col("caliber_code") == CALIBER_INTERNAL)
             .select(F.col("caliber_version").alias("internal_caliber_version"),
                     F.col("threshold_minutes").alias("internal_threshold_minutes"),
                     F.col("effective_from").alias("i_eff"),
                     F.col("effective_to").alias("i_exp")))
    cal_c = (cal.where(F.col("caliber_code") == CALIBER_CUSTOMER)
             .select(F.col("caliber_version").alias("customer_caliber_version"),
                     F.col("threshold_minutes").alias("customer_threshold_minutes"),
                     F.col("reschedule_deduct_minutes"),
                     F.col("stockup_deduct_flag"),
                     F.col("effective_from").alias("c_eff"),
                     F.col("effective_to").alias("c_exp")))

    f = (anchors
         .withColumn("biz_date", F.to_date(F.col("sign_time")))
         # 口径版本按完结日时点解析（严格生效区间，禁止拿最新定义回刷历史）
         .join(cal_i,
               (F.col("biz_date") >= F.col("i_eff"))
               & (F.col("biz_date") < F.col("i_exp")), "left")
         .join(cal_c,
               (F.col("biz_date") >= F.col("c_eff"))
               & (F.col("biz_date") < F.col("c_exp")), "left")
         .drop("i_eff", "i_exp", "c_eff", "c_exp"))

    missing_ver = (f.where(F.col("internal_caliber_version").isNull()
                           | F.col("customer_caliber_version").isNull())
                   .limit(1).count())
    if missing_ver:
        raise RuntimeError("存在完结日找不到生效口径版本的运单，请先 seed-calibers "
                           "或检查 caliber_definition 生效区间")

    # 客户口径起点/终点兜底（旧链路无 ORDER、或仅有签收无派送）
    f = (f
         .withColumn("order_time_fallback", F.col("order_time").isNull())
         .withColumn("_order_eff", F.coalesce(F.col("order_time"), F.col("pickup_time")))
         .withColumn("_deliver_eff", F.coalesce(F.col("deliver_time"), F.col("sign_time")))
         .withColumn("stockup_minutes", F.coalesce(F.col("stockup_minutes"), F.lit(0)))
         .withColumn("reschedule_seq", F.coalesce(F.col("reschedule_cnt"), F.lit(0)))
         .withColumn("stockup_ready_time",
                     F.when(F.col("order_time").isNotNull(),
                            F.expr("order_time + make_interval(0,0,0,0,0,stockup_minutes,0)"))
                      .otherwise(F.lit(None))))

    # ---- 内部口径：SIGN - PICKUP ----
    f = (f
         .withColumn("internal_duration_minutes",
                     F.when(F.col("pickup_time").isNotNull(),
                            _minutes(F.col("pickup_time"), F.col("sign_time"))))
         .withColumn("internal_met",
                     F.col("internal_duration_minutes") <= F.col("internal_threshold_minutes"))
         # ---- 客户口径：毛时长、两项扣减、净时长 ----
         .withColumn("customer_gross_minutes",
                     _minutes(F.col("_order_eff"), F.col("_deliver_eff")))
         .withColumn("customer_deduct_stockup_minutes",
                     F.when(F.col("stockup_deduct_flag"), F.col("stockup_minutes"))
                      .otherwise(F.lit(0)))
         .withColumn("customer_deduct_reschedule_minutes",
                     F.col("reschedule_seq") * F.col("reschedule_deduct_minutes"))
         .withColumn("customer_net_minutes",
                     F.col("customer_gross_minutes")
                     - F.col("customer_deduct_stockup_minutes")
                     - F.col("customer_deduct_reschedule_minutes"))
         .withColumn("customer_met",
                     F.col("customer_net_minutes") <= F.col("customer_threshold_minutes"))
         .withColumn("complaint_flag", F.col("customer_met") == False))  # noqa: E712

    # ---- 差异来源拆解（四项有符号分量之和恰好等于 diff_minutes）----
    # 恒等式：净时长-内部时长 = (揽收-下单) + (送达-签收) - 备货扣减 - 改约扣减
    # 注意 deliver-sign 有符号：送达早于签收时为负（多数正常运单为 -60 分钟左右）。
    f = (f
         .withColumn("_wait_pickup",
                     F.when(F.col("order_time_fallback"), F.lit(0))
                      .otherwise(_minutes(F.col("order_time"), F.col("pickup_time"))))
         .withColumn("_deliver_to_sign",
                     # 恒等式分量为 送达-签收（注意方向）：送达早于签收时为负
                     _minutes(F.col("sign_time"), F.col("_deliver_eff")))
         .withColumn("diff_minutes",
                     F.col("customer_net_minutes") - F.col("internal_duration_minutes"))
         .withColumn("diff_breakdown",
                     F.to_json(F.struct(
                         F.col("_wait_pickup").alias("order_to_pickup_minutes"),
                         F.col("_deliver_to_sign").alias("deliver_to_sign_minutes"),
                         (-F.col("customer_deduct_stockup_minutes"))
                             .alias("stockup_deduct_minutes"),
                         (-F.col("customer_deduct_reschedule_minutes"))
                             .alias("reschedule_deduct_minutes"))))
         .withColumn("diff_source", F.concat_ws("；",
             F.concat(F.lit("下单到揽收等待 "), F.col("_wait_pickup"), F.lit(" 分钟")),
             F.concat(F.lit("送达至签收时差(有符号) "), F.col("_deliver_to_sign"), F.lit(" 分钟")),
             F.concat(F.lit("扣减商家备货 "),
                      F.col("customer_deduct_stockup_minutes"), F.lit(" 分钟")),
             F.concat(F.lit("扣减客户改约 "),
                      F.col("customer_deduct_reschedule_minutes"), F.lit(" 分钟")))))

    out = (f.withColumn("ingest_batch_id", F.lit(batch_id))
            .withColumn("ingest_time", F.current_timestamp())
            .select(*FACT_COLUMNS))
    out.createOrReplaceTempView("stg_timeliness_fact")
    spark.sql(f"""
        MERGE INTO {TIMELINESS_FACT} t
        USING stg_timeliness_fact s
        ON t.waybill_no = s.waybill_no
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = out.count()
    print(f"[timeliness] compute biz_date={biz_date} 写入/刷新运单 {n} 条")
    return out


# ============================ 周期考核报表（版本快照） ============================
def _rank_snapshot(rows) -> str:
    """出具时刻冻结排名：达标率降序、总数降序、维度码升序兜底，保证可复现。"""
    by_customer, by_hub = {}, {}
    for r in rows:
        bucket = by_hub if r.dim_type == "HUB" else by_customer
        bucket[r.dim_code] = {
            "total": r.total, "met": r.met,
            "rate": round(r.met / r.total, 6) if r.total else 0.0,
            "complaint": r.complaint}

    def _ranked(bucket):
        return [
            {"dim_code": k, **v, "rank": idx + 1}
            for idx, (k, v) in enumerate(sorted(
                bucket.items(),
                key=lambda kv: (-kv[1]["rate"], -kv[1]["total"], kv[0])))]

    return json.dumps({"by_customer": _ranked(by_customer),
                       "by_hub": _ranked(by_hub)}, ensure_ascii=False)


def issue_period(spark, args, batch_id: str) -> dict:
    """追加出具周期报表；同周期同口径重出只增新版本，旧版 SUPERSEDED 且排名冻结。"""
    period_fact = (spark.table(TIMELINESS_FACT)
                   .where((F.col("biz_date") >= F.lit(args.start))
                          & (F.col("biz_date") <= F.lit(args.end))))
    if period_fact.limit(1).count() == 0:
        raise RuntimeError(f"周期 {args.start}~{args.end} 内无时效事实，请先 compute")

    calibers = [CALIBER_INTERNAL, CALIBER_CUSTOMER] if args.caliber == "BOTH" \
        else [args.caliber]
    issued = []
    for code in calibers:
        ver_col = ("internal_caliber_version" if code == CALIBER_INTERNAL
                   else "customer_caliber_version")
        met_col = "internal_met" if code == CALIBER_INTERNAL else "customer_met"
        dur_col = ("internal_duration_minutes" if code == CALIBER_INTERNAL
                   else "customer_net_minutes")
        versions = [r[ver_col] for r in period_fact.select(ver_col).distinct().collect()]
        target_version = args.caliber_version or sorted(versions)[-1]
        pf = period_fact.where(F.col(ver_col) == target_version)
        if pf.limit(1).count() == 0:
            raise RuntimeError(
                f"周期内没有口径版本 {target_version} 的事实行；"
                "口径切换后须先按新版本重算该周期运单事实再出具报表")

        # 同周期同口径历史版本：新版追加，旧版置 SUPERSEDED（行与排名快照保留）
        prior = (spark.table(PERIOD_REPORT)
                 .where(f"period_id = '{args.period_id}' AND caliber_code = '{code}'")
                 .orderBy(F.col("report_version").desc()).collect())
        report_version = (prior[0].report_version + 1) if prior else 1
        if prior:
            spark.sql(f"""
                UPDATE {PERIOD_REPORT}
                SET report_status = 'SUPERSEDED'
                WHERE period_id = '{args.period_id}' AND caliber_code = '{code}'
                  AND report_status = 'CURRENT'
            """)
        supersedes = prior[0].report_version if prior else None
        old_ver = prior[0].caliber_version if prior else None
        if prior and old_ver != target_version:
            reason = (f"口径切换 {old_ver}->{target_version}，重新出具周期结果；"
                      f"历史排名快照保留不重排；{args.reason or ''}").strip("；")
        else:
            reason = args.reason or ("INITIAL" if report_version == 1 else "周期结果重出")

        agg_rows = pf.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col(met_col), 1).otherwise(0)).alias("met"),
            F.avg(F.col(dur_col)).alias("avg_dur"),
            F.sum(F.when(F.col("complaint_flag"), 1).otherwise(0)).alias("complaint"),
        ).collect()[0]

        # 排名在出具时刻计算并冻结为 JSON 文本，之后口径再变也不回写本快照
        # 内部口径报表不统计客户投诉，投诉计数强制 0，避免与客户口径混淆
        complaint_expr = (F.lit(0) if code == CALIBER_INTERNAL
                          else F.sum(F.when(F.col("complaint_flag"), 1).otherwise(0)))
        rank_rows = (
            pf.groupBy(F.lit("CUSTOMER").alias("dim_type"),
                       F.col("customer_code").alias("dim_code"))
              .agg(F.count("*").alias("total"),
                   F.sum(F.when(F.col(met_col), 1).otherwise(0)).alias("met"),
                   complaint_expr.alias("complaint"))
            .unionByName(
                pf.groupBy(F.lit("HUB").alias("dim_type"),
                           F.col("sign_hub_code").alias("dim_code"))
                  .agg(F.count("*").alias("total"),
                       F.sum(F.when(F.col(met_col), 1).otherwise(0)).alias("met"),
                       complaint_expr.alias("complaint")))
            .collect())
        rank_json = _rank_snapshot(rank_rows)
        complaint_total = (None if code == CALIBER_INTERNAL
                           else int(agg_rows.complaint or 0))
        report_id = f"{args.period_id}|{code}|v{report_version}"
        spark.sql(f"""
            INSERT INTO {PERIOD_REPORT}
            VALUES (
                '{args.period_id}', '{args.period_type}',
                DATE'{args.start}', DATE'{args.end}',
                '{code}', '{target_version}', {report_version},
                '{supersedes if supersedes is not None else 'NA'}',
                'CURRENT', {int(agg_rows.total)}, {int(agg_rows.met)},
                {round(float(agg_rows.met) / int(agg_rows.total), 6)},
                {round(float(agg_rows.avg_dur), 3)},
                {('NULL' if complaint_total is None else complaint_total)},
                '{rank_json.replace("'", "''")}', '{reason.replace("'", "''")}',
                '{batch_id}', '{batch_id}', current_timestamp()
            )
        """)
        issued.append({"period_id": args.period_id, "caliber": code,
                       "report_version": report_version,
                       "caliber_version": target_version,
                       "report_id": report_id})
    print("[timeliness] issue-period " + json.dumps(issued, ensure_ascii=False))
    return issued


def main() -> None:
    ap = argparse.ArgumentParser(description="双口径时效作业")
    ap.add_argument("action", choices=["seed-calibers", "compute",
                                       "issue-period", "register-version"])
    ap.add_argument("--biz-date", help="compute：运单完结日(SIGN业务日) YYYY-MM-DD")
    ap.add_argument("--period-id", help="issue-period：考核周期ID，如 2026-11-W1")
    ap.add_argument("--period-type", default="WEEK", choices=["WEEK", "MONTH", "DATE_RANGE"])
    ap.add_argument("--start", help="周期起（含）")
    ap.add_argument("--end", help="周期止（含）")
    ap.add_argument("--caliber", default="BOTH",
                    choices=["INTERNAL", "CUSTOMER", "BOTH"])
    ap.add_argument("--caliber-version", default=None,
                    help="显式指定出具使用的口径版本；缺省取周期内最新版本")
    ap.add_argument("--version", help="register-version：新版本号，如 v2026.2")
    ap.add_argument("--effective-from", help="register-version：新版本生效日")
    ap.add_argument("--threshold", type=int, default=None, help="新版本达标阈值(分钟)")
    ap.add_argument("--reschedule-deduct", type=int, default=None,
                    help="新版本单次改约扣减分钟数")
    ap.add_argument("--reason", default=None, help="变更/重出原因")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_timeliness", conf)
    batch_id = new_batch_id("tms")
    try:
        if args.action == "seed-calibers":
            seed_calibers(spark, conf, batch_id)
        elif args.action == "compute":
            if not args.biz_date:
                raise ValueError("compute 必须传 --biz-date")
            compute_facts(spark, conf, args.biz_date, batch_id)
        elif args.action == "register-version":
            register_version(spark, args, batch_id)
        else:
            missing = [n for n in ("period_id", "start", "end")
                       if not getattr(args, n.replace("-", "_"))]
            if missing:
                raise ValueError(f"issue-period 缺少参数: {missing}")
            issue_period(spark, args, batch_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
