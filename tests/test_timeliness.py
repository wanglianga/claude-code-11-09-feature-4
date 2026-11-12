"""双口径时效端到端断言（只读；数据由 scripts/run_local.sh 先行灌好）。

覆盖需求：
  14. 两套口径算术正确、锚点与扣减项齐全且各自可解释；
  15. 同一运单可同时"内部达标 + 客户投诉"，报表并列展示并给出差异来源；
  16. 差异拆解四项分量之和 = 两口径时长之差（可解释、可对账）；
  17. 口径切换只新增版本：旧版本闭合保留，历史定义不被覆盖；
  18. 周期报表版本化：重出追加 report_version、旧版 SUPERSEDED、排名快照冻结，
      禁止用新口径重排历史排名；
  19. 事实/口径登记/周期出具均幂等（重跑不产生重复行）。
"""
from __future__ import annotations

import json

from common.spark_utils import (
    CALIBER_DEF, PERIOD_REPORT, TIMELINESS_FACT,
)

FACT_VIEW = "ltl.app.v_timeliness_dual"
RANK_VIEW = "ltl.app.v_timeliness_ranking_current"


def _fact(spark, waybill: str):
    return (spark.table(TIMELINESS_FACT)
            .where(f"waybill_no = '{waybill}'").collect()[0])


# ---------- 14. 口径算术：内部=揽收->签收；客户=下单->送达扣备货/改约 ----------
def test_14_dual_caliber_arithmetic(spark):
    wb010 = _fact(spark, "WB010")
    # 内部：11-01 16:00 -> 11-03 15:00 = 47 小时 = 2820 分钟
    assert wb010.internal_duration_minutes == 47 * 60
    assert wb010.internal_met is True  # 2820 <= 2880
    # 客户：毛时长 61h；扣备货 120m + 两次改约 240m；净 3300m
    assert wb010.customer_gross_minutes == 61 * 60
    assert wb010.customer_deduct_stockup_minutes == 120
    assert wb010.reschedule_seq == 2
    assert wb010.customer_deduct_reschedule_minutes == 240
    assert wb010.customer_net_minutes == 61 * 60 - 120 - 240
    assert wb010.customer_met is False
    # 阈值/版本快照随事实保存；WB010 于 11-03 签收，演练后按 v2026.2(阈值1800) 重算，
    # 内部口径未切换仍为 v2026.1
    assert wb010.internal_caliber_version == "v2026.1"
    assert wb010.internal_threshold_minutes == 2880
    assert wb010.customer_caliber_version == "v2026.2"
    assert wb010.customer_threshold_minutes == 1800

    wb011 = _fact(spark, "WB011")
    # 内部 25h=1500m；客户毛时长 26h=1560m，扣备货 180m，净 1380m，无改约
    assert wb011.internal_duration_minutes == 1500
    assert wb011.customer_gross_minutes == 1560
    assert wb011.customer_deduct_stockup_minutes == 180
    assert wb011.customer_deduct_reschedule_minutes == 0
    assert wb011.customer_net_minutes == 1380
    assert wb011.internal_met is True and wb011.customer_met is True

    wb012 = _fact(spark, "WB012")
    # 内部 75h=4500m；客户毛 77h=4620m，扣 60+120，净 4440m，双不达标
    assert wb012.internal_duration_minutes == 4500
    assert wb012.customer_net_minutes == 4440
    assert wb012.internal_met is False and wb012.customer_met is False


# ---------- 15. 同单双结论并列：内部达标/客户投诉可同时成立 ----------
def test_15_same_waybill_dual_conclusion_side_by_side(spark):
    row = spark.sql(
        f"SELECT * FROM {FACT_VIEW} WHERE waybill_no = 'WB010'").collect()[0]
    assert row.internal_met is True
    assert row.customer_met is False
    assert row.complaint_flag is True
    assert row.dual_conclusion == "内部达标/客户投诉"
    # 差异来源必须可读且含四类构成说明
    assert "下单到揽收等待" in row.diff_source
    assert "扣减商家备货" in row.diff_source
    assert "扣减客户改约" in row.diff_source
    # 双达标与双不达标结论同样可并列展示
    concl = {r.waybill_no: r.dual_conclusion for r in spark.sql(
        f"SELECT waybill_no, dual_conclusion FROM {FACT_VIEW} "
        "WHERE waybill_no IN ('WB011','WB012')").collect()}
    assert concl["WB011"] == "双达标"
    assert concl["WB012"] == "双不达标"


# ---------- 16. 差异拆解可对账：分量之和恰好等于口径时长差 ----------
def test_16_diff_breakdown_reconciles(spark):
    rows = spark.table(TIMELINESS_FACT).where(
        "waybill_no IN ('WB010','WB011','WB012')").collect()
    assert rows, "双口径事实必须已计算"
    for r in rows:
        parts = json.loads(r.diff_breakdown)
        component_sum = (parts["order_to_pickup_minutes"]
                         + parts["deliver_to_sign_minutes"]
                         + parts["stockup_deduct_minutes"]
                         + parts["reschedule_deduct_minutes"])
        assert component_sum == r.diff_minutes, (
            f"{r.waybill_no} 差异拆解分量之和 {component_sum} "
            f"!= 口径时长差 {r.diff_minutes}")
        assert r.customer_net_minutes - r.internal_duration_minutes == r.diff_minutes


# ---------- 17. 口径切换只新增版本：历史定义闭合保留、不被覆盖 ----------
def test_17_caliber_version_append_only(spark):
    versions = (spark.table(CALIBER_DEF)
                .where("caliber_code = 'CUSTOMER'")
                .orderBy("effective_from").collect())
    assert [v.caliber_version for v in versions] == ["v2026.1", "v2026.2"]
    old, new = versions
    # 旧版本仍保留原始阈值 3000 且已闭合，新版本开放生效
    assert old.threshold_minutes == 3000
    assert old.is_current is False and old.status == "RETIRED"
    assert str(old.effective_to) == "2026-11-03"
    assert new.threshold_minutes == 1800
    assert new.is_current is True and new.status == "ACTIVE"
    # 内部口径未参与切换，仍是唯一当前版本，历史结论口径不变
    internal = (spark.table(CALIBER_DEF)
                .where("caliber_code = 'INTERNAL'").collect())
    assert len(internal) == 1 and internal[0].caliber_version == "v2026.1"
    # 11-02 及以前完结的运单仍关联旧版本，新版本不回刷历史（WB001 未被重算为新版本）
    wb001 = _fact(spark, "WB001")
    assert wb001.customer_caliber_version == "v2026.1"
    assert wb001.customer_threshold_minutes == 3000
    # 11-03 起完结的运单按新版本计算（阈值快照 1800）
    wb010 = _fact(spark, "WB010")
    assert wb010.customer_caliber_version == "v2026.2"
    assert wb010.customer_threshold_minutes == 1800


# ---------- 18. 周期报表版本化：重出追加、旧版冻结、禁止新口径重排历史排名 ----------
def test_18_period_report_versioning_and_frozen_ranking(spark):
    reports = (spark.table(PERIOD_REPORT)
               .where("period_id = '2026-11-W1'")
               .orderBy("caliber_code", "report_version").collect())
    # 内部/客户各出具两版，共 4 行；没有任何历史行被删除
    assert len(reports) == 4
    by_cal = {}
    for r in reports:
        by_cal.setdefault(r.caliber_code, []).append(r)

    for code, vers in by_cal.items():
        assert [v.report_version for v in vers] == [1, 2]
        assert vers[0].report_status == "SUPERSEDED"
        assert vers[1].report_status == "CURRENT"
        assert vers[1].supersedes_report_id == str(vers[0].report_version)
        # 每版都带口径版本标注，可解释该版结论基于哪套定义
        assert vers[0].caliber_version and vers[1].caliber_version

    cust = by_cal["CUSTOMER"]
    # 重出原因写明口径版本变更
    assert "v2026.1->v2026.2" in cust[1].reissue_reason
    # 历史排名快照原样冻结：v1 行内的 JSON 不随新口径重算
    rank_v1 = json.loads(cust[0].rank_snapshot)
    rank_v2 = json.loads(cust[1].rank_snapshot)
    assert isinstance(rank_v1["by_customer"], list) and rank_v1["by_customer"]
    # v1 快照里每个客户都带当时排名与达标率，v2 是独立快照（阈值收紧后达标率下降）
    v1_rates = {x["dim_code"]: x["rate"] for x in rank_v1["by_customer"]}
    v2_rates = {x["dim_code"]: x["rate"] for x in rank_v2["by_customer"]}
    assert "K001" in v1_rates and "K001" in v2_rates
    assert v2_rates["K001"] < v1_rates["K001"]
    # 旧版报表行的总单量/达标率等冻结值未被覆盖
    assert cust[0].total_waybills >= cust[1].total_waybills

    # 内部口径未换版本：重出后两版结论与排名完全一致（证明重出不篡改历史）
    intern = by_cal["INTERNAL"]
    assert intern[0].timeliness_rate == intern[1].timeliness_rate
    assert intern[0].rank_snapshot == intern[1].rank_snapshot
    # 内部口径报表不产生投诉计数
    assert all(r.complaint_waybills is None for r in intern)


# ---------- 19. 幂等：口径登记与事实 MERGE 重跑不产生重复 ----------
def test_19_idempotency_no_duplicates(spark):
    # 口径定义每个口径版本只有一行
    dup_def = (spark.table(CALIBER_DEF).groupBy("caliber_code", "caliber_version")
               .count().where("count > 1").count())
    assert dup_def == 0
    # 事实表一行一运单
    dup_fact = (spark.table(TIMELINESS_FACT).groupBy("waybill_no")
                .count().where("count > 1").count())
    assert dup_fact == 0
    # 当前排名视图按口径版本隔离，不会跨版本混算
    rank_rows = spark.sql(
        f"SELECT DISTINCT customer_caliber_version FROM {RANK_VIEW}").collect()
    assert {r.customer_caliber_version for r in rank_rows} == {"v2026.1", "v2026.2"}


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
