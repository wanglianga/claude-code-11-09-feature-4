"""双口径时效治理 DAG（手动触发）：口径版本登记/切换与周期考核报表出具。

与每日增量 DAG 分离：运单事实计算在 ltl_daily_incremental 内自动完成，
本 DAG 只承载有治理含义、需要人工确认的两个动作。

触发示例（Airflow UI / CLI 传 params）：
  # 周期内更换口径后重新出具周期结果（自动追加 report_version，旧版 SUPERSEDED）
  {"action": "issue_period", "period_id": "2026-11-W1", "period_type": "WEEK",
   "start": "2026-11-01", "end": "2026-11-05", "caliber": "BOTH",
   "caliber_version": "v2026.2", "reason": "客户口径阈值调整，重出本周结果"}
  # 登记口径新版本（旧版本闭合保留，历史定义不覆盖）
  {"action": "register_version", "caliber": "CUSTOMER", "version": "v2026.2",
   "effective_from": "2026-11-04", "threshold": 2880,
   "reschedule_deduct": 120, "reason": "客户承诺时效收紧"}

纪律：禁止用新口径回写历史周期报表；重出只追加版本并冻结各自排名快照。
"""
from __future__ import annotations

import subprocess

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from dag_common import spark_submit


def run_timeliness_governance(**_context) -> None:
    p = get_current_context()["params"]
    args = [p["action"].replace("_", "-")]
    if p["action"] == "issue_period":
        args += ["--period-id", p["period_id"], "--period-type", p["period_type"],
                 "--start", p["start"], "--end", p["end"],
                 "--caliber", p["caliber"], "--reason", p.get("reason", "INITIAL")]
        if p.get("caliber_version"):
            args += ["--caliber-version", p["caliber_version"]]
    elif p["action"] == "register_version":
        args += ["--caliber", p["caliber"], "--version", p["version"],
                 "--effective-from", p["effective_from"], "--reason",
                 p.get("reason", "口径切换")]
        if p.get("threshold") is not None:
            args += ["--threshold", str(p["threshold"])]
        if p.get("reschedule_deduct") is not None:
            args += ["--reschedule-deduct", str(p["reschedule_deduct"])]
    else:
        raise ValueError(f"未知 action: {p['action']}")
    subprocess.run(
        spark_submit("timeliness_job.py", *args), shell=True, check=True)


with DAG(
    dag_id="ltl_timeliness_governance",
    description="双口径时效治理：口径版本切换与周期报表重新出具（手动）",
    default_args={"owner": "data_platform", "retries": 0},
    start_date=pendulum.datetime(2026, 11, 10, tz="Asia/Shanghai"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ltl", "event_warehouse", "timeliness", "governance"],
    params={
        "action": "issue_period",
        "period_id": "2026-11-W1",
        "period_type": "WEEK",
        "start": "2026-11-01",
        "end": "2026-11-05",
        "caliber": "BOTH",
        "caliber_version": "",
        "reason": "INITIAL",
        "caliber_code": "CUSTOMER",
        "version": "v2026.2",
        "effective_from": "2026-11-04",
        "threshold": 2880,
        "reschedule_deduct": 120,
    },
) as dag:
    PythonOperator(
        task_id="timeliness_governance_run",
        python_callable=run_timeliness_governance,
    )
