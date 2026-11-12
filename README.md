# 零担物流事件型数据仓库（LTL Event Warehouse）

把"三十多个时间列、4.2 亿行"的运单宽表改造为**四层事件型仓库**：原始层原样保存每次
推送，明细层以"运单 × 事件序号 × 事件类型"为粒度并支持乱序/去重/重放，维表独立拉链
按时点关联，应用层以视图替代旧宽表。本仓库**没有业务界面、没有常驻服务**，交付物是
建表语句、Spark 批处理作业、Airflow 编排、补数脚本与迁移说明。

## 1. 解决的核心问题

| 旧宽表痛点 | 本方案对应机制 |
|---|---|
| 只留最后一次异常，无法回答"在哪个分拨压了两天" | 事件明细逐行保留多次异常；`app.v_hub_dwell` 逐段计算分拨停留，>48h 打标 |
| 网点改名/派件员换片区，用今年架构重算去年报表对不上 | 4 张 SCD2 拉链维表 + 严格时点关联，禁止最新维表回刷历史 |
| 上游丢包/断连/回调丢失要补 30 天数据，一改带偏整月汇总 | 独立补数入口，按单号/区间只重跑受影响分区，影子表对比后受控切换 |
| 新增预约时段、冷链、明年新作业节点，加字段阻力大 | 只加列+默认值回填的演进规则；新节点只是 `event_type` 新枚举，无需加时间列 |

## 2. 分层与目录结构

```
sql/                         四层建表与视图（Spark 3.5 SQL + Iceberg 1.8）
  00_setup.sql               命名空间
  01_ods.sql                 ODS 原始事件表（分区 biz_date × source_system）
  02_dwd.sql                 DWD 事件明细表（去重/乱序序号/代理键）
  03_dim_stg.sql             维表每日快照落地
  04_dim.sql                 网点/派件员/路由/客户 4 张 SCD2 拉链维表
  05_meta.sql                水位线 / DQ 结果 / 补数台账
  06_app_views.sql           应用层视图（含旧宽表兼容视图、压货、绩效、投诉归因）
  07_evolution.sql           Schema 演进规则参考（实际由作业幂等执行）
  08_legacy_source.sql       旧宽表迁移源登记
  09_timeliness.sql          双口径时效：口径版本台账/运单双口径事实/周期版本报表/并列视图
jobs/                        PySpark 作业
  ddl_init.py                一键建仓（幂等）
  ods_load.py                原始接入（原样保存、文件级幂等）
  dwd_detail_job.py          去重/乱序排序/重放/代理键解析
  dim_stg_load.py            维表快照落地
  dim_scd2_job.py            SCD2 拉链（--preview 出重算影响预览）
  backfill_job.py            补数（影子表+前后对比+受控切换/回滚）
  dq_check_job.py            五类质量指标按天×来源×分拨
  schema_evolution_job.py    只加列+历史回填（幂等）
  legacy_migrate_job.py      旧宽表时间列逆透视迁移
  timeliness_job.py          双口径时效：口径登记/事实计算/周期出具/口径版本切换
orchestration/dags/          Airflow DAG（每日增量、手动补数、时效治理手动 DAG）
docker/                      Dockerfile + entrypoint
docker-compose.yml           一键启动 Airflow+Spark
conf/pipeline.conf           全局配置（catalog、阈值、目录）
scripts/run_local.sh         本地无 Docker 一键验证
tests/                       夹具生成、19 项端到端断言（含 6 项双口径）、旧宽表迁移冒烟
docs/migration_guide.md      迁移步骤、执行顺序与回滚点（R0–R5）
```

## 3. 技术栈与 Catalog

- Spark 3.5（PySpark / Spark SQL）+ Iceberg 1.8（format-version=2，行级 MERGE/OVERWRITE）。
- Catalog（`conf/pipeline.conf` 的 `catalog.type`）：
  - **jdbc（本地默认）**：Iceberg JdbcCatalog + H2 文件库，免外部依赖且**支持视图**；
  - **hive（生产推荐）**：`catalog.uri=thrift://<metastore>:9083`；
  - **hadoop**：极简模式，元数据随表目录，不支持视图。
- 编排：Airflow 2.9（本地 standalone；生产可换 DolphinScheduler，作业命令不变）。

> 项目类型说明：本项目是**纯批处理数据仓库工程**，没有 HTTP 接口、没有常驻业务服务、
> 也没有前端页面（与需求"最终没有界面也没有常驻服务"一致），因此不提供
> OpenAPI/Swagger UI；对外的"服务"就是 Airflow 编排下的批处理作业与可查询的表/视图。

## 4. 一键启动（Docker，推荐）

前置：Docker 24+、Compose v2。

```bash
docker compose up -d --build
# Airflow UI: http://localhost:8080
```

容器首次启动自动执行 `ddl_init.py` 建仓。两个服务：
- `ltl-airflow`：Airflow standalone（含本地 Spark，DAG 通过 spark-submit 调作业）；
- `ltl-spark`（profiles=tools）：手动跑作业的一次性容器，例如
  ```bash
  docker compose run --rm ltl-spark \
    spark-submit /opt/ltl/jobs/ddl_init.py --conf /opt/ltl/conf/pipeline.conf
  ```

### 测试账号

| 系统 | 地址 | 用户名 | 密码 |
|---|---|---|---|
| Airflow UI | http://localhost:8080 | `ltl_admin` | `ltl_admin_2026` |

账号可通过 `.env` 或 compose 的 `AIRFLOW_USER/AIRFLOW_PASSWORD` 覆盖。
本项目无业务前端、无常驻 API 服务，因此不需要其他账号；Airflow 仅作编排器。

## 5. 本地开发与验证（无需 Docker）

前置：Python 3.11、JDK 17、Spark 3.5（脚本默认取 `/tmp/spark-3.5.9-bin-hadoop3`，
可用 `SPARK_HOME` 覆盖），Iceberg/H2 jar 已放入 `$SPARK_HOME/jars`（版本见 Dockerfile）。

```bash
python3 -m venv .venv && .venv/bin/pip install duckdb pytest
bash scripts/run_local.sh
```

该脚本会：生成夹具 → 建仓 → 两期维表拉链 → 四天事件增量 → DQ →
11-05 迟到事件补数 → 旧宽表迁移冒烟 → 双口径时效（口径登记、事实计算、
口径切换演练、周期报表重出）→ 跑 `pytest`。结束打印 `ALL LOCAL STEPS DONE` 与
`19 passed` 即通过。

也可单步执行（均为幂等）：

```bash
export SPARK_HOME=/path/to/spark PYTHONPATH=$PWD/jobs \
       PIPELINE_CONF=$PWD/conf/pipeline.local.conf
$SPARK_HOME/bin/spark-submit --master local[2] jobs/ddl_init.py            --conf $PIPELINE_CONF
$SPARK_HOME/bin/spark-submit --master local[2] jobs/dim_scd2_job.py        --conf $PIPELINE_CONF --snapshot-date 2026-11-02 --preview
$SPARK_HOME/bin/spark-submit --master local[2] jobs/backfill_job.py        --conf $PIPELINE_CONF --scope WAYBILL --waybill-nos WB006 --switch
```

## 6. 验证方式与覆盖点

`tests/test_pipeline.py` 的 13 项断言与 `tests/test_timeliness.py` 的 6 项断言
（共 19 项，已在本地 Spark+Iceberg 实际执行通过）覆盖：

1. ODS 原样保存重复上报与原始报文，不清洗不合并；
2. DWD 按 `event_id` 去重；
3. 乱序事件按**业务发生时间**而非上报时间生成连续 `event_seq`；
4. 多次异常独立保留，兼容视图末次原因取最后一次、异常链完整；
5. 缺失节点迟到补报后整单重放、序号重排、业务日期仍取事件时间；
6. SCD2 时点关联：改名/换片区/客户降级前后，历史事实关联到当时版本；合并撤销不物理删除；
7. 分拨压货 >48 小时可定位（`v_hub_dwell`，带当时网点名）；
8. 未知网点代理键 = -1；
9. DQ 五类指标齐全、可下钻到来源系统×分拨、超阈值带 breach；
10. 补数只动受影响运单，台账含分区清单/前后对比/差异，对比文件落盘；
11. 水位线按 ODS/DWD 分层按日推进；
12. Schema 只加列+回填，既有视图与报表不受影响、加列幂等；
13. 旧宽表兼容视图一行一运单、关键时间列与新列正确透视；
14. 双口径算术正确：内部=揽收→签收；客户=下单→送达并扣备货/改约，阈值与版本随事实快照；
15. 同一运单可同时"内部达标/客户投诉"，`v_timeliness_dual` 并列展示并给出差异来源；
16. 差异拆解四项分量之和恰好等于两口径时长差（可对账、可向客服解释）；
17. 口径切换只新增版本：旧版本闭合保留、11-02 及以前完结的运单仍挂旧版本，不回刷历史；
18. 周期报表追加 `report_version`，旧版 `SUPERSEDED` 且排名快照冻结，禁止新口径重排历史排名；
19. 口径登记/事实 MERGE 幂等，排名视图按口径版本隔离不跨版本混算。

另有 `tests/smoke_legacy_migration.py` 验证旧宽表逆透视迁移与重跑幂等。

> 说明：以上结论均来自本机实际执行（Spark 3.5.9 + Iceberg 1.8.1 + H2 JdbcCatalog）。
> 未在本环境构建/启动 Docker 镜像（沙箱无 Docker daemon），Dockerfile/compose 已按
> 相同版本与命令编写，需在具备 Docker 的环境执行第 4 节验证。

## 7. 双口径时效指标（内部考核 + 客户感知）

两套口径**并存、各自可解释、版本化治理**，建表与视图见 `sql/09_timeliness.sql`，
作业见 `jobs/timeliness_job.py`。

### 7.1 口径定义

| 维度 | 内部考核口径 `INTERNAL` | 客户感知口径 `CUSTOMER` |
|---|---|---|
| 计时起点 | 揽收扫描 `PICKUP` | 下单 `ORDER`（旧链路无 ORDER 时兜底 PICKUP，并打 `order_time_fallback`） |
| 计时终点 | 签收扫描 `SIGN` | 实际送达 `DELIVER`（缺失兜底 SIGN） |
| 扣减项 | 无 | 商家备货时长 `stockup_minutes` + 改约次数 × 单次扣减分钟 |
| 公式 | `SIGN - PICKUP` | `净时长 = (送达-下单) - 备货 - 改约次数×单次扣减` |
| 结论 | `internal_met`（时长 ≤ 阈值） | `customer_met` / `complaint_flag`（净时长 > 阈值即投诉） |

阈值与单次改约扣减在 `conf/pipeline.conf` 的 `timeliness.*` 配置，登记进
`meta.caliber_definition` 后由作业快照到每条事实，报表数字可回溯到当时口径。

### 7.2 并列展示与差异解释

- `app.timeliness_waybill_fact` 一行一运单，两套口径的锚点时间、毛/净时长、
  扣减项、阈值、达标结论同表保存；`app.v_timeliness_dual` 直接给出
  `dual_conclusion`（内部达标/客户投诉、双达标、双不达标等）。
- 夹具 WB010 即"同一运单同一天内部达标（2820m ≤ 2880m）且客户投诉
  （净 3300m > 3000m）"的实例。
- `diff_breakdown` 把两口径时长差拆成四个有符号分量：下单→揽收等待、
  送达→签收时差、备货扣减、改约扣减，分量之和恒等于 `diff_minutes`，可逐单对账。

### 7.3 版本治理纪律（强制）

1. **口径切换只增版本**：`register-version` 把旧版本 `effective_to` 闭合、
   `status=RETIRED`（行与原始阈值保留不删），再插入新版本；禁止 UPDATE 历史定义。
2. **历史结论不覆盖**：事实按运单完结日时点解析生效口径版本（严格
   `[effective_from, effective_to)`），并把版本号与阈值快照到事实行；
   新版本生效日之前完结的运单永远挂旧版本。
3. **周期内换口径必须重出并标注版本**：`issue-period` 对同一
   `period_id + caliber` 追加 `report_version`，旧版置 `SUPERSEDED`，
   `reissue_reason` 写明 `旧版本->新版本`；历史排名只存在 `rank_snapshot`
   JSON 快照里，**禁止用新口径回写或重排历史排名**。
4. 当期实时排名只能看 `v_timeliness_ranking_current`（显式带口径版本列），
   历史周期排名只能读周期报表快照。

常用命令：

```bash
# 一次性登记两套口径初始版本（容器首次启动自动执行，幂等）
spark-submit jobs/timeliness_job.py seed-calibers --conf $PIPELINE_CONF
# 每日按运单完结日计算双口径事实（已接入每日 DAG，MERGE 幂等）
spark-submit jobs/timeliness_job.py compute --biz-date 2026-11-03 --conf $PIPELINE_CONF
# 口径切换：客户口径 v2026.2 收紧阈值，11-03 起生效
spark-submit jobs/timeliness_job.py register-version --caliber CUSTOMER \
    --version v2026.2 --effective-from 2026-11-03 --threshold 1800 --reason "承诺时效收紧"
# 重出周期结果（追加版本，旧版 SUPERSEDED，排名快照冻结）
spark-submit jobs/timeliness_job.py issue-period --period-id 2026-11-W1 \
    --period-type WEEK --start 2026-11-01 --end 2026-11-05 --caliber BOTH \
    --reason "考核周期内客户口径切换，按新版本重新出具"
```

## 8. 关键作业口径速查

- **增量**：`meta.watermark_log` 记事件时间水位，每日推进；失败不写 SUCCESS、不推进。
- **补数**：默认只产出对比（`DIFF_READY`），加 `--switch` 才生效；
  全量回刷只能 `--full-rebuild --switch`（独立表重建+换表，旧表留 backup）。
- **维表变更**：先 `--preview` 看重算影响（历史不受影响行数），再正式拉链；
  调整只在快照日新分区生效。
- **质量阈值**：`conf/pipeline.conf` 中 `dq.threshold.*`。
- **双口径时效**：阈值/改约扣减在 `conf/pipeline.conf` 的 `timeliness.*`；
  口径切换只能 `register-version` 增版本（旧版 RETIRED 保留），周期重出走
  `issue-period` 追加 `report_version`，历史排名只读 `rank_snapshot`，
  禁止新口径重排（详见第 7 节）。
- **历史仓库升级**：重跑 `ddl_init.py` 补建新表/视图，再执行
  `schema_evolution_job.py --apply-builtin` 给 DWD 补 `stockup_minutes`、
  `reschedule_seq` 两列（历史回填 0），然后 `seed-calibers` + 逐日 `compute`。

迁移的完整执行顺序与回滚点见 **[docs/migration_guide.md](docs/migration_guide.md)**（R0–R5）。
