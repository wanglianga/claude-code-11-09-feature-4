-- =====================================================================
-- 09_timeliness.sql  双口径时效指标（内部考核口径 + 客户感知口径）
--
-- 需求约束与本文件的对应机制：
--   1. 两套口径并存、各自可解释：
--      meta.caliber_definition 登记两套口径的"定义版本"（起止事件、扣减规则、
--      达标阈值、公式说明），任何时效结论都带 caliber_version，可回溯到当时定义。
--   2. 同一运单可同时内部达标、客户投诉：
--      app.timeliness_waybill_fact 一行一运单，同时保存两套口径的时长、扣减项、
--      阈值与达标结论；app.v_timeliness_dual 并列展示并给出差异来源拆解。
--   3. 口径切换不得覆盖历史结论：
--      口径切换只允许在 caliber_definition 中"新增版本"（旧版本闭合保留），
--      绝不 UPDATE/DELETE 历史定义；运单事实重算只改写事实表当前行，
--      周期结论以 meta.timeliness_period_report 的不可变快照为准。
--   4. 考核周期内换口径必须重新出具并标注版本，禁止用新口径重排历史排名：
--      issue-period 每次出具都追加新 report_version，旧版置 SUPERSEDED 并保留
--      当时冻结的 rank_snapshot 排名；历史排名只能读快照，不能按新口径重算。
-- 方言：Spark 3.5 SQL + Iceberg 1.8（format-version=2）。
-- =====================================================================

-- ---------- 1. 口径定义版本台账（口径治理的唯一事实来源） ----------
CREATE TABLE IF NOT EXISTS ltl.meta.caliber_definition (
    caliber_code              STRING    COMMENT '口径编码：INTERNAL内部考核 / CUSTOMER客户感知',
    caliber_version           STRING    COMMENT '口径版本号，如 v2026.1；同口径切换只新增版本',
    caliber_name              STRING    COMMENT '口径中文名',
    start_event               STRING    COMMENT '计时起点事件：INTERNAL=PICKUP / CUSTOMER=ORDER',
    end_event                 STRING    COMMENT '计时终点事件：INTERNAL=SIGN / CUSTOMER=DELIVER',
    end_event_fallback        STRING    COMMENT '终点缺失时的兜底事件（CUSTOMER 兜底 SIGN），无兜底为 NA',
    stockup_deduct_flag       BOOLEAN   COMMENT '是否扣除商家备货时长（仅 CUSTOMER=true）',
    reschedule_deduct_minutes BIGINT    COMMENT '每次客户改约的扣减分钟数；不改约或不扣减为 0',
    threshold_minutes         BIGINT    COMMENT '达标阈值（分钟），时长 <= 阈值判定达标',
    formula_desc              STRING    COMMENT '公式自然语言说明，面向业务/客服解释',
    effective_from            DATE      COMMENT '版本生效日（含）',
    effective_to              DATE      COMMENT '版本失效日（不含）；9999-12-31 表示当前开放版本',
    is_current                BOOLEAN   COMMENT '是否当前生效版本；同口径同时只有一个 true',
    status                    STRING    COMMENT 'ACTIVE 当前生效 / RETIRED 已停用（历史版本保留不删）',
    change_reason             STRING    COMMENT '本版本变更原因；首次登记为 INITIAL',
    ingest_batch_id           STRING,
    ingest_time               TIMESTAMP
)
USING iceberg
COMMENT '时效口径定义版本台账：口径切换只新增版本，旧版本闭合保留，永不覆盖历史定义'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 2. 运单双口径时效事实表（一行一运单，两套口径并列） ----------
CREATE TABLE IF NOT EXISTS ltl.app.timeliness_waybill_fact (
    waybill_no      STRING  COMMENT '运单号（MERGE 幂等键）',
    biz_date        DATE    COMMENT '运单完结业务日期=终态(SIGN)事件日，周期归属与分区键',
    customer_code   STRING  COMMENT '客户业务键',
    sign_hub_code   STRING  COMMENT '签收网点业务键（排名下钻维度）',

    -- 可解释锚点时间戳（报表可直接展示"从哪个时间算到哪个时间"）
    order_time            TIMESTAMP COMMENT '下单时间（ORDER 事件；缺失回退 PICKUP）',
    order_time_fallback   BOOLEAN   COMMENT '下单时间是否由 PICKUP 兜底（旧链路无 ORDER 时为 true）',
    pickup_time           TIMESTAMP COMMENT '揽收扫描时间（内部口径起点）',
    stockup_ready_time    TIMESTAMP COMMENT '商家备货完成时间（ORDER 时间+备货时长，仅供解释展示）',
    deliver_time          TIMESTAMP COMMENT '实际送达/派送时间（客户口径终点）',
    sign_time             TIMESTAMP COMMENT '签收扫描时间（内部口径终点）',
    stockup_minutes       BIGINT    COMMENT '商家备货时长（分钟，来自 ORDER 报文，客户口径扣减）',
    reschedule_seq        INT       COMMENT '客户改约次数（RESCHEDULE 事件计数，客户口径按次扣减）',

    -- 内部考核口径：揽收扫描 -> 签收扫描
    internal_caliber_version     STRING  COMMENT '计算时生效的内部口径版本',
    internal_duration_minutes    BIGINT  COMMENT '内部口径时长=SIGN-PICKUP（分钟）',
    internal_threshold_minutes   BIGINT  COMMENT '计算时内部口径阈值快照（分钟）',
    internal_met                 BOOLEAN COMMENT '内部口径是否达标',

    -- 客户感知口径：下单 -> 实际送达，扣备货时长与改约次数
    customer_caliber_version           STRING  COMMENT '计算时生效的客户口径版本',
    customer_gross_minutes             BIGINT  COMMENT '客户口径毛时长=送达-下单（分钟，未扣减）',
    customer_deduct_stockup_minutes    BIGINT  COMMENT '备货扣减分钟数',
    customer_deduct_reschedule_minutes BIGINT  COMMENT '改约扣减分钟数=改约次数×单次扣减',
    customer_net_minutes               BIGINT  COMMENT '客户口径净时长=毛时长-备货扣减-改约扣减',
    customer_threshold_minutes         BIGINT  COMMENT '计算时客户口径阈值快照（分钟）',
    customer_met                       BOOLEAN COMMENT '客户口径是否达标',
    complaint_flag                     BOOLEAN COMMENT '客户投诉标志=客户口径不达标',

    -- 两套口径差异的可解释拆解
    diff_minutes     BIGINT  COMMENT '差异分钟数=客户净时长-内部时长',
    diff_source      STRING  COMMENT '差异来源自然语言说明（等待/备货/改约/起止点差）',
    diff_breakdown   STRING  COMMENT '差异拆解 JSON（机器可读：各组件分钟数）',

    ingest_batch_id  STRING,
    ingest_time      TIMESTAMP
)
USING iceberg
PARTITIONED BY (biz_date)
COMMENT '运单双口径时效事实：内部/客户两套结论同表并列，扣减项与差异来源完整留痕'
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
);

-- ---------- 3. 周期考核报表（不可变版本快照；换口径重出只追加新版本） ----------
CREATE TABLE IF NOT EXISTS ltl.meta.timeliness_period_report (
    period_id            STRING  COMMENT '考核周期ID，如 2026-11-W1 / 2026-11',
    period_type          STRING  COMMENT 'WEEK/MONTH/DATE_RANGE',
    period_start         DATE    COMMENT '周期起（含）',
    period_end           DATE    COMMENT '周期止（含）',
    caliber_code         STRING  COMMENT 'INTERNAL / CUSTOMER',
    caliber_version      STRING  COMMENT '出具本报表时使用的口径版本（历史结论随版本冻结）',
    report_version       INT     COMMENT '同周期同口径的出具序号：首次=1，重出递增，绝不覆盖',
    supersedes_report_id STRING  COMMENT '本版替代的上一版报表ID；首版为 NA',
    report_status        STRING  COMMENT 'CURRENT 当前版 / SUPERSEDED 被新版替代（行保留不删）',
    total_waybills       BIGINT  COMMENT '周期内纳入考核运单数',
    met_waybills         BIGINT  COMMENT '达标运单数',
    timeliness_rate      DOUBLE  COMMENT '达标率=达标数/总数',
    avg_duration_minutes DOUBLE  COMMENT '平均时长（内部=内部时长，客户=客户净时长，分钟）',
    complaint_waybills   BIGINT  COMMENT '客户口径下投诉运单数；内部口径报表为 NULL',
    rank_snapshot        STRING  COMMENT '排名快照 JSON（出具时按客户/网点冻结，历史排名禁止重排）',
    reissue_reason       STRING  COMMENT '重出原因；首版为 INITIAL，换口径重出写明口径版本变更',
    issued_by            STRING  COMMENT '出具作业批次ID',
    ingest_batch_id      STRING,
    ingest_time          TIMESTAMP
)
USING iceberg
PARTITIONED BY (period_start)
COMMENT '时效周期考核报表：追加写版本快照，换口径重出标注版本，历史排名随快照冻结'
TBLPROPERTIES ('format-version' = '2');

-- ---------- 4. 应用层视图：双口径并列展示 + 差异结论 ----------
CREATE OR REPLACE VIEW ltl.app.v_timeliness_dual AS
SELECT
    waybill_no,
    biz_date,
    customer_code,
    sign_hub_code,
    -- 内部考核口径
    internal_caliber_version,
    internal_duration_minutes,
    internal_threshold_minutes,
    internal_met,
    -- 客户感知口径（毛时长、两项扣减、净时长）
    customer_caliber_version,
    customer_gross_minutes,
    customer_deduct_stockup_minutes,
    customer_deduct_reschedule_minutes,
    customer_net_minutes,
    customer_threshold_minutes,
    customer_met,
    complaint_flag,
    -- 差异来源
    diff_minutes,
    diff_source,
    diff_breakdown,
    -- 同单双结论：允许"内部达标 + 客户投诉"同时成立
    CASE
        WHEN internal_met = true  AND customer_met = false THEN '内部达标/客户投诉'
        WHEN internal_met = false AND customer_met = true  THEN '内部不达标/客户达标'
        WHEN internal_met = true  AND customer_met = true  THEN '双达标'
        ELSE '双不达标'
    END AS dual_conclusion
FROM ltl.app.timeliness_waybill_fact;

-- ---------- 5. 当前周期实时排名视图（仅用于当期；历史排名以周期报表快照为准） ----------
-- 显式标注口径版本列：下游必须带 caliber 版本过滤，杜绝跨版本混算。
-- 历史周期的排名禁止用本视图重算，只能读 timeliness_period_report.rank_snapshot。
CREATE OR REPLACE VIEW ltl.app.v_timeliness_ranking_current AS
SELECT
    'CUSTOMER' AS dim_type,
    customer_code AS dim_code,
    internal_caliber_version,
    customer_caliber_version,
    count(*) AS total_waybills,
    sum(CASE WHEN internal_met THEN 1 ELSE 0 END) AS internal_met_cnt,
    round(sum(CASE WHEN internal_met THEN 1 ELSE 0 END) / count(*), 6) AS internal_rate,
    sum(CASE WHEN customer_met THEN 1 ELSE 0 END) AS customer_met_cnt,
    round(sum(CASE WHEN customer_met THEN 1 ELSE 0 END) / count(*), 6) AS customer_rate,
    sum(CASE WHEN complaint_flag THEN 1 ELSE 0 END) AS complaint_cnt
FROM ltl.app.timeliness_waybill_fact
GROUP BY customer_code, internal_caliber_version, customer_caliber_version
UNION ALL
SELECT
    'HUB' AS dim_type,
    sign_hub_code AS dim_code,
    internal_caliber_version,
    customer_caliber_version,
    count(*) AS total_waybills,
    sum(CASE WHEN internal_met THEN 1 ELSE 0 END),
    round(sum(CASE WHEN internal_met THEN 1 ELSE 0 END) / count(*), 6),
    sum(CASE WHEN customer_met THEN 1 ELSE 0 END),
    round(sum(CASE WHEN customer_met THEN 1 ELSE 0 END) / count(*), 6),
    sum(CASE WHEN complaint_flag THEN 1 ELSE 0 END)
FROM ltl.app.timeliness_waybill_fact
GROUP BY sign_hub_code, internal_caliber_version, customer_caliber_version;

-- ---------- 6. 治理台账浏览视图 ----------
CREATE OR REPLACE VIEW ltl.app.v_caliber_definition AS
SELECT * FROM ltl.meta.caliber_definition;

CREATE OR REPLACE VIEW ltl.app.v_timeliness_period_report AS
SELECT * FROM ltl.meta.timeliness_period_report;
