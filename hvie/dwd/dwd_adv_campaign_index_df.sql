drop table if exists amz.dwd_adv_campaign_index_df;
CREATE TABLE IF NOT EXISTS amz.dwd_adv_campaign_index_df (
    id STRING COMMENT '主键',
    campaign_id STRING COMMENT '广告活动id',
    campaign_name STRING COMMENT '广告活动名称',
    tenant_id STRING COMMENT '租户id',
    profile_id STRING COMMENT '配置id',
    marketplace_id STRING COMMENT '站点id',
    marketplace_name STRING COMMENT '站点名称',
    seller_id STRING COMMENT '卖家id',
    seller_name STRING COMMENT '卖家名称',
    ad_type STRING COMMENT '广告类型',
    targeting_type STRING COMMENT '投放类型',
    adv_days STRING COMMENT '广告天数',
    budget STRING COMMENT '广告预算',
    n7_camp_budget_lack_rate STRING COMMENT '预算缺失率 (近7天)',
    n7_max_budget STRING COMMENT '最大广告预算 (近7天)',
    n7_min_budget STRING COMMENT '最小广告预算 (近7天)',
    n7_avg_cost STRING COMMENT '平均广告花费 (近7天)',
    impressions STRING COMMENT '曝光量',
    clicks STRING COMMENT '点击数',
    cost STRING COMMENT '广告花费',
    order_num STRING COMMENT '广告销售量',
    sale_amt STRING COMMENT '广告销售额',
    ctr STRING COMMENT 'CTR',
    cvr STRING COMMENT 'CVR',
    cpc STRING COMMENT 'CPC',
    cpa STRING COMMENT 'CPA',
    acos STRING COMMENT 'ACOS',
    tacos STRING COMMENT 'TACOS'
)
    PARTITIONED BY (ds STRING COMMENT '数据日期（日）')
    STORED AS orc;




INSERT INTO amz.dwd_adv_campaign_index_df PARTITION (ds='20240821')
SELECT
    MD5(CONCAT(t1.tenant_id, '_', t1.profile_id, '_', t1.campaign_id))  AS id,
    t1.campaign_id AS campaign_id,
    t1.campaign_name AS campaign_name,
    t1.tenant_id AS tenant_id,
    t1.profile_id AS profile_id,
    t1.marketplace_id AS marketplace_id,
    CONCAT(t2.country_cn_name, '站') AS marketplace_name,
    t1.seller_id AS seller_id,
    t3.seller_name AS seller_name,
    t4.ad_type AS ad_type,
    t4.targeting_type AS targeting_type,
    t5.adv_days AS adv_days,
    t1.campaign_budget_amount AS budget,
    t6.n7_camp_budget_lack_rate AS n7_camp_budget_lack_rate,  -- 预算缺失率
    t6.n7_max_budget AS n7_max_budget,  -- 近7天最大广告预算
    t6.n7_min_budget AS n7_min_budget,  -- 近7天最小广告预算
    t6.n7_avg_cost AS n7_avg_cost,  -- 近7天平均广告花费
    t1.impressions AS impressions,
    t1.clicks AS clicks,
    t1.cost AS cost,
    t1.purchases_7d AS order_num,  -- 使用7天内购买量作为广告销售量
    t1.sales_7d AS sale_amt,  -- 使用7天内销售额作为广告销售额
    t1.click_through_rate AS ctr,
    CAST(CASE   WHEN clicks <> 0 THEN t1.purchases_7d / clicks
        END AS DECIMAL(18,6))  AS cvr,  -- CVR
    t1.cost_per_click AS cpc,
    case when t1.purchases_7d = 0 then null else t1.cost/t1.purchases_7d end as cpa,
    CAST(CASE   WHEN t1.sales_7d  <> 0 THEN t1.cost / t1.sales_7d
        END AS DECIMAL(18,6))  AS acos,  -- ACOS
    null as tacos
-- ,case when sale_amt_total >0 then t1.cost/sale_amt_total else null end tacos

FROM
    (
        select * from ods.ods_report_amzn_sp_campaigns_by_campaign_report_df where ds ='20240821'
    ) t1
        left join (
        SELECT *
        FROM amz.dim_base_marketplace_info_df
        WHERE ds = '20240821'
    ) t2 ON t1.marketplace_id = t2.market_place_id
        left join (
        SELECT *
        FROM ods.ods_report_authorization_account_df
        WHERE ds = '20240821'
    ) t3      ON t1.tenant_id = t3.tenant_id
        AND t1.seller_id = t3.seller_id
        AND t1.marketplace_id = t3.marketplace_id
        left join (
        SELECT *
        FROM ods.ods_report_amzn_ad_campaign_data_df
        WHERE ds = '20240821'
    ) t4 on t1.tenant_id = t4.tenant_id
        AND t1.profile_id = t4.profile_id
        AND t1. campaign_id  = t4.campaign_id
        left join (
        select
            t.tenant_id
             ,t.profile_id
             ,t.marketplace_id
             ,t.seller_id
             ,t.campaign_id
             ,t.campaign_name
             ,MIN(report_date) min_report_date
             ,DATEDIFF(CURRENT_DATE(),MIN(report_date) ) as adv_days
        from ods.ods_report_amzn_sp_campaigns_by_campaign_report_df t
        where ds ='20240813'
        group by       t.tenant_id
               ,t.profile_id
               ,t.marketplace_id
               ,t.seller_id
               ,t.campaign_id
               ,t.campaign_name
    ) t5  on t1.tenant_id = t5.tenant_id
        AND t1.profile_id = t5.profile_id
        AND t1. campaign_id  = t5.campaign_id
        left join (
        SELECT
            campaign_id,
            profile_id,
            tenant_id,
            -- 计算预算缺失率
            SUM(CASE WHEN campaign_budget_amount - cost <= 1 THEN 1 ELSE 0 END) / COUNT(1) AS n7_camp_budget_lack_rate,
            -- 计算近7天最大广告预算
            MAX(campaign_budget_amount) AS n7_max_budget,
            -- 计算近7天最小广告预算
            MIN(campaign_budget_amount) AS n7_min_budget,
            -- 计算近7天平均广告花费
            AVG(cost) AS n7_avg_cost
        FROM
            ods.ods_report_amzn_sp_advertised_product_by_advertiser_report_df --  dwd_amzn_sp_advertised_product_by_advertiser_report_ds
        WHERE  ds ='20240821' and
            report_date >= DATE_SUB(CURRENT_DATE, 7)  -- 限制数据为过去7天内
        GROUP BY
            campaign_id,
            profile_id,
            tenant_id

    ) t6 on t1.tenant_id = t6.tenant_id
        AND t1.profile_id = t6.profile_id
        AND t1. campaign_id  = t6.campaign_id ;