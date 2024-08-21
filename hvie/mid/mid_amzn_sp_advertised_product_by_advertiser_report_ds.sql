-- drop table if EXISTS  amz.mid_amzn_sp_advertised_product_by_advertiser_report_ds;
CREATE TABLE IF NOT EXISTS amz.mid_amzn_sp_advertised_product_by_advertiser_report_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    market_place_id STRING COMMENT '站点Id',
    report_id STRING COMMENT '报告Id',
    report_type STRING COMMENT '报告类型',
    seller_id STRING COMMENT '卖家ID',
    report_date timestamp COMMENT '报告日期',
    data_last_update_time timestamp COMMENT '亚马逊数据生产时间',
    country_code STRING COMMENT '国家编码',
    portfolio_id STRING COMMENT 'portfolioID',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    campaign_status STRING COMMENT '广告活动状态',
    campaign_budget_amt DECIMAL(18,6) COMMENT '广告活动预算',
    campaign_budget_type STRING COMMENT '广告活动预算类型',
    campaign_budget_currency_code STRING COMMENT '广告活动预算币种',
    ad_group_id STRING COMMENT '广告组ID',
    ad_group_name STRING COMMENT '广告组名称',
    ad_id STRING COMMENT '广告ID',
    advertised_asin STRING COMMENT '广告asin',
    advertised_sku STRING COMMENT '广告sku',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost_per_click DECIMAL(18,6) COMMENT '平均每次点击花费',
    click_through_rate DECIMAL(18,6) COMMENT '点击除以曝光',
    cost DECIMAL(18,6) COMMENT '花费',
    spend DECIMAL(18,6) COMMENT '花费(与cost一样)',
    purchases_1d BIGINT COMMENT '广告点击后1天内发生的归因转化事件数量',
    purchases_7d BIGINT COMMENT '广告点击后7天内发生的归因转化事件数量',
    purchases_14d BIGINT COMMENT '广告点击后14天内发生的归因转化事件数量',
    purchases_30d BIGINT COMMENT '广告点击后30天内发生的归因转化事件数量',
    purchases_same_sku_1d BIGINT COMMENT '广告点击后1天内同sku发生的归因转化事件数量',
    purchases_same_sku_7d BIGINT COMMENT '广告点击后7天内同sku发生的归因转化事件数量',
    purchases_same_sku_14d BIGINT COMMENT '广告点击后14天内同sku发生的归因转化事件数量',
    purchases_same_sku_30d BIGINT COMMENT '广告点击后30天内同sku发生的归因转化事件数量',
    units_sold_clicks_1d BIGINT COMMENT '广告点击后的1天内订购的总单位数',
    units_sold_clicks_7d BIGINT COMMENT '广告点击后的7天内订购的总单位数',
    units_sold_clicks_14d BIGINT COMMENT '广告点击后的14天内订购的总单位数',
    units_sold_clicks_30d BIGINT COMMENT '广告点击后的30天内订购的总单位数',
    sales_1d DECIMAL(18,6) COMMENT '广告点击后的1天内订购的总销售额',
    sales_7d DECIMAL(18,6) COMMENT '广告点击后的7天内订购的总销售额',
    sales_14d DECIMAL(18,6) COMMENT '广告点击后的14天内订购的总销售额',
    sales_30d DECIMAL(18,6) COMMENT '广告点击后的30天内订购的总销售额',
    attributed_sales_same_sku_1d DECIMAL(18,6) COMMENT '广告点击后的1天内同sku订购的总销售额',
    attributed_sales_same_sku_7d DECIMAL(18,6) COMMENT '广告点击后的7天内同sku订购的总销售额',
    attributed_sales_same_sku_14d DECIMAL(18,6) COMMENT '广告点击后的14天内同sku订购的总销售额',
    attributed_sales_same_sku_30d DECIMAL(18,6) COMMENT '广告点击后的30天内同sku订购的总销售额',
    units_sold_same_sku_1d BIGINT COMMENT '广告点击后的1天内同sku订购的总单位数',
    units_sold_same_sku_7d BIGINT COMMENT '广告点击后的7天内同sku订购的总单位数',
    units_sold_same_sku_14d BIGINT COMMENT '广告点击后的14天内同sku订购的总单位数',
    units_sold_same_sku_30d BIGINT COMMENT '广告点击后的30天内同sku订购的总单位数',
    kindle_edition_normalized_pages_read_14d BIGINT COMMENT ' 广告点击后14天内归因的Kindle版标准化阅读页数',
    kindle_edition_normalized_pages_royalties_14d DECIMAL(18,6) COMMENT '点击广告后14天内归因的估计Kindle版标准化页面的预计版税',
    sales_other_sku_7d DECIMAL(18,6) COMMENT '广告点击后的7天内其他sku订购的总销售额',
    units_sold_other_sku_7d BIGINT COMMENT '广告点击后的7天内其他sku订购的总单位数',
    acos_clicks_7d DECIMAL(18,6) COMMENT '基于广告点击后7天内的购买情况计算的广告销售成本',
    acos_clicks_14d DECIMAL(18,6) COMMENT '基于广告点击后14天内的购买情况计算的广告销售成本',
    roas_clicks_7d DECIMAL(18,6) COMMENT '广告点击后7天内产生的购买所得到的广告支出回报率',
    roas_clicks_14d DECIMAL(18,6) COMMENT '广告点击后14天内产生的购买所得到的广告支出回报率',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊sp广告推广品报告 数据分析请用ds增量表 ')
;

-- 设置 Hive 动态分区的参数
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;

-- Step 1: 计算 n_lag 并存储到临时表
WITH dwd_adv_sp_advertised_product_by_advertiser_report_df_temp as (
    SELECT
        COALESCE(tenant_id, '') AS tenant_id,
        COALESCE(profile_id, '') AS profile_id,
        COALESCE(market_place_id, '') AS market_place_id,
        COALESCE(report_id, '') AS report_id,
        COALESCE(report_type, '') AS report_type,
        COALESCE(seller_id, '') AS seller_id,
        FROM_UNIXTIME(UNIX_TIMESTAMP(report_date, 'yyyy-MM-dd')) AS report_date,
        FROM_UNIXTIME(UNIX_TIMESTAMP(data_last_update_time, 'yyyy-MM-dd HH:mm:ss')) AS data_last_update_time,
        COALESCE(country_code, '') AS country_code,
        COALESCE(portfolio_id, '') AS portfolio_id,
        COALESCE(campaign_id, '') AS campaign_id,
        COALESCE(campaign_name, '') AS campaign_name,
        COALESCE(campaign_status, '') AS campaign_status,
        COALESCE(CAST(campaign_budget_amount AS DECIMAL(18,6)), 0) AS campaign_budget_amt,
        COALESCE(campaign_budget_type, '') AS campaign_budget_type,
        COALESCE(campaign_budget_currency_code, '') AS campaign_budget_currency_code,
        COALESCE(ad_group_id, '') AS ad_group_id,
        COALESCE(ad_group_name, '') AS ad_group_name,
        COALESCE(ad_id, '') AS ad_id,
        COALESCE(advertised_asin, '') AS advertised_asin,
        COALESCE(advertised_sku, '') AS advertised_sku,
        COALESCE(CAST(impressions AS BIGINT), 0) AS impressions,
        COALESCE(CAST(clicks AS BIGINT), 0) AS clicks,
        COALESCE(CAST(cost_per_click AS DECIMAL(18,6)), 0) AS cost_per_click,
        COALESCE(CAST(click_through_rate AS DECIMAL(18,6)), 0) AS click_through_rate,
        COALESCE(CAST(cost AS DECIMAL(18,6)), 0) AS cost,
        COALESCE(CAST(spend AS DECIMAL(18,6)), 0) AS spend,
        COALESCE(CAST(purchases_1d AS BIGINT), 0) AS purchases_1d,
        COALESCE(CAST(purchases_7d AS BIGINT), 0) AS purchases_7d,
        COALESCE(CAST(purchases_14d AS BIGINT), 0) AS purchases_14d,
        COALESCE(CAST(purchases_30d AS BIGINT), 0) AS purchases_30d,
        COALESCE(CAST(purchases_same_sku_1d AS BIGINT), 0) AS purchases_same_sku_1d,
        COALESCE(CAST(purchases_same_sku_7d AS BIGINT), 0) AS purchases_same_sku_7d,
        COALESCE(CAST(purchases_same_sku_14d AS BIGINT), 0) AS purchases_same_sku_14d,
        COALESCE(CAST(purchases_same_sku_30d AS BIGINT), 0) AS purchases_same_sku_30d,
        COALESCE(CAST(units_sold_clicks_1d AS BIGINT), 0) AS units_sold_clicks_1d,
        COALESCE(CAST(units_sold_clicks_7d AS BIGINT), 0) AS units_sold_clicks_7d,
        COALESCE(CAST(units_sold_clicks_14d AS BIGINT), 0) AS units_sold_clicks_14d,
        COALESCE(CAST(units_sold_clicks_30d AS BIGINT), 0) AS units_sold_clicks_30d,
        COALESCE(CAST(sales_1d AS DECIMAL(18,6)), 0) AS sales_1d,
        COALESCE(CAST(sales_7d AS DECIMAL(18,6)), 0) AS sales_7d,
        COALESCE(CAST(sales_14d AS DECIMAL(18,6)), 0) AS sales_14d,
        COALESCE(CAST(sales_30d AS DECIMAL(18,6)), 0) AS sales_30d,
        COALESCE(CAST(attributed_sales_same_sku_1d AS DECIMAL(18,6)), 0) AS attributed_sales_same_sku_1d,
        COALESCE(CAST(attributed_sales_same_sku_7d AS DECIMAL(18,6)), 0) AS attributed_sales_same_sku_7d,
        COALESCE(CAST(attributed_sales_same_sku_14d AS DECIMAL(18,6)), 0) AS attributed_sales_same_sku_14d,
        COALESCE(CAST(attributed_sales_same_sku_30d AS DECIMAL(18,6)), 0) AS attributed_sales_same_sku_30d,
        COALESCE(CAST(units_sold_same_sku_1d AS BIGINT), 0) AS units_sold_same_sku_1d,
        COALESCE(CAST(units_sold_same_sku_7d AS BIGINT), 0) AS units_sold_same_sku_7d,
        COALESCE(CAST(units_sold_same_sku_14d AS BIGINT), 0) AS units_sold_same_sku_14d,
        COALESCE(CAST(units_sold_same_sku_30d AS BIGINT), 0) AS units_sold_same_sku_30d,
        COALESCE(CAST(kindle_edition_normalized_pages_read_14d AS BIGINT), 0) AS kindle_edition_normalized_pages_read_14d,
        COALESCE(CAST(kindle_edition_normalized_pages_royalties_14d AS DECIMAL(18,6)), 0) AS kindle_edition_normalized_pages_royalties_14d,
        COALESCE(CAST(sales_other_sku_7d AS DECIMAL(18,6)), 0) AS sales_other_sku_7d,
        COALESCE(CAST(units_sold_other_sku_7d AS BIGINT), 0) AS units_sold_other_sku_7d,
        COALESCE(CAST(acos_clicks_7d AS DECIMAL(18,6)), 0) AS acos_clicks_7d,
        COALESCE(CAST(acos_clicks_14d AS DECIMAL(18,6)), 0) AS acos_clicks_14d,
        COALESCE(CAST(roas_clicks_7d AS DECIMAL(18,6)), 0) AS roas_clicks_7d,
        COALESCE(CAST(roas_clicks_14d AS DECIMAL(18,6)), 0) AS roas_clicks_14d,
        'adv_report' AS data_src,
        'amzn_sp_advertised_product_by_advertiser_report' AS table_src,
        CURRENT_DATE() AS etl_data_dt,
        ds AS ds
FROM
        (
            SELECT  case when profile_id = 'null' then null else profile_id end as profile_id,
                    case when report_id = 'null' then null else report_id end as report_id,
                    case when marketplace_id = 'null' then null else marketplace_id end as market_place_id,
                    case when report_type = 'null' then null else report_type end as report_type,
                    case when report_date = 'null' then null else report_date end as report_date,
                    case when data_last_update_time = 'null' then null else data_last_update_time end as data_last_update_time,
                    case when seller_id = 'null' then null else seller_id end as seller_id,
                    case when tenant_id = 'null' then null else tenant_id end as tenant_id,
                    case when country_code = 'null' then null else country_code end as country_code,
                    case when portfolio_id = 'null' then null else portfolio_id end as portfolio_id,
                    case when campaign_name = 'null' then null else campaign_name end as campaign_name,
                    case when campaign_id = 'null' then null else campaign_id end as campaign_id,
                    case when campaign_status = 'null' then null else campaign_status end as campaign_status,
                    case when campaign_budget_amount = 'null' then null else campaign_budget_amount end as campaign_budget_amount,
                    case when campaign_budget_type = 'null' then null else campaign_budget_type end as campaign_budget_type,
                    case when campaign_budget_currency_code = 'null' then null else campaign_budget_currency_code end as campaign_budget_currency_code,
                    case when ad_group_name = 'null' then null else ad_group_name end as ad_group_name,
                    case when ad_group_id = 'null' then null else ad_group_id end as ad_group_id,
                    case when ad_id = 'null' then null else ad_id end as ad_id,
                    case when advertised_asin = 'null' then null else advertised_asin end as advertised_asin,
                    case when advertised_sku = 'null' then null else advertised_sku end as advertised_sku,
                    case when impressions = 'null' then null else impressions end as impressions,
                    case when clicks = 'null' then null else clicks end as clicks,
                    case when cost_per_click = 'null' then null else cost_per_click end as cost_per_click,
                    case when click_through_rate = 'null' then null else click_through_rate end as click_through_rate,
                    case when cost = 'null' then null else cost end as cost,
                    case when spend = 'null' then null else spend end as spend,
                    case when purchases_1d = 'null' then null else purchases_1d end as purchases_1d,
                    case when purchases_7d = 'null' then null else purchases_7d end as purchases_7d,
                    case when purchases_14d = 'null' then null else purchases_14d end as purchases_14d,
                    case when purchases_30d = 'null' then null else purchases_30d end as purchases_30d,
                    case when purchases_same_sku_1d = 'null' then null else purchases_same_sku_1d end as purchases_same_sku_1d,
                    case when purchases_same_sku_7d = 'null' then null else purchases_same_sku_7d end as purchases_same_sku_7d,
                    case when purchases_same_sku_14d = 'null' then null else purchases_same_sku_14d end as purchases_same_sku_14d,
                    case when purchases_same_sku_30d = 'null' then null else purchases_same_sku_30d end as purchases_same_sku_30d,
                    case when units_sold_clicks_1d = 'null' then null else units_sold_clicks_1d end as units_sold_clicks_1d,
                    case when units_sold_clicks_7d = 'null' then null else units_sold_clicks_7d end as units_sold_clicks_7d,
                    case when units_sold_clicks_14d = 'null' then null else units_sold_clicks_14d end as units_sold_clicks_14d,
                    case when units_sold_clicks_30d = 'null' then null else units_sold_clicks_30d end as units_sold_clicks_30d,
                    case when sales_1d = 'null' then null else sales_1d end as sales_1d,
                    case when sales_7d = 'null' then null else sales_7d end as sales_7d,
                    case when sales_14d = 'null' then null else sales_14d end as sales_14d,
                    case when sales_30d = 'null' then null else sales_30d end as sales_30d,
                    case when attributed_sales_same_sku_1d = 'null' then null else attributed_sales_same_sku_1d end as attributed_sales_same_sku_1d,
                    case when attributed_sales_same_sku_7d = 'null' then null else attributed_sales_same_sku_7d end as attributed_sales_same_sku_7d,
                    case when attributed_sales_same_sku_14d = 'null' then null else attributed_sales_same_sku_14d end as attributed_sales_same_sku_14d,
                    case when attributed_sales_same_sku_30d = 'null' then null else attributed_sales_same_sku_30d end as attributed_sales_same_sku_30d,
                    case when units_sold_same_sku_1d = 'null' then null else units_sold_same_sku_1d end as units_sold_same_sku_1d,
                    case when units_sold_same_sku_7d = 'null' then null else units_sold_same_sku_7d end as units_sold_same_sku_7d,
                    case when units_sold_same_sku_14d = 'null' then null else units_sold_same_sku_14d end as units_sold_same_sku_14d,
                    case when units_sold_same_sku_30d = 'null' then null else units_sold_same_sku_30d end as units_sold_same_sku_30d,
                    case when kindle_edition_normalized_pages_read_14d = 'null' then null else kindle_edition_normalized_pages_read_14d end as kindle_edition_normalized_pages_read_14d,
                    case when kindle_edition_normalized_pages_royalties_14d = 'null' then null else kindle_edition_normalized_pages_royalties_14d end as kindle_edition_normalized_pages_royalties_14d,
                    case when sales_other_sku_7d = 'null' then null else sales_other_sku_7d end as sales_other_sku_7d,
                    case when units_sold_other_sku_7d = 'null' then null else units_sold_other_sku_7d end as units_sold_other_sku_7d,
                    case when acos_clicks_7d = 'null' then null else acos_clicks_7d end as acos_clicks_7d,
                    case when acos_clicks_14d = 'null' then null else acos_clicks_14d end as acos_clicks_14d,
                    case when roas_clicks_7d = 'null' then null else roas_clicks_7d end as roas_clicks_7d,
                    case when roas_clicks_14d = 'null' then null else roas_clicks_14d end as roas_clicks_14d
                    ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,ad_id,advertised_sku,report_date ORDER BY ds DESC ) AS rn
                    ,ds
            FROM    ods.ods_report_amzn_sp_advertised_product_by_advertiser_report_df
            WHERE   ds = '${last_day}'
        ) s
    WHERE rn=1

),n_lag_table AS (
    SELECT
        CASE
            WHEN DATEDIFF(MAX(report_date), MIN(report_date)) <= ${report_days} THEN ${n_lag1}
            ELSE ${n_lag2}
            END AS n_lag
    FROM dwd_adv_sp_advertised_product_by_advertiser_report_df_temp
    WHERE ds = '${last_day}'
)

-- Step 2: 使用计算的 n_lag 插入数据到目标表
INSERT OVERWRITE TABLE amz.mid_amzn_sp_advertised_product_by_advertiser_report_ds  PARTITION (ds)
SELECT
    tenant_id, profile_id, market_place_id, report_id, report_type, seller_id, report_date, data_last_update_time, country_code, portfolio_id,
    campaign_id, campaign_name, campaign_status, campaign_budget_amt, campaign_budget_type, campaign_budget_currency_code, ad_group_id,
    ad_group_name, ad_id, advertised_asin, advertised_sku, impressions, clicks, cost_per_click, click_through_rate, cost, spend, purchases_1d,
    purchases_7d, purchases_14d, purchases_30d, purchases_same_sku_1d, purchases_same_sku_7d, purchases_same_sku_14d, purchases_same_sku_30d,
    units_sold_clicks_1d, units_sold_clicks_7d, units_sold_clicks_14d, units_sold_clicks_30d, sales_1d, sales_7d, sales_14d, sales_30d,
    attributed_sales_same_sku_1d, attributed_sales_same_sku_7d, attributed_sales_same_sku_14d, attributed_sales_same_sku_30d, units_sold_same_sku_1d,
    units_sold_same_sku_7d, units_sold_same_sku_14d, units_sold_same_sku_30d, kindle_edition_normalized_pages_read_14d, kindle_edition_normalized_pages_royalties_14d,
    sales_other_sku_7d, units_sold_other_sku_7d, acos_clicks_7d, acos_clicks_14d, roas_clicks_7d, roas_clicks_14d,data_src,table_src
    , current_date() AS etl_data_dt,
    date_format(report_date, 'yyyyMMdd') AS ds
FROM (
         SELECT
             tenant_id, profile_id, market_place_id, report_id, report_type, seller_id, report_date, data_last_update_time, country_code, portfolio_id,
             campaign_id, campaign_name, campaign_status, campaign_budget_amt, campaign_budget_type, campaign_budget_currency_code, ad_group_id,
             ad_group_name, ad_id, advertised_asin, advertised_sku, impressions, clicks, cost_per_click, click_through_rate, cost, spend, purchases_1d,
             purchases_7d, purchases_14d, purchases_30d, purchases_same_sku_1d, purchases_same_sku_7d, purchases_same_sku_14d, purchases_same_sku_30d,
             units_sold_clicks_1d, units_sold_clicks_7d, units_sold_clicks_14d, units_sold_clicks_30d, sales_1d, sales_7d, sales_14d, sales_30d,
             attributed_sales_same_sku_1d, attributed_sales_same_sku_7d, attributed_sales_same_sku_14d, attributed_sales_same_sku_30d, units_sold_same_sku_1d,
             units_sold_same_sku_7d, units_sold_same_sku_14d, units_sold_same_sku_30d, kindle_edition_normalized_pages_read_14d, kindle_edition_normalized_pages_royalties_14d,
             sales_other_sku_7d, units_sold_other_sku_7d, acos_clicks_7d, acos_clicks_14d, roas_clicks_7d, roas_clicks_14d,data_src,table_src,
             ROW_NUMBER() OVER (PARTITION BY tenant_id, profile_id, campaign_id, ad_group_id, ad_id, advertised_sku, report_date ORDER BY current_date() DESC) AS rn
         FROM dwd_adv_sp_advertised_product_by_advertiser_report_df_temp
                  CROSS JOIN n_lag_table
         WHERE ds >= date_format(date_sub(from_unixtime(unix_timestamp('${last_day}', 'yyyyMMdd'), 'yyyy-MM-dd'), n_lag_table.n_lag), 'yyyyMMdd')
     ) tmp
WHERE rn = 1;