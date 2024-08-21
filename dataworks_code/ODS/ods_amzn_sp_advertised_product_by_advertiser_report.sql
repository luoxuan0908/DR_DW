--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-24 19:57:16
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.ods_amzn_sp_advertised_product_by_advertiser_report(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    market_place_id STRING COMMENT '站点id',
    report_id STRING COMMENT '报告Id',
    report_type STRING COMMENT '报告类型',
    seller_id STRING COMMENT '卖家ID',
    report_date DATETIME COMMENT '报告日期',
    data_last_update_time DATETIME COMMENT '亚马逊数据生产时间',
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
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊sp广告推广品报告（数据分析请用ds增量表）')
    LIFECYCLE 30;



INSERT OVERWRITE TABLE ods_amzn_sp_advertised_product_by_advertiser_report PARTITION (ds = '${bizdate}')
SELECT      COALESCE(tenant_id,''),
            COALESCE(profile_id,''),
            COALESCE(market_place_id,''),
            COALESCE(report_id,''),
            COALESCE(report_type,''),
            COALESCE(seller_id,''),
            to_date(report_date,'yyyy-mm-dd'),
            to_date(data_last_update_time, 'yyyy-MM-dd HH:mi:ss')data_last_update_time,
            COALESCE(country_code,''),
            COALESCE(portfolio_id,''),
            COALESCE(campaign_id,''),
            COALESCE(campaign_name,''),
            COALESCE(campaign_status,''),
            COALESCE(cast(campaign_budget_amount as DECIMAL (18,6)),0),
            COALESCE(campaign_budget_type,''),
            COALESCE(campaign_budget_currency_code,''),
            COALESCE(ad_group_id,''),
            COALESCE(ad_group_name,''),
            COALESCE(ad_id,''),
            COALESCE(advertised_asin,''),
            COALESCE(advertised_sku,''),
            COALESCE(cast(impressions as bigint),0),
            COALESCE(cast(clicks as bigint),0),
            COALESCE(cast(cost_per_click as DECIMAL (18,6)),0),
            COALESCE(cast(click_through_rate as DECIMAL (18,6)),0),
            COALESCE(cast(cost as DECIMAL (18,6)),0),
            COALESCE(cast(spend as DECIMAL (18,6)),0),
            COALESCE(cast(purchases_1d as bigint),0),
            COALESCE(cast(purchases_7d as bigint),0),
            COALESCE(cast(purchases_14d as bigint),0),
            COALESCE(cast(purchases_30d as bigint),0),
            COALESCE(cast(purchases_same_sku_1d as bigint),0),
            COALESCE(cast(purchases_same_sku_7d as bigint),0),
            COALESCE(cast(purchases_same_sku_14d as bigint),0),
            COALESCE(cast(purchases_same_sku_30d as bigint),0),
            COALESCE(cast(units_sold_clicks_1d as bigint),0),
            COALESCE(cast(units_sold_clicks_7d as bigint),0),
            COALESCE(cast(units_sold_clicks_14d as bigint),0),
            COALESCE(cast(units_sold_clicks_30d as bigint),0),
            COALESCE(cast(sales_1d as DECIMAL (18,6)),0),
            COALESCE(cast(sales_7d as DECIMAL (18,6)),0),
            COALESCE(cast(sales_14d as DECIMAL (18,6)),0),
            COALESCE(cast(sales_30d as DECIMAL (18,6)),0),
            COALESCE(cast(attributed_sales_same_sku_1d as DECIMAL (18,6)),0),
            COALESCE(cast(attributed_sales_same_sku_7d as DECIMAL (18,6)),0),
            COALESCE(cast(attributed_sales_same_sku_14d as DECIMAL (18,6)),0),
            COALESCE(cast(attributed_sales_same_sku_30d as DECIMAL (18,6)),0),
            COALESCE(cast(units_sold_same_sku_1d as bigint),0),
            COALESCE(cast(units_sold_same_sku_7d as bigint),0),
            COALESCE(cast(units_sold_same_sku_14d as bigint),0),
            COALESCE(cast(units_sold_same_sku_30d as bigint),0),
            COALESCE(cast(kindle_edition_normalized_pages_read_14d as bigint),0),
            COALESCE(cast(kindle_edition_normalized_pages_royalties_14d as DECIMAL (18,6)),0),
            COALESCE(cast(sales_other_sku_7d as DECIMAL (18,6)),0),
            COALESCE(cast(units_sold_other_sku_7d as BIGINT ),0),
            COALESCE(cast(acos_clicks_7d as DECIMAL (18,6)),0),
            COALESCE(cast(acos_clicks_14d as DECIMAL (18,6)),0),
            COALESCE(cast(roas_clicks_7d as DECIMAL (18,6)),0),
            COALESCE(cast(roas_clicks_14d as DECIMAL (18,6)),0),
            "adv_report" data_src,
            "amzn_sp_advertised_product_by_advertiser_report" table_src,
            '${bizdate}' data_dt,
            GETDATE()  etl_data_dt
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
                ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,ad_id,advertised_sku,report_date ORDER BY pt DESC ) AS rn
        FROM    whde.amzn_sp_advertised_product_by_advertiser_report
        WHERE   pt = '${bizdate}'
    ) s
WHERE rn=1
;

