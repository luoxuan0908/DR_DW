--@exclude_output=whde.dwd_amzn_sp_purchased_product_by_asin_report_ds
--@exclude_output=whde.dwd_mkt_adv_amazon_sp_purchased_ds
--@exclude_output=WHDE.dwd_amzn_sp_targeting_by_targeting_report_ds
--@exclude_output=whde.dwd_amzn_sp_campaigns_by_campaign_placement_report_ds
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 18:53:46
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_sp_search_term_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    market_place_id  STRING COMMENT '站点Id',
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
    keyword_id STRING COMMENT '关键词ID',
    keyword STRING COMMENT '关键词',
    keyword_type STRING COMMENT '关键词类型',
    keyword_bid DECIMAL(18,6) COMMENT '关键词竞价',
    match_type STRING COMMENT '匹配类型',
    targeting STRING COMMENT '投放词',
    ad_keyword_status STRING COMMENT '关键词状态.',
    search_term STRING COMMENT '搜索词',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost_per_click DECIMAL(18,6) COMMENT '平均每次点击花费',
    click_through_rate DECIMAL(18,6) COMMENT '点击除以曝光',
    cost DECIMAL(18,6) COMMENT '花费',
    w1d_purchases BIGINT COMMENT '广告点击后1天内发生的归因转化事件数量',
    w7d_purchases BIGINT COMMENT '广告点击后7天内发生的归因转化事件数量',
    w14d_purchases BIGINT COMMENT '广告点击后14天内发生的归因转化事件数量',
    w30d_purchases BIGINT COMMENT '广告点击后30天内发生的归因转化事件数量',
    w1d_purchases_same_sku BIGINT COMMENT '广告点击后1天内同sku发生的归因转化事件数量',
    w7d_purchases_same_sku BIGINT COMMENT '广告点击后7天内同sku发生的归因转化事件数量',
    w14d_purchases_same_sku BIGINT COMMENT '广告点击后14天内同sku发生的归因转化事件数量',
    w30d_purchases_same_sku BIGINT COMMENT '广告点击后30天内同sku发生的归因转化事件数量',
    w1d_units_sold_clicks BIGINT COMMENT '广告点击后的1天内订购的总单位数',
    w7d_units_sold_clicks BIGINT COMMENT '广告点击后的7天内订购的总单位数',
    w14d_units_sold_clicks BIGINT COMMENT '广告点击后的14天内订购的总单位数',
    w30d_units_sold_clicks BIGINT COMMENT '广告点击后的30天内订购的总单位数',
    w1d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的1天内订购的总销售额',
    w7d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的7天内订购的总销售额',
    w14d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的14天内订购的总销售额',
    w30d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的30天内订购的总销售额',
    w1d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的1天内同sku订购的总销售额',
    w7d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内同sku订购的总销售额',
    w14d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的14天内同sku订购的总销售额',
    w30d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的30天内同sku订购的总销售额',
    w1d_units_sold_same_sku BIGINT COMMENT '广告点击后的1天内同sku订购的总单位数',
    w7d_units_sold_same_sku BIGINT COMMENT '广告点击后的7天内同sku订购的总单位数',
    w14d_units_sold_same_sku BIGINT COMMENT '广告点击后的14天内同sku订购的总单位数',
    w30d_units_sold_same_sku BIGINT COMMENT '广告点击后的30天内同sku订购的总单位数',
    w14d_kindle_pages_read BIGINT COMMENT ' 广告点击后14天内归因的Kindle版标准化阅读页数',
    w14d_kindle_pages_royalties DECIMAL(18,6) COMMENT '点击广告后14天内归因的估计Kindle版标准化页面的预计版税',
    w7d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内其他sku订购的总销售额',
    w7d_units_sold_other_sku BIGINT COMMENT '广告点击后的7天内其他sku订购的总单位数',
    w7d_acos_clicks DECIMAL(18,6) COMMENT '基于广告点击后7天内的购买情况计算的广告销售成本',
    w14d_acos_clicks DECIMAL(18,6) COMMENT '基于广告点击后14天内的购买情况计算的广告销售成本',
    w7d_roas_clicks DECIMAL(18,6) COMMENT '广告点击后7天内产生的购买所得到的广告支出回报率',
    w14d_roas_clicks DECIMAL(18,6) COMMENT '广告点击后14天内产生的购买所得到的广告支出回报率',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    ds string
    )
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊sp广告搜索词报告(增量表，每一天的数据在一个分区)')
    LIFECYCLE 2000;

insert OVERWRITE table dwd_mkt_adv_amazon_sp_search_term_ds
select  tenant_id
     ,profile_id
     ,market_place_id
     ,report_id
     ,report_type
     ,seller_id
     ,report_date
     ,data_last_update_time
     ,country_code
     ,portfolio_id
     ,campaign_id
     ,campaign_name
     ,campaign_status
     ,campaign_budget_amt
     ,campaign_budget_type
     ,campaign_budget_currency_code
     ,ad_group_id
     ,ad_group_name
     ,keyword_id
     ,keyword
     ,keyword_type
     ,keyword_bid
     ,match_type
     ,targeting
     ,ad_keyword_status
     ,search_term
     ,impressions
     ,clicks
     ,cost_per_click
     ,click_through_rate
     ,cost
     ,purchases_1d
     ,purchases_7d
     ,purchases_14d
     ,purchases_30d
     ,purchases_same_sku_1d
     ,purchases_same_sku_7d
     ,purchases_same_sku_14d
     ,purchases_same_sku_30d
     ,units_sold_clicks_1d
     ,units_sold_clicks_7d
     ,units_sold_clicks_14d
     ,units_sold_clicks_30d
     ,sales_1d
     ,sales_7d
     ,sales_14d
     ,sales_30d
     ,attributed_sales_same_sku_1d
     ,attributed_sales_same_sku_7d
     ,attributed_sales_same_sku_14d
     ,attributed_sales_same_sku_30d
     ,units_sold_same_sku_1d
     ,units_sold_same_sku_7d
     ,units_sold_same_sku_14d
     ,units_sold_same_sku_30d
     ,kindle_edition_normalized_pages_read_14d
     ,kindle_edition_normalized_pages_royalties_14d
     ,sales_other_sku_7d
     ,units_sold_other_sku_7d
     ,acos_clicks_7d
     ,acos_clicks_14d
     ,roas_clicks_7d
     ,roas_clicks_14d
     ,etl_data_dt
     ,ds
from whde.dwd_amzn_sp_search_term_by_search_term_report_ds
where ds is not null
;


CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_sp_target_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
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
    keyword_id STRING COMMENT '关键词ID',
    keyword STRING COMMENT '关键词',
    keyword_type STRING COMMENT '关键词类型',
    keyword_bid DECIMAL(18,6) COMMENT '关键词竞价',
    match_type STRING COMMENT '匹配类型',
    targeting STRING COMMENT '投放词',
    ad_keyword_status STRING COMMENT '关键词状态.',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost_per_click DECIMAL(18,6) COMMENT '平均每次点击花费',
    click_through_rate DECIMAL(18,6) COMMENT '点击除以曝光',
    cost DECIMAL(18,6) COMMENT '花费',
    w1d_purchases BIGINT COMMENT '广告点击后1天内发生的归因转化事件数量',
    w7d_purchases BIGINT COMMENT '广告点击后7天内发生的归因转化事件数量',
    w14d_purchases BIGINT COMMENT '广告点击后14天内发生的归因转化事件数量',
    w30d_purchases BIGINT COMMENT '广告点击后30天内发生的归因转化事件数量',
    w1d_purchases_same_sku BIGINT COMMENT '广告点击后1天内同sku发生的归因转化事件数量',
    w7d_purchases_same_sku BIGINT COMMENT '广告点击后7天内同sku发生的归因转化事件数量',
    w14d_purchases_same_sku BIGINT COMMENT '广告点击后14天内同sku发生的归因转化事件数量',
    w30d_purchases_same_sku BIGINT COMMENT '广告点击后30天内同sku发生的归因转化事件数量',
    w1d_units_sold_clicks BIGINT COMMENT '广告点击后的1天内订购的总单位数',
    w7d_units_sold_clicks BIGINT COMMENT '广告点击后的7天内订购的总单位数',
    w14d_units_sold_clicks BIGINT COMMENT '广告点击后的14天内订购的总单位数',
    w30d_units_sold_clicks BIGINT COMMENT '广告点击后的30天内订购的总单位数',
    w1d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的1天内订购的总销售额',
    w7d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的7天内订购的总销售额',
    w14d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的14天内订购的总销售额',
    w30d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的30天内订购的总销售额',
    w1d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的1天内同sku订购的总销售额',
    w7d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内同sku订购的总销售额',
    w14d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的14天内同sku订购的总销售额',
    w30d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的30天内同sku订购的总销售额',
    w1d_units_sold_same_sku BIGINT COMMENT '广告点击后的1天内同sku订购的总单位数',
    w7d_units_sold_same_sku BIGINT COMMENT '广告点击后的7天内同sku订购的总单位数',
    w14d_units_sold_same_sku BIGINT COMMENT '广告点击后的14天内同sku订购的总单位数',
    w30d_units_sold_same_sku BIGINT COMMENT '广告点击后的30天内同sku订购的总单位数',
    w14d_kindle_pages_read BIGINT COMMENT ' 广告点击后14天内归因的Kindle版标准化阅读页数',
    w14d_kindle_pages_royalties DECIMAL(18,6) COMMENT '点击广告后14天内归因的估计Kindle版标准化页面的预计版税',
    w7d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内其他sku订购的总销售额',
    w7d_units_sold_other_sku BIGINT COMMENT '广告点击后的7天内其他sku订购的总单位数',
    w7d_acos_clicks DECIMAL(18,6) COMMENT '基于广告点击后7天内的购买情况计算的广告销售成本',
    w14d_acos_clicks DECIMAL(18,6) COMMENT '基于广告点击后14天内的购买情况计算的广告销售成本',
    w7d_roas_clicks DECIMAL(18,6) COMMENT '广告点击后7天内产生的购买所得到的广告支出回报率',
    w14d_roas_clicks DECIMAL(18,6) COMMENT '广告点击后14天内产生的购买所得到的广告支出回报率',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    --top_of_search_impression_share DECIMAL(18,6) COMMENT '搜索结果顶部展示的份额',
    ds string
    )
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊sp广告投放报告(增量表，每一天的数据在一个分区)')
    LIFECYCLE 2000;

insert OVERWRITE table  dwd_mkt_adv_amazon_sp_target_ds
SELECT
    tenant_id,
    profile_id,
    report_id,
    report_type,
    seller_id,
    report_date,
    data_last_update_time,
    country_code,
    portfolio_id,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_budget_amt,
    campaign_budget_type,
    campaign_budget_currency_code,
    ad_group_id,
    ad_group_name,
    keyword_id,
    keyword,
    keyword_type,
    keyword_bid,
    match_type,
    targeting,
    ad_keyword_status,
    impressions,
    clicks,
    cost_per_click,
    click_through_rate,
    cost,
    purchases_1d,
    purchases_7d,
    purchases_14d,
    purchases_30d,
    purchases_same_sku_1d,
    purchases_same_sku_7d,
    purchases_same_sku_14d,
    purchases_same_sku_30d,
    units_sold_clicks_1d,
    units_sold_clicks_7d,
    units_sold_clicks_14d,
    units_sold_clicks_30d,
    sales_1d,
    sales_7d,
    sales_14d,
    sales_30d,
    attributed_sales_same_sku_1d,
    attributed_sales_same_sku_7d,
    attributed_sales_same_sku_14d,
    attributed_sales_same_sku_30d,
    units_sold_same_sku_1d,
    units_sold_same_sku_7d,
    units_sold_same_sku_14d,
    units_sold_same_sku_30d,
    kindle_edition_normalized_pages_read_14d,
    kindle_edition_normalized_pages_royalties_14d,
    sales_other_sku_7d,
    units_sold_other_sku_7d,
    acos_clicks_7d,
    acos_clicks_14d,
    roas_clicks_7d,
    roas_clicks_14d,
    etl_data_dt,
    ds
from WHDE.dwd_amzn_sp_targeting_by_targeting_report_ds
where ds is not null
;


CREATE TABLE IF NOT EXISTS whde.dwd_amzn_sp_campaigns_by_campaign_placement_report_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    report_id STRING COMMENT '报告Id',
    report_type STRING COMMENT '报告类型',
    seller_id STRING COMMENT '卖家ID',
    report_date DATETIME COMMENT '报告日期',
    data_last_update_time DATETIME COMMENT '亚马逊数据生产时间',
    country_code STRING COMMENT '国家编码',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    campaign_status STRING COMMENT '广告活动状态',
    campaign_budget_amt DECIMAL(18,6) COMMENT '广告活动预算',
    campaign_budget_type STRING COMMENT '广告活动预算类型',
    campaign_budget_currency_code STRING COMMENT '广告活动预算币种',
    campaign_bidding_strategy STRING COMMENT '广告活动竞价策略',
    placement_classification STRING COMMENT '广告位',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost_per_click DECIMAL(18,6) COMMENT '平均每次点击花费',
    click_through_rate DECIMAL(18,6) COMMENT '点击除以曝光',
    cost DECIMAL(18,6) COMMENT '花费',
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
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊sp广告广告位报告(增量表，每一天的数据在一个分区)')
    LIFECYCLE 2000;


CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_sp_product_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    market_place_id STRING COMMENT '站点Id',
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
    w1d_purchases BIGINT COMMENT '广告点击后1天内发生的归因转化事件数量',
    w7d_purchases BIGINT COMMENT '广告点击后7天内发生的归因转化事件数量',
    w14d_purchases BIGINT COMMENT '广告点击后14天内发生的归因转化事件数量',
    w30d_purchases BIGINT COMMENT '广告点击后30天内发生的归因转化事件数量',
    w1d_purchases_same_sku BIGINT COMMENT '广告点击后1天内同sku发生的归因转化事件数量',
    w7d_purchases_same_sku BIGINT COMMENT '广告点击后7天内同sku发生的归因转化事件数量',
    w14d_purchases_same_sku BIGINT COMMENT '广告点击后14天内同sku发生的归因转化事件数量',
    w30d_purchases_same_sku BIGINT COMMENT '广告点击后30天内同sku发生的归因转化事件数量',
    w1d_units_sold_clicks BIGINT COMMENT '广告点击后的1天内订购的总单位数',
    w7d_units_sold_clicks BIGINT COMMENT '广告点击后的7天内订购的总单位数',
    w14d_units_sold_clicks BIGINT COMMENT '广告点击后的14天内订购的总单位数',
    w30d_units_sold_clicks BIGINT COMMENT '广告点击后的30天内订购的总单位数',
    w1d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的1天内订购的总销售额',
    w7d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的7天内订购的总销售额',
    w14d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的14天内订购的总销售额',
    w30d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的30天内订购的总销售额',
    w1d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的1天内同sku订购的总销售额',
    w7d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内同sku订购的总销售额',
    w14d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的14天内同sku订购的总销售额',
    w30d_sale_same_sku_amt DECIMAL(18,6) COMMENT '广告点击后的30天内同sku订购的总销售额',
    w1d_units_sold_same_sku BIGINT COMMENT '广告点击后的1天内同sku订购的总单位数',
    w7d_units_sold_same_sku BIGINT COMMENT '广告点击后的7天内同sku订购的总单位数',
    w14d_units_sold_same_sku BIGINT COMMENT '广告点击后的14天内同sku订购的总单位数',
    w30d_units_sold_same_sku BIGINT COMMENT '广告点击后的30天内同sku订购的总单位数',
    w14d_kindle_pages_read BIGINT COMMENT ' 广告点击后14天内归因的Kindle版标准化阅读页数',
    w14d_kindle_pages_royalties DECIMAL(18,6) COMMENT '点击广告后14天内归因的估计Kindle版标准化页面的预计版税',
    w7d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内其他sku订购的总销售额',
    w7d_units_sold_other_sku BIGINT COMMENT '广告点击后的7天内其他sku订购的总单位数',
    w7d_acos_clicks DECIMAL(18,6) COMMENT '基于广告点击后7天内的购买情况计算的广告销售成本',
    w14d_acos_clicks DECIMAL(18,6) COMMENT '基于广告点击后14天内的购买情况计算的广告销售成本',
    w7d_roas_clicks DECIMAL(18,6) COMMENT '广告点击后7天内产生的购买所得到的广告支出回报率',
    w14d_roas_clicks DECIMAL(18,6) COMMENT '广告点击后14天内产生的购买所得到的广告支出回报率',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    ds string

    )
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊sp广告推广品报告（增量表每一天的数据在一个分区）')
    LIFECYCLE 2000;

insert OVERWRITE table dwd_mkt_adv_amazon_sp_product_ds
select tenant_id
     ,profile_id
     ,market_place_id
     ,report_id
     ,report_type
     ,seller_id
     ,report_date
     ,data_last_update_time
     ,country_code
     ,portfolio_id
     ,campaign_id
     ,campaign_name
     ,campaign_status
     ,campaign_budget_amt
     ,campaign_budget_type
     ,campaign_budget_currency_code
     ,ad_group_id
     ,ad_group_name
     ,ad_id
     ,advertised_asin
     ,advertised_sku
     ,impressions
     ,clicks
     ,cost_per_click
     ,click_through_rate
     ,cost
     ,spend
     ,purchases_1d
     ,purchases_7d
     ,purchases_14d
     ,purchases_30d
     ,purchases_same_sku_1d
     ,purchases_same_sku_7d
     ,purchases_same_sku_14d
     ,purchases_same_sku_30d
     ,units_sold_clicks_1d
     ,units_sold_clicks_7d
     ,units_sold_clicks_14d
     ,units_sold_clicks_30d
     ,sales_1d
     ,sales_7d
     ,sales_14d
     ,sales_30d
     ,attributed_sales_same_sku_1d
     ,attributed_sales_same_sku_7d
     ,attributed_sales_same_sku_14d
     ,attributed_sales_same_sku_30d
     ,units_sold_same_sku_1d
     ,units_sold_same_sku_7d
     ,units_sold_same_sku_14d
     ,units_sold_same_sku_30d
     ,kindle_edition_normalized_pages_read_14d
     ,kindle_edition_normalized_pages_royalties_14d
     ,sales_other_sku_7d
     ,units_sold_other_sku_7d
     ,acos_clicks_7d
     ,acos_clicks_14d
     ,roas_clicks_7d
     ,roas_clicks_14d
     ,etl_data_dt
     ,ds
from  whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
where ds >= '20240101'
;

CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_sp_purchased_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    report_id STRING COMMENT '报告Id',
    report_type STRING COMMENT '报告类型',
    seller_id STRING COMMENT '卖家ID',
    report_date DATETIME COMMENT '报告日期',
    data_last_update_time DATETIME COMMENT '亚马逊数据生产时间',
    country_code STRING COMMENT '国家编码',
    portfolio_id STRING COMMENT 'portfolioID',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    ad_group_id STRING COMMENT '广告组ID',
    ad_group_name STRING COMMENT '广告组名称',
    campaign_budget_currency_code STRING COMMENT '广告活动预算币种',
    advertised_asin STRING COMMENT '广告asin',
    purchased_asin STRING COMMENT '购买asin',
    advertised_sku STRING COMMENT '广告sku',
    keyword_id STRING COMMENT '关键词ID',
    keyword STRING COMMENT '关键词',
    keyword_type STRING COMMENT '关键词类型',
    match_type STRING COMMENT '匹配类型',
    w1d_purchases BIGINT COMMENT '广告点击后1天内发生的归因转化事件数量',
    w7d_purchases BIGINT COMMENT '广告点击后7天内发生的归因转化事件数量',
    w14d_purchases BIGINT COMMENT '广告点击后14天内发生的归因转化事件数量',
    w30d_purchases BIGINT COMMENT '广告点击后30天内发生的归因转化事件数量',
    w1d_purchases_other_sku BIGINT COMMENT '广告点击后1天内其他sku发生的归因转化事件数量',
    w7d_purchases_other_sku BIGINT COMMENT '广告点击后7天内其他sku发生的归因转化事件数量',
    w14d_purchases_other_sku BIGINT COMMENT '广告点击后14天内其他sku发生的归因转化事件数量',
    w30d_purchases_other_sku BIGINT COMMENT '广告点击后30天内其他sku发生的归因转化事件数量',
    w1d_units_sold_clicks BIGINT COMMENT '广告点击后的1天内订购的总单位数',
    w7d_units_sold_clicks BIGINT COMMENT '广告点击后的7天内订购的总单位数',
    w14d_units_sold_clicks BIGINT COMMENT '广告点击后的14天内订购的总单位数',
    w30d_units_sold_clicks BIGINT COMMENT '广告点击后的30天内订购的总单位数',
    w1d_units_sold_other_sku BIGINT COMMENT '广告点击后的1天内其他sku订购的总单位数',
    w7d_units_sold_other_sku BIGINT COMMENT '广告点击后的7天内其他sku订购的总单位数',
    w14d_units_sold_other_sku BIGINT COMMENT '广告点击后的14天内其他sku订购的总单位数',
    w30d_units_sold_other_sku BIGINT COMMENT '广告点击后的30天内其他sku订购的总单位数',
    w1d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的1天内订购的总销售额',
    w7d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的7天内订购的总销售额',
    w14d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的14天内订购的总销售额',
    w30d_sale_amt DECIMAL(18,6) COMMENT '广告点击后的30天内订购的总销售额',
    w1d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的1天内其他sku订购的总销售额',
    w7d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的7天内其他sku订购的总销售额',
    w14d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的14天内其他sku订购的总销售额',
    w30d_sale_other_sku_amt DECIMAL(18,6) COMMENT '广告点击后的30天内其他sku订购的总销售额',
    w14d_kindle_pages_read BIGINT COMMENT ' 广告点击后14天内归因的Kindle版标准化阅读页数',
    w14d_kindle_pages_royalties DECIMAL(18,6) COMMENT '点击广告后14天内归因的估计Kindle版标准化页面的预计版税',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    ds string
    )
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊sp广告出单报告(增量表，每一天的数据在一个分区)')
    LIFECYCLE 2000;

insert OVERWRITE table dwd_mkt_adv_amazon_sp_purchased_ds
select   tenant_id
     ,profile_id
     ,report_id
     ,report_type
     ,seller_id
     ,report_date
     ,data_last_update_time
     ,country_code
     ,portfolio_id
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,campaign_budget_currency_code
     ,advertised_asin
     ,purchased_asin
     ,advertised_sku
     ,keyword_id
     ,keyword
     ,keyword_type
     ,match_type
     ,purchases_1d
     ,purchases_7d
     ,purchases_14d
     ,purchases_30d
     ,purchases_other_sku_1d
     ,purchases_other_sku_7d
     ,purchases_other_sku_14d
     ,purchases_other_sku_30d
     ,units_sold_clicks_1d
     ,units_sold_clicks_7d
     ,units_sold_clicks_14d
     ,units_sold_clicks_30d
     ,units_sold_other_sku_1d
     ,units_sold_other_sku_7d
     ,units_sold_other_sku_14d
     ,units_sold_other_sku_30d
     ,sales_1d
     ,sales_7d
     ,sales_14d
     ,sales_30d
     ,sales_other_sku_1d
     ,sales_other_sku_7d
     ,sales_other_sku_14d
     ,sales_other_sku_30d
     ,kindle_edition_normalized_pages_read_14d
     ,kindle_edition_normalized_pages_royalties_14d
     ,etl_data_dt
     ,ds
from whde.dwd_amzn_sp_purchased_product_by_asin_report_ds
where ds is not null
;
