
CREATE TABLE IF NOT EXISTS amz.dwd_adv_sp_purchased_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    report_id STRING COMMENT '报告Id',
    report_type STRING COMMENT '报告类型',
    seller_id STRING COMMENT '卖家ID',
    report_date timestamp COMMENT '报告日期',
    data_last_update_time timestamp COMMENT '亚马逊数据生产时间',
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
    etl_data_dt timestamp COMMENT '数据加载日期'
    ) PARTITIONED BY (ds string)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊sp广告出单报告(增量表，每一天的数据在一个分区)')
    ;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
insert OVERWRITE table amz.dwd_adv_sp_purchased_ds partition (ds)
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
from amz.mid_amzn_sp_purchased_product_by_asin_report_ds
where ds is not null
;