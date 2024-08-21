CREATE TABLE IF NOT EXISTS ad_group_tags (
   id STRING COMMENT '主键',
   ad_group_id STRING COMMENT '广告组id',
   ad_group_name STRING COMMENT '广告组名称',
   campaign_id STRING COMMENT '广告活动id',
   campaign_name STRING COMMENT '广告活动名称',
   tenant_id STRING COMMENT '租户id',
   profile_id STRING COMMENT '配置id',
   marketplace_id STRING COMMENT '站点id',
   marketplace_name STRING COMMENT '站点名称',
   seller_id STRING COMMENT '卖家id',
   seller_name STRING COMMENT '卖家名称',
   ad_type STRING COMMENT '广告类型',
   top_parent_asin STRING COMMENT '父asin',
   category STRING COMMENT '类目',
   impressions STRING COMMENT '曝光量',
   clicks STRING COMMENT '点击数',
   cost STRING COMMENT '广告花费',
   order_num STRING COMMENT '广告销售量',
   sale_amt STRING COMMENT '广告销售额',
   order_cnt STRING COMMENT '广告订单量',
   ctr STRING COMMENT 'CTR',
   cvr STRING COMMENT 'CVR',
   cpc STRING COMMENT 'CPC',
   cpa STRING COMMENT 'CPA',
   acos STRING COMMENT 'ACOS',
   tacos STRING COMMENT 'TACOS'
)
    COMMENT '广告组标签数据表'
    PARTITIONED BY (ds STRING COMMENT '数据日期')
    STORED AS orc;





SELECT
    a.id AS id,
    a.ad_group_id AS ad_group_id,
    a.ad_group_name AS ad_group_name,
    b.campaign_id AS campaign_id,
    b.campaign_name AS campaign_name,
    a.tenant_id AS tenant_id,
    a.profile_id AS profile_id,
    b.marketplace_id AS marketplace_id,
    b.country_code AS marketplace_name,
    b.seller_id AS seller_id,
    '' AS seller_name,
    a.ad_type AS ad_type,
    '' AS top_parent_asin,
    '' AS category,
    b.impressions AS impressions,
    b.clicks AS clicks,
    b.cost AS cost,
    b.purchases_1d AS order_num,
    b.sales_1d AS sale_amt,
    b.units_sold_clicks_1d AS order_cnt,
    b.click_through_rate AS ctr,
    '' AS cvr, -- CVR 需要计算或者从其他字段获取
    b.cost_per_click AS cpc,
    '' AS cpa, -- CPA 需要计算或者从其他字段获取
    '' AS acos, -- ACOS 需要计算或者从其他字段获取
    '' AS tacos, -- TACOS 需要计算或者从其他字段获取
    current_date() AS ds -- 数据日期
FROM
    amzn_ad_group_data a
        JOIN
    amzn_sp_campaigns_by_ad_group_report b
    ON
        a.ad_group_id = b.ad_group_id
            AND a.tenant_id = b.tenant_id
            AND a.profile_id = b.profile_id
            and a.campaign_id = b.campaign_id;

来源表：
-- auto-generated definition
create table amzn_ad_group_data
(
    id              int auto_increment
        primary key,
    tenant_id       varchar(32)  null,
    profile_id      varchar(32)  null,
    campaign_id     varchar(32)  null,
    ad_group_id     varchar(32)  null,
    ad_type         varchar(255) null,
    ad_group_name   varchar(255) null,
    status          varchar(255) null,
    default_bid     float        null,
    tactic          varchar(255) null,
    creative_type   varchar(255) null,
    bid_type        varchar(255) null,
    serving_status  varchar(255) null,
    create_datetime datetime     null,
    update_datetime datetime     null
)
    charset = utf8mb4;



-- auto-generated definition
create table amzn_sp_campaigns_by_ad_group_report
(
    id                                            int auto_increment
        primary key,
    store_id                                      varchar(128) null,
    marketplace_id                                varchar(128) null,
    profile_id                                    varchar(128) null,
    report_id                                     varchar(128) null,
    report_type                                   varchar(128) null,
    start_date                                    varchar(128) null,
    end_date                                      varchar(128) null,
    report_date                                   varchar(128) null,
    data_last_update_time                         varchar(128) null,
    seller_id                                     varchar(128) null,
    tenant_id                                     varchar(128) null,
    country_code                                  varchar(128) null,
    campaign_name                                 varchar(128) null,
    campaign_id                                   varchar(128) null,
    campaign_status                               varchar(128) null,
    campaign_budget_amount                        varchar(128) null,
    campaign_budget_type                          varchar(128) null,
    campaign_rule_based_budget_amount             varchar(128) null,
    campaign_applicable_budget_rule_id            varchar(128) null,
    campaign_applicable_budget_rule_name          varchar(128) null,
    campaign_budget_currency_code                 varchar(128) null,
    impressions                                   varchar(128) null,
    clicks                                        varchar(128) null,
    cost                                          varchar(128) null,
    spend                                         varchar(128) null,
    purchases_1d                                  varchar(128) null,
    purchases_7d                                  varchar(128) null,
    purchases_14d                                 varchar(128) null,
    purchases_30d                                 varchar(128) null,
    purchases_same_sku_1d                         varchar(128) null,
    purchases_same_sku_7d                         varchar(128) null,
    purchases_same_sku_14d                        varchar(128) null,
    purchases_same_sku_30d                        varchar(128) null,
    units_sold_clicks_1d                          varchar(128) null,
    units_sold_clicks_7d                          varchar(128) null,
    units_sold_clicks_14d                         varchar(128) null,
    units_sold_clicks_30d                         varchar(128) null,
    sales_1d                                      varchar(128) null,
    sales_7d                                      varchar(128) null,
    sales_14d                                     varchar(128) null,
    sales_30d                                     varchar(128) null,
    attributed_sales_same_sku_1d                  varchar(128) null,
    attributed_sales_same_sku_7d                  varchar(128) null,
    attributed_sales_same_sku_14d                 varchar(128) null,
    attributed_sales_same_sku_30d                 varchar(128) null,
    units_sold_same_sku_1d                        varchar(128) null,
    units_sold_same_sku_7d                        varchar(128) null,
    units_sold_same_sku_14d                       varchar(128) null,
    units_sold_same_sku_30d                       varchar(128) null,
    kindle_edition_normalized_pages_read_14d      varchar(128) null,
    kindle_edition_normalized_pages_royalties_14d varchar(128) null,
    campaign_bidding_strategy                     varchar(128) null,
    cost_per_click                                varchar(128) null,
    click_through_rate                            varchar(128) null,
    ad_group_name                                 varchar(128) null,
    ad_group_id                                   varchar(128) null,
    ad_status                                     varchar(128) null
)
    charset = utf8mb4;

根据来源表生成 ad_group_tags insert 语句