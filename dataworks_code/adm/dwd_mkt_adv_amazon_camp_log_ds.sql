--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-28 00:19:05
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_camp_log_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID',
    ad_type STRING COMMENT '广告类型(SPONSORED_PRODUCTS,SPONSORED_BRANDS,SPONSORED_DISPLAY)',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    portfolio_id STRING COMMENT '组合id',
    start_date DATETIME COMMENT '活动开始时间',
    end_date DATETIME COMMENT '活动结束时间',
    `status` STRING COMMENT '广告活动状态（enabled, paused, archived）',
    targeting_type STRING COMMENT '投放类型',
    budget DECIMAL(18,6) COMMENT '广告预算',
    budget_type STRING COMMENT '预算类型',
    effective_budget DECIMAL(18,6) COMMENT '有效预算',
    rule_based_budget STRING COMMENT '基础预算规则',
    dynamic_bidding STRING COMMENT '对广告位的竞价调整配置',
    tags STRING COMMENT '访问标签',
    tactic STRING COMMENT '广告活动投放类型（T00020:图片,T00030:视频）',
    cost_type STRING COMMENT '花费类型',
    delivery_profile STRING COMMENT '配送配置',
    brand_entity_id STRING COMMENT '品牌实体ID',
    if_multi_ad_groups BIGINT COMMENT '是否多广告组',
    product_location STRING COMMENT '产品位置',
    serving_status STRING COMMENT '广告活动实时状态',
    create_datetime DATETIME COMMENT '广告活动创建时间',
    updated_datetime DATETIME COMMENT '广告活动更新时间',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告活动操作日志记录表')
    LIFECYCLE 2000;

INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_camp_log_ds PARTITION (ds = '${bizdate}')
SELECT  tenant_id
     ,profile_id
     ,ad_type
     ,campaign_id
     ,campaign_name
     ,portfolio_id
     ,null
     ,null
     ,status
     ,targeting_type
     ,CAST(budget AS DECIMAL(18,6))
     ,budget_type
     ,CAST(effective_budget AS DECIMAL(18,6))
     ,CAST(rule_based_budget AS DECIMAL(18,6))
     ,dynamic_bidding
     ,tags
     ,tactic
     ,cost_type
     ,delivery_profile
     ,brand_entity_id
     ,cast(case when if_multi_ad_groups = '' then null end as bigint)
     ,product_location
     ,serving_status
     ,create_datetime
     ,update_datetime
     ,"report" data_src
     ,"amzn_ad_campaign_data" table_src
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
FROM    (
            SELECT  *
                 ,to_date(create_datetime ,'yyyy-mm-dd hh:mi:ss') creation_date
                 ,to_date(update_datetime ,'yyyy-mm-dd hh:mi:ss')  last_updated_date
            --,SUBSTR(hs,1,8) ds
            FROM    whde.amzn_ad_campaign_data
            WHERE   pt = '${bizdate}' --SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)
        ) a
;
