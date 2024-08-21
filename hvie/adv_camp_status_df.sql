CREATE TABLE IF NOT EXISTS adm_amazon_adv_camp_status_df
(
    tenant_id           STRING COMMENT '租户ID'
    ,profile_id         STRING COMMENT '配置ID'
    ,ad_type            STRING COMMENT '广告类型(SPONSORED_PRODUCTS,SPONSORED_BRANDS,SPONSORED_DISPLAY)'
    ,campaign_id        STRING COMMENT '广告活动ID'
    ,campaign_name      STRING COMMENT '广告活动名称'
    ,portfolio_id       STRING COMMENT '组合id'
    ,start_date         DATETIME COMMENT '活动开始时间'
    ,end_date           DATETIME COMMENT '活动结束时间'
    ,status           STRING COMMENT '广告活动状态（ENABLED, PAUSED, ARCHIVED）'
    ,targeting_type     STRING COMMENT '投放类型'
    ,budget             DECIMAL(18,6) COMMENT '广告预算'
    ,budget_type        STRING COMMENT '预算类型'
    ,effective_budget   DECIMAL(18,6) COMMENT '有效预算'
    ,rule_based_budget  STRING COMMENT '基础预算规则'
    ,dynamic_bidding    STRING COMMENT '对广告位的竞价调整配置'
    ,tags               STRING COMMENT '访问标签'
    ,tactic             STRING COMMENT '广告活动投放类型（T00020:图片,T00030:视频）'
    ,cost_type          STRING COMMENT '花费类型'
    ,delivery_profile   STRING COMMENT '配送配置'
    ,brand_entity_id    STRING COMMENT '品牌实体ID'
    ,if_multi_ad_groups BIGINT COMMENT '是否多广告组'
    ,product_location   STRING COMMENT '产品位置'
    ,serving_status     STRING COMMENT '广告活动实时状态'
    ,create_datetime    DATETIME COMMENT '创建时间'
    ,updated_datetime   DATETIME COMMENT '更新时间'
    ,data_dt            STRING COMMENT '数据日期'
    ,etl_data_dt        DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds                  STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告活动最新状态表，全量日更新')
    LIFECYCLE 365
;



SELECT  tenant_id
     ,profile_id
     ,ad_type
     ,campaign_id
     ,campaign_name
     ,portfolio_id
     ,start_date
     ,end_date
     ,status
     ,targeting_type
     ,budget
     ,budget_type
     ,effective_budget
     ,rule_based_budget
     ,dynamic_bidding
     ,tags
     ,tactic
     ,cost_type
     ,delivery_profile
     ,brand_entity_id
     ,if_multi_ad_groups
     ,product_location
     ,serving_status
     ,create_datetime
     ,update_datetime
from (
         SELECT  tenant_id
              ,profile_id
              ,ad_type
              ,campaign_id
              ,campaign_name
              ,portfolio_id
              ,start_date
              ,end_date
              ,status
              ,targeting_type
              ,CAST(budget AS DECIMAL(18,6)) as budget
              ,budget_type
              ,CAST(effective_budget AS DECIMAL(18,6)) as effective_budget
              ,CAST(rule_based_budget AS DECIMAL(18,6)) as rule_based_budget
              ,dynamic_bidding
              ,tags
              ,tactic
              ,cost_type
              ,delivery_profile
              ,brand_entity_id
              ,cast(case when if_multi_ad_groups = '' then null end as bigint) as if_multi_ad_groups
              ,product_location
              ,serving_status
              ,create_datetime
              ,update_datetime
              ,"report" data_src
              ,"amzn_ad_campaign_data" table_src
              ,'${bizdate}' data_dt
              ,CURRENT_DATE() etl_data_dt
              ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_type ORDER BY update_datetime DESC ) rn
         FROM    (
                     SELECT  *
                     FROM    ods.ods_report_amzn_ad_campaign_data_df
                     WHERE   ds = '20240813' --SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)
                 ) a


     ) t
where rn =1