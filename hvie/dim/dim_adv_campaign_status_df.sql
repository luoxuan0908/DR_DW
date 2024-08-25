-- drop table if exists amz.dim_adv_campaign_status_df;
CREATE TABLE IF NOT EXISTS amz.dim_adv_campaign_status_df
(
    tenant_id           STRING COMMENT '租户ID'
    ,profile_id         STRING COMMENT '配置ID'
    ,ad_type            STRING COMMENT '广告类型(SPONSORED_PRODUCTS,SPONSORED_BRANDS,SPONSORED_DISPLAY)'
    ,campaign_id        STRING COMMENT '广告活动ID'
    ,campaign_name      STRING COMMENT '广告活动名称'
    ,portfolio_id       STRING COMMENT '组合id'
    ,start_date         timestamp COMMENT '活动开始时间'
    ,end_date           timestamp COMMENT '活动结束时间'
    ,`status`           STRING COMMENT '广告活动状态（ENABLED, PAUSED, ARCHIVED）'
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
    ,create_datetime    timestamp COMMENT '创建时间'
    ,updated_datetime   timestamp COMMENT '更新时间'
    ,data_dt            STRING COMMENT '数据日期'
    ,etl_data_dt        timestamp COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds                  STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '广告活动最新状态表，全量日更新')
;

INSERT OVERWRITE TABLE amz.dim_adv_campaign_status_df PARTITION (ds = '20240823')
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
     ,'20240823' data_dt
     ,current_date() etl_data_dt
FROM    (
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
                 ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_type ORDER BY update_datetime DESC ) rn
            FROM    (
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
                             ,updated_datetime update_datetime
                        FROM    amz.dim_adv_campaign_status_df
                        WHERE   ds = '20240822'
                        UNION ALL
                        SELECT  distinct tenant_id
                                       ,profile_id
                                       ,ad_type
                                       ,campaign_id
                                       ,campaign_name
                                       ,portfolio_id
                                       ,null
                                       ,null
                                       ,status
                                       ,targeting_type
                                       , budget
                                       ,budget_type
                                       , effective_budget
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
                                       ,updated_datetime
                        FROM    (
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
                                         ,CAST(budget AS DECIMAL(18,6)) as budget
                                         ,budget_type
                                         ,CAST(effective_budget AS DECIMAL(18,6)) as effective_budget
                                         , rule_based_budget
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
                                         ,update_datetime as updated_datetime
                                         ,'report' data_src
                                         ,'amzn_ad_campaign_data' table_src
                                         ,'${bizdate}' data_dt
                                         ,current_date() etl_data_dt
                                    FROM    (
                                                SELECT  *
                                                FROM    ods.ods_report_amzn_ad_campaign_data_df
                                                WHERE   ds = '20240823' --SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)
                                            ) a
                                ) t1
                    ) t2
        ) t3
WHERE   rn = 1
;


select count(1) from amz.dim_adv_campaign_status_df WHERE   ds = '20240823'; -- 176
select * from amz.dim_adv_campaign_status_df WHERE   ds = '20240823';






SELECT
    COUNT(DISTINCT id) AS distinct_id,
    COUNT(DISTINCT tenant_id) AS distinct_tenant_id,
    COUNT(DISTINCT profile_id) AS distinct_profile_id,
    COUNT(DISTINCT ad_type) AS distinct_ad_type,
    COUNT(DISTINCT campaign_id) AS distinct_campaign_id,
    COUNT(DISTINCT campaign_name) AS distinct_campaign_name,
    COUNT(DISTINCT portfolio_id) AS distinct_portfolio_id,
    COUNT(DISTINCT start_date) AS distinct_start_date,
    COUNT(DISTINCT end_date) AS distinct_end_date,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(DISTINCT targeting_type) AS distinct_targeting_type,
    COUNT(DISTINCT budget) AS distinct_budget,
    COUNT(DISTINCT budget_type) AS distinct_budget_type,
    COUNT(DISTINCT effective_budget) AS distinct_effective_budget,
    COUNT(DISTINCT rule_based_budget) AS distinct_rule_based_budget,
    COUNT(DISTINCT dynamic_bidding) AS distinct_dynamic_bidding,
    COUNT(DISTINCT tags) AS distinct_tags,
    COUNT(DISTINCT tactic) AS distinct_tactic,
    COUNT(DISTINCT cost_type) AS distinct_cost_type,
    COUNT(DISTINCT delivery_profile) AS distinct_delivery_profile,
    COUNT(DISTINCT brand_entity_id) AS distinct_brand_entity_id,
    COUNT(DISTINCT if_multi_ad_groups) AS distinct_if_multi_ad_groups,
    COUNT(DISTINCT product_location) AS distinct_product_location,
    COUNT(DISTINCT serving_status) AS distinct_serving_status,
    COUNT(DISTINCT create_datetime) AS distinct_create_datetime,
    COUNT(DISTINCT update_datetime) AS distinct_update_datetime,
    COUNT(DISTINCT ds) AS distinct_pt
FROM ods.ods_report_amzn_ad_campaign_data_df
WHERE   ds = '20240823'