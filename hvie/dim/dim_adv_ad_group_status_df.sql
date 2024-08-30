--********************************************************************--
--author:luoxuan
--create time:2024-08-23
--********************************************************************--

--广告组最新状态表，日更新全量表
-- drop table if exists amz.dim_adv_ad_group_status_df;
CREATE TABLE IF NOT EXISTS amz.dim_adv_ad_group_status_df
(
    tenant_id        STRING COMMENT '租户ID'
    ,profile_id      STRING COMMENT '配置ID'
    ,campaign_id     STRING COMMENT '广告活动Id'
    ,ad_group_id     STRING COMMENT '广告组ID'
    ,ad_type         STRING COMMENT '广告类型'
    ,ad_group_name   STRING COMMENT '广告组名称'
    ,`status`        STRING COMMENT '广告组状态：ENABLE, PAUSED, ARCHIVED'
    ,default_bid     DECIMAL(18,6) COMMENT '默认竞价'
    ,tactic          STRING COMMENT '投放类型[T00020,T00030]'
    ,creative_type   STRING COMMENT '广告素材类型（VIDEO：视频,IMAGE：图片）'
    ,bid_type        STRING COMMENT '竞价类型[clicks, conversions, reach]'
    ,serving_status  STRING COMMENT '广告实时状态'
    ,create_datetime timestamp COMMENT '创建时间'
    ,update_datetime timestamp COMMENT '更新时间'
    ,data_dt         STRING COMMENT '数据日期'
    ,etl_data_dt     date COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds               STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '广告组最新状态表，日更新全量表')
;

INSERT OVERWRITE TABLE amz.dim_adv_ad_group_status_df PARTITION (ds = '20240827')
SELECT  tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,ad_type
     ,ad_group_name
     ,status
     ,default_bid
     ,tactic
     ,creative_type
     ,bid_type
     ,serving_status
     ,create_datetime
     ,update_datetime
     ,'20240827' data_dt
     ,current_date() etl_data_dt
FROM    (
            SELECT  tenant_id
                 ,profile_id
                 ,campaign_id
                 ,ad_group_id
                 ,ad_type
                 ,ad_group_name
                 ,status
                 ,default_bid
                 ,tactic
                 ,creative_type
                 ,bid_type
                 ,serving_status
                 ,create_datetime
                 ,update_datetime
                 ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,ad_type ORDER BY update_datetime DESC ) rn
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,campaign_id
                             ,ad_group_id
                             ,ad_type
                             ,ad_group_name
                             ,status
                             ,default_bid
                             ,tactic
                             ,creative_type
                             ,bid_type
                             ,serving_status
                             ,create_datetime
                             ,update_datetime
                        FROM    amz.dim_adv_ad_group_status_df
                        WHERE   ds = '20240826'
                        UNION ALL
                        SELECT distinct tenant_id
                                      ,profile_id
                                      ,campaign_id
                                      ,ad_group_id
                                      ,ad_type
                                      ,ad_group_name
                                      ,status
                                      ,cast(default_bid as  DECIMAL(18,6))
                                      ,tactic
                                      ,creative_type
                                      ,bid_type
                                      ,serving_status
                                      ,create_datetime
                                      ,update_datetime
                        FROM    ods.ods_report_amzn_ad_group_data_df
                        WHERE   ds ='20240827'
                    ) a
        ) b
WHERE   rn = 1
;

select count(1) from amz.dim_adv_ad_group_status_df where ds ='20240827'; -- 257


select * from amz.dim_adv_ad_group_status_df where ds ='20240827';