--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:11:16
--********************************************************************--
CREATE TABLE IF NOT EXISTS adm_amazon_adv_ad_group_status_df
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
    ,create_datetime DATETIME COMMENT '创建时间'
    ,update_datetime DATETIME COMMENT '更新时间'
    ,data_dt         STRING COMMENT '数据日期'
    ,etl_data_dt     DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds               STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告组最新状态表，日更新全量表')
    LIFECYCLE 365
;

INSERT OVERWRITE TABLE adm_amazon_adv_ad_group_status_df PARTITION (ds = '${bizdate}')
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
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
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
                        FROM    whde.adm_amazon_adv_ad_group_status_df
                        WHERE   ds = TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
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
                        FROM    whde.amzn_ad_group_data
                        WHERE   pt ='${bizdate}'
                    )
        )
WHERE   rn = 1
;
