--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-28 00:18:07
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_ad_group_log_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID',
    campaign_id STRING COMMENT '亚马逊Id',
    ad_group_id STRING COMMENT '广告组ID',
    ad_type STRING COMMENT '广告类型',
    ad_group_name STRING COMMENT '广告组名称',
    `status` STRING COMMENT '广告组状态：enabled, paused, archived',
    default_bid DECIMAL(18,6) COMMENT '默认竞价',
    tactic STRING COMMENT '投放类型[T00020,T00030]',
    creative_type STRING COMMENT '广告素材类型（VIDEO：视频,IMAGE：图片）',
    bid_type STRING COMMENT '竞价类型[clicks, conversions, reach]',
    serving_status STRING COMMENT '广告实时状态',
    create_datetime DATETIME COMMENT '创建时间',
    update_datetime DATETIME COMMENT '更新时间',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告组操作日志表')
    LIFECYCLE 2000;

INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_ad_group_log_ds PARTITION (ds ='${bizdate}' )
SELECT  tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,'SP' ad_type
     ,ad_group_name
     ,campaign_status
     ,CAST(keyword_bid AS DECIMAL(18,6))
     ,'' tactic
     ,'IMAGE' creative_type
     ,'clicks' bid_optimization
     ,campaign_status serving_status
     ,GETDATE() creation_date
     ,GETDATE() last_updated_date
     ,"report" data_src
     ,"dwd_amzn_sp_search_term_by_search_term_report_ds" table_src
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt

from  (select *,ROW_NUMBER() OVER(PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id ORDER BY report_date desc) AS rn
       from whde.dwd_amzn_sp_search_term_by_search_term_report_ds
       where ds>= '20240401'
      ) a
where rn =1
;


--SELECT  tenant_id
--        ,a.profile_id
--        ,campaign_id
--        ,ad_group_id
--        ,ad_type
--        ,name
--        ,state
--        ,CAST(default_bid AS DECIMAL(18,6))
--        ,tactic
--        ,creative_type
--        ,bid_optimization
--        ,serving_status
--        ,creation_date
--        ,last_updated_date
--        ,"fenghuo_metadata" data_src
--        ,"fenghuo_common_ad_group" table_src
--        ,'${bizdate}' data_dt
--        ,GETDATE() etl_data_dt
--        ,ds
--FROM    (
--            SELECT  ad_group_id
--                    ,profile_id
--                    ,ad_type
--                    ,campaign_id
--                    ,name
--                    ,state
--                    ,default_bid
--                    ,tactic
--                    ,creative_type
--                    ,bid_optimization
--                    ,serving_status
--                    ,FROM_UNIXTIME(CAST(creation_date / 1000 AS BIGINT)) creation_date
--                    ,FROM_UNIXTIME(CAST(last_updated_date / 1000 AS BIGINT)) last_updated_date
--                    ,SUBSTR(hs,1,8) ds
--            FROM    open_ods.ods_fenghuo_fenghuo_metadata_2_fenghuo_common_ad_group
--            WHERE   SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)
--        ) a
--LEFT JOIN   (
--                SELECT  profile_id
--                        ,tenant_id
--                FROM    open_dw.dwd_sit_shp_amazon_seller_sites_store_df
--                WHERE   ds = MAX_PT('open_dw.dwd_sit_shp_amazon_seller_sites_store_df')
--            ) b
--ON      a.profile_id = b.profile_id
--;

