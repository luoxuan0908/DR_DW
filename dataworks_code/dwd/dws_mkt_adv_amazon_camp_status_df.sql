--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:41:08
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.dws_mkt_adv_amazon_camp_status_df(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID',
    seller_id STRING COMMENT '卖家ID',
    portfolio_id STRING COMMENT '广告活动组ID',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    ad_group_id STRING COMMENT '广告组ID',
    ad_group_name STRING COMMENT '广告组名称',
    campaign_status STRING COMMENT '广告活动状态',
    campaign_biding_strategy STRING COMMENT '广告活动竞价策略',
    latest_report_date DATETIME COMMENT '最新报告日期',
    ad_mode STRING COMMENT '广告投放模式',
    advertising_type STRING COMMENT '广告类型',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载时间'
)
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告活动最新状态全量表(profile_id,seller_id,campaing_id,ad_group_id )')
    LIFECYCLE 30;

--取sp最新报告时间
DROP TABLE IF EXISTS tmp_cmp_status_sp_latest_${bizdate}
;

CREATE TABLE tmp_cmp_status_sp_latest_${bizdate} AS
SELECT  tenant_id
     ,profile_id
     ,seller_id
     ,portfolio_id
     ,campaign_id
     ,ad_group_id
     ,campaign_name
     ,ad_group_name
     ,campaign_status
     ,report_date
     ,advertising_type
FROM    (
            SELECT  tenant_id
                 ,profile_id
                 ,seller_id
                 ,portfolio_id
                 ,campaign_id
                 ,ad_group_id
                 ,campaign_name
                 ,ad_group_name
                 ,campaign_status
                 ,report_date
                 ,'商品推广' advertising_type
                 ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,seller_id,campaign_id,ad_group_id ORDER BY report_date DESC ) AS rn
            FROM   whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
            WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-60,'dd'),'yyyymmdd')
              AND     ds <= '${bizdate}'
        )
WHERE   rn = 1
;

--取竞价策略
DROP TABLE IF EXISTS tmp_cmp_status_all_strategy_${bizdate}
;

CREATE TABLE tmp_cmp_status_all_strategy_${bizdate} AS
SELECT  profile_id
     ,seller_id
     ,campaign_id
     ,campaign_bidding_strategy
FROM    (
            SELECT  profile_id
                 ,seller_id
                 ,campaign_id
                 ,campaign_bidding_strategy
                 ,ROW_NUMBER() OVER (PARTITION BY profile_id,seller_id,campaign_id ORDER BY report_date DESC ) AS rn
            FROM     whde.dwd_amzn_sp_campaigns_by_campaign_placement_report_ds
            WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-60,'dd'),'yyyymmdd')
              AND     ds <= '${bizdate}'
              AND     campaign_bidding_strategy IS NOT NULL
        )
WHERE   rn = 1
;

--取广告活动模式
DROP TABLE IF EXISTS tmp_cmp_status_all_admode_${bizdate}
;

CREATE TABLE tmp_cmp_status_all_admode_${bizdate} AS
SELECT  DISTINCT profile_id
               ,seller_id
               ,campaign_id
               ,ad_group_id
               ,CASE   WHEN targeting_type = 'TARGETING_EXPRESSION_PREDEFINED' THEN '自动投放'
                       ELSE '手动投放'
    END ad_mode
FROM    (
            SELECT  profile_id
                 ,seller_id
                 ,campaign_id
                 ,ad_group_id
                 ,keyword_type targeting_type
            FROM    whde.dwd_amzn_sp_targeting_by_targeting_report_ds
            WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-60,'dd'),'yyyymmdd')
              AND     ds <= '${bizdate}'
              AND     keyword_type IS NOT NULL
        )
;

INSERT OVERWRITE TABLE dws_mkt_adv_amazon_camp_status_df PARTITION (ds = '${bizdate}')
SELECT  tenant_id
     ,profile_id
     ,seller_id
     ,portfolio_id
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,campaign_status
     ,campaign_biding_strategy
     ,latest_report_date
     ,ad_mode
     ,advertising_type
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
FROM    (
            SELECT  tenant_id
                 ,profile_id
                 ,seller_id
                 ,portfolio_id
                 ,campaign_id
                 ,campaign_name
                 ,ad_group_id
                 ,ad_group_name
                 ,campaign_status
                 ,campaign_biding_strategy
                 ,latest_report_date
                 ,ad_mode
                 ,advertising_type
                 ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,seller_id,campaign_id,ad_group_id ORDER BY etl_data_dt DESC ) rn
            FROM    (
                        SELECT  tenant_id
                             ,t1.profile_id
                             ,t1.seller_id
                             ,t1.portfolio_id
                             ,t1.campaign_id
                             ,t1.campaign_name
                             ,t1.ad_group_id
                             ,t1.ad_group_name
                             ,CASE   WHEN TOLOWER(t1.campaign_status) = 'enabled' THEN '已启动'
                                     WHEN TOLOWER(t1.campaign_status) = 'archived' THEN '已归档'
                                     WHEN TOLOWER(t1.campaign_status) = 'paused' THEN '已暂停'
                                     ELSE '其他'
                            END campaign_status
                             ,CASE   WHEN t2.campaign_bidding_strategy = 'manual' THEN '固定竞价'
                                     WHEN t2.campaign_bidding_strategy = 'optimizeForSales' THEN '动态竞价-提高和降低'
                                     WHEN t2.campaign_bidding_strategy = 'legacy' THEN '动态竞价-只降低'
                            END campaign_biding_strategy
                             ,t1.report_date latest_report_date
                             ,t3.ad_mode
                             ,t1.advertising_type
                             ,'${bizdate}' data_dt
                             ,GETDATE() etl_data_dt
                        FROM    tmp_cmp_status_sp_latest_${bizdate} t1
                                    LEFT JOIN tmp_cmp_status_all_strategy_${bizdate} t2
                                              ON      t1.profile_id = t2.profile_id
                                                  AND     t1.seller_id = t2.seller_id
                                                  AND     t1.campaign_id = t2.campaign_id
                                    LEFT JOIN tmp_cmp_status_all_admode_${bizdate} t3
                                              ON      t1.profile_id = t3.profile_id
                                                  AND     t1.seller_id = t3.seller_id
                                                  AND     t1.campaign_id = t3.campaign_id
                                                  AND     t1.ad_group_id = t3.ad_group_id
                        UNION ALL
                        SELECT  tenant_id
                             ,profile_id
                             ,seller_id
                             ,portfolio_id
                             ,campaign_id
                             ,campaign_name
                             ,ad_group_id
                             ,ad_group_name
                             ,campaign_status
                             ,campaign_biding_strategy
                             ,latest_report_date
                             ,ad_mode
                             ,advertising_type
                             ,data_dt
                             ,etl_data_dt
                        FROM    whde.dws_mkt_adv_amazon_camp_status_df
                        WHERE   ds = '${bizdate1}' --  ds='${bizdate1}'
                    )
        )
WHERE   rn = 1
;

--删除所有临时表
DROP TABLE IF EXISTS tmp_cmp_status_sp_latest_${bizdate}
;

DROP TABLE IF EXISTS tmp_cmp_status_all_strategy_${bizdate}
;

DROP TABLE IF EXISTS tmp_cmp_status_all_admode_${bizdate}
;

