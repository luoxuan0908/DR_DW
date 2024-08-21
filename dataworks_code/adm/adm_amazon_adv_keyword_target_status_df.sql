--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:15:42
--********************************************************************--
CREATE TABLE IF NOT EXISTS adm_amazon_adv_keyword_target_status_df
(
    tenant_id                STRING COMMENT '租户ID'
    ,profile_id              STRING COMMENT '配置Id'
    ,campaign_id             STRING COMMENT '广告活动ID'
    ,ad_group_id             STRING COMMENT '广告组ID'
    ,ad_type                 STRING COMMENT '广告类型'
    ,keyword_id              STRING COMMENT '投放词ID'
    ,keyword_text            STRING COMMENT '投放词'
    ,keyword_stem            STRING COMMENT '投放词的词干，去除单复数'
    ,parent_asin             STRING COMMENT '父asin'
    ,match_type              STRING COMMENT '匹配类型'
    ,`status`                STRING COMMENT '关键词状态:ENABLED, PAUSED, ARCHIVED'
    ,bid                     DECIMAL(18,6) COMMENT '竞价'
    ,serving_status          STRING COMMENT '广告活动实时状态'
    ,native_language_keyword STRING COMMENT '目标受众的本地语言关键词'
    ,create_datetime         DATETIME COMMENT '创建时间'
    ,update_datetime         DATETIME COMMENT '更新时间'
    ,data_dt                 STRING COMMENT '数据日期'
    ,etl_data_dt             DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds                       STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告投放词最新状态，全量表日更新')
    LIFECYCLE 365
;
SET odps.sql.python.version = cp37;
INSERT OVERWRITE TABLE adm_amazon_adv_keyword_target_status_df PARTITION (ds = '${bizdate}')
SELECT   a.tenant_id
     ,a.profile_id
     ,a.campaign_id
     ,a.ad_group_id
     ,ad_type
     ,keyword_id
     ,keyword_text
     --,zby_bi.udf_seach_term_clean(keyword_text) keyword_text
     ,keyword_text
     ,parent_asin
     ,match_type
     ,status
     ,bid
     ,serving_status
     ,native_language_keyword
     ,create_datetime
     ,update_datetime
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
FROM
    (SELECT  tenant_id
          ,profile_id
          ,ad_type
          ,keyword_id
          ,keyword_text
          ,match_type
          ,ad_group_id
          ,campaign_id
          ,status
          ,bid
          ,serving_status
          ,native_language_keyword
          ,create_datetime
          ,update_datetime
     FROM    (
                 SELECT  tenant_id
                      ,profile_id
                      ,ad_type
                      ,keyword_id
                      ,keyword_text
                      ,match_type
                      ,ad_group_id
                      ,campaign_id
                      ,status
                      ,bid
                      ,serving_status
                      ,native_language_keyword
                      ,create_datetime
                      ,update_datetime
                      ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,ad_type,keyword_id ORDER BY update_datetime DESC ) rn
                 FROM    (
                             SELECT  tenant_id
                                  ,profile_id
                                  ,ad_type
                                  ,keyword_id
                                  ,keyword_text
                                  ,match_type
                                  ,ad_group_id
                                  ,campaign_id
                                  ,status
                                  ,bid
                                  ,serving_status
                                  ,native_language_keyword
                                  ,create_datetime
                                  ,update_datetime
                             FROM    whde.adm_amazon_adv_keyword_target_status_df
                             WHERE   ds = TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
                             UNION ALL
                             SELECT  tenant_id
                                  ,profile_id
                                  ,ad_type
                                  ,keyword_id
                                  ,keyword_text
                                  ,match_type
                                  ,ad_group_id
                                  ,campaign_id
                                  ,status
                                  ,cast(bid as DECIMAL(18,6))
                                  ,serving_status
                                  ,native_language_keyword
                                  ,create_datetime
                                  ,update_datetime
                             FROM    whde.amzn_ad_keyword_data
                             WHERE   pt ='${bizdate}'
                         )
             )
     WHERE   rn = 1
    )a
        LEFT JOIN
    (
        SELECT  tenant_id
             ,profile_id
             ,campaign_id
             ,ad_group_id
             ,top_cost_parent_asin parent_asin
        FROM    whde.adm_amazon_adv_sku_wide_d
        WHERE   ds = '${bizdate}'
        GROUP BY tenant_id
               ,profile_id
               ,campaign_id
               ,ad_group_id
               ,top_cost_parent_asin
    )b
    ON a.tenant_id=b.tenant_id
        AND a.profile_id=b.profile_id
        AND a.campaign_id=b.campaign_id
        AND a.ad_group_id=b.ad_group_id
;