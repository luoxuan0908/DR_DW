--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:16:08
--********************************************************************--

drop table IF EXISTS amz.dim_adv_neg_keyword_status_df;
CREATE TABLE IF NOT EXISTS amz.dim_adv_neg_keyword_status_df
(
    tenant_id        STRING COMMENT '租户ID'
    ,profile_id      STRING COMMENT '配置ID'
    ,campaign_id     STRING COMMENT '广告活动ID'
    ,ad_group_id     STRING COMMENT '广告组ID'
    ,ad_type         STRING COMMENT '广告类型'
    ,keyword_id      STRING COMMENT '关键字ID'
    ,keyword_text    STRING COMMENT '关键字'
    ,match_type      STRING COMMENT '匹配类型'
    ,`status`        STRING COMMENT '关键词状态：ENABLED, PAUSED, ARCHIVED'
    ,serving_status  STRING COMMENT '广告实时状态'
    ,create_datetime timestamp COMMENT '创建时间'
    ,update_datetime timestamp COMMENT '更新时间'
    ,data_dt         STRING COMMENT '数据日期'
    ,etl_data_dt     date COMMENT '数据加载日期'
    ,parent_asin     STRING COMMENT '父asin'
)
    PARTITIONED BY
(
    ds               STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '广告否词状态表，全量表日更新')

;

INSERT OVERWRITE TABLE amz.dim_adv_neg_keyword_status_df PARTITION (ds = '20240822')
SELECT  a.tenant_id
     ,a.profile_id
     ,a.campaign_id
     ,a.ad_group_id
     ,ad_type
     ,keyword_id
     ,keyword_text
     ,match_type
     ,status
     ,serving_status
     ,create_datetime
     ,update_datetime
     ,'20240822' data_dt
     ,current_date() etl_data_dt
     ,parent_asin
FROM    (
            SELECT  tenant_id
                 ,profile_id
                 ,campaign_id
                 ,ad_group_id
                 ,ad_type
                 ,keyword_id
                 ,keyword_text
                 ,match_type
                 ,status
                 ,serving_status
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
                             ,serving_status
                             ,create_datetime
                             ,update_datetime
                             ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,keyword_id,ad_type ORDER BY update_datetime DESC ) rn
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
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM    amz.dwd_adv_neg_keyword_status_df
                                    WHERE   ds = '20240821'
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
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM    ods.ods_report_amzn_ad_negword_data_df
                                    WHERE   ds = '20240822'
                                ) t1
                    ) t2
            WHERE   rn = 1
        ) a
            LEFT JOIN   (
    SELECT
        tenant_id
         , profile_id
         , campaign_id
         , ad_group_id
         , parent_asin
    FROM (
             SELECT   tenant_id
                  , profile_id
                  , campaign_id
                  , ad_group_id
                  , parent_asin
                  , ROW_NUMBER() OVER (
                 PARTITION BY tenant_id, profile_id, campaign_id, ad_group_id
                 ORDER BY sum_cost DESC
                 ) AS rn
             FROM (
                      SELECT a.tenant_id
                           , a.profile_id
                           , a.campaign_id
                           , a.ad_group_id
                           , g.parent_asin
                           , SUM(cost) AS sum_cost
                      FROM (
                               SELECT tenant_id
                                    , profile_id
                                    , seller_id
                                    , campaign_id
                                    , ad_group_id
                                    , ad_group_name
                                    , advertised_asin
                                    , advertised_sku
                                    , cost
                               FROM amz.mid_amzn_sp_advertised_product_by_advertiser_report_ds -- 9968
                               WHERE ds >= '20240722'
                                 AND ds <= '20240822' -- 只保存最近30天
                           ) a
                               LEFT JOIN (
                          SELECT tenant_id
                               , profile_id
                               , marketplace_id
                               , marketplace_name
                               , timezone
                               , seller_id
                               , seller_name
                               , ds
                          FROM amz.dim_base_seller_sites_store_df
                          WHERE ds = '20240822'
                      ) b ON a.profile_id = b.profile_id
                          AND a.tenant_id = b.tenant_id
                               LEFT JOIN (
                          SELECT *
                               , market_place_id AS marketplace_id
                               , ROW_NUMBER() OVER (
                              PARTITION BY market_place_id, asin
                              ORDER BY data_dt DESC
                              ) AS rn
                          FROM amz.mid_amzn_asin_to_parent_df
                          WHERE ds = '20240822'
                      ) g ON b.marketplace_id = g.marketplace_id
                          AND a.advertised_asin = g.asin
                      WHERE g.rn = 1
                      --  AND g.parent_asin IS NOT NULL
                      GROUP BY a.tenant_id
                             , a.profile_id
                             , a.seller_id
                             , a.campaign_id
                             , a.ad_group_id
                             , g.parent_asin
                  ) t1
         ) t2
    WHERE rn = 1
) b
                        ON      a.tenant_id = b.tenant_id
                            AND     a.profile_id = b.profile_id
                            AND     a.campaign_id = b.campaign_id
                            AND     a.ad_group_id = b.ad_group_id
;
