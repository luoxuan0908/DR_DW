--odps sql 
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:18:33
--********************************************************************--
drop table if exists amz.dim_adv_product_target_status_df;
CREATE TABLE IF NOT EXISTS amz.dim_adv_product_target_status_df
(
    tenant_id            STRING COMMENT '租户ID'
    ,target_id           STRING COMMENT '投放品ID'
    ,profile_id          STRING COMMENT '配置ID'
    ,ad_type             STRING COMMENT '广告类型'
    ,campaign_id         STRING COMMENT '广告活动ID'
    ,ad_group_id         STRING COMMENT '广告组ID'
    ,expression_type     STRING COMMENT '表达式类型：MANUAL：手动投放，AUTO：自动投放'
    ,bid                 DECIMAL(18,6) COMMENT '竞价'
    ,expression          STRING COMMENT '投放表达式'
    ,resolved_expression STRING COMMENT '相关表达式'
    ,asin                STRING COMMENT '投放品'
    ,category            STRING COMMENT '投放类目'
    ,brand               STRING COMMENT '投放品牌'
    ,parent_asin         STRING COMMENT '推广品对应的父aisn'
    ,`status`            STRING COMMENT '投放品状态:ENABLED, PAUSED, ARCHIVED'
    ,serving_status      STRING COMMENT '广告活动实时状态'
    ,create_datetime     timestamp COMMENT '创建时间'
    ,update_datetime     timestamp COMMENT '更新时间'
    ,data_dt             STRING COMMENT '数据日期'
    ,etl_data_dt         date COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds                   STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '广告投放品最新状态表，全量表日更新')
;

INSERT OVERWRITE TABLE amz.dim_adv_product_target_status_df PARTITION (ds = '20240822')
SELECT  a.tenant_id
     ,target_id
     ,a.profile_id
     ,ad_type
     ,a.campaign_id
     ,a.ad_group_id
     ,expression_type
     ,bid
     ,expression
     ,resolved_expression
     ,asin
     ,category
     ,BRAND
     ,parent_asin
     ,status
     ,serving_status
     ,create_datetime
     ,update_datetime
     ,'20240822' data_dt
     ,current_date() etl_data_dt
FROM    (
            SELECT  tenant_id
                 ,target_id
                 ,profile_id
                 ,ad_type
                 ,campaign_id
                 ,ad_group_id
                 ,expression_type
                 ,bid
                 ,expression
                 ,resolved_expression
                 ,asin
                 ,category
                 ,BRAND
                 ,status
                 ,serving_status
                 ,create_datetime
                 ,update_datetime
            FROM    (
                        SELECT  tenant_id
                             ,target_id
                             ,profile_id
                             ,ad_type
                             ,ad_group_id
                             ,campaign_id
                             ,expression_type
                             ,bid
                             ,expression
                             ,resolved_expression
                             ,asin
                             ,category
                             ,BRAND
                             ,status
                             ,serving_status
                             ,create_datetime
                             ,update_datetime
                             ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,target_id ORDER BY update_datetime DESC ) rn
                        FROM    (
                                    SELECT  tenant_id
                                         ,target_id
                                         ,profile_id
                                         ,ad_type
                                         ,ad_group_id
                                         ,campaign_id
                                         ,expression_type
                                         ,bid
                                         ,expression
                                         ,resolved_expression
                                         ,asin
                                         ,category
                                         ,BRAND
                                         ,status
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM    amz.dwd_adv_product_target_status_df
                                    WHERE   ds = '20240822'
                                    UNION ALL
                                    SELECT  tenant_id
                                         ,target_id
                                         ,profile_id
                                         ,ad_type
                                         ,ad_group_id
                                         ,campaign_id
                                         ,expression_type
                                         ,bid
                                         ,expression
                                         ,resolved_expression
                                         ,CASE   WHEN INSTR(resolved_expression,'ASIN') >= 1
                                        AND INSTR(resolved_expression,'CATEGORY') < 1 THEN GET_JSON_OBJECT(REPLACE(REPLACE(resolved_expression,'[',''),']',''),'$.value')
                                        END asin
                                         ,CASE   WHEN INSTR(resolved_expression,'CATEGORY') >= 1 THEN GET_JSON_OBJECT(
                                            SPLIT(REPLACE(REPLACE(REPLACE(resolved_expression,'[',''),']',''),'}, {','}&&{'),'&&')[0]
                                        ,'$.value')
                                        END category
                                         ,CASE   WHEN INSTR(resolved_expression,'BRAND') >= 1 THEN GET_JSON_OBJECT(
                                            SPLIT(REPLACE(REPLACE(REPLACE(resolved_expression,'[',''),']',''),'}, {','}&&{'),'&&')[1]
                                        ,'$.value')
                                        END BRAND
                                         ,status
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM   (
                                               SELECT
                                                   tenant_id,
                                                   keyword_id as target_id,
                                                   profile_id,
                                                   'SP' ad_type,
                                                   ad_group_id,
                                                   campaign_id,
                                                   '' as expression_type,
                                                   cast( keyword_bid  as DECIMAL (18,6)) as bid,
                                                   '' as expression,
                                                   '' as resolved_expression,
                                                   ad_keyword_status as status,
                                                   campaign_status as serving_status,
                                                   current_date() as create_datetime,
                                                   current_date() as update_datetime,
                                                   "report" data_src
                                                       ,"dwd_amzn_sp_targeting_by_targeting_report_ds" table_src
                                                       ,'20240822' data_dt
                                                       ,current_date()  etl_data_dt
                                               FROM   (select *,ROW_NUMBER() OVER(PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,keyword_id ORDER BY report_date desc) AS rn
                                                       from amz.mid_amzn_sp_targeting_by_targeting_report_ds
                                                       where ds>= '20240401'
                                                      ) a
                                               where rn =1
                                           ) t1
                                    
                                ) t2
                    ) t3
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
                          FROM dwd.dwd_base_seller_sites_store_df
                          WHERE ds = '20240809'
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