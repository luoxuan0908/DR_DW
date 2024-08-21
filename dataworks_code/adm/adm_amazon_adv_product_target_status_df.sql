--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:18:33
--********************************************************************--
CREATE TABLE IF NOT EXISTS adm_amazon_adv_product_target_status_df
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
    ,create_datetime     DATETIME COMMENT '创建时间'
    ,update_datetime     DATETIME COMMENT '更新时间'
    ,data_dt             STRING COMMENT '数据日期'
    ,etl_data_dt         DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    ds                   STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告投放品最新状态表，全量表日更新')
    LIFECYCLE 365
;

INSERT OVERWRITE TABLE adm_amazon_adv_product_target_status_df PARTITION (ds = '${bizdate}')
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
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
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
                                    FROM    whde.adm_amazon_adv_product_target_status_df
                                    WHERE   ds = TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
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
                                            SPLIT_PART(REPLACE(REPLACE(REPLACE(resolved_expression,'[',''),']',''),'}, {','}&&{'),'&&',1)
                                        ,'$.value')
                                        END category
                                         ,CASE   WHEN INSTR(resolved_expression,'BRAND') >= 1 THEN GET_JSON_OBJECT(
                                            SPLIT_PART(REPLACE(REPLACE(REPLACE(resolved_expression,'[',''),']',''),'}, {','}&&{'),'&&',2)
                                        ,'$.value')
                                        END BRAND
                                         ,status
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM    whde.dwd_mkt_adv_amazon_product_target_log_ds
                                    WHERE   ds = '${bizdate}'
                                )
                    )
            WHERE   rn = 1
        ) a
            LEFT JOIN   (
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
) b
                        ON      a.tenant_id = b.tenant_id
                            AND     a.profile_id = b.profile_id
                            AND     a.campaign_id = b.campaign_id
                            AND     a.ad_group_id = b.ad_group_id
;