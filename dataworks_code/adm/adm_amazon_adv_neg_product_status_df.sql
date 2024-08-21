--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:16:49
--********************************************************************--

CREATE TABLE IF NOT EXISTS adm_amazon_adv_neg_product_status_df
(
    tenant_id            STRING COMMENT '租户ID'
    ,profile_id          STRING COMMENT '配置ID'
    ,campaign_id         STRING COMMENT '广告活动ID'
    ,ad_group_id         STRING COMMENT '广告组ID'
    ,ad_type             STRING COMMENT '广告类型'
    ,target_id           STRING COMMENT '投放品ID'
    ,expression_type     STRING COMMENT '表达式类型'
    ,expression          STRING COMMENT '投放表达式'
    ,resolved_expression STRING COMMENT '相关表达式'
    ,asin                STRING COMMENT '投放品'
    ,`status`            STRING COMMENT '否品状态:ENABLED, PAUSED, ARCHIVED'
    ,serving_status      STRING COMMENT '广告活动实时状态'
    ,create_datetime     DATETIME COMMENT '创建时间'
    ,update_datetime     DATETIME COMMENT '更新时间'
    ,data_dt             STRING COMMENT '数据日期'
    ,etl_data_dt         DATETIME COMMENT '数据加载日期'
    ,parent_asin         STRING COMMENT '父asin'
)
    PARTITIONED BY
(
    ds                   STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告否品状态表，全量表日更新')
    LIFECYCLE 365
;

INSERT OVERWRITE TABLE adm_amazon_adv_neg_product_status_df PARTITION (ds = '${bizdate}')
SELECT  a.tenant_id
     ,a.profile_id
     ,a.campaign_id
     ,a.ad_group_id
     ,ad_type
     ,target_id
     ,expression_type
     ,expression
     ,resolved_expression
     ,ASIN
     ,status
     ,serving_status
     ,create_datetime
     ,update_datetime
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
     ,parent_asin
FROM    (
            SELECT  tenant_id
                 ,profile_id
                 ,campaign_id
                 ,ad_group_id
                 ,ad_type
                 ,target_id
                 ,expression_type
                 ,expression
                 ,resolved_expression
                 ,ASIN
                 ,status
                 ,serving_status
                 ,create_datetime
                 ,update_datetime
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,ad_type
                             ,target_id
                             ,ad_group_id
                             ,campaign_id
                             ,expression_type
                             ,expression
                             ,resolved_expression
                             ,ASIN
                             ,status
                             ,serving_status
                             ,create_datetime
                             ,update_datetime
                             ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,ad_type,target_id ORDER BY update_datetime DESC ) rn
                        FROM    (
                                    SELECT  tenant_id
                                         ,profile_id
                                         ,ad_type
                                         ,target_id
                                         ,ad_group_id
                                         ,campaign_id
                                         ,expression_type
                                         ,expression
                                         ,resolved_expression
                                         ,ASIN
                                         ,status
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM    whde.adm_amazon_adv_neg_product_status_df
                                    WHERE   ds = TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
                                    UNION ALL
                                    SELECT  tenant_id
                                         ,profile_id
                                         ,ad_type
                                         ,target_id
                                         ,ad_group_id
                                         ,campaign_id
                                         ,expression_type
                                         ,expression
                                         ,resolved_expression
                                         ,GET_JSON_OBJECT(REPLACE(REPLACE(resolved_expression,'[',''),']',''),'$.value') ASIN
                                         ,status
                                         ,serving_status
                                         ,create_datetime
                                         ,update_datetime
                                    FROM    whde.amzn_ad_negproduct_data
                                    WHERE   pt = '${bizdate}'
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
