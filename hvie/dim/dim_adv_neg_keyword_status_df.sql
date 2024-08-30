--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:16:08
--********************************************************************--

-- drop table IF EXISTS amz.dim_adv_neg_keyword_status_df;
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

INSERT OVERWRITE TABLE amz.dim_adv_neg_keyword_status_df PARTITION (ds = '${last_day}')
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
     ,'${last_day}' data_dt
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
                                    FROM    amz.dim_adv_neg_keyword_status_df
                                    WHERE   ds = '${last_2_day}'
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
                                    WHERE   ds = '${last_day}'
                                ) t1
                    ) t2
            WHERE   rn = 1
        ) a
            LEFT JOIN   (
    SELECT  tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,top_cost_parent_asin parent_asin
    FROM    amz.mid_amazon_adv_sku_wide_d
    WHERE   ds = '${last_2_day}'
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
