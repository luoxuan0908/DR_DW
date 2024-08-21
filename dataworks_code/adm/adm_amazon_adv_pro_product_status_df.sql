--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:17:51
--********************************************************************--
CREATE TABLE IF NOT EXISTS adm_amazon_adv_pro_product_status_df
(
    tenant_id            STRING COMMENT '租户ID'
    ,profile_id          STRING COMMENT '配置ID'
    ,ad_id               STRING COMMENT '广告组合ID'
    ,ad_type             STRING COMMENT '广告类型'
    ,ad_group_id         STRING COMMENT '广告组ID'
    ,campaign_id         STRING COMMENT '广告活动ID'
    ,`status`            STRING COMMENT '推广品状态：ENABLED, PAUSED, ARCHIVED'
    ,sku                 STRING COMMENT '推广sku'
    ,asin                STRING COMMENT '推广asin'
    ,ad_name             STRING COMMENT '广告组合名称'
    ,landing_page_detail STRING COMMENT '点击广告后的跳转页面'
    ,adv_materials_type  STRING COMMENT '广告素材类型'
    ,serving_status      STRING COMMENT '广告实时状态'
    ,create_date         DATETIME COMMENT '创建时间'
    ,update_date         DATETIME COMMENT '更新时间'
    ,data_dt             STRING COMMENT '数据日期'
    ,etl_data_dt         DATETIME COMMENT '数据加载日期'
)
    PARTITIONED BY
(
    ds                   STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告推广品最新状态表，全量表日更新')
    LIFECYCLE 365
;

INSERT OVERWRITE TABLE adm_amazon_adv_pro_product_status_df PARTITION (ds = '${bizdate}')
SELECT  tenant_id
     ,profile_id
     ,ad_id
     ,ad_type
     ,ad_group_id
     ,campaign_id
     ,status
     ,sku
     ,asin
     ,ad_name
     ,landing_page_detail
     ,adv_materials_type
     ,serving_status
     ,create_date
     ,update_date
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
FROM    (
            SELECT  tenant_id
                 ,profile_id
                 ,ad_id
                 ,ad_type
                 ,ad_group_id
                 ,campaign_id
                 ,status
                 ,sku
                 ,asin
                 ,ad_name
                 ,landing_page_detail
                 ,adv_materials_type
                 ,serving_status
                 ,create_date
                 ,update_date
                 ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,ad_id,campaign_id,ad_group_id,sku,asin ORDER BY update_date DESC ) rn
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,ad_id
                             ,ad_type
                             ,ad_group_id
                             ,campaign_id
                             ,status
                             ,sku
                             ,asin
                             ,ad_name
                             ,landing_page_detail
                             ,adv_materials_type
                             ,serving_status
                             ,create_date
                             ,update_date
                        FROM    whde.adm_amazon_adv_pro_product_status_df
                        WHERE   ds = TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
                        UNION ALL
                        SELECT  tenant_id
                             ,profile_id
                             ,ad_id
                             ,ad_type
                             ,ad_group_id
                             ,campaign_id
                             ,status
                             ,sku
                             ,asin
                             ,ad_name
                             ,landing_page_detail
                             ,adv_materials_type
                             ,serving_status
                             ,create_datetime
                             ,update_datetime
                        FROM    whde.amzn_ad_product_data
                        WHERE   pt = '${bizdate}'
                    )
        )
WHERE   rn = 1
;


SELECT  tenant_id
     ,profile_id
     ,ad_id
     ,ad_type
     ,ad_group_id
     ,campaign_id
     ,status
     ,sku
     ,asin
     ,ad_name
     ,landing_page_detail
     ,adv_materials_type
     ,serving_status
     ,create_datetime
     ,update_datetime
FROM    whde.amzn_ad_product_data
WHERE   pt = '${bizdate}'
  and tenant_id = '67354cc2df65894139011e1c4ca153c5'
;
