--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-02 00:32:43
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_product_log_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID',
    ad_id STRING COMMENT '广告ID',
    ad_type STRING COMMENT '广告类型',
    ad_group_id STRING COMMENT '广告组ID',
    campaign_id STRING COMMENT '广告活动ID',
    `status` STRING COMMENT '推广品状态：enabled, paused, archived',
    sku STRING COMMENT '推广sku',
    asin STRING COMMENT '推广asin',
    ad_name STRING COMMENT '广告名称',
    landing_page_detail STRING COMMENT '点击广告后的跳转页面',
    adv_materials_type STRING COMMENT '广告素材类型',
    serving_status STRING COMMENT '广告实时状态',
    create_date DATETIME COMMENT '创建时间',
    update_date DATETIME COMMENT '更新时间',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
)
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告推广品操作日志表')
    LIFECYCLE 2000;


INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_product_log_ds PARTITION (ds ='${bizdate}')
SELECT
    tenant_id,
    profile_id,
    ad_id,
    'SP' ad_type,
    ad_group_id,
    campaign_id,
    campaign_status,
    advertised_sku,
    advertised_asin,
    campaign_name,
    '',
    'image' ,
    campaign_status,
    GETDATE(),
    GETDATE(),
    "report" data_src
        ,"dwd_mkt_adv_amazon_sp_product_ds" table_src
        ,'${bizdate}' data_dt
        ,GETDATE()  etl_data_dt
from  (select *,ROW_NUMBER() OVER(PARTITION BY tenant_id,profile_id,ad_id,campaign_id,ad_group_id,advertised_sku,advertised_asin ORDER BY report_date desc) AS rn
       from whde.dwd_mkt_adv_amazon_sp_product_ds
       where ds>= '20240401'
      ) a
where rn =1
;

--INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_product_log_ds PARTITION (ds)
--SELECT
--        tenant_id,
--        a.profile_id,
--        ad_id,
--        ad_type,
--        ad_group_id,
--        campaign_id,
--        state,
--        sku,
--        asin,
--        ad_name,
--        landing_page_detail,
--        creative,
--        serving_status,
--        creation_date,
--        last_updated_date,
--        "" data_src
--        ,"fenghuo_common_product_ad" table_src
--        ,'${bizdate}' data_dt
--        ,GETDATE()  etl_data_dt
--        ,ds
--    FROM
--(SELECT
--        profile_id,
--        ad_id,
--        ad_type,
--        ad_group_id,
--        campaign_id,
--        state,
--        sku,
--        asin,
--        ad_name,
--        landing_page_detail,
--        creative,
--        serving_status
--        ,FROM_UNIXTIME(cast(creation_date/1000 as BIGINT ))creation_date
--        ,FROM_UNIXTIME(cast(last_updated_date/1000 as BIGINT )) last_updated_date
--        ,SUBSTR(hs,1,8) ds
-- FROM open_ods.ods_fenghuo_fenghuo_metadata_2_fenghuo_common_product_ad
--         WHERE   SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)) a
--        LEFT JOIN
--        (SELECT profile_id,tenant_id FROM  open_dw.dwd_sit_shp_amazon_seller_sites_store_df
--        WHERE ds=MAX_PT ('open_dw.dwd_sit_shp_amazon_seller_sites_store_df')) b
--        on a.profile_id=b.profile_id
-- ;