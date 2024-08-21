--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-27 23:40:41
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_neg_product_log_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID',
    ad_type STRING COMMENT '广告类型',
    target_id STRING COMMENT '投放品ID',
    ad_group_id STRING COMMENT '广告组ID',
    campaign_id STRING COMMENT '广告活动ID',
    expression_type STRING COMMENT '表达式类型',
    expression STRING COMMENT '投放表达式',
    resolved_expression STRING COMMENT '相关表达式',
    `status` STRING COMMENT '否品状态',
    serving_status STRING COMMENT '广告活动实时状态',
    create_datetime DATETIME COMMENT '创建时间',
    update_datetime DATETIME COMMENT '更新时间',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
)
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告否品操作日志')
    LIFECYCLE 2000;


--INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_neg_product_log_ds PARTITION (ds)
--SELECT
--        tenant_id,
--         a.profile_id,
--         ad_type,
--        target_id,
--        ad_group_id,
--        campaign_id,
--        expression_type,
--        expression,
--        resolved_expression,
--        state,
--        serving_status,
--        creation_date,
--        last_updated_date,
--        "fenghuo_metadata" data_src
--        ,"fenghuo_common_neg_product_target" table_src
--        ,'${bizdate}' data_dt
--        ,GETDATE()  etl_data_dt
--        ,ds
--    FROM
--
--(SELECT
--        target_id,
--        profile_id,
--        ad_type,
--        ad_group_id,
--        campaign_id,
--        expression_type,
--        expression,
--        resolved_expression,
--        state,
--        serving_status,
--        FROM_UNIXTIME(cast(creation_date/1000 as BIGINT ))creation_date
--        ,FROM_UNIXTIME(cast(last_updated_date/1000 as BIGINT )) last_updated_date
--        , SUBSTR(hs,1,8) ds
-- FROM   ods_fenghuo_fenghuo_metadata_2_fenghuo_common_neg_product_target
--         WHERE   SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)) a
--        LEFT JOIN
--        (SELECT profile_id,tenant_id FROM  open_dw.dwd_sit_shp_amazon_seller_sites_store_df
--        WHERE ds=MAX_PT ('open_dw.dwd_sit_shp_amazon_seller_sites_store_df')) b
--        on a.profile_id=b.profile_id
--;
