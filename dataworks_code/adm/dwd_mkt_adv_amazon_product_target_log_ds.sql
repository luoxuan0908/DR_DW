--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 22:50:13
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.dwd_mkt_adv_amazon_product_target_log_ds(
    tenant_id STRING COMMENT '租户ID',
    target_id STRING COMMENT '投放品ID',
    profile_id STRING COMMENT '配置ID',
    ad_type STRING COMMENT '广告类型',
    ad_group_id STRING COMMENT '广告组ID',
    campaign_id STRING COMMENT '广告活动ID',
    expression_type STRING COMMENT '表达式类型',
    bid DECIMAL(18,6) COMMENT '竞价',
    expression STRING COMMENT '投放表达式',
    resolved_expression STRING COMMENT '相关表达式',
    `status` STRING COMMENT '投放品状态',
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
    TBLPROPERTIES ('comment'='广告投放品操作日志')
    LIFECYCLE 2000;

INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_product_target_log_ds PARTITION (ds='${bizdate}' )
SELECT
    tenant_id,
    keyword_id,
    profile_id,
    'SP' ad_type,
    ad_group_id,
    campaign_id,
    '',
    cast( keyword_bid  as DECIMAL (18,6)),
    '',
    '',
    ad_keyword_status,
    campaign_status,
    GETDATE(),
    GETDATE(),
    "report" data_src
        ,"dwd_amzn_sp_targeting_by_targeting_report_ds" table_src
        ,'${bizdate}' data_dt
        ,GETDATE()  etl_data_dt
FROM   (select *,ROW_NUMBER() OVER(PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,keyword_id ORDER BY report_date desc) AS rn
        from whde.dwd_amzn_sp_targeting_by_targeting_report_ds
        where ds>= '20240401'
       ) a
where rn =1
;

--INSERT OVERWRITE TABLE dwd_mkt_adv_amazon_product_target_log_ds PARTITION (ds)
--SELECT
--        tenant_id,
--        target_id,
--        a.profile_id,
--        ad_type,
--        ad_group_id,
--        campaign_id,
--        expression_type,
--        cast(bid as DECIMAL (18,6)),
--        expression,
--        resolved_expression,
--        state,
--        serving_status,
--        creation_date,
--        last_updated_date,
--        "fenghuo_metadata" data_src
--        ,"fenghuo_common_product_target" table_src
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
--        bid,
--        expression,
--        resolved_expression,
--        state,
--        serving_status,
--        FROM_UNIXTIME(cast(creation_date/1000 as BIGINT ))creation_date
--        ,FROM_UNIXTIME(cast(last_updated_date/1000 as BIGINT )) last_updated_date
--        ,SUBSTR(hs,1,8) ds
-- FROM open_ods.ods_fenghuo_fenghuo_metadata_2_fenghuo_common_product_target
--         WHERE    SUBSTR(hs,1,8) = SUBSTR('${bizdate}',1,8)) a
--        LEFT JOIN
--        (SELECT profile_id,tenant_id FROM  open_dw.dwd_sit_shp_amazon_seller_sites_store_df
--        WHERE ds=MAX_PT ('open_dw.dwd_sit_shp_amazon_seller_sites_store_df')) b
--        on a.profile_id=b.profile_id
--;