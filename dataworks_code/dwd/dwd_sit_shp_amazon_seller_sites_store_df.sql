--@exclude_input=whde.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:37:20
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dwd_sit_shp_amazon_seller_sites_store_df(
    id BIGINT COMMENT 'ID',
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID（没有则没有授权广告，不会下载广告数据）',
    country_code STRING COMMENT '国家编码',
    timezone STRING COMMENT '时区',
    currency_code STRING COMMENT '币种',
    marketplace_id STRING COMMENT '站点ID',
    marketplace_name STRING COMMENT '站点名称',
    seller_id STRING COMMENT '卖家ID',
    seller_name STRING COMMENT '亚马逊上的卖家名称',
    `status` STRING COMMENT '店铺状态(0:已停用;1:启用)',
    ship_from_address STRING COMMENT '店铺发货地址',
    adv_refresh_token STRING COMMENT '广告数据刷新Token',
    adv_first_auth_time DATETIME COMMENT '广告数据首次授权时间',
    adv_latest_auth_time DATETIME COMMENT '广告最近一次授权时间',
    adv_auth_status STRING COMMENT '广告授权状态状态（AUTHED:已授权，WAIT:未授权）',
    sp_api_refresh_token STRING COMMENT 'sp数据api token',
    endpoint_code STRING COMMENT '端点缩写(na-北美|eu-欧洲|fe-远东)',
    store_first_auth_time DATETIME COMMENT '店铺首次授权时间',
    store_latest_auth_time DATETIME COMMENT '店铺最近一次授权时间',
    store_auth_status STRING COMMENT '店铺数据授权状态（AUTHED:已授权，WAIT:未授权）',
    create_time DATETIME COMMENT '数据创建日期',
    modified_time DATETIME COMMENT '数据修改日期',
    creator STRING COMMENT '创建者',
    modifier STRING COMMENT '修改者',
    is_deleted BIGINT COMMENT '是否删除',
    is_enabled BIGINT COMMENT '是否生效',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    store_type STRING COMMENT '店铺类型，SC：Seller Central，卖家；VC：Vendor Central，供应商'
)
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='烽火系统中亚马逊的店铺站点信息')
    LIFECYCLE 7;


INSERT OVERWRITE TABLE dwd_sit_shp_amazon_seller_sites_store_df PARTITION (ds='${bizdate}')
select distinct abs(hash(tenant_id,profile_id)) id
              ,a.tenant_id
              ,a.profile_id
              ,a.countrycode
              ,b.timezone
              ,b.currency_en
              ,a.marketplace_id
              ,concat(b.country_cn_name,'站') market_place_name
              ,a.seller_id
              ,a.seller_name
              ,a.status
              ,'' ship_from_address
              ,'' adv_refresh_token
              ,create_time adv_first_auth_time
              ,update_time adv_latest_auth_time
              ,case when ad_status = 1 then '已授权' else '未授权' end adv_auth_status
              ,'' sp_api_refresh_token
              ,b.endpoint_code
              ,create_time store_first_auth_time
              ,update_time store_latest_auth_time
              ,case when sp_status = 1 then '已授权' else '未授权' end store_auth_status
              ,GETDATE() create_time
              ,GETDATE() modified_time
              ,'租户超级管理员' creator
              ,'租户超级管理员' modifier
              ,0 is_deleted
              ,0 is_enabled
              ,'${bizdate}' data_dt
              ,GETDATE() etl_data_dt
              ,'SC' store_type
from (select * from authorization_account where pt = '${bizdate}')a
         left outer join whde.dim_marketplace_info_df b
                         on a.marketplace_id = b.market_place_id
;
