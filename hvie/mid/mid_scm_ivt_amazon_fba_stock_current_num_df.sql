-- dwd_scm_ivt_amazon_fba_stock_current_num_df
CREATE TABLE IF NOT EXISTS amz.mid_scm_ivt_amazon_fba_stock_current_num_df(
    tenant_id STRING COMMENT '租户ID',
    report_id STRING COMMENT '报告ID',
    record_id STRING COMMENT '记录ID',
    marketplace_id STRING COMMENT '站点ID',
    seller_id STRING COMMENT '卖家ID',
    erp_store_id STRING COMMENT 'erp店铺id',
    seller_sku STRING COMMENT '店铺SKU',
    fnsku STRING COMMENT '平台SKU',
    asin STRING COMMENT '子asin',
    product_name STRING COMMENT '产品名称',
    status STRING COMMENT '状态',
    product_price DECIMAL(18,6) COMMENT '产品单价',
    mfn_listing_exists STRING COMMENT '自配送列表',
    mfn_fulfillable_quantity BIGINT COMMENT '自配送发货数量',
    afn_listing_exists STRING COMMENT 'FBA配送列表',
    afn_warehouse_quantity BIGINT COMMENT 'FBA库存数量',
    afn_fulfillable_quantity BIGINT COMMENT 'FBA发货数量',
    afn_unsellable_quantity BIGINT COMMENT 'FBA不可售数量',
    afn_reserved_quantity BIGINT COMMENT 'FBA预留数量',
    afn_total_quantity BIGINT COMMENT 'FBA合计数量',
    per_unit_volume DECIMAL(18,6) COMMENT '单位体积',
    afn_inbound_working_quantity BIGINT COMMENT 'AFN入境工作数量',
    afn_inbound_shipped_quantity BIGINT COMMENT 'AFN入境的装船数量',
    afn_inbound_receiving_quantity BIGINT COMMENT 'AFN入境接待量',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期',
    afn_researching_quantity BIGINT COMMENT 'AFN调查中数量',
    afn_reserved_future_supply BIGINT COMMENT '库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量',
    afn_future_supply_buyable BIGINT COMMENT '库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量',
    afn_fulfillable_quantity_local BIGINT COMMENT 'FBA本地可售数量',
    afn_fulfillable_quantity_remote BIGINT COMMENT 'FBA远程可售数量',
    marketplace_type STRING COMMENT '站点类型',
    en_country_name STRING COMMENT '国家英文名',
    cn_country_name STRING COMMENT '国家中文名',
    country_code STRING COMMENT '国家编码'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='FBA实时库存明细表（参考dwd_scm_ivt_fba_stock_current_num_df的处理逻辑）') ;

INSERT OVERWRITE TABLE  amz.mid_scm_ivt_amazon_fba_stock_current_num_df PARTITION (ds = '${last_day}')
SELECT
    tenant_id,
    report_id,
    record_id,
    T1.marketplace_id,
    seller_id,
    erp_store_id,
    seller_sku,
    fnsku,
    asin,
    product_name,
    status,
    product_price,
    mfn_listing_exists,
    mfn_fulfillable_quantity,
    afn_listing_exists,
    afn_warehouse_quantity,
    afn_fulfillable_quantity,
    afn_unsellable_quantity,
    afn_reserved_quantity,
    afn_total_quantity,
    per_unit_volume,
    afn_inbound_working_quantity,
    afn_inbound_shipped_quantity,
    afn_inbound_receiving_quantity,
    data_src,
    table_src,
    data_dt,
    etl_data_dt,
    afn_researching_quantity,
    afn_reserved_future_supply,
    afn_future_supply_buyable,
    afn_fulfillable_quantity_local,
    afn_fulfillable_quantity_remote,

    T2.marketplace_type,
    T2.en_country_name,
    T2.cn_country_name,
    T2.country_code

FROM
    (SELECT  tenant_id,
             report_id,
             ''record_id,
             marketplace_id,
             seller_id,
             store_id erp_store_id,
             sku seller_sku,
             fnsku,
             asin,
             product_name,
             status,
             cast(your_price as DECIMAL (18,6)) product_price,
             mfn_listing_exists,
             cast(mfn_fulfillable_quantity as BIGINT ) mfn_fulfillable_quantity,
             afn_listing_exists,
             cast(afn_warehouse_quantity as BIGINT ) afn_warehouse_quantity,
             cast(afn_fulfillable_quantity as BIGINT ) afn_fulfillable_quantity,
             cast(afn_unsellable_quantity as BIGINT ) afn_unsellable_quantity,
             cast(afn_reserved_quantity as BIGINT ) afn_reserved_quantity,
             cast(afn_total_quantity as BIGINT ) afn_total_quantity,
             cast(per_unit_volume as DECIMAL (18,6) ) per_unit_volume,
             cast(afn_inbound_working_quantity as BIGINT ) afn_inbound_working_quantity,
             cast(afn_inbound_shipped_quantity as BIGINT ) afn_inbound_shipped_quantity,
             cast(afn_inbound_receiving_quantity as BIGINT ) afn_inbound_receiving_quantity,
             'report'data_src,
             'get_fba_myi_unsuppressed_inventory_data'table_src,
             '${last_day}'data_dt,
             current_date() etl_data_dt,

             --//新增的字段
             cast(afn_researching_quantity AS BIGINT) afn_researching_quantity,
             cast(afn_reserved_future_supply AS BIGINT) afn_reserved_future_supply,
             cast(afn_future_supply_buyable  AS BIGINT) afn_future_supply_buyable,
             cast(afn_fulfillable_quantity_local  AS BIGINT) afn_fulfillable_quantity_local,
             cast(afn_fulfillable_quantity_remote  AS BIGINT) afn_fulfillable_quantity_remote,
             ROW_NUMBER()OVER(PARTITION BY tenant_id,marketplace_id,seller_id,sku,fnsku,asin ORDER BY data_last_update_time DESC  ) AS RANK_ID --取最新下载的数据

     FROM   (select
                 case when id = '' then null else id end as id,
                 case when report_type = '' then null else report_type end as report_type,
                 case when store_id = '' then null else store_id end as store_id,
                 case when seller_id = '' then null else seller_id end as seller_id,
                 case when marketplace_id = '' then null else marketplace_id end as marketplace_id,
                 case when report_id = '' then null else report_id end as report_id,
                 case when start_date = '' then null else start_date end as start_date,
                 case when end_date = '' then null else end_date end as end_date,
                 case when data_last_update_time = '' then null else data_last_update_time end as data_last_update_time,
                 case when sku = '' then null else sku end as sku,
                 case when fnsku = '' then null else fnsku end as fnsku,
                 case when asin = '' then null else asin end as asin,
                 case when product_name = '' then null else product_name end as product_name,
                 case when status = '' then null else status end as status,
                 case when your_price = '' then null else your_price end as your_price,
                 case when mfn_listing_exists = '' then null else mfn_listing_exists end as mfn_listing_exists,
                 case when mfn_fulfillable_quantity = '' then null else mfn_fulfillable_quantity end as mfn_fulfillable_quantity,
                 case when afn_listing_exists = '' then null else afn_listing_exists end as afn_listing_exists,
                 case when afn_warehouse_quantity = '' then null else afn_warehouse_quantity end as afn_warehouse_quantity,
                 case when afn_fulfillable_quantity = '' then null else afn_fulfillable_quantity end as afn_fulfillable_quantity,
                 case when afn_unsellable_quantity = '' then null else afn_unsellable_quantity end as afn_unsellable_quantity,
                 case when afn_reserved_quantity = '' then null else afn_reserved_quantity end as afn_reserved_quantity,
                 case when afn_total_quantity = '' then null else afn_total_quantity end as afn_total_quantity,
                 case when per_unit_volume = '' then null else per_unit_volume end as per_unit_volume,
                 case when afn_inbound_working_quantity = '' then null else afn_inbound_working_quantity end as afn_inbound_working_quantity,
                 case when afn_inbound_shipped_quantity = '' then null else afn_inbound_shipped_quantity end as afn_inbound_shipped_quantity,
                 case when afn_inbound_receiving_quantity = '' then null else afn_inbound_receiving_quantity end as afn_inbound_receiving_quantity,
                 case when afn_researching_quantity = '' then null else afn_researching_quantity end as afn_researching_quantity,
                 case when afn_reserved_future_supply = '' then null else afn_reserved_future_supply end as afn_reserved_future_supply,
                 case when afn_future_supply_buyable = '' then null else afn_future_supply_buyable end as afn_future_supply_buyable,
                 case when afn_fulfillable_quantity_local = '' then null else afn_fulfillable_quantity_local end as afn_fulfillable_quantity_local,
                 case when afn_fulfillable_quantity_remote = '' then null else afn_fulfillable_quantity_remote end as afn_fulfillable_quantity_remote,
                 case when tenant_id = '' then null else tenant_id end as tenant_id
             from ods.ods_report_get_fba_myi_unsuppressed_inventory_data_df -- whde.get_fba_myi_unsuppressed_inventory_data
             where  ds = '${last_day}'
               and afn_listing_exists ='Yes'
            ) t
    ) T1
        LEFT JOIN

    (SELECT market_place_id marketplace_id
          ,market_place_type marketplace_type
          ,country_en_name  en_country_name
          ,country_cn_name cn_country_name
          ,country_code
     FROM dim.dim_base_marketplace_info_df  where  ds = '${last_day}'  -- whde.dim_marketplace_info_df
    )T2
    ON T1.marketplace_id=T2.marketplace_id
where T1.RANK_ID=1
;
