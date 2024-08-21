--@exclude_input=whde.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 14:55:14
--********************************************************************--
-- 主键：tenant_id,seller_id,marketplace_id,seller_sku,asin,currency

drop table if exists temp_dws_trd_ord_amazon_sale_info_df_${bizdate}_orders;
create table if not exists temp_dws_trd_ord_amazon_sale_info_df_${bizdate}_orders
as
SELECT  tenant_id
     ,seller_id
     ,a.marketplace_id
     ,seller_sku
     ,asin
     ,market_place_type marketplace_type
     ,marketplace_website
     ,country_code
     ,country_cn_name as cn_country_name
     ,currency
     --,parent_asin
     ,sum(case when to_char(purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then ordered_num else 0 end) as n1d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n7d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n15d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n30d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n60d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n90d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-180,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n180d_sale_num
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-365,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as n365d_sale_num
     ,sum(case when to_char(purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then item_amt else 0 end) as n1d_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n7d_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n15d_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n30d_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n60d_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n90d_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-180,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n180_sale_amt
     ,sum(case when purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-365,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as n365_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and to_char(purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then ordered_num else 0 end) as afn_n1d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n7d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n15d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n30d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n60d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n90d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-180,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n180d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-365,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afn_n365d_sale_num
     ,sum(case when fulfillment_channel = 'Amazon' and to_char(purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then item_amt else 0 end) as afn_n1d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n7d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n15d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n30d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n60d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n90d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-180,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n180d_sale_amt
     ,sum(case when fulfillment_channel = 'Amazon' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-365,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as afn_n365d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and to_char(purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then ordered_num else 0 end) as mfn_n1d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n7d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n15d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n30d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n60d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n90d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-180,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n180d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-365,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as mfn_n365d_sale_num
     ,sum(case when fulfillment_channel = 'Merchant' and to_char(purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then item_amt else 0 end) as mfn_n1d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n7d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n15d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n30d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n60d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n90d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-180,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n180d_sale_amt
     ,sum(case when fulfillment_channel = 'Merchant' and  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-365,'dd') and purchase_time < to_date('${bizdate}','yyyymmdd')then item_amt else 0 end) as mfn_n365d_sale_amt
from  (select * from whde.dwd_amzn_all_orders_df   --dwd_trd_ord_amazon_raw_order_info_df
       where ds = '${bizdate}'
         and ordered_num >0
         and nvl(order_status,'') <> 'Canceled'
      )a
          left outer join whde.dim_marketplace_info_df b
                          on a.marketplace_id = b.market_place_id
group by tenant_id
       ,seller_id
       ,marketplace_id
       ,seller_sku
       ,asin
       ,market_place_type
       ,marketplace_website
       ,country_code
       ,country_cn_name
       ,currency
;

CREATE TABLE IF NOT EXISTS WHDE.dws_trd_ord_amazon_sale_info_df(
    tenant_id STRING COMMENT '租户ID',
    seller_id STRING COMMENT '卖家ID',
    marketplace_id STRING COMMENT '站点ID',
    seller_sku STRING COMMENT '卖家SKU',
    asin STRING COMMENT '子ASIN',
    marketplace_type STRING COMMENT '站点类型',
    marketplace_website STRING COMMENT '站点链接',
    country_code STRING COMMENT '国家编码',
    cn_country_name STRING COMMENT '国家中文名称',
    currency STRING COMMENT '货币简称',
    parent_asin STRING COMMENT '父ASIN',
    n1d_sale_num BIGINT COMMENT '近1天销量',
    n7d_sale_num BIGINT COMMENT '近7天销量',
    n15d_sale_num BIGINT COMMENT '近15天销量',
    n30d_sale_num BIGINT COMMENT '近30天销量',
    n60d_sale_num BIGINT COMMENT '近60天销量',
    n90d_sale_num BIGINT COMMENT '近90天销量',
    n180d_sale_num BIGINT COMMENT '近180天销量',
    n365d_sale_num BIGINT COMMENT '近365天销量',
    n1d_sale_amt DECIMAL(18,6) COMMENT '近1天销售额',
    n7d_sale_amt DECIMAL(18,6) COMMENT '近7天销售额',
    n15d_sale_amt DECIMAL(18,6) COMMENT '近15天销售额',
    n30d_sale_amt DECIMAL(18,6) COMMENT '近30天销售额',
    n60d_sale_amt DECIMAL(18,6) COMMENT '近60天销售额',
    n90d_sale_amt DECIMAL(18,6) COMMENT '近90天销售额',
    n180_sale_amt DECIMAL(18,6) COMMENT '近180天销售额',
    n365_sale_amt DECIMAL(18,6) COMMENT '近365天销售额',
    afn_n1d_sale_num BIGINT COMMENT 'FBA发货近1天销量',
    afn_n7d_sale_num BIGINT COMMENT 'FBA发货近7天销量',
    afn_n15d_sale_num BIGINT COMMENT 'FBA发货近15天销量',
    afn_n30d_sale_num BIGINT COMMENT 'FBA发货近30天销量',
    afn_n60d_sale_num BIGINT COMMENT 'FBA发货近60天销量',
    afn_n90d_sale_num BIGINT COMMENT 'FBA发货近90天销量',
    afn_n180d_sale_num BIGINT COMMENT 'FBA发货近180天销量',
    afn_n365d_sale_num BIGINT COMMENT 'FBA发货近365天销量',
    afn_n1d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近1天销售额',
    afn_n7d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近7天销售额',
    afn_n15d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近15天销售额',
    afn_n30d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近30天销售额',
    afn_n60d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近60天销售额',
    afn_n90d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近90天销售额',
    afn_n180d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近180天销售额',
    afn_n365d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近365天销售额',
    mfn_n1d_sale_num BIGINT COMMENT '自发货近1天销量',
    mfn_n7d_sale_num BIGINT COMMENT '自发货近7天销量',
    mfn_n15d_sale_num BIGINT COMMENT '自发货近15天销量',
    mfn_n30d_sale_num BIGINT COMMENT '自发货近30天销量',
    mfn_n60d_sale_num BIGINT COMMENT '自发货近60天销量',
    mfn_n90d_sale_num BIGINT COMMENT '自发货近90天销量',
    mfn_n180d_sale_num BIGINT COMMENT '自发货近180天销量',
    mfn_n365d_sale_num BIGINT COMMENT '自发货近365天销量',
    mfn_n1d_sale_amt DECIMAL(18,6) COMMENT '自发货近1天销售额',
    mfn_n7d_sale_amt DECIMAL(18,6) COMMENT '自发货近7天销售额',
    mfn_n15d_sale_amt DECIMAL(18,6) COMMENT '自发货近15天销售额',
    mfn_n30d_sale_amt DECIMAL(18,6) COMMENT '自发货近30天销售额',
    mfn_n60d_sale_amt DECIMAL(18,6) COMMENT '自发货近60天销售额',
    mfn_n90d_sale_amt DECIMAL(18,6) COMMENT '自发货近90天销售额',
    mfn_n180d_sale_amt DECIMAL(18,6) COMMENT '自发货近180天销售额',
    mfn_n365d_sale_amt DECIMAL(18,6) COMMENT '自发货近365天销售额',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊销售数据汇总表')
    LIFECYCLE 365;

-- 偏移2天 修改为不偏移
insert overwrite table dws_trd_ord_amazon_sale_info_df
partition(ds = '${bizdate}')
select  a1.tenant_id                                                     -- '租户ID',
     ,a1.seller_id                                                     -- '卖家ID',
     ,a1.marketplace_id                                                -- '站点ID',
     ,a1.seller_sku                                                    -- '卖家SKU',
     ,a1.asin                                                          -- '子ASIN',
     ,a1.marketplace_type                                              -- '站点类型',
     ,a1.marketplace_website                                           -- '站点链接',
     ,a1.country_code                                                  -- '国家编码',
     ,a1.cn_country_name                                               -- '国家中文名称',
     ,a1.currency                                                      -- '货币简称',
     ,a2.parent_asin                                                   -- '父ASIN',
     ,a1.n1d_sale_num                                                  -- '近1天销量',
     ,a1.n7d_sale_num                                                  -- '近7天销量',
     ,a1.n15d_sale_num                                                 -- '近15天销量',
     ,a1.n30d_sale_num                                                 -- '近30天销量',
     ,a1.n60d_sale_num                                                 -- '近60天销量',
     ,a1.n90d_sale_num                                                 -- '近90天销量',
     ,a1.n180d_sale_num                                                -- '近180天销量',
     ,a1.n365d_sale_num                                                -- '近365天销量',
     ,cast(a1.n1d_sale_amt  as decimal(18,6))  as n1d_sale_amt         -- '近1天销售额',
     ,cast(a1.n7d_sale_amt  as decimal(18,6))  as n7d_sale_amt         -- '近7天销售额',
     ,cast(a1.n15d_sale_amt as decimal(18,6))  as n15d_sale_amt        -- '近15天销售额',
     ,cast(a1.n30d_sale_amt as decimal(18,6))  as n30d_sale_amt        -- '近30天销售额',
     ,cast(a1.n60d_sale_amt as decimal(18,6))  as n60d_sale_amt        -- '近60天销售额',
     ,cast(a1.n90d_sale_amt as decimal(18,6))  as n90d_sale_amt        -- '近90天销售额',
     ,cast(a1.n180_sale_amt as decimal(18,6))  as n180_sale_amt        -- '近180天销售额',
     ,cast(a1.n365_sale_amt as decimal(18,6))  as n365_sale_amt        -- '近365天销售额',
     ,a1.afn_n1d_sale_num                                              -- 'FBA发货近1天销量',
     ,a1.afn_n7d_sale_num                                              -- 'FBA发货近7天销量',
     ,a1.afn_n15d_sale_num                                             -- 'FBA发货近15天销量',
     ,a1.afn_n30d_sale_num                                             -- 'FBA发货近30天销量',
     ,a1.afn_n60d_sale_num                                             -- 'FBA发货近60天销量',
     ,a1.afn_n90d_sale_num                                             -- 'FBA发货近90天销量',
     ,a1.afn_n180d_sale_num                                            -- 'FBA发货近180天销量',
     ,a1.afn_n365d_sale_num                                            -- 'FBA发货近365天销量',
     ,cast(a1.afn_n1d_sale_amt   as decimal(18,6))  as afn_n1d_sale_amt         -- 'FBA发货近1天销售额',
     ,cast(a1.afn_n7d_sale_amt   as decimal(18,6))  as afn_n7d_sale_amt       -- 'FBA发货近7天销售额',
     ,cast(a1.afn_n15d_sale_amt  as decimal(18,6))  as afn_n15d_sale_amt      -- 'FBA发货近15天销售额',
     ,cast(a1.afn_n30d_sale_amt  as decimal(18,6))  as afn_n30d_sale_amt      -- 'FBA发货近30天销售额',
     ,cast(a1.afn_n60d_sale_amt  as decimal(18,6))  as afn_n60d_sale_amt      -- 'FBA发货近60天销售额',
     ,cast(a1.afn_n90d_sale_amt  as decimal(18,6))  as afn_n90d_sale_amt      -- 'FBA发货近90天销售额',
     ,cast(a1.afn_n180d_sale_amt as decimal(18,6))  as afn_n180d_sale_amt     -- 'FBA发货近180天销售额'
     ,cast(a1.afn_n365d_sale_amt as decimal(18,6))  as afn_n365d_sale_amt     -- 'FBA发货近365天销售额'
     ,a1.mfn_n1d_sale_num                                                     -- '自发货近1天销量',
     ,a1.mfn_n7d_sale_num                                                     -- '自发货近7天销量',
     ,a1.mfn_n15d_sale_num                                                    -- '自发货近15天销量',
     ,a1.mfn_n30d_sale_num                                                    -- '自发货近30天销量',
     ,a1.mfn_n60d_sale_num                                                    -- '自发货近60天销量',
     ,a1.mfn_n90d_sale_num                                                    -- '自发货近90天销量',
     ,a1.mfn_n180d_sale_num                                                   -- '自发货近180天销量',
     ,a1.mfn_n365d_sale_num                                                   -- '自发货近365天销量',
     ,cast(a1.mfn_n1d_sale_amt    as decimal(18,6)) as mfn_n1d_sale_amt       -- '自发货近1天销售额',
     ,cast(a1.mfn_n7d_sale_amt    as decimal(18,6)) as mfn_n7d_sale_amt       -- '自发货近7天销售额',
     ,cast(a1.mfn_n15d_sale_amt   as decimal(18,6)) as mfn_n15d_sale_amt      -- '自发货近15天销售额',
     ,cast(a1.mfn_n30d_sale_amt   as decimal(18,6)) as mfn_n30d_sale_amt      -- '自发货近30天销售额',
     ,cast(a1.mfn_n60d_sale_amt   as decimal(18,6)) as mfn_n60d_sale_amt      -- '自发货近60天销售额',
     ,cast(a1.mfn_n90d_sale_amt   as decimal(18,6)) as mfn_n90d_sale_amt      -- '自发货近90天销售额',
     ,cast(a1.mfn_n180d_sale_amt  as decimal(18,6)) as mfn_n180d_sale_amt     -- '自发货近180天销售额',
     ,cast(a1.mfn_n365d_sale_amt  as decimal(18,6)) as mfn_n365d_sale_amt     -- '自发货近365天销售额'
     ,'${bizdate}' data_dt                                                        -- 数据日期
     ,GETDATE() AS etl_data_dt                                                    -- 数据加载日期
from temp_dws_trd_ord_amazon_sale_info_df_${bizdate}_orders a1
         left join
     (select * from  (select *,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                      from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
      where rn =1
     ) a2
     on a1.marketplace_id=a2.market_place_id
         and a1.asin=a2.asin
;



drop table if exists temp_dws_trd_ord_amazon_sale_info_df_${bizdate}_orders;
