
drop table if exists amz.mid_scm_ivt_amazon_asin_df;
CREATE TABLE IF NOT EXISTS amz.mid_scm_ivt_amazon_asin_df(
    tenant_id STRING COMMENT '租户ID',
    marketplace_id STRING COMMENT '站点ID',
    marketplace_type STRING COMMENT '站点类型',
    en_country_name STRING COMMENT '国家英文名',
    cn_country_name STRING COMMENT '国家中文名',
    country_code STRING COMMENT '国家编码',
    seller_id STRING COMMENT '卖家ID',
    seller_sku STRING COMMENT '店铺SKU',
    fnsku STRING COMMENT '平台SKU',
    asin STRING COMMENT '子asin',
    afn_total_num BIGINT COMMENT 'FBA库存总计',
    afn_warehouse_num BIGINT COMMENT 'FBA在库库存',
    afn_fulfillable_num BIGINT COMMENT 'FBA可售数量',
    afn_unsellable_num BIGINT COMMENT 'FBA不可售数量',
    afn_reserved_num BIGINT COMMENT 'FBA预留数量',
    --afn_reserved_customerorders_num BIGINT COMMENT 'FBA预留_为买家订单预留的商品数量',
    --afn_reserved_fc_transfers_num BIGINT COMMENT 'FBA预留_正在从一个运营中心转运至另一运营中心的商品数量',
    --afn_reserved_fc_processing_num BIGINT COMMENT 'FBA预留_搁置在运营中心等待进行其他处理的商品数量，包括与移除订单关联的商品',
    afn_researching_num BIGINT COMMENT 'FBA调查中数量',
    afn_inbound_num BIGINT COMMENT 'FBA在途库存',
    afn_inbound_working_num BIGINT COMMENT 'AFN入境工作数量',
    afn_inbound_shipped_num BIGINT COMMENT 'AFN入境的装船数量',
    afn_inbound_receiving_num BIGINT COMMENT 'AFN入境接待量',
    afn_reserved_future_supply BIGINT COMMENT '库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量',
    afn_future_supply_buyable BIGINT COMMENT '库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量',
    afnstock_n1d_sale_num BIGINT COMMENT '近1天销量（剔除大促）_计算FBA库存天数',
    afnstock_n7d_avg_sale_num DECIMAL(18,6) COMMENT '近7天日均销量（剔除大促）_计算FBA库存天数',
    afnstock_n15d_avg_sale_num DECIMAL(18,6) COMMENT '近15天日均销量（剔除大促）_计算FBA库存天数',
    afnstock_n30d_avg_sale_num DECIMAL(18,6) COMMENT '近30天日均销量（剔除大促）_计算FBA库存天数',
    afnstock_n60d_avg_sale_num DECIMAL(18,6) COMMENT '近60天日均销量（剔除大促）_计算FBA库存天数',

    fba_first_instock_date STRING COMMENT '首次入库时间',
    fba_instock_num BIGINT COMMENT '入库次数',
    fba_instock_days BIGINT COMMENT '首次入库距今天数',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt timestamp COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊asin粒度FBA库存数据');

with dws_scm_ivt_amazon_asin_zt_tmp1 as (
    SELECT
        tenant_id
         ,marketplace_id
         ,marketplace_type
         ,en_country_name
         ,cn_country_name
         ,country_code
         ,seller_id
         ,seller_sku
         ,fnsku
         ,asin
         ,afn_total_quantity -- 'FBA总库存' =FBA在库+FBA在途
         ,afn_warehouse_quantity --//'FBA在库数量'=FBA可售+FBA不可售+FBA预留
         ,afn_fulfillable_quantity  --'FBA可售数量'
         ,afn_unsellable_quantity -- 'FBA不可售数量'
         ,afn_reserved_quantity --'FBA预留数量'
         ,afn_inbound_working_quantity+afn_inbound_shipped_quantity+afn_inbound_receiving_quantity AS AFN_inbound_total_num --fba在途
         ,afn_inbound_working_quantity --'AFN入境工作数量'
         ,afn_inbound_shipped_quantity  -- 'AFN入境的装船数量'
         ,afn_inbound_receiving_quantity --AFN入境接待量'
         ,afn_researching_quantity --'AFN调查中数量'
         ,afn_reserved_future_supply  --库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量
         ,afn_future_supply_buyable --库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量
    FROM  amz.mid_scm_ivt_amazon_fba_stock_current_num_df
    WHERE ds='20240822'
      AND
        (
            marketplace_type='Far East'
                OR
            (marketplace_type='Europe' and marketplace_id='A1F83G8C2ARO7P') --英国站点
                OR marketplace_type='North America'
            )
),dws_scm_ivt_amazon_asin_zt_tmp2 as (
    SELECT tenant_id
         ,marketplace_id
         ,marketplace_type
         ,en_country_name
         ,cn_country_name
         ,country_code
         ,seller_id
         ,seller_sku
         ,fnsku
         ,asin
--//
         ,afn_total_quantity -- 'FBA总库存' =FBA在库+FBA在途

         ,afn_warehouse_quantity --//'FBA在库数量'=FBA可售+FBA不可售+FBA预留


         ,afn_fulfillable_quantity  --'FBA可售数量'
         ,afn_unsellable_quantity -- 'FBA不可售数量'
         ,afn_reserved_quantity --'FBA预留数量'
         ,afn_researching_quantity --'AFN调查中数量'


         ,AFN_inbound_total_num --fba在途
         ,afn_inbound_working_quantity --'AFN入境工作数量'
         ,afn_inbound_shipped_quantity  -- 'AFN入境的装船数量'
         ,afn_inbound_receiving_quantity --AFN入境接待量'

         ,afn_reserved_future_supply  --库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量
         ,afn_future_supply_buyable --库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量

    FROM
        (SELECT
             tenant_id
              ,marketplace_id
              ,marketplace_type
              ,en_country_name
              ,cn_country_name
              ,country_code
              ,seller_id
              ,seller_sku
              ,fnsku
              ,asin
--//
              ,afn_total_quantity -- 'FBA总库存' =FBA在库+FBA在途

              ,afn_warehouse_quantity --//'FBA在库数量'=FBA可售+FBA不可售+FBA预留


              ,afn_fulfillable_quantity  --'FBA可售数量'
              ,afn_unsellable_quantity -- 'FBA不可售数量'
              ,afn_reserved_quantity --'FBA预留数量'


              ,afn_inbound_working_quantity+afn_inbound_shipped_quantity+afn_inbound_receiving_quantity AS AFN_inbound_total_num --fba在途
              ,afn_inbound_working_quantity --'AFN入境工作数量'
              ,afn_inbound_shipped_quantity  -- 'AFN入境的装船数量'
              ,afn_inbound_receiving_quantity --AFN入境接待量'


              ,afn_researching_quantity --'AFN调查中数量'
              ,afn_reserved_future_supply  --库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量
              ,afn_future_supply_buyable --库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量

              ,ROW_NUMBER()OVER(PARTITION BY tenant_id,seller_id,asin,seller_sku,marketplace_type ORDER BY afn_total_quantity DESC ) AS RANK_ID
         FROM  amz.mid_scm_ivt_amazon_fba_stock_current_num_df
         WHERE ds='20240822'
           and marketplace_type='Europe'
           and marketplace_id<>'A1F83G8C2ARO7P' --不是英国站点的，取库存最多的那个国家
        ) T1
    WHERE  T1.RANK_ID=1
     -- and seller_id ='A2MGVMX7S4A416'
-- --       and marketplace_id ='A13V1IB3VIYZZH'
--       and asin ='B07WZTYXQ2'
--       and seller_sku ='19-5LX9-ZXJI'
) ,dws_scm_ivt_amazon_asin_zt_tmp3 as (
    SELECT T1.tenant_id
         ,T1.marketplace_id
         ,T1.marketplace_type
         ,T1.marketplace_type2
         ,T1.en_country_name
         ,T1.cn_country_name
         ,T1.country_code
         ,T1.seller_id
         ,T1.seller_sku
         ,T1.fnsku
         ,T1.asin
--//
         ,NVL(T1.afn_total_quantity,0) afn_total_num-- 'FBA总库存' =FBA在库+FBA在途

         ,NVL(T1.afn_warehouse_quantity,0) afn_warehouse_num --//'FBA在库数量'=FBA可售+FBA不可售+FBA预留


         ,NVL(T1.afn_fulfillable_quantity,0) afn_fulfillable_num  --'FBA可售数量'
         ,NVL(T1.afn_unsellable_quantity,0) afn_unsellable_num -- 'FBA不可售数量'
         ,NVL(T1.afn_reserved_quantity,0) afn_reserved_num --'FBA预留数量'
         ,NVL(T1.afn_researching_quantity,0) afn_researching_num --'AFN调查中数量'


         ,NVL(T1.AFN_inbound_total_num,0) afn_inbound_num--fba在途
         ,NVL(T1.afn_inbound_working_quantity,0) afn_inbound_working_num--'AFN入境工作数量'
         ,NVL(T1.afn_inbound_shipped_quantity,0) afn_inbound_shipped_num -- 'AFN入境的装船数量'
         ,NVL(T1.afn_inbound_receiving_quantity,0) afn_inbound_receiving_num--AFN入境接待量'

         ,NVL(T1.afn_reserved_future_supply,0) afn_reserved_future_supply --库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量
         ,NVL(T1.afn_future_supply_buyable,0) afn_future_supply_buyable--库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量


    FROM
        (SELECT tenant_id
              ,marketplace_id
              ,marketplace_type
              ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国'  ELSE marketplace_type END marketplace_type2
              ,en_country_name
              ,cn_country_name
              ,country_code
              ,seller_id
              ,seller_sku
              ,fnsku
              ,asin
--//
              ,afn_total_quantity -- 'FBA总库存' =FBA在库+FBA在途

              ,afn_warehouse_quantity --//'FBA在库数量'=FBA可售+FBA不可售+FBA预留


              ,afn_fulfillable_quantity  --'FBA可售数量'
              ,afn_unsellable_quantity -- 'FBA不可售数量'
              ,afn_reserved_quantity --'FBA预留数量'
              ,afn_researching_quantity --'AFN调查中数量'


              ,AFN_inbound_total_num --fba在途
              ,afn_inbound_working_quantity --'AFN入境工作数量'
              ,afn_inbound_shipped_quantity  -- 'AFN入境的装船数量'
              ,afn_inbound_receiving_quantity --AFN入境接待量'

              ,afn_reserved_future_supply  --库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量
              ,afn_future_supply_buyable --库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量
         FROM  dws_scm_ivt_amazon_asin_zt_tmp1

         UNION ALL

         SELECT tenant_id
              ,marketplace_id
              ,marketplace_type
              ,marketplace_type marketplace_type2
              ,en_country_name
              ,cn_country_name
              ,country_code
              ,seller_id
              ,seller_sku
              ,fnsku
              ,asin
--//
              ,afn_total_quantity -- 'FBA总库存' =FBA在库+FBA在途

              ,afn_warehouse_quantity --//'FBA在库数量'=FBA可售+FBA不可售+FBA预留


              ,afn_fulfillable_quantity  --'FBA可售数量'
              ,afn_unsellable_quantity -- 'FBA不可售数量'
              ,afn_reserved_quantity --'FBA预留数量'
              ,afn_researching_quantity --'AFN调查中数量'


              ,AFN_inbound_total_num --fba在途
              ,afn_inbound_working_quantity --'AFN入境工作数量'
              ,afn_inbound_shipped_quantity  -- 'AFN入境的装船数量'
              ,afn_inbound_receiving_quantity --AFN入境接待量'

              ,afn_reserved_future_supply  --库存通过库存预售运往亚马逊运营中心途中，其可供买家查找和购买的商品数量
              ,afn_future_supply_buyable --库存通过库存预售运往亚马逊运营中心途中，买家已购买该库存商品的数量
         FROM  dws_scm_ivt_amazon_asin_zt_tmp2
        ) T1
) --select  * from dws_scm_ivt_amazon_asin_zt_tmp3
   ,dws_scm_ivt_amazon_asin_zt_tmp4 as (
    SELECT
        COUNT(DISTINCT n1d_flag) AS n1d_CNT
         ,COUNT(DISTINCT n7d_flag) AS n7d_CNT
         ,COUNT(DISTINCT n15d_flag) AS n15d_CNT
         ,COUNT(DISTINCT n30d_flag) AS n30d_CNT
         ,COUNT(DISTINCT n60d_flag) AS n60d_CNT
    FROM
        (
            SELECT
                case WHEN raw_purchase_time = date_format(date_add('2024-08-21',-1),'yyyyMMdd')
                         then raw_purchase_time end as n1d_flag

                 ,case when raw_purchase_time >= date_format(date_add('2024-08-21',-7),'yyyyMMdd') and raw_purchase_time < '20240822'
                           then raw_purchase_time end  as n7d_flag


                 ,case when   raw_purchase_time >=  date_format(date_add('2024-08-21',-15),'yyyyMMdd') and raw_purchase_time < '20240822'
                           then raw_purchase_time end as n15d_flag

                 ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-30),'yyyyMMdd') and raw_purchase_time < '20240822'
                           then raw_purchase_time end as n30d_flag

                 ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-60),'yyyyMMdd') and raw_purchase_time < '20240822'
                           then raw_purchase_time end as n60d_flag

            FROM
                (
                    SELECT raw_purchase_time
                    FROM
                        (SELECT  date_format(purchase_time,'yyyyMMdd')  raw_purchase_time
                         from  amz.mid_amzn_all_orders_df
                         where ds = '20240822'
                           AND   purchase_time >= date_add('2024-08-21',-60)
                        ) T1
                            LEFT JOIN
                        (SELECT
                             promotion_day
                         FROM dim.dim_base_amazon_promotion_calendar
                         group by promotion_day
                        )T2
                        ON T1.raw_purchase_time=T2.promotion_day
                    WHERE T2.promotion_day IS NULL
                    GROUP BY raw_purchase_time
                )M
            GROUP BY
                case WHEN raw_purchase_time = date_format(date_add('2024-08-21',-1),'yyyyMMdd')
                         then raw_purchase_time end

                   ,case when raw_purchase_time >= date_format(date_add('2024-08-21',-7),'yyyyMMdd') and raw_purchase_time < '20240822'
                             then raw_purchase_time end


                   ,case when   raw_purchase_time >=  date_format(date_add('2024-08-21',-15),'yyyyMMdd') and raw_purchase_time < '20240822'
                             then raw_purchase_time end

                   ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-30),'yyyyMMdd') and raw_purchase_time < '20240822'
                             then raw_purchase_time end

                   ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-60),'yyyyMMdd') and raw_purchase_time < '20240822'
                             then raw_purchase_time end


        ) L
),dws_scm_ivt_amazon_asin_zt_tmp5 as (
    SELECT
        tenant_id
         ,seller_id
         ,seller_sku
         ,asin
         ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国'  else marketplace_type end as marketplace_type2
         ,sum(case when raw_purchase_FLAG = date_format(date_add('2024-08-21',-1),'yyyyMMdd') then ordered_num else 0 end) as afnstock_n1d_sale_num
         ,sum(case when raw_purchase_FLAG >= date_format(date_add('2024-08-21',-7),'yyyyMMdd') and raw_purchase_FLAG < '20240822' then ordered_num else 0 end) as afnstock_n7d_sale_num
         ,sum(case when  raw_purchase_FLAG >= date_format(date_add('2024-08-21',-15),'yyyyMMdd')  and raw_purchase_FLAG <  '20240822' then ordered_num else 0 end) as afnstock_n15d_sale_num
         ,sum(case when  raw_purchase_FLAG >= date_format(date_add('2024-08-21',-30),'yyyyMMdd')  and raw_purchase_FLAG < '20240822' then ordered_num else 0 end) as afnstock_n30d_sale_num
         ,sum(case when  raw_purchase_FLAG >= date_format(date_add('2024-08-21',-60),'yyyyMMdd')  and raw_purchase_FLAG < '20240822' then ordered_num else 0 end) as afnstock_n60d_sale_num
-- select  raw_purchase_FLAG,raw_purchase_time,date_format(date_add('2024-08-21',-1),'yyyyMMdd'), date_format(date_add('2024-08-21',-7),'yyyyMMdd')
    FROM
        (select   tenant_id
              ,seller_id
              ,seller_sku
              ,asin
              ,marketplace_id
              ,fulfillment_channel
              ,purchase_time raw_purchase_time
              ,ordered_num
              ,date_format(purchase_time,'yyyyMMdd')  raw_purchase_FLAG
              ,marketplace_type
         from  amz.mid_amzn_all_orders_df
         where ds = '20240822'
           and ordered_num >0
           and nvl(order_status,'') <> 'Canceled'
           AND  purchase_time >= date_add('2024-08-21',-60)
        )T1

            LEFT JOIN
        (SELECT
             promotion_day
         FROM dim.dim_base_amazon_promotion_calendar
         group by promotion_day
        )T2
        ON T1.raw_purchase_FLAG=T2.promotion_day
    WHERE T2.promotion_day IS NULL
    group by   tenant_id
           ,seller_id
           ,seller_sku
           ,asin
           ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国'  else marketplace_type end
),dws_scm_ivt_amazon_asin_zt_tmp6 as (
    SELECT
        tenant_id
         ,seller_id
         ,seller_sku
         ,asin
         ,marketplace_type2
         ,afnstock_n1d_sale_num
         ,afnstock_n7d_sale_num
         ,afnstock_n15d_sale_num
         ,afnstock_n30d_sale_num
         ,afnstock_n60d_sale_num
         ,if(n7d_CNT>0, afnstock_n7d_sale_num/n7d_CNT, Null) afnstock_n7d_avg_sale_num
         ,if(n15d_CNT>0, afnstock_n15d_sale_num/n15d_CNT, Null) afnstock_n15d_avg_sale_num
         ,if(n30d_CNT>0, afnstock_n30d_sale_num/n30d_CNT, Null) afnstock_n30d_avg_sale_num
         ,if(n60d_CNT>0, afnstock_n60d_sale_num/n60d_CNT, Null) afnstock_n60d_avg_sale_num
    from
        (SELECT
             tenant_id
              ,seller_id
              ,seller_sku
              ,asin
              ,marketplace_type2
              ,afnstock_n1d_sale_num
              ,afnstock_n7d_sale_num
              ,afnstock_n15d_sale_num
              ,afnstock_n30d_sale_num
              ,afnstock_n60d_sale_num
              ,'20240822' ds
         FROM dws_scm_ivt_amazon_asin_zt_tmp5
        )T1
            LEFT JOIN
        (SELECT
             '20240822' DS
              ,n1d_CNT
              ,n7d_CNT
              ,n15d_CNT
              ,n30d_CNT
              ,n60d_CNT
         FROM  dws_scm_ivt_amazon_asin_zt_tmp4
        ) T2
        ON T1.ds=T2.ds
),ledger_data AS (
    SELECT  tenant_id
         ,marketplace_id
         ,seller_id
         ,asin
         ,MIN(operation_date) AS fba_first_instock_date
         ,COUNT(DISTINCT operation_date) fba_instock_num
    FROM   amz.mid_scm_ivt_amazon_ledger_detail_view_df
    WHERE   ds='20240820'
      AND     event_type = 'Receipts' --是已接收库存
    GROUP BY tenant_id
           ,marketplace_id
           ,seller_id
           ,asin
)
insert overwrite table amz.mid_scm_ivt_amazon_asin_df partition(ds='20240822')
SELECT
    T1.tenant_id,
    T1.marketplace_id,
    T1.marketplace_type,
--     T1.marketplace_type2,
--     T6.marketplace_type2 as marketplace_type3,
    T1.en_country_name,
    T1.cn_country_name,
    T1.country_code,
    T1.seller_id,
    T1.seller_sku,
    T1.fnsku,
    T1.asin,
    T1.afn_total_num,
    T1.afn_warehouse_num,
    T1.afn_fulfillable_num,
    T1.afn_unsellable_num,
    T1.afn_reserved_num,


    T1.afn_researching_num,
    T1.afn_inbound_num,
    T1.afn_inbound_working_num,
    T1.afn_inbound_shipped_num,
    T1.afn_inbound_receiving_num,
    T1.afn_reserved_future_supply,
    T1.afn_future_supply_buyable,

    T6.afnstock_n1d_sale_num,
    cast(T6.afnstock_n7d_avg_sale_num as decimal(18,6)) as afnstock_n7d_avg_sale_num,
    cast(T6.afnstock_n15d_avg_sale_num as decimal(18,6)) as afnstock_n15d_avg_sale_num,
    cast(T6.afnstock_n30d_avg_sale_num as decimal(18,6)) as afnstock_n30d_avg_sale_num,
    cast(T6.afnstock_n60d_avg_sale_num as decimal(18,6)) as afnstock_n60d_avg_sale_num,
    T2.fba_first_instock_date,
    T2.fba_instock_num,
    datediff(current_date(),T2.fba_first_instock_date) as fba_instock_days,
    '20240822' data_dt,
    current_date()  etl_data_dt


FROM dws_scm_ivt_amazon_asin_zt_tmp3  T1
         LEFT JOIN dws_scm_ivt_amazon_asin_zt_tmp6 T6
                   ON  T1.tenant_id=T6.tenant_id
                       AND T1.marketplace_type2=T6.marketplace_type2
                       AND T1.seller_id=T6.seller_id
                       AND T1.seller_sku=T6.seller_sku
                       AND T1.asin=T6.asin
         LEFT JOIN ledger_data T2
                   ON  T1.tenant_id=T2.tenant_id
                       AND T1.marketplace_id=T2.marketplace_id
                       AND T1.seller_id=T2.seller_id
                       AND T1.asin=T2.asin
;
