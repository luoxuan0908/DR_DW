--@exclude_input=whde.ods_manual_dim_amazon_promotion_calendar
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 17:16:12
--********************************************************************--

---//
--下游使用方法1：seller_SKU+asin+【站点】作为主键，这样有些站点的库存为0，但是整体的库存和销量是对的上的

--下游使用方法2：英国站单独关联；其他站点：店铺+seller_SKU+asin+【站点类型】作为主键，
--这样可以保证每个店铺下每个站点下的每个asin都有库存，但是汇总的库存和库存销量是大于整体的



--库存管理逻辑：按照店铺管理
--库存共享逻辑：北美和欧洲不会跨区共享（店铺一般不会跨区域）；但是北美内 和 欧洲内会分别各自共享

--站点类型是欧洲的话，是都会下载，德国站的库存会包含除英国外的其他站的库存，所以如果一个asin有卖德国，有卖其他，有卖英国，
--只需要取德国的库存+英国的库存即可
--一个asin卖欧洲站，默认肯定卖德国站，不卖德国站的 现在没有这个处理


--1.fba总库存（原字段）=fba在库（原字段）+fba在途（衍生字段）；afn_total_quantity=afn_warehouse_quantity+AFN_inbound_total_num
--2.fba在库（原字段）=FBA可售数量+FBA不可售+FBA预留+FBA调查中数量
--3.fba在途（衍生字段）=afn_inbound_working_quantity+afn_inbound_shipped_quantity+afn_inbound_receiving_quantity



--库存数据处理

--英国站的库存单独拿出来
--按照共享逻辑 把非欧洲站的数据拿出来，欧洲站的数据 在tmp2 处理
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp1;
create TABLE IF NOT EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp1  LIFECYCLE 3 --其实最细粒度是seller_sku
AS

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

FROM  whde.dwd_scm_ivt_amazon_fba_stock_current_num_df
WHERE ds='${bizdate}'

  AND
    (
        marketplace_type='Far East'
            OR
        (marketplace_type='Europe' and marketplace_id='A1F83G8C2ARO7P') --英国站点
            OR marketplace_type='North America'
        )
;



--/// 欧洲站（除英国站之外的库存）库存是共享的，所以取 库存最多的那个就可以
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp2;
create TABLE IF NOT EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp2 LIFECYCLE 3 --其实最细粒度是seller_sku
AS
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
     FROM  whde.dwd_scm_ivt_amazon_fba_stock_current_num_df
     WHERE ds='${bizdate}'
       and marketplace_type='Europe'
       and marketplace_id<>'A1F83G8C2ARO7P' --不是英国站点的，取库存最多的那个国家
    ) T1
WHERE  T1.RANK_ID=1
;







--没加字段之前的逻辑加工好一张临时表，不想再重新组合逻辑了




drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp3;
create TABLE IF NOT EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp3 LIFECYCLE 3 --其实最细粒度是seller_sku
AS
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
     FROM  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp1

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
     FROM  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp2
    ) T1
;

--------------销量计算

--//剔除大促 销量天数计算
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp4;
create TABLE IF NOT EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp4 LIFECYCLE 3 --其实最细粒度是seller_sku
AS

SELECT
    COUNT(DISTINCT n1d_flag) AS n1d_CNT
     ,COUNT(DISTINCT n7d_flag) AS n7d_CNT
     ,COUNT(DISTINCT n15d_flag) AS n15d_CNT
     ,COUNT(DISTINCT n30d_flag) AS n30d_CNT
     ,COUNT(DISTINCT n60d_flag) AS n60d_CNT
FROM
    (
        SELECT
            case WHEN raw_purchase_time = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
                     then raw_purchase_time end as n1d_flag

             ,case when raw_purchase_time >= to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                       then raw_purchase_time end  as n7d_flag


             ,case when   raw_purchase_time >=  to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                       then raw_purchase_time end as n15d_flag

             ,case when raw_purchase_time >=  to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                       then raw_purchase_time end as n30d_flag

             ,case when raw_purchase_time >=  to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                       then raw_purchase_time end as n60d_flag

        FROM
            (
                SELECT raw_purchase_time
                FROM
                    (SELECT  to_char(purchase_time,'yyyymmdd')  raw_purchase_time
                     from  whde.dwd_amzn_all_orders_df
                     where ds = '${bizdate}'
                       AND   purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd')
                    ) T1
                        LEFT JOIN
                    (SELECT
                         promotion_day
                     FROM whde.ods_manual_dim_amazon_promotion_calendar
                     group by promotion_day
                    )T2
                    ON T1.raw_purchase_time=T2.promotion_day
                WHERE T2.promotion_day IS NULL
                GROUP BY raw_purchase_time
            )M
        GROUP BY
            case WHEN raw_purchase_time = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
                     then raw_purchase_time end
               ,case when raw_purchase_time >= to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                         then raw_purchase_time end


               ,case when   raw_purchase_time >=  to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                         then raw_purchase_time end

               ,case when raw_purchase_time >=  to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                         then raw_purchase_time end

               ,case when raw_purchase_time >=  to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') ,'yyyymmdd') and raw_purchase_time < '${bizdate}'
                         then raw_purchase_time end

    ) L
;




--//剔除大促 销量计算

drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp5;
create TABLE IF NOT EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp5 LIFECYCLE 3 --其实最细粒度是seller_sku
AS

SELECT
    tenant_id
     ,seller_id
     ,seller_sku
     ,asin
     ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国'  else marketplace_type end as marketplace_type2
     ,sum(case when to_char(raw_purchase_time,'yyyymmdd') = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd') then ordered_num else 0 end) as afnstock_n1d_sale_num
     ,sum(case when raw_purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and raw_purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afnstock_n7d_sale_num
     ,sum(case when  raw_purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-15,'dd') and raw_purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afnstock_n15d_sale_num
     ,sum(case when  raw_purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-30,'dd') and raw_purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afnstock_n30d_sale_num
     ,sum(case when  raw_purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd') and raw_purchase_time < to_date('${bizdate}','yyyymmdd')then ordered_num else 0 end) as afnstock_n60d_sale_num

FROM
    (select   tenant_id
          ,seller_id
          ,seller_sku
          ,asin
          ,marketplace_id
          ,fulfillment_channel
          ,purchase_time raw_purchase_time
          ,ordered_num
          ,to_char(purchase_time,'yyyymmdd')  raw_purchase_FLAG
          ,marketplace_type
     from  whde.dwd_amzn_all_orders_df
     where ds = '${bizdate}'
       and ordered_num >0
       and nvl(order_status,'') <> 'Canceled'
       AND  purchase_time >= dateadd(to_date('${bizdate}','yyyymmdd'),-60,'dd')
    )T1

        LEFT JOIN
    (SELECT
         promotion_day
     FROM whde.ods_manual_dim_amazon_promotion_calendar
     group by promotion_day
    )T2
    ON T1.raw_purchase_FLAG=T2.promotion_day
WHERE T2.promotion_day IS NULL
group by   tenant_id
       ,seller_id
       ,seller_sku
       ,asin
       ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国'  else marketplace_type end
;



drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp6;
create TABLE IF NOT EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp6 LIFECYCLE 3 --其实最细粒度是seller_sku
AS

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
          ,'${bizdate}' ds
     FROM dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp5
    )T1
        LEFT JOIN
    (SELECT
         '${bizdate}' DS
          ,n1d_CNT
          ,n7d_CNT
          ,n15d_CNT
          ,n30d_CNT
          ,n60d_CNT
     FROM  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp4
    ) T2
    ON T1.ds=T2.ds
;

CREATE TABLE IF NOT EXISTS WHDE.dws_scm_ivt_amazon_asin_df(
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
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊asin粒度FBA库存数据')
    LIFECYCLE 366;




INSERT OVERWRITE TABLE  dws_scm_ivt_amazon_asin_df PARTITION (ds = '${bizdate}')


SELECT
    T1.tenant_id,
    T1.marketplace_id,
    T1.marketplace_type,
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
    --T2.afn_reserved_customerorders_num,
    --T2.afn_reserved_fc_transfers_num,
    --T2.afn_reserved_fc_processing_num,

    T1.afn_researching_num,
    T1.afn_inbound_num,
    T1.afn_inbound_working_num,
    T1.afn_inbound_shipped_num,
    T1.afn_inbound_receiving_num,
    T1.afn_reserved_future_supply,
    T1.afn_future_supply_buyable,

    T6.afnstock_n1d_sale_num,
    cast(T6.afnstock_n7d_avg_sale_num as decimal(18,6)),
    cast(T6.afnstock_n15d_avg_sale_num as decimal(18,6)),
    cast(T6.afnstock_n30d_avg_sale_num as decimal(18,6)),
    cast(T6.afnstock_n60d_avg_sale_num as decimal(18,6)),
    '${bizdate}' data_dt,
    GETDATE()  etl_data_dt


FROM dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp3  T1
         LEFT JOIN dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp6 T6
                   ON  T1.tenant_id=T6.tenant_id
                       AND T1.marketplace_type2=T6.marketplace_type2
                       AND T1.seller_id=T6.seller_id
                       AND T1.seller_sku=T6.seller_sku
                       AND T1.asin=T6.asin
--LEFT JOIN
--
--(select
--
-- tenant_id
--,marketplace_id
--,seller_id
--,seller_sku
--,fnsku
--,asin
--,reserved_customerorders AS afn_reserved_customerorders_num
--,reserved_fc_transfers  AS afn_reserved_fc_transfers_num
--,reserved_fc_processing  AS afn_reserved_fc_processing_num
--FROM
--     dwd_scm_ivt_amazon_fba_reserved_inventory_report_df
--WHERE DS='${bizdate}'
--) T2
--
--ON T1.tenant_id=T2.tenant_id
--AND T1.marketplace_id=T2.marketplace_id
--AND T1.seller_id=T2.seller_id
--AND T1.seller_sku=T2.seller_sku
--AND T1.asin=T2.asin
;




--删除临时表
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp1;
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp2;
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp3;
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp4;
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp5;
drop table  IF EXISTS  dws_scm_ivt_amazon_asin_${bizdate}_zt_tmp6;


