-- 粒度：    tenant_id marketplace_id seller_id asin sku  country_code currency_en
drop table  if exists amz.dwd_prd_asin_sku_index_df;
CREATE TABLE if not exists amz.dwd_prd_asin_sku_index_df (
    id STRING COMMENT '由tenant_id、profile_id和asin生成的MD5哈希值',
    asin STRING COMMENT '卖家ASIN',
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT 'Profile ID',
    marketplace_id STRING COMMENT '市场ID',
    marketplace_name STRING COMMENT '包含国家中文名称的市场名称',
    seller_id STRING COMMENT '卖家ID',
    seller_name STRING COMMENT '卖家名称',
    seller_sku STRING COMMENT 'SKU',

    market_place_type STRING COMMENT '站点类型',
    market_place_website STRING COMMENT '站点链接',
    country_code STRING COMMENT '国家编码',
    country_en_name STRING COMMENT '国家英文名称',
    country_cn_name STRING COMMENT '国家中文名称',
    currency_en STRING COMMENT '货币简称',
    currency_cn STRING COMMENT '货币中文名称',
    time_zone STRING COMMENT '时区',
    endpoint_code STRING COMMENT '站点缩写',

    fba_total_num INT COMMENT 'FBA总库存量',
    fba_inbound_num INT COMMENT 'FBA入库总数量',
    fba_warehouse_num INT COMMENT 'FBA仓库库存量',
    fba_first_instock_days INT COMMENT '首次入库日期至今的天数',
    fba_instock_cnt INT COMMENT 'FBA入库次数',
    afn_fulfillable_quantity DOUBLE COMMENT 'AFN可配送库存量',
    color STRING COMMENT '商品颜色',
    size STRING COMMENT '商品尺寸',
    order_num_rank INT COMMENT '订单数量排名',
    order_num_rate DOUBLE COMMENT '订单数量占比',
    breadcrumbs_category_one STRING COMMENT '面包屑类别一',
    breadcrumbs_category_two STRING COMMENT '面包屑类别二',
    breadcrumbs_category_three STRING COMMENT '面包屑类别三',
    breadcrumbs_category_four STRING COMMENT '面包屑类别四',
    breadcrumbs_category_five STRING COMMENT '面包屑类别五',
    breadcrumbs_category_six STRING COMMENT '面包屑类别六',
    title STRING COMMENT '商品标题',
    link STRING COMMENT '商品链接',
    brand STRING COMMENT '商品品牌',
    main_image_url STRING COMMENT '主图URL',
    n1d_sale_num BIGINT COMMENT '过去1天的销量',
    n1d_sale_amt DECIMAL(18, 6) COMMENT '过去1天的销售金额',
    n7d_sale_num BIGINT COMMENT '过去7天的销量',
    n7d_sale_amt DECIMAL(18, 6) COMMENT '过去7天的销售金额',
    n15d_sale_num BIGINT COMMENT '过去15天的销量',
    n15d_sale_amt DECIMAL(18, 6) COMMENT '过去15天的销售金额',
    n30d_sale_num BIGINT COMMENT '过去30天的销量',
    n30d_sale_amt DECIMAL(18, 6) COMMENT '过去30天的销售金额',
    n60d_sale_num BIGINT COMMENT '过去60天的销量',
    n60d_sale_amt DECIMAL(18, 6) COMMENT '过去60天的销售金额',
    n90d_sale_num BIGINT COMMENT '过去90天的销量',
    n90d_sale_amt DECIMAL(18, 6) COMMENT '过去90天的销售金额',
    n180d_sale_num BIGINT COMMENT '过去180天的销量',
    n180d_sale_amt DECIMAL(18, 6) COMMENT '过去180天的销售金额',
    n365d_sale_num BIGINT COMMENT '过去365天的销量',
    n365d_sale_amt DECIMAL(18, 6) COMMENT '过去365天的销售金额'
    )  COMMENT '子asin宽表'
partitioned by (ds string)
STORED AS orc;

set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2048m;
set mapreduce.reduce.java.opts=-Xmx2048m;

drop table if exists amz.temp_scm_ivt_amazon_asin_df;
CREATE TABLE IF NOT EXISTS amz.temp_scm_ivt_amazon_asin_df(
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
    WHERE ds='20240821'
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
         WHERE ds='20240821'
           and marketplace_type='Europe'
           and marketplace_id<>'A1F83G8C2ARO7P' --不是英国站点的，取库存最多的那个国家
        ) T1
    WHERE  T1.RANK_ID=1
     and seller_id ='A2MGVMX7S4A416'
-- --       and marketplace_id ='A13V1IB3VIYZZH'
     and asin ='B07WZTYXQ2'
     and seller_sku ='19-5LX9-ZXJI'
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

                 ,case when raw_purchase_time >= date_format(date_add('2024-08-21',-7),'yyyyMMdd') and raw_purchase_time < '20240821'
                           then raw_purchase_time end  as n7d_flag


                 ,case when   raw_purchase_time >=  date_format(date_add('2024-08-21',-15),'yyyyMMdd') and raw_purchase_time < '20240821'
                           then raw_purchase_time end as n15d_flag

                 ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-30),'yyyyMMdd') and raw_purchase_time < '20240821'
                           then raw_purchase_time end as n30d_flag

                 ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-60),'yyyyMMdd') and raw_purchase_time < '20240821'
                           then raw_purchase_time end as n60d_flag

            FROM
                (
                    SELECT raw_purchase_time
                    FROM
                        (SELECT  date_format(purchase_time,'yyyyMMdd')  raw_purchase_time
                         from  amz.mid_amzn_all_orders_df
                         where ds = '20240821'
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

                   ,case when raw_purchase_time >= date_format(date_add('2024-08-21',-7),'yyyyMMdd') and raw_purchase_time < '20240821'
                             then raw_purchase_time end


                   ,case when   raw_purchase_time >=  date_format(date_add('2024-08-21',-15),'yyyyMMdd') and raw_purchase_time < '20240821'
                             then raw_purchase_time end

                   ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-30),'yyyyMMdd') and raw_purchase_time < '20240821'
                             then raw_purchase_time end

                   ,case when raw_purchase_time >=  date_format(date_add('2024-08-21',-60),'yyyyMMdd') and raw_purchase_time < '20240821'
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
         ,sum(case when raw_purchase_FLAG >= date_format(date_add('2024-08-21',-7),'yyyyMMdd') and raw_purchase_FLAG < '20240821' then ordered_num else 0 end) as afnstock_n7d_sale_num
         ,sum(case when  raw_purchase_FLAG >= date_format(date_add('2024-08-21',-15),'yyyyMMdd')  and raw_purchase_FLAG <  '20240821' then ordered_num else 0 end) as afnstock_n15d_sale_num
         ,sum(case when  raw_purchase_FLAG >= date_format(date_add('2024-08-21',-30),'yyyyMMdd')  and raw_purchase_FLAG < '20240821' then ordered_num else 0 end) as afnstock_n30d_sale_num
         ,sum(case when  raw_purchase_FLAG >= date_format(date_add('2024-08-21',-60),'yyyyMMdd')  and raw_purchase_FLAG < '20240821' then ordered_num else 0 end) as afnstock_n60d_sale_num
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
         where ds = '20240821'
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
              ,'20240821' ds
         FROM dws_scm_ivt_amazon_asin_zt_tmp5
        )T1
            LEFT JOIN
        (SELECT
             '20240821' DS
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
insert overwrite table amz.temp_scm_ivt_amazon_asin_df partition(ds='20240821')
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
    '20240821' data_dt,
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

---------------------------------------------------------------------------------asin-sku index ---------------------------------------------------------------------------------
--
---------------------------------------------------------------------------------asin-sku index ---------------------------------------------------------------------------------
WITH all_asin AS (
    SELECT
        tenant_id,
        seller_id,
        marketplace_id,
        seller_sku,
        asin

    FROM
        (
            -- 所有商品报告
            SELECT
                tenant_id,
                seller_id,
                marketplace_id,
                seller_sku,
                asin
            FROM
                amz.mid_scm_ivt_amazon_fba_stock_current_num_df
            WHERE
                ds = '20240821'
            GROUP BY
                tenant_id,
                seller_id,
                marketplace_id,
                seller_sku,
                asin

            UNION ALL

            -- 销售数据
            SELECT
                tenant_id,
                seller_id,
                marketplace_id,
                seller_sku,
                asin
            FROM
                amz.mid_amzn_all_orders_df
            WHERE
                ds = '20240821'
            GROUP BY
                tenant_id,
                seller_id,
                marketplace_id,
                seller_sku,
                asin
        ) T
    GROUP BY
        tenant_id,
        seller_id,
        marketplace_id,
        seller_sku,
        asin
)
     ,
     authing AS (
         SELECT
             tenant_id,
             seller_id,
             marketplace_id,
             profile_id,
             seller_name
         FROM
             ods.ods_report_authorization_account_df
         WHERE
             ds = '20240820'
     ),
     market_info AS (
         SELECT
             t.*,
             case when market_place_id='A1F83G8C2ARO7P' then  '英国' --先把英国拿出来
                  WHEN market_place_type='Europe' THEN market_place_type --除英国外的欧洲站其他也是用站点类型关联
                  when market_place_type='North America' THEN market_place_type
                  else market_place_id end	 marketplace_type2
         FROM
             dim.dim_base_marketplace_info_df t
         WHERE
             ds = '20240821'
     ),product_detail AS (
    SELECT
        market_place_id AS marketplace_id,
        DIM_ASIN AS `asin`,
        REPLACE(breadcrumbs_feature,'            >               ','>') AS breadcrumbs_feature,
        SPLIT(breadcrumbs_feature,'>')[0] AS breadcrumbs_category_one,
        SPLIT(breadcrumbs_feature,'>')[1] AS breadcrumbs_category_two,
        SPLIT(breadcrumbs_feature,'>')[2] AS breadcrumbs_category_three,
        SPLIT(breadcrumbs_feature,'>')[3] AS breadcrumbs_category_four,
        SPLIT(breadcrumbs_feature,'>')[4] AS breadcrumbs_category_five,
        SPLIT(breadcrumbs_feature,'>')[5] AS breadcrumbs_category_six,
        link,
        brand,
        scribing_price,
        selling_price,
        reviews_ratings AS ratings_num,
        reviews_stars AS ratings_stars,
        sellers_rank_category AS best_sellers_rank_category,
        sellers_rank AS best_sellers_rank,
        sellers_rank_last_detail AS best_sellers_rank_detail,
        date_first_available AS first_available_time,
        created_at,
        title,
        main_image_url,
        ROW_NUMBER() OVER(PARTITION BY market_place_id, DIM_asin ORDER BY created_at DESC) AS rn
    FROM
        ods.ods_crawler_amazon_product_details_df
    WHERE ds = '20240821'
),   product_sale AS (
    SELECT
        tenant_id,
        seller_id,
        a.marketplace_id,
        seller_sku,
        asin,
--         market_place_type AS marketplace_type,
--         marketplace_website,
--         country_code,
--         country_cn_name AS cn_country_name,
--         currency,
        -- 销量统计
        SUM(CASE WHEN DATE_FORMAT(purchase_time, 'yyyyMMdd') = DATE_FORMAT(date_sub('2024-08-21', 1), 'yyyyMMdd') THEN ordered_num ELSE 0 END) AS n1d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 7) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n7d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 15) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n15d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 30) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n30d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 60) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n60d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 90) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n90d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 180) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n180d_sale_num,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 365) AND purchase_time < '2024-08-21' THEN ordered_num ELSE 0 END) AS n365d_sale_num,
        -- 销售金额统计
        SUM(CASE WHEN DATE_FORMAT(purchase_time, 'yyyyMMdd') = DATE_FORMAT(date_sub('2024-08-21', 1), 'yyyyMMdd') THEN item_amt ELSE 0 END) AS n1d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 7) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n7d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 15) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n15d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 30) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n30d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 60) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n60d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 90) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n90d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 180) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n180d_sale_amt,
        SUM(CASE WHEN purchase_time >= date_sub('2024-08-21', 365) AND purchase_time < '2024-08-21' THEN item_amt ELSE 0 END) AS n365d_sale_amt

    from  (select * from amz.mid_amzn_all_orders_df   --dwd_trd_ord_amazon_raw_order_info_df
           where ds = '20240820'
             and ordered_num >0
             and nvl(order_status,'') <> 'Canceled' and  asin ='B07WZTYXQ2'
          )a
              left  join (select * from dim.dim_base_marketplace_info_df where ds = '20240820') b
                         on a.marketplace_id = b.market_place_id
    group by tenant_id
           ,seller_id
           ,marketplace_id
           ,seller_sku
           ,asin
--            ,market_place_type
--            ,marketplace_website
--            ,country_code
--            ,country_cn_name
--            ,currency

),master_slave as (
    SELECT  marketplace_id
         ,asin
         ,parent_asin
    FROM    (
                SELECT  market_place_id marketplace_id
                     ,asin
                     ,parent_asin
                     ,ROW_NUMBER() OVER (PARTITION BY market_place_id,asin ORDER BY data_dt DESC ) AS rn
                FROM    amz.mid_amzn_asin_to_parent_df
                WHERE   ds = '20240821'
            ) t
    WHERE   rn = 1
) ,sale_rank AS (
   select
           a.tenant_id
           ,a.marketplace_id
           ,a.seller_id
           ,b.parent_asin
           ,a.asin
           , a.n30d_sale_num
             -- 求 同一个parent_asin 下的总销量
          , sum(n30d_sale_num) over (partition by a.tenant_id,a.marketplace_id,a.seller_id,b.parent_asin) as parent_asin_n30d_sale_num

           ,row_number()over(partition by a.tenant_id,a.marketplace_id,a.seller_id,b.parent_asin order by n30d_sale_num desc) as order_num_rank --近30天总销量
    from (
    select tenant_id,marketplace_id,seller_id,asin,sum(n30d_sale_num) as n30d_sale_num from product_sale
    group by tenant_id,marketplace_id,seller_id,asin
    ) a
    inner join master_slave b on a.marketplace_id = b.marketplace_id and a.asin = b.asin


), temp_scm_ivt_amazon_asin_df as (
    SELECT
        tenant_id
         ,seller_id
         ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国' --先把英国拿出来
               WHEN marketplace_type='Europe' THEN marketplace_type --除英国外的欧洲站其他也是用站点类型关联
               when marketplace_type='North America' THEN marketplace_type
               else marketplace_id end	 marketplace_type2  --其他用站点id
         ,seller_sku
         ,asin


         ,afnstock_n1d_sale_num		--近1天销量_计算FBA库存天数
         ,afnstock_n7d_avg_sale_num		--近7天销量_计算FBA库存天数
         ,afnstock_n15d_avg_sale_num		--近15天销量_计算FBA库存天数
         ,afnstock_n30d_avg_sale_num		--近30天销量_计算FBA库存天数
         ,afnstock_n60d_avg_sale_num		--近60天销量_计算FBA库存天数

         ,afn_total_num		--FBA总计(FBA在库+FBA在途）
         ,afn_warehouse_num		--FBA在库(FBA可售+FBA不可售+FBA预留+FBA货件入库差异)
         ,afn_fulfillable_num		--FBA可售
         ,afn_unsellable_num		--FBA不可售
         ,afn_reserved_num		--FBA预留

         ,afn_researching_num		--FBA货件入库差异数量
         ,afn_inbound_num		--FBA在途
         ,afn_inbound_working_num		--FBA在途_入境工作数量
         ,afn_inbound_shipped_num		--FBA在途_入境的装船数量
         ,afn_inbound_receiving_num		--FBA在途_入境接待量
         ,fba_instock_days		--FBA在库天数
         ,fba_instock_num

    FROM amz.temp_scm_ivt_amazon_asin_df
    WHERE ds='20240821'
--       and seller_id ='A2MGVMX7S4A416'
-- -- and marketplace_id ='A13V1IB3VIYZZH'
--       and asin ='B07WZTYXQ2'

)
--select * from sale_rank where tenant_id = '45d20b1d2cc2d52e74b3cbf1750a2e31'  and marketplace_id = 'A1PA6795UKMFR9' and seller_id = 'A2MGVMX7S4A416' and asin = 'B07WZM79PH'
  -- select tenant_id,marketplace_id,seller_id,asin, count(*) from sale_rank group by tenant_id,marketplace_id,seller_id,asin having count(*)>1
insert overwrite table amz.dwd_prd_asin_sku_index_df partition (ds='20240821')
SELECT
    MD5(CONCAT(t1.tenant_id, '_', t2.profile_id, '_', t1.asin)) AS id,
    t1.asin as seller_asin,
    t1.tenant_id as tenant_id,
    t2.profile_id as profile_id,
    t1.marketplace_id as marketplace_id,
    CONCAT(t3.country_cn_name, '站') AS marketplace_name ,
    t1.seller_id as seller_id,
    t2.seller_name as seller_name,
    t1.seller_sku as seller_sku,

    t3.market_place_type ,
    t3.marketplace_website ,
    t3.country_code  ,
    t3.country_en_name ,
    t3.country_cn_name ,
    t3.currency_en ,
    t3.currency_cn ,
    t3.timezone ,
    t3.endpoint_code ,

    t7.afn_total_num as fba_total_num,
    t7.afn_inbound_num as fba_inbound_num,
    t7.afn_warehouse_num as fba_warehouse_num,
    t7.fba_instock_days,
    t7.fba_instock_num as fba_instock_cnt,
    t7.afn_fulfillable_num as afn_fulfillable_quantity,
    null as color,
    null as size,
    t6.order_num_rank as order_num_rank,
    case when t6.parent_asin_n30d_sale_num = 0 then 0 else cast(t6.n30d_sale_num/t6.parent_asin_n30d_sale_num as decimal(10,2)) end as order_num_rate,
    t4.breadcrumbs_category_one as breadcrumbs_category_one,
    t4.breadcrumbs_category_two as breadcrumbs_category_two,
    t4.breadcrumbs_category_three as breadcrumbs_category_three,
    t4.breadcrumbs_category_four as breadcrumbs_category_four,
    t4.breadcrumbs_category_five as breadcrumbs_category_five,
    t4.breadcrumbs_category_six as breadcrumbs_category_six,
    t4.title as title,
    t4.link as link,
    t4.brand as brand,
    t4.main_image_url as main_image_url,
    t5.n1d_sale_num as n1d_sale_num,
    t5.n1d_sale_amt as n1d_sale_amt,
    t5.n7d_sale_num as n7d_sale_num,
    t5.n7d_sale_amt as n7d_sale_amt,
    t5.n15d_sale_num as n15d_sale_num,
    t5.n15d_sale_amt as n15d_sale_amt,
    t5.n30d_sale_num as n30d_sale_num,
    t5.n30d_sale_amt as n30d_sale_amt,
    t5.n60d_sale_num as n60d_sale_num,
    t5.n60d_sale_amt as n60d_sale_amt,
    t5.n90d_sale_num as n90d_sale_num,
    t5.n90d_sale_amt as n90d_sale_amt,
    t5.n180d_sale_num as n180d_sale_num,
    t5.n180d_sale_amt as n180d_sale_amt,
    t5.n365d_sale_num as n365d_sale_num,
    t5.n365d_sale_amt as n365d_sale_amt
-- select count(1)
FROM
    all_asin t1
    LEFT JOIN authing t2
ON t1.tenant_id = t2.tenant_id AND t1.seller_id = t2.seller_id AND t1.marketplace_id = t2.marketplace_id
    LEFT JOIN market_info t3
    ON t1.marketplace_id = t3.market_place_id
    LEFT JOIN product_detail t4
    ON t1.marketplace_id = t4.marketplace_id AND t1.asin = t4.asin AND t4.rn = 1
    left join product_sale t5
    ON  t1.tenant_id = t5.tenant_id AND t1.seller_id = t5.seller_id AND t1.marketplace_id = t5.marketplace_id AND t1.asin = t5.asin and t1.seller_sku = t5.seller_sku
    left join sale_rank t6
    ON t1.tenant_id = t6.tenant_id AND t1.seller_id = t6.seller_id AND t1.marketplace_id = t6.marketplace_id AND t1.asin = t6.asin
left join temp_scm_ivt_amazon_asin_df t7
ON t1.seller_id = t7.seller_id AND t3.marketplace_type2 = t7.marketplace_type2 AND t1.asin = t7.asin and t1.seller_sku = t7.seller_sku;

select count(1) from amz.dwd_prd_asin_sku_index_df where ds = '20240821'; -- 7388  7182  6797


select * from amz.dwd_prd_asin_sku_index_df where ds = '20240821'
--                                               and marketplace_type='Europe'
--                                               and marketplace_id<>'A1F83G8C2ARO7P'
                                              and seller_id ='A2MGVMX7S4A416'
                                              and asin ='B07WZTYXQ2';


select * from temp_scm_ivt_amazon_asin_df where ds = '20240821'
                                            and seller_id ='A2MGVMX7S4A416'
                                            and asin ='B07WZTYXQ2';

select * from amz.temp_scm_ivt_amazon_asin_df where ds = '20240821'
and tenant_id ='45d20b1d2cc2d52e74b3cbf1750a2e31'
and seller_id ='A2MGVMX7S4A416'
-- and marketplace_id ='A13V1IB3VIYZZH'
and asin ='B07WZTYXQ2'


select *
FROM  amz.mid_scm_ivt_amazon_fba_stock_current_num_df
WHERE ds='20240821'
  and marketplace_type='Europe'
  and marketplace_id<>'A1F83G8C2ARO7P'
  and seller_id ='A2MGVMX7S4A416'
  and asin ='B07WZTYXQ2';


select * from dws_itm_sku_amazon_asin_index_df    where ds = '20240821'
                                                    and tenant_id ='45d20b1d2cc2d52e74b3cbf1750a2e31'
                                                    and seller_id ='A2MGVMX7S4A416'
                                                    and marketplace_id ='A13V1IB3VIYZZH'
                                                    and asin ='B07WZTYXQ2';




tenant_id	seller_id	marketplace_id	seller_sku	asin	marketplace_type	marketplace_website	country_code	cn_country_name	currency	fnsku	parent_asin	link	brand	scribing_price	selling_price	ratings_num	ratings_stars	best_sellers_rank_category	best_sellers_rank	best_sellers_rank_detail	best_sellers_rank_detail_first_category	best_sellers_rank_detail_first	first_available_time	title	main_image_url	breadcrumbs_feature	breadcrumbs_category_one	breadcrumbs_category_two	breadcrumbs_category_three	breadcrumbs_category_four	breadcrumbs_category_five	breadcrumbs_category_six	cn_breadcrumbs_category_one	cn_breadcrumbs_category_two	cn_breadcrumbs_category_three	cn_breadcrumbs_category_four	cn_breadcrumbs_category_five	cn_breadcrumbs_category_six	n1d_sale_num	n7d_sale_num	n15d_sale_num	n30d_sale_num	n60d_sale_num	n180d_sale_num	n365d_sale_num	n1d_sale_amt	n7d_sale_amt	n15d_sale_amt	n30d_sale_amt	n60d_sale_amt	n180_sale_amt	n365_sale_amt	afn_n1d_sale_num	afn_n7d_sale_num	afn_n15d_sale_num	afn_n30d_sale_num	afn_n60d_sale_num	afn_n180d_sale_num	afn_n365d_sale_num	afn_n1d_sale_amt	afn_n7d_sale_amt	afn_n15d_sale_amt	afn_n30d_sale_amt	afn_n60d_sale_amt	afn_n180d_sale_amt	afn_n365d_sale_amt	mfn_n1d_sale_num	mfn_n7d_sale_num	mfn_n15d_sale_num	mfn_n30d_sale_num	mfn_n60d_sale_num	mfn_n180d_sale_num	mfn_n365d_sale_num	mfn_n1d_sale_amt	mfn_n7d_sale_amt	mfn_n15d_sale_amt	mfn_n30d_sale_amt	mfn_n60d_sale_amt	mfn_n180d_sale_amt	mfn_n365d_sale_amt	afnstock_n1d_sale_num	afnstock_n7d_avg_sale_num	afnstock_n15d_avg_sale_num	afnstock_n30d_avg_sale_num	afnstock_n60d_avg_sale_num	fba_first_instock_time	fba_instock_cnt	afn_total_num	afn_warehouse_num	afn_fulfillable_num	afn_unsellable_num	afn_reserved_num	afn_reserved_customerorders_num	afn_reserved_fc_transfers_num	afn_reserved_fc_processing_num	afn_researching_num	afn_inbound_num	afn_inbound_working_num	afn_inbound_shipped_num	afn_inbound_receiving_num	data_dt	etl_data_dt	asin_rk	order_num_rate	ds
45d20b1d2cc2d52e74b3cbf1750a2e31	A2MGVMX7S4A416	A13V1IB3VIYZZH	19-5LX9-ZXJI	B07WZTYXQ2	Europe	https://www.amazon.fr	FR	法国	欧元	B07WZTYXQ2	B084T62MWN	https://www.amazon.fr/dp/B07WZTYXQ2	boutique NEW	\N	13,99 €	1	50	Mode	0	\N	\N	\N	0001-12-30 00:05:43	NEWVISION®,Lunettes de lecture pour lunettes de soleil avec verres foncés clipsables – Protection 100 % UVA et UVB, verres polarisés pour lunettes de lecture ou de vue pour homme et femme. NV8126	https://m.media-amazon.com/images/I/31BMAqrX1tL._AC_SR38,50_.jpg	Mode>Femme>Accessoires>LunettesetAccessoires>Monturesdelunettes	Mode	Femme	Accessoires	LunettesetAccessoires	Monturesdelunettes								0	0	2	4	5	5	6	0	0	23.98	49.96	61.95	61.95	73.94	0	0	2	4	5	5	5	0	0	23.98	49.96	61.95	61.95	61.95	0	0	0	0	0	0	1	0	0	0	0	0	0	11.99	0	0	0.533333	0.533333	0.333333	\N	\N	10	7	4	0	3	0	0	0	0	3	0	0	3	20240821	2024-08-22 04:03:14	18	0.0	20240821



