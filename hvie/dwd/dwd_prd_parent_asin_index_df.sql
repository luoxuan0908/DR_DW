
CREATE TABLE IF NOT EXISTS amz.dwd_prd_parent_asin_index_df(
    tenant_id STRING COMMENT '租户ID',
    seller_id STRING COMMENT '卖家ID',
    marketplace_id STRING COMMENT '站点ID',
    parent_asin STRING COMMENT '父ASIN',
    marketplace_type STRING COMMENT '站点类型',
    marketplace_website STRING COMMENT '站点链接',
    country_code STRING COMMENT '国家编码',
    cn_country_name STRING COMMENT '国家中文名称',
    currency STRING COMMENT '货币简称',
    season_label STRING COMMENT '销售季节',
    main_asin STRING COMMENT '主卖ASIN',
    main_image_url STRING COMMENT '主图链接',
    link STRING COMMENT '商品链接',
    brand STRING COMMENT '品牌名或店铺',
    scribing_price STRING COMMENT '划线价',
    selling_price STRING COMMENT '售价',
    ratings_num BIGINT COMMENT 'ratings数量',
    ratings_stars DECIMAL(18,6) COMMENT 'ratings评分',
    best_sellers_rank_category STRING COMMENT 'BS榜一级分类名称',
    best_sellers_rank BIGINT COMMENT 'BS榜一级分类排名',
    best_sellers_rank_detail STRING COMMENT 'BS榜小类排名情况，可能有多个',
    best_sellers_rank_detail_first_category STRING COMMENT 'BS榜小类名称(解析的第一个类目）',
    best_sellers_rank_detail_first BIGINT COMMENT 'BS榜小类排名（解析的第一个类目对应的排名）',
    first_available_time timestamp COMMENT '上架时间',
    title STRING COMMENT '标题',
    breadcrumbs_feature STRING COMMENT '面包屑导航',
    breadcrumbs_category_one STRING COMMENT '面包屑导航一级类目',
    breadcrumbs_category_two STRING COMMENT '面包屑导航二级类目',
    breadcrumbs_category_three STRING COMMENT '面包屑导航三级类目',
    breadcrumbs_category_four STRING COMMENT '面包屑导航四级类目',
    breadcrumbs_category_five STRING COMMENT '面包屑导航五级类目',
    breadcrumbs_category_six STRING COMMENT '面包屑导航六级类目',
    cn_breadcrumbs_category_one STRING COMMENT '面包屑导航一级类目中文',
    cn_breadcrumbs_category_two STRING COMMENT '面包屑导航二级类目中文',
    cn_breadcrumbs_category_three STRING COMMENT '面包屑导航三级类目中文',
    cn_breadcrumbs_category_four STRING COMMENT '面包屑导航四级类目中文',
    cn_breadcrumbs_category_five STRING COMMENT '面包屑导航五级类目中文',
    cn_breadcrumbs_category_six STRING COMMENT '面包屑导航六级类目中文',
    n1d_sale_num BIGINT COMMENT '近1天销量',
    n7d_sale_num BIGINT COMMENT '近7天销量',
    n15d_sale_num BIGINT COMMENT '近15天销量',
    n30d_sale_num BIGINT COMMENT '近30天销量',
    n60d_sale_num BIGINT COMMENT '近60天销量',
    n180d_sale_num BIGINT COMMENT '近180天销量',
    n365d_sale_num BIGINT COMMENT '近365天销量',
    n1d_sale_amt DECIMAL(18,6) COMMENT '近1天销售额',
    n7d_sale_amt DECIMAL(18,6) COMMENT '近7天销售额',
    n15d_sale_amt DECIMAL(18,6) COMMENT '近15天销售额',
    n30d_sale_amt DECIMAL(18,6) COMMENT '近30天销售额',
    n60d_sale_amt DECIMAL(18,6) COMMENT '近60天销售额',
    n180_sale_amt DECIMAL(18,6) COMMENT '近180天销售额',
    n365_sale_amt DECIMAL(18,6) COMMENT '近365天销售额',
    afn_n1d_sale_num BIGINT COMMENT 'FBA发货近1天销量',
    afn_n7d_sale_num BIGINT COMMENT 'FBA发货近7天销量',
    afn_n15d_sale_num BIGINT COMMENT 'FBA发货近15天销量',
    afn_n30d_sale_num BIGINT COMMENT 'FBA发货近30天销量',
    afn_n60d_sale_num BIGINT COMMENT 'FBA发货近60天销量',
    afn_n180d_sale_num BIGINT COMMENT 'FBA发货近180天销量',
    afn_n365d_sale_num BIGINT COMMENT 'FBA发货近365天销量',
    afn_n1d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近1天销售额',
    afn_n7d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近7天销售额',
    afn_n15d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近15天销售额',
    afn_n30d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近30天销售额',
    afn_n60d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近60天销售额',
    afn_n180d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近180天销售额',
    afn_n365d_sale_amt DECIMAL(18,6) COMMENT 'FBA发货近365天销售额',
    mfn_n1d_sale_num BIGINT COMMENT '自发货近1天销量',
    mfn_n7d_sale_num BIGINT COMMENT '自发货近7天销量',
    mfn_n15d_sale_num BIGINT COMMENT '自发货近15天销量',
    mfn_n30d_sale_num BIGINT COMMENT '自发货近30天销量',
    mfn_n60d_sale_num BIGINT COMMENT '自发货近60天销量',
    mfn_n180d_sale_num BIGINT COMMENT '自发货近180天销量',
    mfn_n365d_sale_num BIGINT COMMENT '自发货近365天销量',
    mfn_n1d_sale_amt DECIMAL(18,6) COMMENT '自发货近1天销售额',
    mfn_n7d_sale_amt DECIMAL(18,6) COMMENT '自发货近7天销售额',
    mfn_n15d_sale_amt DECIMAL(18,6) COMMENT '自发货近15天销售额',
    mfn_n30d_sale_amt DECIMAL(18,6) COMMENT '自发货近30天销售额',
    mfn_n60d_sale_amt DECIMAL(18,6) COMMENT '自发货近60天销售额',
    mfn_n180d_sale_amt DECIMAL(18,6) COMMENT '自发货近180天销售额',
    mfn_n365d_sale_amt DECIMAL(18,6) COMMENT '自发货近365天销售额',
    afnstock_n1d_sale_num BIGINT COMMENT '近1天销量（剔除大促）_计算FBA库存天数',
    afnstock_n7d_avg_sale_num DECIMAL(18,6) COMMENT '近7天日均销量（剔除大促）_计算FBA库存天数',
    afnstock_n15d_avg_sale_num DECIMAL(18,6) COMMENT '近15天日均销量（剔除大促）_计算FBA库存天数',
    afnstock_n30d_avg_sale_num DECIMAL(18,6) COMMENT '近30天日均销量（剔除大促）_计算FBA库存天数',
    afnstock_n60d_avg_sale_num DECIMAL(18,6) COMMENT '近60天日均销量（剔除大促）_计算FBA库存天数',
    fba_first_instock_time timestamp COMMENT 'FBA首单入库时间',
    fba_instock_cnt BIGINT COMMENT 'FBA入库次数',
    afn_total_num BIGINT COMMENT 'FBA总计(FBA在库+FBA在途）',
    afn_warehouse_num BIGINT COMMENT 'FBA在库(FBA可售+FBA不可售+FBA预留+FBA货件入库差异)',
    afn_fulfillable_num BIGINT COMMENT 'FBA可售',
    afn_unsellable_num BIGINT COMMENT 'FBA不可售',
    afn_reserved_num BIGINT COMMENT 'FBA预留(和3个预留拆分的加和数据会略有差异，不是一张表）',
    afn_reserved_customerorders_num BIGINT COMMENT 'FBA预留_为买家订单预留的商品数量',
    afn_reserved_fc_transfers_num BIGINT COMMENT 'FBA预留_正在从一个运营中心转运至另一运营中心的商品数量',
    afn_reserved_fc_processing_num BIGINT COMMENT 'FBA预留_搁置在运营中心等待进行其他处理的商品数量，包括与移除订单关联的商品',
    afn_researching_num BIGINT COMMENT 'FBA货件入库差异',
    afn_inbound_num BIGINT COMMENT 'FBA在途',
    afn_inbound_working_num BIGINT COMMENT 'FBA在途_入境工作数量',
    afn_inbound_shipped_num BIGINT COMMENT 'FBA在途_入境的装船数量',
    afn_inbound_receiving_num BIGINT COMMENT 'FBA在途_入境接待量',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊父asin粒度指标宽表')
    ;
---//可以聚合的数据放一起 取库存和销量数据

-- drop table  IF EXISTS  dws_itm_spu_amazon_parent_asin_index_zt_tmp1;
-- CREATE  table  IF NOT EXISTS  dws_itm_spu_amazon_parent_asin_index_20240822_zt_tmp1
-- AS

with order_detail as (
    select
        tenant_id	--租户ID
         ,seller_id	--卖家ID
         ,marketplace_id	--站点ID
         ,parent_asin	--父ASIN
         ,marketplace_type	--站点类型
         ,marketplace_website	--站点链接
         ,country_code	--国家编码
         ,cn_country_name	--国家中文名称
         ,currency	--货币简称
         ,SUM(n1d_sale_num)n1d_sale_num	--近1天销量
         ,SUM(n7d_sale_num)n7d_sale_num	--近7天销量
         ,SUM(n15d_sale_num)n15d_sale_num	--近15天销量
         ,SUM(n30d_sale_num)n30d_sale_num	---近30天销量
         ,SUM(n60d_sale_num)	n60d_sale_num--近60天销量
         ,SUM(n180d_sale_num)n180d_sale_num	--近180天销量
         ,SUM(n365d_sale_num)n365d_sale_num	--近365天销量
         ,SUM(n1d_sale_amt)n1d_sale_amt	--近1天销售额
         ,SUM(n7d_sale_amt)n7d_sale_amt	--近7天销售额
         ,SUM(n15d_sale_amt)	n15d_sale_amt--近15天销售额
         ,SUM(n30d_sale_amt)n30d_sale_amt	--近30天销售额
         ,SUM(n60d_sale_amt)n60d_sale_amt	--近60天销售额
         ,SUM(n180_sale_amt)	n180_sale_amt--近180天销售额
         ,SUM(n365_sale_amt)n365_sale_amt	--近365天销售额
         ,SUM(afn_n1d_sale_num)	afn_n1d_sale_num--FBA发货近1天销量
         ,SUM(afn_n7d_sale_num)afn_n7d_sale_num	--FBA发货近7天销量
         ,SUM(afn_n15d_sale_num)	afn_n15d_sale_num--FBA发货近15天销量
         ,SUM(afn_n30d_sale_num)	afn_n30d_sale_num--FBA发货近30天销量
         ,SUM(afn_n60d_sale_num)	afn_n60d_sale_num--FBA发货近60天销量
         ,SUM(afn_n180d_sale_num)afn_n180d_sale_num	--FBA发货近180天销量
         ,SUM(afn_n365d_sale_num)afn_n365d_sale_num	--FBA发货近365天销量
         ,SUM(afn_n1d_sale_amt)afn_n1d_sale_amt	--FBA发货近1天销售额
         ,SUM(afn_n7d_sale_amt)afn_n7d_sale_amt	--FBA发货近7天销售额
         ,SUM(afn_n15d_sale_amt)	afn_n15d_sale_amt--FBA发货近15天销售额
         ,SUM(afn_n30d_sale_amt)afn_n30d_sale_amt	--FBA发货近30天销售额
         ,SUM(afn_n60d_sale_amt)afn_n60d_sale_amt	--FBA发货近60天销售额
         ,SUM(afn_n180d_sale_amt)afn_n180d_sale_amt	--FBA发货近180天销售额
         ,SUM(afn_n365d_sale_amt)afn_n365d_sale_amt	--FBA发货近365天销售额
         ,SUM(mfn_n1d_sale_num)mfn_n1d_sale_num	--自发货近1天销量
         ,SUM(mfn_n7d_sale_num)mfn_n7d_sale_num	--自发货近7天销量
         ,SUM(mfn_n15d_sale_num)mfn_n15d_sale_num	--自发货近15天销量
         ,SUM(mfn_n30d_sale_num)	mfn_n30d_sale_num--自发货近30天销量
         ,SUM(mfn_n60d_sale_num)mfn_n60d_sale_num	--自发货近60天销量
         ,SUM(mfn_n180d_sale_num) mfn_n180d_sale_num	--自发货近180天销量
         ,SUM(mfn_n365d_sale_num)mfn_n365d_sale_num	--自发货近365天销量
         ,SUM(mfn_n1d_sale_amt)mfn_n1d_sale_amt	--自发货近1天销售额
         ,SUM(mfn_n7d_sale_amt)mfn_n7d_sale_amt	--自发货近7天销售额
         ,SUM(mfn_n15d_sale_amt)mfn_n15d_sale_amt	--自发货近15天销售额
         ,SUM(mfn_n30d_sale_amt)mfn_n30d_sale_amt	--自发货近30天销售额
         ,SUM(mfn_n60d_sale_amt)mfn_n60d_sale_amt	--自发货近60天销售额
         ,SUM(mfn_n180d_sale_amt)mfn_n180d_sale_amt	--自发货近180天销售额
         ,SUM(mfn_n365d_sale_amt)mfn_n365d_sale_amt	--自发货近365天销售额

    from amz.mid_trd_ord_amazon_sale_info_df
    WHERE ds= '20240822'
      AND parent_asin IS NOT NULL
    GROUP BY tenant_id	--租户ID
           ,seller_id	--卖家ID
           ,marketplace_id	--站点ID
           ,parent_asin	--父ASIN
           ,marketplace_type	--站点类型
           ,marketplace_website	--站点链接
           ,country_code	--国家编码
           ,cn_country_name	--国家中文名称
           ,currency	--货币简称
),inv_detail as (select tenant_id                                                       --租户ID
                      , seller_id                                                       --卖家ID
                      , marketplace_id                                                  --站点ID
                      , marketplace_type                                                --站点类型
                      , country_code                                                    --国家编码
                      , cn_country_name                                                 --国家中文名称
                      , SUM(afnstock_n1d_sale_num)      afnstock_n1d_sale_num--近1天销量（剔除大促）_计算FBA库存天数
                      , SUM(afnstock_n7d_avg_sale_num)  afnstock_n7d_avg_sale_num--近7天日均销量（剔除大促）_计算FBA库存天数
                      , SUM(afnstock_n15d_avg_sale_num) afnstock_n15d_avg_sale_num      --近15天日均销量（剔除大促）_计算FBA库存天数
                      , SUM(afnstock_n30d_avg_sale_num) afnstock_n30d_avg_sale_num      --近30天日均销量（剔除大促）_计算FBA库存天数
                      , SUM(afnstock_n60d_avg_sale_num) afnstock_n60d_avg_sale_num      --近60天日均销量（剔除大促）_计算FBA库存天数

                      , SUM(afn_total_num)              afn_total_num                   --FBA总计(FBA在库+FBA在途）
                      , SUM(afn_warehouse_num)          afn_warehouse_num--FBA在库(FBA可售+FBA不可售+FBA预留+FBA货件入库差异)
                      , SUM(afn_fulfillable_num)        afn_fulfillable_num             --FBA可售
                      , SUM(afn_unsellable_num)         afn_unsellable_num              --FBA不可售
                      , SUM(afn_reserved_num)           afn_reserved_num                --FBA预留(和3个预留拆分的加和数据会略有差异，不是一张表）
                      , null as                         afn_reserved_customerorders_num --FBA预留_为买家订单预留的商品数量
                      , null as                         afn_reserved_fc_transfers_num   --FBA预留_正在从一个运营中心转运至另一运营中心的商品数量
                      , null as                         afn_reserved_fc_processing_num--FBA预留_搁置在运营中心等待进行其他处理的商品数量，包括与移除订单关联的商品
                      , SUM(afn_researching_num)        afn_researching_num             --FBA货件入库差异
                      , SUM(afn_inbound_num)            afn_inbound_num                 --FBA在途
                      , SUM(afn_inbound_working_num)    afn_inbound_working_num         --FBA在途_入境工作数量
                      , SUM(afn_inbound_shipped_num)    afn_inbound_shipped_num         --FBA在途_入境的装船数量
                      , SUM(afn_inbound_receiving_num)  afn_inbound_receiving_num--FBA在途_入境接待量
                 from amz.mid_scm_ivt_amazon_asin_df
                 WHERE ds = '20240822'
                 GROUP BY tenant_id        --租户ID
                        , seller_id        --卖家ID
                        , marketplace_id   --站点ID
                        , marketplace_type --站点类型
                        , country_code     --国家编码
                        , cn_country_name --国家中文名称


),dws_itm_spu_amazon_parent_asin_index_zt_tmp as (
    select *
    from order_detail t1
    left join inv_detail t2 on t1.tenant_id = t2.tenant_id
                               and t1.seller_id = t2.seller_id
                               and t1.marketplace_id = t2.marketplace_id
                               and t1.country_code = t2.country_code
),dws_itm_spu_amazon_parent_asin_index_zt_tmp1 as (
select tenant_id,seller_id,marketplace_id,parent_asin,marketplace_type,marketplace_website,country_code,cn_country_name,currency,n1d_sale_num,n7d_sale_num,n15d_sale_num,n30d_sale_num,n60d_sale_num,n180d_sale_num,n365d_sale_num,n1d_sale_amt,n7d_sale_amt,n15d_sale_amt,n30d_sale_amt,n60d_sale_amt,n180_sale_amt,n365_sale_amt,afn_n1d_sale_num,afn_n7d_sale_num,afn_n15d_sale_num,afn_n30d_sale_num,afn_n60d_sale_num,afn_n180d_sale_num,afn_n365d_sale_num,afn_n1d_sale_amt,afn_n7d_sale_amt,afn_n15d_sale_amt,afn_n30d_sale_amt,afn_n60d_sale_amt,afn_n180d_sale_amt,afn_n365d_sale_amt,mfn_n1d_sale_num,mfn_n7d_sale_num,mfn_n15d_sale_num,mfn_n30d_sale_num,mfn_n60d_sale_num,mfn_n180d_sale_num,mfn_n365d_sale_num,mfn_n1d_sale_amt,mfn_n7d_sale_amt,mfn_n15d_sale_amt,mfn_n30d_sale_amt,mfn_n60d_sale_amt,mfn_n180d_sale_amt,mfn_n365d_sale_amt,afnstock_n1d_sale_num,afnstock_n7d_avg_sale_num,afnstock_n15d_avg_sale_num,afnstock_n30d_avg_sale_num,afnstock_n60d_avg_sale_num,afn_total_num,afn_warehouse_num,afn_fulfillable_num,afn_unsellable_num,afn_reserved_num,afn_reserved_customerorders_num,afn_reserved_fc_transfers_num,afn_reserved_fc_processing_num,afn_researching_num,afn_inbound_num,afn_inbound_working_num,afn_inbound_shipped_num,afn_inbound_receiving_num from dws_itm_spu_amazon_parent_asin_index_zt_tmp
)

,dws_itm_spu_amazon_parent_asin_index_zt_tmp2 as (SELECT tenant_id      --租户ID
                                                       , seller_id      --卖家ID
                                                       , marketplace_id --站点ID
                                                       , parent_asin    --父ASIN
                                                       , ASIN AS main_asin
                                                  FROM (SELECT tenant_id      --租户ID
                                                             , seller_id      --卖家ID
                                                             , marketplace_id --站点ID
                                                             , parent_asin    --父ASIN
                                                             , ASIN
                                                             , n7d_sale_num
                                                             , n15d_sale_num
                                                             , n30d_sale_num
                                                             , n60d_sale_num
                                                             , n180d_sale_num
                                                             , n365d_sale_num
                                                             , ROW_NUMBER() OVER (PARTITION BY tenant_id,seller_id,marketplace_id, parent_asin ORDER BY n7d_sale_num DESC,
                                                          n15d_sale_num DESC,n30d_sale_num DESC, n60d_sale_num DESC,n180d_sale_num DESC,n365d_sale_num DESC) AS RANK_ID
                                                        FROM (select tenant_id                          --租户ID
                                                                   , seller_id                          --卖家ID
                                                                   , marketplace_id                     --站点ID
                                                                   , parent_asin                        --父ASIN
                                                                   , ASIN
                                                                   , SUM(n1d_sale_num)   n1d_sale_num   --近1天销量
                                                                   , SUM(n7d_sale_num)   n7d_sale_num   --近7天销量
                                                                   , SUM(n15d_sale_num)  n15d_sale_num  --近15天销量
                                                                   , SUM(n30d_sale_num)  n30d_sale_num  ---近30天销量
                                                                   , SUM(n60d_sale_num)  n60d_sale_num--近60天销量
                                                                   , SUM(n180d_sale_num) n180d_sale_num --近180天销量
                                                                   , SUM(n365d_sale_num) n365d_sale_num --近365天销量
                                                              from amz.mid_trd_ord_amazon_sale_info_df
                                                              WHERE DS = '20240822'
                                                                AND parent_asin IS NOT NULL
                                                              group by tenant_id      --租户ID
                                                                     , seller_id      --卖家ID
                                                                     , marketplace_id --站点ID
                                                                     , parent_asin    --父ASIN
                                                                     , ASIN) T) M
                                                  WHERE RANK_ID = 1
), dws_itm_spu_amazon_parent_asin_index_zt_tmp3 as (
select * from amz.dim_prd_product_detail_df where ds ='20240822'
)
,dws_itm_spu_amazon_parent_asin_index_zt_tmp4 as (
SELECT
    T1.tenant_id
     ,T1.marketplace_id
     ,T1.seller_id
     ,T3.parent_asin
     ,MIN(operation_date) AS fba_first_instock_date
     ,COUNT(DISTINCT operation_date) fba_instock_num
FROM
    (SELECT  tenant_id
          ,marketplace_id
          ,seller_id
          ,asin
          ,operation_date
     FROM    amz.mid_scm_ivt_amazon_ledger_detail_view_df
     WHERE   ds='20240822'
       AND     event_type = 'Receipts' --是已接收库存

    )T1
        LEFT JOIN amz.mid_scm_ivt_amazon_asin_df T2
    ON T1.tenant_id=T2.tenant_id
        AND T1.marketplace_id=T2.marketplace_id
        AND T1.seller_id=T2.seller_id
        AND T1.asin=T2.asin
    left join (
        SELECT  marketplace_id
             ,asin
             ,parent_asin
        FROM    (
                    SELECT  market_place_id marketplace_id
                         ,asin
                         ,parent_asin
                         ,ROW_NUMBER() OVER (PARTITION BY market_place_id,asin ORDER BY data_dt DESC ) AS rn
                    FROM    amz.mid_amzn_asin_to_parent_df
                    WHERE   ds = '20240822'
                ) t
        WHERE   rn = 1
    ) t3 on t3.marketplace_id=T1.marketplace_id
        and t3.asin=T1.asin
GROUP BY T1.tenant_id
       ,T1.marketplace_id
       ,T1.seller_id
       ,T3.parent_asin
), dws_itm_spu_amazon_parent_asin_index_zt_tmp10 as (
select
    T1.tenant_id	--租户ID
     ,T1.seller_id	--卖家ID
     ,T1.marketplace_id	--站点ID
     ,T1.parent_asin	--父ASIN
     ,T1.marketplace_type	--站点类型
     ,T1.marketplace_website	--站点链接
     ,T1.country_code	--国家编码
     ,T1.cn_country_name	--国家中文名称
     ,T1.currency	--货币简称
     ,T1.n1d_sale_num	--近1天销量
     ,T1.n7d_sale_num	--近7天销量
     ,T1.n15d_sale_num	--近15天销量
     ,T1.n30d_sale_num	---近30天销量
     ,T1.n60d_sale_num--近60天销量
     ,T1.n180d_sale_num	--近180天销量
     ,T1.n365d_sale_num	--近365天销量
     ,T1.n1d_sale_amt	--近1天销售额
     ,T1.n7d_sale_amt	--近7天销售额
     ,T1.n15d_sale_amt--近15天销售额
     ,T1.n30d_sale_amt	--近30天销售额
     ,T1.n60d_sale_amt	--近60天销售额
     ,T1.n180_sale_amt--近180天销售额
     ,T1.n365_sale_amt	--近365天销售额
     ,T1.afn_n1d_sale_num--FBA发货近1天销量
     ,T1.afn_n7d_sale_num	--FBA发货近7天销量
     ,T1.afn_n15d_sale_num--FBA发货近15天销量
     ,T1.afn_n30d_sale_num--FBA发货近30天销量
     ,T1.afn_n60d_sale_num--FBA发货近60天销量
     ,T1.afn_n180d_sale_num	--FBA发货近180天销量
     ,T1.afn_n365d_sale_num	--FBA发货近365天销量
     ,T1.afn_n1d_sale_amt	--FBA发货近1天销售额
     ,T1.afn_n7d_sale_amt	--FBA发货近7天销售额
     ,T1.afn_n15d_sale_amt--FBA发货近15天销售额
     ,T1.afn_n30d_sale_amt	--FBA发货近30天销售额
     ,T1.afn_n60d_sale_amt	--FBA发货近60天销售额
     ,T1.afn_n180d_sale_amt	--FBA发货近180天销售额
     ,T1.afn_n365d_sale_amt	--FBA发货近365天销售额
     ,T1.mfn_n1d_sale_num	--自发货近1天销量
     ,T1.mfn_n7d_sale_num	--自发货近7天销量
     ,T1.mfn_n15d_sale_num	--自发货近15天销量
     ,T1.mfn_n30d_sale_num--自发货近30天销量
     ,T1.mfn_n60d_sale_num	--自发货近60天销量
     ,T1.mfn_n180d_sale_num	--自发货近180天销量
     ,T1.mfn_n365d_sale_num	--自发货近365天销量
     ,T1.mfn_n1d_sale_amt	--自发货近1天销售额
     ,T1.mfn_n7d_sale_amt	--自发货近7天销售额
     ,T1.mfn_n15d_sale_amt	--自发货近15天销售额
     ,T1.mfn_n30d_sale_amt	--自发货近30天销售额
     ,T1.mfn_n60d_sale_amt	--自发货近60天销售额
     ,T1.mfn_n180d_sale_amt	--自发货近180天销售额
     ,T1.mfn_n365d_sale_amt	--自发货近365天销售额
     ,T1.afnstock_n1d_sale_num--近1天销量（剔除大促）_计算FBA库存天数
     ,T1.afnstock_n7d_avg_sale_num--近7天日均销量（剔除大促）_计算FBA库存天数
     ,T1.afnstock_n15d_avg_sale_num	--近15天日均销量（剔除大促）_计算FBA库存天数
     ,T1.afnstock_n30d_avg_sale_num	--近30天日均销量（剔除大促）_计算FBA库存天数
     ,T1.afnstock_n60d_avg_sale_num	--近60天日均销量（剔除大促）_计算FBA库存天数

     ,T1.afn_total_num	--FBA总计(FBA在库+FBA在途）
     ,T1.afn_warehouse_num--FBA在库(FBA可售+FBA不可售+FBA预留+FBA货件入库差异)
     ,T1.afn_fulfillable_num	--FBA可售
     ,T1.afn_unsellable_num	--FBA不可售
     ,T1.afn_reserved_num	--FBA预留(和3个预留拆分的加和数据会略有差异，不是一张表）
     ,T1.afn_reserved_customerorders_num	--FBA预留_为买家订单预留的商品数量
     ,T1.afn_reserved_fc_transfers_num	--FBA预留_正在从一个运营中心转运至另一运营中心的商品数量
     ,T1.afn_reserved_fc_processing_num--FBA预留_搁置在运营中心等待进行其他处理的商品数量，包括与移除订单关联的商品
     ,T1.afn_researching_num	--FBA货件入库差异
     ,T1.afn_inbound_num	--FBA在途
     ,T1.afn_inbound_working_num	--FBA在途_入境工作数量
     ,T1.afn_inbound_shipped_num	--FBA在途_入境的装船数量
     ,T1.afn_inbound_receiving_num--FBA在途_入境接待量

     ,T2.main_asin



     ,T3.link	--商品链接(这个本就是父asin的链接)
     ,T3.brand	--品牌名或店铺
     ,T3.scribing_price	--划线价
     ,T3.selling_price	--售价
     ,T3.ratings_num	--ratings数量
     ,T3.ratings_stars	--ratings评分
     ,T3.best_sellers_rank_category	--BS榜一级分类名称
     ,T3.best_sellers_rank	--BS榜一级分类排名
     ,T3.best_sellers_rank_detail	--BS榜小类排名情况，可能有多个
     ,T3.best_sellers_rank_detail_first_category	--BS榜小类名称(解析的第一个类目）
     ,T3.best_sellers_rank_detail_first	--BS榜小类排名（解析的第一个类目对应的排名）
     ,T3.first_available_time	--上架时间
     ,T3.title	--标题
     ,T3.main_image_url	--主图链接
     ,T3.breadcrumbs_feature	--面包屑导航
     ,T3.breadcrumbs_category_one	--面包屑导航一级类目
     ,T3.breadcrumbs_category_two	--面包屑导航二级类目
     ,T3.breadcrumbs_category_three	--面包屑导航三级类目
     ,T3.breadcrumbs_category_four	--面包屑导航四级类目
     ,T3.breadcrumbs_category_five	--面包屑导航五级类目
     ,T3.breadcrumbs_category_six	--面包屑导航六级类目
     ,T3.cn_breadcrumbs_category_one	--面包屑导航一级类目中文
     ,T3.cn_breadcrumbs_category_two	--面包屑导航二级类目中文
     ,T3.cn_breadcrumbs_category_three	--面包屑导航三级类目中文
     ,T3.cn_breadcrumbs_category_four	--面包屑导航四级类目中文
     ,T3.cn_breadcrumbs_category_five	--面包屑导航五级类目中文
     ,T3.cn_breadcrumbs_category_six	--面包屑导航六级类目中

     ,T4.fba_first_instock_date
     ,T4.fba_instock_num

FROM  dws_itm_spu_amazon_parent_asin_index_zt_tmp1 T1
          LEFT JOIN  dws_itm_spu_amazon_parent_asin_index_zt_tmp2 T2
                     ON T1.tenant_id=T2.tenant_id
                         AND T1.seller_id=T2.seller_id
                         AND T1.marketplace_id=T2.marketplace_id
                         AND T1.parent_asin=T2.parent_asin
          LEFT JOIN  dws_itm_spu_amazon_parent_asin_index_zt_tmp3 T3
                     ON T1.tenant_id=T3.tenant_id
                         AND T1.seller_id=T3.seller_id
                         AND T1.marketplace_id=T3.marketplace_id
                         AND T1.parent_asin=T3.parent_asin
          LEFT JOIN  dws_itm_spu_amazon_parent_asin_index_zt_tmp4 T4
                     ON T1.tenant_id=T4.tenant_id
                         AND T1.seller_id=T4.seller_id
                         AND T1.marketplace_id=T4.marketplace_id
                         AND T1.parent_asin=T4.parent_asin
)
-- select * from  dws_itm_spu_amazon_parent_asin_index_zt_tmp10


--插入结果表

INSERT OVERWRITE TABLE  amz.dwd_prd_parent_asin_index_df  PARTITION (ds = '20240822')
SELECT
    tenant_id,
    seller_id,
    marketplace_id,
    parent_asin,
    marketplace_type,
    marketplace_website,
    country_code,
    cn_country_name,
    currency,
    '' season_label,
    main_asin,
    main_image_url,
    link,
    brand,
    scribing_price,
    selling_price,
    ratings_num,
    ratings_stars,
    best_sellers_rank_category,
    best_sellers_rank,
    best_sellers_rank_detail,
    best_sellers_rank_detail_first_category,
    best_sellers_rank_detail_first,
    first_available_time,
    title,
    breadcrumbs_feature,
    breadcrumbs_category_one,
    breadcrumbs_category_two,
    breadcrumbs_category_three,
    breadcrumbs_category_four,
    breadcrumbs_category_five,
    breadcrumbs_category_six,
    cn_breadcrumbs_category_one,
    cn_breadcrumbs_category_two,
    cn_breadcrumbs_category_three,
    cn_breadcrumbs_category_four,
    cn_breadcrumbs_category_five,
    cn_breadcrumbs_category_six,
    n1d_sale_num,
    n7d_sale_num,
    n15d_sale_num,
    n30d_sale_num,
    n60d_sale_num,
    n180d_sale_num,
    n365d_sale_num,
    CAST(n1d_sale_amt AS DECIMAL(18,6)),
    CAST(n7d_sale_amt AS DECIMAL(18,6)),
    CAST(n15d_sale_amt AS DECIMAL(18,6)),
    CAST(n30d_sale_amt AS DECIMAL(18,6)),
    CAST(n60d_sale_amt AS DECIMAL(18,6)),
    CAST(n180_sale_amt AS DECIMAL(18,6)),
    CAST(n365_sale_amt AS DECIMAL(18,6)),
    afn_n1d_sale_num,
    afn_n7d_sale_num,
    afn_n15d_sale_num,
    afn_n30d_sale_num,
    afn_n60d_sale_num,
    afn_n180d_sale_num,
    afn_n365d_sale_num,
    CAST(afn_n1d_sale_amt AS DECIMAL(18,6)),
    CAST(afn_n7d_sale_amt AS DECIMAL(18,6)),
    CAST(afn_n15d_sale_amt AS DECIMAL(18,6)),
    CAST(afn_n30d_sale_amt AS DECIMAL(18,6)),
    CAST(afn_n60d_sale_amt AS DECIMAL(18,6)),
    CAST(afn_n180d_sale_amt AS DECIMAL(18,6)),
    CAST(afn_n365d_sale_amt AS DECIMAL(18,6)),
    mfn_n1d_sale_num,
    mfn_n7d_sale_num,
    mfn_n15d_sale_num,
    mfn_n30d_sale_num,
    mfn_n60d_sale_num,
    mfn_n180d_sale_num,
    mfn_n365d_sale_num,
    CAST(mfn_n1d_sale_amt AS DECIMAL(18,6)),
    CAST(mfn_n7d_sale_amt AS DECIMAL(18,6)),
    CAST(mfn_n15d_sale_amt AS DECIMAL(18,6)),
    CAST(mfn_n30d_sale_amt AS DECIMAL(18,6)),
    CAST(mfn_n60d_sale_amt AS DECIMAL(18,6)),
    CAST(mfn_n180d_sale_amt AS DECIMAL(18,6)),
    CAST(mfn_n365d_sale_amt AS DECIMAL(18,6)),
    afnstock_n1d_sale_num ,
    CAST(afnstock_n7d_avg_sale_num AS DECIMAL(18,6)),
    CAST(afnstock_n15d_avg_sale_num AS DECIMAL(18,6)),
    CAST(afnstock_n30d_avg_sale_num AS DECIMAL(18,6)),
    CAST(afnstock_n60d_avg_sale_num AS DECIMAL(18,6)),
    fba_first_instock_date fba_first_instock_time,
    fba_instock_num fba_instock_cnt,
    afn_total_num,
    afn_warehouse_num,
    afn_fulfillable_num,
    afn_unsellable_num,
    afn_reserved_num,
    afn_reserved_customerorders_num,
    afn_reserved_fc_transfers_num,
    afn_reserved_fc_processing_num,
    afn_researching_num,
    afn_inbound_num,
    afn_inbound_working_num,
    afn_inbound_shipped_num,
    afn_inbound_receiving_num,
    '20240822' data_dt,
    current_date()  etl_data_dt
FROM dws_itm_spu_amazon_parent_asin_index_zt_tmp10
;


select count(*) from amz.dwd_prd_parent_asin_index_df where ds='20240822';