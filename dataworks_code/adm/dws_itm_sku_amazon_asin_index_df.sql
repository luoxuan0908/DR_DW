--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:39:42
--********************************************************************-


--基础属性
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp1;
CREATE  table  IF NOT EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp1
AS
select
    tenant_id	--租户ID
     ,seller_id	--卖家ID
     ,marketplace_id	--站点ID
     ,seller_sku	--卖家SKU
     ,asin	--子ASIN
     ,marketplace_type	--站点类型
     ,case when marketplace_id='A1F83G8C2ARO7P' then  '英国' --先把英国拿出来
           WHEN marketplace_type='Europe' THEN marketplace_type --除英国外的欧洲站其他也是用站点类型关联
           when marketplace_type='North America' THEN marketplace_type
           else marketplace_id end	 marketplace_type2  --其他用站点id
     ,marketplace_website	--站点链接
     ,country_code	--国家编码
     ,en_country_name
     ,cn_country_name	--国家中文名称
     ,currency	--货币简称
     ,fnsku	--平台sku
     ,parent_asin	--父ASIN
     ,link	--商品链接
     ,brand	--品牌名或店铺
     ,scribing_price	--划线价
     ,selling_price	--售价
     ,ratings_num	--ratings数量
     ,ratings_stars	--ratings评分
     ,best_sellers_rank_category	--BS榜一级分类名称
     ,best_sellers_rank	--BS榜一级分类排名
     ,best_sellers_rank_detail	--BS榜小类排名情况，可能有多个
     ,best_sellers_rank_detail_first_category	--BS榜小类名称(解析的第一个类目）
     ,best_sellers_rank_detail_first	--BS榜小类排名（解析的第一个类目对应的排名）
     ,first_available_time	--上架时间
     ,title	--标题
     ,main_image_url	--主图链接
     ,breadcrumbs_feature	--面包屑导航
     ,breadcrumbs_category_one	--面包屑导航一级类目
     ,breadcrumbs_category_two	--面包屑导航二级类目
     ,breadcrumbs_category_three	--面包屑导航三级类目
     ,breadcrumbs_category_four	--面包屑导航四级类目
     ,breadcrumbs_category_five	--面包屑导航五级类目
     ,breadcrumbs_category_six	--面包屑导航六级类目
     ,cn_breadcrumbs_category_one	--面包屑导航一级类目中文
     ,cn_breadcrumbs_category_two	--面包屑导航二级类目中文
     ,cn_breadcrumbs_category_three	--面包屑导航三级类目中文
     ,cn_breadcrumbs_category_four	--面包屑导航四级类目中文
     ,cn_breadcrumbs_category_five	--面包屑导航五级类目中文
     ,cn_breadcrumbs_category_six	--面包屑导航六级类目中文
FROM whde.dwd_itm_sku_amazon_asin_attr_info_df
WHERE ds='${bizdate}'
;


--//销量销售额

--备注：当前销售额数据是直接原币种加和的，一个asin如果存在不同的币种数据会有问题，后续会统一优化成本币


drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp2;
CREATE  table  IF NOT EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp2
AS
select
    tenant_id	--租户ID
     ,seller_id	--卖家ID
     ,marketplace_id	--站点ID
     ,seller_sku	--卖家SKU
     ,asin	--子ASIN

     ,SUM(n1d_sale_num) n1d_sale_num	--近1天销量
     ,SUM(n7d_sale_num)	n7d_sale_num--近7天销量
     ,SUM(n15d_sale_num)	n15d_sale_num--近15天销量
     ,SUM(n30d_sale_num)	n30d_sale_num--近30天销量
     ,SUM(n60d_sale_num)	n60d_sale_num--近60天销量
     ,SUM(n180d_sale_num) n180d_sale_num	--近180天销量
     ,SUM(n365d_sale_num) n365d_sale_num	--近365天销量
     ,SUM(n1d_sale_amt)	 n1d_sale_amt--近1天销售额
     ,SUM(n7d_sale_amt)	n7d_sale_amt--近7天销售额
     ,SUM(n15d_sale_amt) n15d_sale_amt	--近15天销售额
     ,SUM(n30d_sale_amt)	n30d_sale_amt--近30天销售额
     ,SUM(n60d_sale_amt)	n60d_sale_amt--近60天销售额

     ,SUM(n180_sale_amt)	n180_sale_amt--近180天销售额
     ,SUM(n365_sale_amt)	n365_sale_amt--近365天销售额
     ,SUM(afn_n1d_sale_num) afn_n1d_sale_num	--FBA发货近1天销量
     ,SUM(afn_n7d_sale_num)	afn_n7d_sale_num--FBA发货近7天销量
     ,SUM(afn_n15d_sale_num)	afn_n15d_sale_num--FBA发货近15天销量
     ,SUM(afn_n30d_sale_num)	afn_n30d_sale_num--FBA发货近30天销量
     ,SUM(afn_n60d_sale_num)	afn_n60d_sale_num--FBA发货近60天销量

     ,SUM(afn_n180d_sale_num) afn_n180d_sale_num	--FBA发货近180天销量
     ,SUM(afn_n365d_sale_num) afn_n365d_sale_num--FBA发货近365天销量
     ,SUM(afn_n1d_sale_amt) afn_n1d_sale_amt	--FBA发货近1天销售额
     ,SUM(afn_n7d_sale_amt)	afn_n7d_sale_amt--FBA发货近7天销售额
     ,SUM(afn_n15d_sale_amt) afn_n15d_sale_amt	--FBA发货近15天销售额
     ,SUM(afn_n30d_sale_amt)	afn_n30d_sale_amt--FBA发货近30天销售额
     ,SUM(afn_n60d_sale_amt)	afn_n60d_sale_amt--FBA发货近60天销售额

     ,SUM(afn_n180d_sale_amt) afn_n180d_sale_amt	--FBA发货近180天销售额
     ,SUM(afn_n365d_sale_amt) afn_n365d_sale_amt	--FBA发货近365天销售额
     ,SUM(mfn_n1d_sale_num)	mfn_n1d_sale_num--自发货近1天销量
     ,SUM(mfn_n7d_sale_num)	mfn_n7d_sale_num--自发货近7天销量
     ,SUM(mfn_n15d_sale_num)	mfn_n15d_sale_num--自发货近15天销量
     ,SUM(mfn_n30d_sale_num)	mfn_n30d_sale_num--自发货近30天销量
     ,SUM(mfn_n60d_sale_num)	mfn_n60d_sale_num--自发货近60天销量
     ,SUM(mfn_n180d_sale_num) mfn_n180d_sale_num	--自发货近180天销量
     ,SUM(mfn_n365d_sale_num) mfn_n365d_sale_num	--自发货近365天销量

     ,SUM(mfn_n1d_sale_amt)	mfn_n1d_sale_amt--自发货近1天销售额
     ,SUM(mfn_n7d_sale_amt)	mfn_n7d_sale_amt--自发货近7天销售额
     ,SUM(mfn_n15d_sale_amt)	mfn_n15d_sale_amt--自发货近15天销售额
     ,SUM(mfn_n30d_sale_amt)	mfn_n30d_sale_amt--自发货近30天销售额
     ,SUM(mfn_n60d_sale_amt)	mfn_n60d_sale_amt--自发货近60天销售额
     ,SUM(mfn_n180d_sale_amt) mfn_n180d_sale_amt	--自发货近180天销售额
     ,SUM(mfn_n365d_sale_amt) mfn_n365d_sale_amt	--自发货近365天销售额
FROM whde.dws_trd_ord_amazon_sale_info_df
WHERE DS= '${bizdate}'  --to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
GROUP BY
    tenant_id
       ,seller_id
       ,marketplace_id
       ,seller_sku
       ,asin
;


--//站点类型去关联，保证共享库存的每个asin上都有销量
--主键是seller_id，marketplace_type2,seller_sku	,asin	 主键没有重复数据

drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp3;
CREATE  table  IF NOT EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp3
AS
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

--,afn_reserved_customerorders_num
--,afn_reserved_fc_transfers_num
--,afn_reserved_fc_processing_num

     ,afn_researching_num		--FBA货件入库差异数量
     ,afn_inbound_num		--FBA在途
     ,afn_inbound_working_num		--FBA在途_入境工作数量
     ,afn_inbound_shipped_num		--FBA在途_入境的装船数量
     ,afn_inbound_receiving_num		--FBA在途_入境接待量

FROM whde.dws_scm_ivt_amazon_asin_df
WHERE ds='${bizdate}'
;



--首单入库时间
--这个次数放在这里比较奇怪了。。 下游用的话话 取max 不能加和
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp4;
CREATE  table  IF NOT EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp4
AS

SELECT  tenant_id
     ,marketplace_id
     ,seller_id
     ,asin
     ,MIN(operation_date) AS fba_first_instock_date
     ,COUNT(DISTINCT operation_date) fba_instock_num
FROM    whde.dwd_scm_ivt_amazon_ledger_detail_view_df
WHERE   ds='${bizdate}'
  AND     event_type = 'Receipts' --是已接收库存
GROUP BY tenant_id
       ,marketplace_id
       ,seller_id
       ,asin
;



drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp100;
CREATE  table  IF NOT EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp100
AS
select
    T1.tenant_id	--租户ID
     ,T1.seller_id	--卖家ID
     ,T1.marketplace_id	--站点ID
     ,T1.seller_sku	--卖家SKU
     ,T1.asin	--子ASIN
     ,T1.marketplace_type	--站点类型
     ,T1.marketplace_type2
     ,T1.marketplace_website	--站点链接
     ,T1.country_code	--国家编码
     ,T1.en_country_name	--国家中文名称
     ,T1.cn_country_name	--国家中文名称
     ,T1.currency	--货币简称
     ,T1.fnsku	--平台sku
     ,T1.parent_asin	--父ASIN
     ,T1.link	--商品链接
     ,T1.brand	--品牌名或店铺
     ,T1.scribing_price	--划线价
     ,T1.selling_price	--售价
     ,T1.ratings_num	--ratings数量
     ,T1.ratings_stars	--ratings评分
     ,T1.best_sellers_rank_category	--BS榜一级分类名称
     ,T1.best_sellers_rank	--BS榜一级分类排名
     ,T1.best_sellers_rank_detail	--BS榜小类排名情况，可能有多个
     ,T1.best_sellers_rank_detail_first_category	--BS榜小类名称(解析的第一个类目）
     ,T1.best_sellers_rank_detail_first	--BS榜小类排名（解析的第一个类目对应的排名）
     ,T1.first_available_time	--上架时间
     ,T1.title	--标题
     ,T1.main_image_url	--主图链接
     ,T1.breadcrumbs_feature	--面包屑导航
     ,T1.breadcrumbs_category_one	--面包屑导航一级类目
     ,T1.breadcrumbs_category_two	--面包屑导航二级类目
     ,T1.breadcrumbs_category_three	--面包屑导航三级类目
     ,T1.breadcrumbs_category_four	--面包屑导航四级类目
     ,T1.breadcrumbs_category_five	--面包屑导航五级类目
     ,T1.breadcrumbs_category_six	--面包屑导航六级类目
     ,T1.cn_breadcrumbs_category_one	--面包屑导航一级类目中文
     ,T1.cn_breadcrumbs_category_two	--面包屑导航二级类目中文
     ,T1.cn_breadcrumbs_category_three	--面包屑导航三级类目中文
     ,T1.cn_breadcrumbs_category_four	--面包屑导航四级类目中文
     ,T1.cn_breadcrumbs_category_five	--面包屑导航五级类目中文
     ,T1.cn_breadcrumbs_category_six	--面包屑导航六级类目中文


     ,T2.n1d_sale_num
     ,T2.n7d_sale_num
     ,T2.n15d_sale_num
     ,T2.n30d_sale_num
     ,T2.n60d_sale_num
     ,T2.n180d_sale_num
     ,T2.n365d_sale_num
     ,T2.n1d_sale_amt
     ,T2.n7d_sale_amt
     ,T2.n15d_sale_amt
     ,T2.n30d_sale_amt
     ,T2.n60d_sale_amt
     ,T2.n180_sale_amt
     ,T2.n365_sale_amt
     ,T2.afn_n1d_sale_num
     ,T2.afn_n7d_sale_num
     ,T2.afn_n15d_sale_num
     ,T2.afn_n30d_sale_num
     ,T2.afn_n60d_sale_num
     ,T2.afn_n180d_sale_num
     ,T2.afn_n365d_sale_num
     ,T2.afn_n1d_sale_amt
     ,T2.afn_n7d_sale_amt
     ,T2.afn_n15d_sale_amt
     ,T2.afn_n30d_sale_amt
     ,T2.afn_n60d_sale_amt
     ,T2.afn_n180d_sale_amt
     ,T2.afn_n365d_sale_amt
     ,T2.mfn_n1d_sale_num
     ,T2.mfn_n7d_sale_num
     ,T2.mfn_n15d_sale_num
     ,T2.mfn_n30d_sale_num
     ,T2.mfn_n60d_sale_num
     ,T2.mfn_n180d_sale_num
     ,T2.mfn_n365d_sale_num
     ,T2.mfn_n1d_sale_amt
     ,T2.mfn_n7d_sale_amt
     ,T2.mfn_n15d_sale_amt
     ,T2.mfn_n30d_sale_amt
     ,T2.mfn_n60d_sale_amt
     ,T2.mfn_n180d_sale_amt
     ,T2.mfn_n365d_sale_amt

     ,T3.afnstock_n1d_sale_num
     ,T3.afnstock_n7d_avg_sale_num
     ,T3.afnstock_n15d_avg_sale_num
     ,T3.afnstock_n30d_avg_sale_num
     ,T3.afnstock_n60d_avg_sale_num

     ,T4.fba_first_instock_date as fba_first_instock_TIME
     ,T4.fba_instock_num as fba_instock_cnt

     ,T3.afn_total_num
     ,T3.afn_warehouse_num
     ,T3.afn_fulfillable_num
     ,T3.afn_unsellable_num
     ,T3.afn_reserved_num

     ,0  afn_reserved_customerorders_num
     ,0  afn_reserved_fc_transfers_num
     ,0  afn_reserved_fc_processing_num

     ,T3.afn_researching_num

     ,T3.afn_inbound_num
     ,T3.afn_inbound_working_num
     ,T3.afn_inbound_shipped_num
     ,T3.afn_inbound_receiving_num



FROM  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp1 T1
          LEFT JOIN dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp2 T2
                    ON T1.tenant_id	=T2.tenant_id
                        AND T1.seller_id=T2.seller_id
                        AND T1.marketplace_id=T2.marketplace_id
                        AND T1.seller_sku=T2.seller_sku
                        AND T1.asin=T2.asin
          LEFT JOIN dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp3 T3
                    ON T1.seller_id=T3.seller_id
                        AND T1.seller_sku=T3.seller_sku
                        AND T1.asin=T3.asin
                        AND T1.marketplace_type2=T3.marketplace_type2
          LEFT JOIN dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp4 T4
                    ON T1.seller_id=T4.seller_id
                        AND T1.marketplace_id=T4.marketplace_id
                        AND T1.asin=T4.asin
;


CREATE TABLE IF NOT EXISTS whde.dws_itm_sku_amazon_asin_index_df(
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
    fnsku STRING COMMENT '平台sku',
    parent_asin STRING COMMENT '父ASIN',
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
    first_available_time DATETIME COMMENT '上架时间',
    title STRING COMMENT '标题',
    main_image_url STRING COMMENT '主图链接',
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
    fba_first_instock_time DATETIME COMMENT 'FBA首单入库时间',
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
    etl_data_dt DATETIME COMMENT '数据加载日期',
    asin_rk  BIGINT,
    order_num_rate double
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊asin粒度指标宽表')
    LIFECYCLE 366;


--//插入结果表
INSERT OVERWRITE TABLE  dws_itm_sku_amazon_asin_index_df  PARTITION (ds = '${bizdate}')
SELECT
    tenant_id,
    seller_id,
    marketplace_id,
    seller_sku,
    asin,
    marketplace_type,
    marketplace_website,
    country_code,
    cn_country_name,
    currency,
    fnsku,
    parent_asin,
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
    main_image_url,
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
    cast(n1d_sale_amt AS DECIMAL(18,6)),
    cast(n7d_sale_amt AS DECIMAL(18,6)),
    cast(n15d_sale_amt AS DECIMAL(18,6)),
    cast(n30d_sale_amt AS DECIMAL(18,6)),
    cast(n60d_sale_amt AS DECIMAL(18,6)),
    cast(n180_sale_amt AS DECIMAL(18,6)),
    cast(n365_sale_amt AS DECIMAL(18,6)),
    afn_n1d_sale_num,
    afn_n7d_sale_num,
    afn_n15d_sale_num,
    afn_n30d_sale_num,
    afn_n60d_sale_num,
    afn_n180d_sale_num,
    afn_n365d_sale_num,
    cast(afn_n1d_sale_amt AS DECIMAL(18,6)),
    cast(afn_n7d_sale_amt AS DECIMAL(18,6)),
    cast(afn_n15d_sale_amt AS DECIMAL(18,6)),
    cast(afn_n30d_sale_amt AS DECIMAL(18,6)),
    cast(afn_n60d_sale_amt AS DECIMAL(18,6)),
    cast(afn_n180d_sale_amt AS DECIMAL(18,6)),
    cast(afn_n365d_sale_amt AS DECIMAL(18,6)),
    mfn_n1d_sale_num,
    mfn_n7d_sale_num,
    mfn_n15d_sale_num,
    mfn_n30d_sale_num,
    mfn_n60d_sale_num,
    mfn_n180d_sale_num,
    mfn_n365d_sale_num,
    cast(mfn_n1d_sale_amt AS DECIMAL(18,6)),
    cast(mfn_n7d_sale_amt AS DECIMAL(18,6)),
    cast(mfn_n15d_sale_amt AS DECIMAL(18,6)),
    cast(mfn_n30d_sale_amt AS DECIMAL(18,6)),
    cast(mfn_n60d_sale_amt AS DECIMAL(18,6)),
    cast(mfn_n180d_sale_amt AS DECIMAL(18,6)),
    cast(mfn_n365d_sale_amt AS DECIMAL(18,6)),
    afnstock_n1d_sale_num,
    afnstock_n7d_avg_sale_num,
    afnstock_n15d_avg_sale_num,
    afnstock_n30d_avg_sale_num,
    afnstock_n60d_avg_sale_num,
    fba_first_instock_time,
    fba_instock_cnt,
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

    '${bizdate}' data_dt,
    GETDATE()  etl_data_dt
        ,row_number() over (partition by tenant_id,marketplace_id,seller_id,parent_asin order by n30d_sale_amt desc ) as asin_rk
   ,0 order_num_rate
FROM dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp100
;

--//删除临时表
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp100;
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp1;
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp2;
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp3;
drop table  IF EXISTS  dwd_itm_sku_amazon_asin_index_${bizdate}_zt_tmp4;





