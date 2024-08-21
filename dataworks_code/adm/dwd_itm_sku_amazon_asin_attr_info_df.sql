--@exclude_input=WHDE.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 16:00:18
--********************************************************************--

--主键：tenant_id,seller_id,marketplace_id,seller_sku,asin
--基础数据（销售 库存 所有商品报告）--
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp1
;

CREATE TABLE IF NOT EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp1
    LIFECYCLE 3 AS
SELECT  tenant_id
     ,seller_id
     ,marketplace_id
     ,seller_sku
     ,asin
FROM     --//所有商品报告
         (           SELECT  tenant_id
                          ,seller_id
                          ,marketplace_id
                          ,seller_sku
                          ,asin
                     FROM    whde.dwd_scm_ivt_amazon_fba_stock_current_num_df
                     WHERE   ds = '${bizdate}'
                     GROUP BY tenant_id
                            ,seller_id
                            ,marketplace_id
                            ,seller_sku
                            ,asin
                     UNION ALL --销售数据
                     SELECT  tenant_id
                          ,seller_id
                          ,marketplace_id
                          ,seller_sku
                          ,asin
                     FROM    whde.dwd_amzn_all_orders_df
                     WHERE   ds = '${bizdate}'
                     GROUP BY tenant_id
                            ,seller_id
                            ,marketplace_id
                            ,seller_sku
                            ,asin
         ) T
GROUP BY tenant_id
       ,seller_id
       ,marketplace_id
       ,seller_sku
       ,asin
;

--//父子关系
--主键marketplace_id+asin
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp3
;
CREATE TABLE IF NOT EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp3
    LIFECYCLE 3 AS
SELECT  marketplace_id
     ,asin
     ,parent_asin
FROM    (
            SELECT  market_place_id marketplace_id
                 ,asin
                 ,parent_asin
                 ,ROW_NUMBER() OVER (PARTITION BY market_place_id,asin ORDER BY data_dt DESC ) AS rn
            FROM    whde.dwd_amzn_asin_to_parent_df
            WHERE   ds = '${bizdate}'
        )
WHERE   rn = 1
;



--//FN_SKU
--主键：tenant_id,seller_id,marketplace_id,seller_sku,asin
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp4
;

CREATE TABLE IF NOT EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp4
    LIFECYCLE 3 AS
SELECT  tenant_id
     ,seller_id
     ,marketplace_id
     ,seller_sku
     ,asin
     ,MAX(fnsku) fnsku
FROM   whde.dwd_scm_ivt_amazon_fba_stock_current_num_df
WHERE   ds = '${bizdate}'
GROUP BY tenant_id
       ,seller_id
       ,marketplace_id
       ,seller_sku
       ,asin
;

--//亚马逊前台信息
--//主键asin+marketplace_id
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp5
;

CREATE TABLE IF NOT EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp5
    LIFECYCLE 3 AS
SELECT  marketplace_id
     ,asin
     ,breadcrumbs_feature
     ,link
     ,brand
     ,scribing_price
     ,selling_price
     ,ratings_num
     ,ratings_stars
     ,best_sellers_rank_category
     ,SPLIT_PART(best_sellers_rank,'100',1) best_sellers_rank
     ,best_sellers_rank_detail
     ,GET_JSON_OBJECT(best_sellers_rank_detail_1,'$.category_name') AS best_sellers_rank_detail_first_category
     ,GET_JSON_OBJECT(best_sellers_rank_detail_1,'$.rank') AS best_sellers_rank_detail_first
     ,first_available_time
     ,title
     ,main_image_url
     ,update_time
FROM     (
             SELECT  market_place_id marketplace_id
                  ,DIM_ASIN asin
                  ,REPLACE(breadcrumbs_feature,'            >               ','>') breadcrumbs_feature
                  ,link
                  ,brand
                  ,scribing_price
                  ,selling_price
                  ,reviews_ratings ratings_num
                  ,reviews_stars ratings_stars
                  ,sellers_rank_category best_sellers_rank_category
                  ,sellers_rank best_sellers_rank
                  ,sellers_rank_last_detail best_sellers_rank_detail
                  ,CONCAT(
                     SPLIT_PART(
                             SUBSTR(sellers_rank_last_detail,2,LENGTH(sellers_rank_last_detail) - 1)
                         ,'}',1)
                 ,'}') AS best_sellers_rank_detail_1 --,best_sellers_rank_detail_first_category
                  --,best_sellers_rank_detail_first
                  ,date_first_available first_available_time
                  ,title
                  ,main_image_url
                  ,created_at update_time
                  ,ROW_NUMBER()OVER(PARTITION BY market_place_id,DIM_asin ORDER BY updated_at DESC) AS RANK_ID
             FROM    whde.amazon_product_details
             WHERE   PT = '${bizdate}'
         ) T2
WHERE RANK_ID =1
;


DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp100
;

CREATE TABLE IF NOT EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp100
    LIFECYCLE 3 AS
SELECT   T1.tenant_id
     ,T1.seller_id
     ,T1.marketplace_id
     ,T1.seller_sku
     ,T1.asin
     ,T7.marketplace_type
     ,T7.marketplace_website
     ,T7.country_code

     ,T7.en_country_name
     ,T7.cn_country_name

     ,T7.currency
     ,T4.fnsku
     ,T3.parent_asin
     ,T5.link
     ,T5.brand
     ,T5.scribing_price
     ,T5.selling_price
     ,T5.ratings_num
     ,T5.ratings_stars
     ,T5.best_sellers_rank_category
     ,T5.best_sellers_rank
     ,T5.best_sellers_rank_detail
     ,T5.best_sellers_rank_detail_first_category
     ,T5.best_sellers_rank_detail_first
     ,T5.first_available_time
     ,T5.title
     ,T5.main_image_url
     ,T5.breadcrumbs_feature
     ,SPLIT_PART(T5.breadcrumbs_feature,'>',1)  breadcrumbs_category_one
     ,SPLIT_PART(T5.breadcrumbs_feature,'>',2)  breadcrumbs_category_two
     ,SPLIT_PART(T5.breadcrumbs_feature,'>',3)  breadcrumbs_category_three
     ,SPLIT_PART(T5.breadcrumbs_feature,'>',4)  breadcrumbs_category_four
     ,SPLIT_PART(T5.breadcrumbs_feature,'>',5)  breadcrumbs_category_five
     ,SPLIT_PART(T5.breadcrumbs_feature,'>',6)  breadcrumbs_category_six
     ,'' cn_breadcrumbs_category_one
     ,'' cn_breadcrumbs_category_two
     ,'' cn_breadcrumbs_category_three
     ,'' cn_breadcrumbs_category_four
     ,'' cn_breadcrumbs_category_five
     ,'' cn_breadcrumbs_category_six
FROM    dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp1 T1
            LEFT JOIN dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp4 T4
                      ON      T1.tenant_id = T4.tenant_id
                          AND     T1.seller_id = T4.seller_id
                          AND     T1.marketplace_id = T4.marketplace_id
                          AND     T1.seller_sku = T4.seller_sku
                          AND     T1.asin = T4.asin
            LEFT JOIN dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp3 T3
                      ON      T1.marketplace_id = T3.marketplace_id
                          AND     T1.asin = T3.asin
            LEFT JOIN dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp5 T5
                      ON      T1.marketplace_id = T5.marketplace_id
                          AND     T1.asin = T5.asin
            LEFT JOIN   (
    SELECT  market_place_id marketplace_id
         ,market_place_type marketplace_type
         ,country_code
         ,marketplace_website
         ,country_en_name en_country_name
         ,country_cn_name cn_country_name
         ,currency_cn currency
    FROM    WHDE.dim_marketplace_info_df
) T7
                        ON      T1.marketplace_id = T7.marketplace_id
;

CREATE TABLE IF NOT EXISTS whde.dwd_itm_sku_amazon_asin_attr_info_df(
    tenant_id STRING COMMENT '租户ID',
    seller_id STRING COMMENT '卖家ID',
    marketplace_id STRING COMMENT '站点ID',
    seller_sku STRING COMMENT '卖家SKU',
    asin STRING COMMENT '子ASIN',
    marketplace_type STRING COMMENT '站点类型',
    marketplace_website STRING COMMENT '站点链接',
    country_code STRING COMMENT '国家编码',
    en_country_name STRING COMMENT '国家英文名称',
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
    best_sellers_rank_detail_first_category STRING COMMENT 'BS榜小类名称',
    best_sellers_rank_detail_first BIGINT COMMENT 'BS榜小类排名',
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
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊asin粒度基础属性表')
    LIFECYCLE 366;

--//插入结果表
INSERT OVERWRITE TABLE dwd_itm_sku_amazon_asin_attr_info_df PARTITION (ds = '${bizdate}')
SELECT  tenant_id
     ,seller_id
     ,marketplace_id
     ,seller_sku
     ,asin
     ,marketplace_type
     ,marketplace_website
     ,country_code
     ,en_country_name
     ,cn_country_name
     ,currency
     ,fnsku
     ,parent_asin
     ,CONCAT(marketplace_website,'/dp/',asin)link  --之前的link是父asin的
     ,brand
     ,scribing_price
     ,selling_price
     ,ratings_num
     ,cast(ratings_stars as DECIMAL(18,6)) as ratings_stars
     ,best_sellers_rank_category
     ,cast(case when best_sellers_rank = '' then null else best_sellers_rank end as bigint) best_sellers_rank
     ,best_sellers_rank_detail
     ,best_sellers_rank_detail_first_category
     ,CAST(best_sellers_rank_detail_first AS BIGINT) best_sellers_rank_detail_first
     ,first_available_time
     ,title
     ,main_image_url
     ,breadcrumbs_feature
     ,breadcrumbs_category_one
     ,breadcrumbs_category_two
     ,breadcrumbs_category_three
     ,breadcrumbs_category_four
     ,breadcrumbs_category_five
     ,breadcrumbs_category_six
     ,cn_breadcrumbs_category_one
     ,cn_breadcrumbs_category_two
     ,cn_breadcrumbs_category_three
     ,cn_breadcrumbs_category_four
     ,cn_breadcrumbs_category_five
     ,cn_breadcrumbs_category_six
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt
FROM    dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp100
;

--删除临时表
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp1;
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp2;
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp3;

DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp4;
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp5;
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp52;
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp6;
DROP TABLE IF EXISTS dwd_itm_sku_amazon_asin_attr_info_${bizdate}_zt_tmp100;