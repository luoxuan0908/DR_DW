CREATE TABLE IF NOT EXISTS amz.dim_prd_product_detail_df (
    tenant_id STRING COMMENT '租户ID',
    seller_id STRING COMMENT '卖家ID',
    marketplace_id STRING COMMENT '站点ID',
    parent_asin STRING COMMENT '父ASIN',
    asin STRING COMMENT 'ASIN',
    link STRING COMMENT '商品链接',
    brand STRING COMMENT '品牌名或店铺',
    scribing_price DECIMAL(10,2) COMMENT '划线价',
    selling_price DECIMAL(10,2) COMMENT '售价',
    ratings_num INT COMMENT 'ratings数量',
    ratings_stars DECIMAL(3,2) COMMENT 'ratings评分',
    best_sellers_rank_category STRING COMMENT 'BS榜一级分类名称',
    best_sellers_rank INT COMMENT 'BS榜一级分类排名',
    best_sellers_rank_detail STRING COMMENT 'BS榜小类排名情况，可能有多个',
    best_sellers_rank_detail_first_category STRING COMMENT 'BS榜小类名称(解析的第一个类目）',
    best_sellers_rank_detail_first INT COMMENT 'BS榜小类排名（解析的第一个类目对应的排名）',
    first_available_time TIMESTAMP COMMENT '上架时间',
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
    cn_breadcrumbs_category_six STRING COMMENT '面包屑导航六级类目中文'
) COMMENT '产品详情表'
    partitioned by (ds STRING)
    STORED AS  orc;



INSERT overwrite TABLE amz.dim_prd_product_detail_df PARTITION(ds = '${last_day}')
SELECT
    t2.tenant_id,
    t1.seller_id,
    t1.market_place_id AS marketplace_id,
    t1.parent_asin,
    t1.dim_asin AS asin,
    t1.link,
    t1.brand,
    CAST(t1.scribing_price AS DECIMAL(10,2)) AS scribing_price,
    CAST(t1.selling_price AS DECIMAL(10,2)) AS selling_price,
    t1.reviews_ratings AS ratings_num,
    t1.reviews_stars  AS ratings_stars,
    t1.sellers_rank_category AS best_sellers_rank_category,
    t1.sellers_rank AS best_sellers_rank,
    t1.sellers_rank_last_detail AS best_sellers_rank_detail,
    SPLIT(sellers_rank_last_detail, ',')[0] AS best_sellers_rank_detail_first_category,
    SPLIT(sellers_rank_last_detail, ',')[1] AS best_sellers_rank_detail_first,
    CAST(date_first_available AS TIMESTAMP) AS first_available_time,  -
    t1.title,
    t1.main_image_url,
    t1.breadcrumbs_feature,
    SPLIT(breadcrumbs_feature, '>')[0] AS breadcrumbs_category_one,
    SPLIT(breadcrumbs_feature, '>')[1] AS breadcrumbs_category_two,
    SPLIT(breadcrumbs_feature, '>')[2] AS breadcrumbs_category_three,
    SPLIT(breadcrumbs_feature, '>')[3] AS breadcrumbs_category_four,
    SPLIT(breadcrumbs_feature, '>')[4] AS breadcrumbs_category_five,
    SPLIT(breadcrumbs_feature, '>')[5] AS breadcrumbs_category_six,
    NULL AS cn_breadcrumbs_category_one,
    NULL AS cn_breadcrumbs_category_two,
    NULL AS cn_breadcrumbs_category_three,
    NULL AS cn_breadcrumbs_category_four,
    NULL AS cn_breadcrumbs_category_five,
    NULL AS cn_breadcrumbs_category_six
FROM
    (  select  t.*,
        ROW_NUMBER() OVER(PARTITION BY market_place_id, DIM_asin ORDER BY created_at DESC) AS rn
    FROM
        ods.ods_crawler_amazon_product_details_df t
    WHERE ds = '${last_day}'
    ) t1
left join
        (
            SELECT
                tenant_id,
                seller_id,
                marketplace_id,
                profile_id,
                seller_name
            FROM
                ods.ods_report_authorization_account_df
            WHERE
                ds = '${last_day}'
        ) t2
    on t1.market_place_id = t2.marketplace_id and t1.seller_id = t2.seller_id
WHERE
   t1.rn =1;
