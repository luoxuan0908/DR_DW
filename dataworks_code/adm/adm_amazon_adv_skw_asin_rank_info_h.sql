--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:20:02
--********************************************************************--
CREATE TABLE IF NOT EXISTS open_dw.adm_amazon_adv_skw_asin_rank_info_h(
    search_term STRING COMMENT '输入的搜索关键词',
    data_date DATETIME COMMENT '日期',
    data_hour BIGINT COMMENT '小时',
    page BIGINT COMMENT '页数',
    search_rank BIGINT COMMENT '搜索词下的排名',
    asin STRING COMMENT '商品子asin',
    parent_asin STRING COMMENT '父asin',
    link STRING COMMENT '商品链接',
    brand STRING COMMENT '品牌',
    title STRING COMMENT '商品标题',
    main_img_url STRING COMMENT '主图链接',
    color_num BIGINT COMMENT '颜色数量',
    ratings_stars DECIMAL(18,6) COMMENT '评分',
    ratings_num BIGINT COMMENT '评论数量',
    n30d_sale_num STRING COMMENT '最近30天销量（100+，1k+的形式，无具体值）',
    discount_label STRING COMMENT '折扣标签',
    discount_info STRING COMMENT '折扣信息',
    prime_info STRING COMMENT '会员试用信息（会员可以先免费试用）',
    scribing_price STRING COMMENT '划线价(Typical:过去一段时间的价格，List:制造商建议价格)',
    selling_price STRING COMMENT '售价',
    low_price_info STRING COMMENT '最低价信息（当前价格是否是过去30天内最低价）',
    delivery_info STRING COMMENT '送货信息',
    feature_rating_info STRING COMMENT '产品的特性评分（etl:产品在轻便性上获得4星以上评分）',
    stock_info STRING COMMENT '库存信息',
    option_info STRING COMMENT '商品可供选择的信息（几种尺码、口味供选择）',
    is_best_seller BIGINT COMMENT '是否有Best Seller标识',
    best_seller_category STRING COMMENT 'best seller对应的类目',
    is_sponsored BIGINT COMMENT '是否是广告商品',
    is_prime BIGINT COMMENT '是否有prime标识',
    is_small_business BIGINT COMMENT '是否有Small Business标识',
    is_climate_pledge_friendly BIGINT COMMENT '是否有环境友好标识',
    is_amazon_brand BIGINT COMMENT '是否带有amazon_brand标识',
    created_time DATETIME COMMENT '创建时间',
    updated_time DATETIME COMMENT '修改时间',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    marketplace_id STRING COMMENT '站点ID'
    )
    PARTITIONED BY (hs STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊关键词搜索结果页asin排名详情数据（小时增量）')
    LIFECYCLE 1000;


INSERT OVERWRITE TABLE adm_amazon_adv_skw_asin_rank_info_h PARTITION (hs='${bizdate}')
SELECT
    search_term,
    data_date,
    data_hour,
    page,
    search_rank,
    a.asin,
    parent_asin,
    link,
    brand,
    title,
    main_img_url,
    color_num,
    ratings_stars,
    ratings_num,
    n30d_sale_num,
    discount_label,
    discount_info,
    prime_info,
    scribing_price,
    selling_price,
    low_price_info,
    delivery_info,
    feature_rating_info,
    stock_info,
    option_info,
    is_best_seller,
    best_seller_category,
    is_sponsored,
    is_prime,
    is_small_business,
    is_climate_pledge_friendly,
    is_amazon_brand,
    created_time,
    updated_time,
    '${bizdate}' data_dt,
    GETDATE() etl_data_dt,
    a.marketplace_id
FROM
    (SELECT
         search_term,
         data_date,
         data_hour,
         page,
         search_rank,
         asin,
         link,
         brand,
         title,
         main_img_url,
         color_num,
         ratings_stars,
         ratings_num,
         n30d_sale_num,
         discount_label,
         discount_info,
         prime_info,
         scribing_price,
         selling_price,
         low_price_info,
         delivery_info,
         feature_rating_info,
         stock_info,
         option_info,
         is_best_seller,
         best_seller_category,
         is_sponsored,
         is_prime,
         is_small_business,
         is_climate_pledge_friendly,
         is_amazon_brand,
         created_time,
         updated_time,
         marketplace_id
     FROM open_dw.dwd_itm_sku_amazon_skw_asin_rank_info_hs
     WHERE   hs = '${bizdate}') a
        LEFT JOIN
    (
        SELECT
            marketplace_id,
            asin,
            MAX(parent_asin)parent_asin
        FROM open_dw.dwd_itm_sku_amazon_asin_to_parent_df
        WHERE ds = MAX_PT ('open_dw.dwd_itm_sku_amazon_asin_to_parent_df')
        GROUP BY
            marketplace_id,
            asin
    )b
    on a.asin=b.asin
        AND a.marketplace_id=b.marketplace_id
;

