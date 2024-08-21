--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-27 23:43:14
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dwd_itm_sku_amazon_skw_asin_rank_info_hs(
   search_term STRING COMMENT '输入的搜索关键词',
   data_date DATETIME COMMENT '日期',
   data_hour BIGINT COMMENT '小时',
   page BIGINT COMMENT '页数',
   search_rank BIGINT COMMENT '搜索词下的排名',
   asin STRING COMMENT '商品子asin',
   link STRING COMMENT '商品链接',
   brand STRING COMMENT '品牌',
   title STRING COMMENT '商品标题',
   main_img_url STRING COMMENT '主图链接',
   color_num BIGINT COMMENT '颜色数量',
   ratings_stars  STRING COMMENT '评分',
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
   data_src STRING COMMENT '数据源名',
   table_src STRING COMMENT '来源表名',
   data_dt STRING COMMENT '数据日期',
   etl_data_dt DATETIME COMMENT '数据加载日期',
   marketplace_id STRING COMMENT '站点ID',
   label_text STRING COMMENT '搜索词结果的广告类型标题，搜索结果默认为Search result，其他广告模块例如：Brands related to your search',
   period STRING COMMENT '频率，day天，hour小时',
   postcode STRING COMMENT '邮编'
)
    PARTITIONED BY (hs STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊关键词搜索结果页asin排名详情数据（小时增量）')
    LIFECYCLE 1000;


INSERT OVERWRITE TABLE dwd_itm_sku_amazon_skw_asin_rank_info_hs PARTITION (hs='${bizdate}')
SELECT
    search_term,
    data_date,
    cast(data_hour as bigint),
    page,
    search_rank,
    data_asin asin,
    link,
    brand,
    title,
    main_img_url,
    number_of_sub_asin color_num,
    reviews_stars ratings_stars,
    cast(reviews_ratings AS  BIGINT ) ratings_num,
    SPLIT_PART(sales_volume,' ',1) n30d_sale_num,
    '' discount_label,
    discount_info,
    '' prime_info,
    scribing_price,
    selling_price,
    '' low_price_info,
    '' delivery_info,
    '' feature_rating_info,
    '' stock_info,
    '' option_info,
    0 is_best_seller,
    '' best_seller_category,
    is_sponsored,
    0 is_prime,
    small_business is_small_business,
    0 is_climate_pledge_friendly,
    0 is_amazon_brand,
    created_at created_time,
    updated_at updated_time,
    'crawler_data' data_src,
    'amazon_search_result' table_src,
    data_date,
    GETDATE() etl_data_dt,
    market_place_id marketplace_id,
    '' label_text,
    frequent period,
    '' postcode
from  whde.amazon_search_result
WHERE pt ='${bizdate}'
  and to_char(data_date,'yyyymmdd') ='${bizdate}'
;

