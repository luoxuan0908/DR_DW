
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-27 23:25:54
--********************************************************************--
-- SELECT  market_place_id
--                   ,child_asin
--                   ,similar_asin
--                   ,MIN(CASE    WHEN (idx / 8) > 1 THEN 2 ELSE 1 END) AS rk --首页只有8个坑位
--           FROM    whde.ods_amazon_amazon_data_2_amazon_asin_similar_item
--           WHERE   ds = '${bizdate}'
--           AND     idx <> 0 --idx=0不是关联坑位
--           GROUP BY market_place_id
--                    ,child_asin
--                    ,similar_asin

CREATE TABLE IF NOT EXISTS whde.ods_amazon_amazon_data_2_amazon_asin_similar_item(
    market_place_id STRING COMMENT '市场id',
    data_date DATETIME COMMENT '采集日期',
    labeling_title STRING COMMENT '模块标题',
    parent_asin STRING COMMENT '父asin',
    child_asin STRING COMMENT '子asin',
    similar_asin STRING COMMENT '相似asin',
    link STRING COMMENT '商品链接',
    title STRING COMMENT '商品标题',
    image_url STRING COMMENT '图片链接',
    reviews_stars STRING COMMENT '评分',
    reviews_ratings BIGINT COMMENT '评论数量',
    selling_price STRING COMMENT '售价',
    is_amazon_choice BIGINT COMMENT '是否带有amazon_choice标识',
    idx BIGINT COMMENT '位置排名',
    is_sponsored BIGINT COMMENT '是否是广告',
    created_at DATETIME COMMENT '创建时间',
    updated_at DATETIME COMMENT '修改时间'

)
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='相似推广')
    LIFECYCLE 30;


insert OVERWRITE table ods_amazon_amazon_data_2_amazon_asin_similar_item PARTITION (ds = '${bizdate}' )
select  marketplace_id
     ,created_at
     ,similar_from
     ,parent_asin
     ,asin
     ,similar_asin
     ,link
     ,title
     ,image_link
     ,ratings_stars
     ,ratings_num
     ,selling_price
     ,is_amazon_choice
     ,position_index
     ,is_sponsored
     ,created_at
     ,updated_at
from whde.amazon_association_details
where pt ='${bizdate}'
;
