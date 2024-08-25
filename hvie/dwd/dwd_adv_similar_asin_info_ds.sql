
CREATE TABLE IF NOT EXISTS amz.dwd_adv_similar_asin_info_ds(
    marketplace_id STRING COMMENT '市场id',
    parent_asin STRING COMMENT '父asin',
    asin STRING COMMENT '子asin',
    similar_asin STRING COMMENT '相似asin(可能是父asin也可能是子aisn，一般情况是默认的第一个子asin)',
    similar_from STRING COMMENT '相似商品来源模块名称',
    link STRING COMMENT '相似商品的链接',
    title STRING COMMENT '相似商品标题',
    image_link STRING COMMENT '图片链接',
    ratings_stars DECIMAL(18,6) COMMENT '评分',
    ratings_num BIGINT COMMENT '评论数量',
    selling_price STRING COMMENT '相似asin售价',
    is_amazon_choice BIGINT COMMENT '是否带有amazon_choice标识',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期',
    position_index BIGINT COMMENT 'asin详情页下相似商品的位置顺序编号，position_index=0时，该相似商品是亚马逊推广，position_index=1,2,3...时，是广告推广',
    is_sponsored BIGINT COMMENT '来源模块是否带有is_sponsored标识；0：没有，1：是'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊asin相似商品信息表')
    ;

INSERT OVERWRITE TABLE amz.dwd_adv_similar_asin_info_ds PARTITION (ds = '20240823')
SELECT  marketplace_id
     ,parent_asin
     ,asin
     ,similar_asin
     ,similar_from
     ,link
     ,title
     ,image_link
     ,ratings_stars
     ,ratings_num
     ,selling_price
     ,is_amazon_choice
     ,data_src --来源数据
     ,table_src -- 来源表
     ,data_dt
     ,etl_data_dt
     ,position_index
     ,is_sponsored
FROM    (
            SELECT  marketplace_id
                 ,parent_asin
                 ,asin
                 ,similar_asin
                 ,similar_from
                 ,link
                 ,title
                 ,image_link
                 ,CAST(ratings_stars AS DECIMAL(18,6)) ratings_stars
                 ,CAST(ratings_num AS BIGINT) ratings_num
                 ,selling_price
                 ,is_amazon_choice
                 ,"crawler_data" data_src --来源数据
                 ,"amazon_association_details" table_src -- 来源表
                 ,created_at data_dt
                 ,current_date() etl_data_dt
                 ,position_index
                 ,is_sponsored
                 ,ROW_NUMBER() OVER (PARTITION BY marketplace_id,parent_asin,asin,similar_asin,similar_from,position_index ORDER BY updated_at DESC  ) AS rn
            FROM    ods.ods_crawler_amazon_association_details_df
            WHERE ds = '20240823'

        ) t1
WHERE   rn = 1
;
