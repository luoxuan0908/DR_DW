CREATE TABLE IF NOT EXISTS amz.mid_itm_spu_amazon_search_keyword_info_ws(
    marketplace_id STRING COMMENT '站点',
    search_term STRING COMMENT '搜索词',
    search_frequency_rank STRING COMMENT '搜索频率排名',
    top1_brand STRING COMMENT '点击量最高的品牌 #1',
    top1_product_asin STRING COMMENT '点击量最高的商品 #1：ASIN',
    top1_product_name STRING COMMENT '点击量最高的商品 #1：商品名称',
    top1_product_click_share DECIMAL(18,6) COMMENT '点击量最高的商品 #1：点击份额',
    top1_product_conversion_share DECIMAL(18,6) COMMENT '点击量最高的商品 #1：转化份额',
    top1_category STRING COMMENT '点击量最高的类别 #1',
    top2_brand STRING COMMENT '点击量最高的品牌 #2',
    top2_product_asin STRING COMMENT '点击量最高的商品 #2：ASIN',
    top2_product_name STRING COMMENT '点击量最高的商品 #2：商品名称',
    top2_product_click_share DECIMAL(18,6) COMMENT '点击量最高的商品 #2：点击份额',
    top2_product_conversion_share DECIMAL(18,6) COMMENT '点击量最高的商品 #2：转化份额',
    top2_category STRING COMMENT '点击量最高的类别 #2',
    top3_brand STRING COMMENT '点击量最高的品牌 #3',
    top3_product_asin STRING COMMENT '点击量最高的商品 #3：ASIN',
    top3_product_name STRING COMMENT '点击量最高的商品 #3：商品名称',
    top3_product_click_share DECIMAL(18,6) COMMENT '点击量最高的商品 #3：点击份额',
    top3_product_conversion_share DECIMAL(18,6) COMMENT '点击量最高的商品 #3：转化份额',
    top3_category STRING COMMENT '点击量最高的类别 #3',
    rank_date date COMMENT '搜索词数据排名日期',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期'
    )
    PARTITIONED BY (ws STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊关键词搜索排名周表')
 ;


with dwd_itm_spu_amazon_search_keyword_info_ws_tmp as (
select   marketplace_id
       , search_term
       , search_frequency_rank
       , top1_clicked_brand    top_brand1
       , top1_clicked_asin     top_product_asin1
       , top1_product_title    top_product_name1
       , top1_click_share      top_product_click_share1
       , top1_conversion_share top_product_conversion_share1
       , top1_clicked_category top_category1
       , top2_clicked_brand    top_brand2
       , top2_clicked_asin     top_product_asin2
       , top2_product_title    top_product_name2
       , top2_click_share      top_product_click_share2
       , top2_conversion_share top_product_conversion_share2
       , top2_clicked_category top_category2
       , top3_clicked_brand    top_brand3
       , top3_clicked_asin     top_product_asin3
       , top3_product_title    top_product_name3
       , top3_click_share      top_product_click_share3
       , top3_conversion_share top_product_conversion_share3
       , top3_clicked_category top_category3
       , report_date
       , row_number()          over(partition by marketplace_id,search_term order by report_date desc,cast(search_frequency_rank as bigint) asc) as rk
      from ods.ods_report_search_data_df
        where ds = '${last_day}' -- 10679289
        )

insert overwrite table amz.mid_itm_spu_amazon_search_keyword_info_ws partition (ws = '${last_day}')
select marketplace_id
     ,search_term
     ,search_frequency_rank
     ,top_brand1
     ,top_product_asin1
     ,top_product_name1
     ,cast(top_product_click_share1 as decimal(18,6)) as top_product_click_share1
     ,cast(top_product_conversion_share1 as decimal(18,6)) as top_product_conversion_share1
     ,top_category1
     ,top_brand2
     ,top_product_asin2
     ,top_product_name2
     ,cast(top_product_click_share2 as decimal(18,6)) as top_product_click_share2
     ,cast(top_product_conversion_share2 as decimal(18,6)) as top_product_conversion_share2
     ,top_category2
     ,top_brand3
     ,top_product_asin3
     ,top_product_name3
     ,cast(top_product_click_share3 as decimal(18,6)) as top_product_click_share3
     ,cast(top_product_conversion_share3 as decimal(18,6)) as top_product_conversion_share3
     ,top_category3
     ,cast(report_date as date) as rank_date
     ,'report' as data_src
     ,'search_data' as table_src
     ,'${bizdate}' as data_dt
     ,current_date() as etl_data_dt
from dwd_itm_spu_amazon_search_keyword_info_ws_tmp
where rk = 1
group by marketplace_id
       ,search_term
       ,search_frequency_rank
       ,top_brand1
       ,top_product_asin1
       ,top_product_name1
       ,cast(top_product_click_share1 as decimal(18,6))
       ,cast(top_product_conversion_share1 as decimal(18,6))
       ,top_category1
       ,top_brand2
       ,top_product_asin2
       ,top_product_name2
       ,cast(top_product_click_share2 as decimal(18,6))
       ,cast(top_product_conversion_share2 as decimal(18,6))
       ,top_category2
       ,top_brand3
       ,top_product_asin3
       ,top_product_name3
       ,cast(top_product_click_share3 as decimal(18,6))
       ,cast(top_product_conversion_share3 as decimal(18,6))
       ,top_category3
       ,cast(report_date as date)
;


select count(1) from amz.mid_itm_spu_amazon_search_keyword_info_ws where ws = '${last_day}';