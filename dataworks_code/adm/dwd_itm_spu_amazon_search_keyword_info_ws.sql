--@exclude_output=open_dw.dwd_itm_spu_amazon_search_keyword_info_ws
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-27 23:30:55
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.dwd_itm_spu_amazon_search_keyword_info_ws(
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
    rank_date DATETIME COMMENT '搜索词数据排名日期',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ws STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊关键词搜索排名周表')
    LIFECYCLE 1000;

drop table if exists dwd_itm_spu_amazon_search_keyword_info_ws_tmp ;
create table dwd_itm_spu_amazon_search_keyword_info_ws_tmp as
select marketplace_id
     ,search_term
     ,search_frequency_rank
     ,top1_clicked_brand top_brand1
     ,top1_clicked_asin top_product_asin1
     ,top1_product_title top_product_name1
     ,top1_click_share top_product_click_share1
     ,top1_conversion_share top_product_conversion_share1
     ,top1_clicked_category top_category1
     ,top2_clicked_brand top_brand2
     ,top2_clicked_asin top_product_asin2
     ,top2_product_title top_product_name2
     ,top2_click_share top_product_click_share2
     ,top2_conversion_share top_product_conversion_share2
     ,top2_clicked_category top_category2
     ,top3_clicked_brand top_brand3
     ,top3_clicked_asin top_product_asin3
     ,top3_product_title top_product_name3
     ,top3_click_share top_product_click_share3
     ,top3_conversion_share top_product_conversion_share3
     ,top3_clicked_category top_category3
     ,report_date
     ,row_number()over(partition by marketplace_id,search_term order by report_date desc,cast(search_frequency_rank as bigint) asc) as rk
from  whde.search_data
where pt = MAX_PT("whde.search_data")
;

insert overwrite table dwd_itm_spu_amazon_search_keyword_info_ws partition (ws = '${bizdate}')
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
     ,cast(to_date(report_date,'yyyy-mm-dd') as datetime) as rank_date
     ,"report" as data_src
     ,"search_data" as table_src
     ,'${bizdate}' as data_dt
     ,getdate() as etl_data_dt
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
       ,cast(to_date(report_date,'yyyy-mm-dd') as datetime)
;



drop table if exists dwd_itm_spu_amazon_search_keyword_info_ws_tmp ;


