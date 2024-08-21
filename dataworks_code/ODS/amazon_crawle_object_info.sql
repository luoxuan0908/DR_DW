--@exclude_input=whde.whde.amazon_product_details
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-07 22:28:35
--********************************************************************--
--select * from amazon_crawle_object_info where ds is not null;

CREATE TABLE IF NOT EXISTS whde.amazon_crawle_object_info(
    id BIGINT  ,
    market_place_code STRING COMMENT '站点code',
    keyword STRING COMMENT '爬虫对象',
    crawl_object STRING COMMENT '爬虫需求',
    frequent STRING COMMENT '爬虫频次',
    data_date STRING COMMENT '需求日期'
)
    PARTITIONED BY (ds STRING)
    TBLPROPERTIES ('comment'='爬虫需求')
    LIFECYCLE 1000;

--select * from  whde.amazon_crawle_object_info where ds is not null  ;

insert OVERWRITE table  whde.amazon_crawle_object_info PARTITION (ds = '${nowdate}')
select   distinct abs(HASH(market_place_id,advertised_asin,crawl_object,frequent)) id, market_place_id,advertised_asin,crawl_object,frequent, '${nowdate}'
from (
         select distinct  market_place_id,advertised_asin,'product_details' crawl_object,'0' frequent
         from whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
         where ds >= '20240101'
         union all
         select distinct  market_place_id,search_term,'search_result' crawl_object,'0' frequent
         from whde.dwd_amzn_sp_search_term_by_search_term_report_ds
         where ds >= '20240101'
           and purchases_30d <3
           and length(search_term) <> 10
         union all
         select distinct  market_place_id,search_term,'search_result' crawl_object,'0' frequent
         from whde.dwd_amzn_sp_search_term_by_search_term_report_ds
         where ds >= '20240101'
           and  keyword_type <> 'TARGETING_EXPRESSION_PREDEFINED'
           and length(search_term) = 10
         union all
         select distinct  market_place_id,search_term,'association_details' crawl_object,'0' frequent
         from whde.dwd_amzn_sp_search_term_by_search_term_report_ds
         where ds >= '20240101'
           and  keyword_type= 'TARGETING_EXPRESSION_PREDEFINED'
           and length(search_term)= 10
           and purchases_30d >0
         union all
         select distinct  marketplace_id,asin,'product_details' crawl_object,'0' frequent
         from whde.dwd_amzn_all_orders_df
         where ds = MAX_PT("whde.dwd_amzn_all_orders_df")
           AND     order_status <> 'Cancelled'
         union all
         select distinct  market_place_id,search_term,'search_result' crawl_object,'1' frequent
         from whde.dwd_amzn_sp_search_term_by_search_term_report_ds
         where ds >= '20240101'
           and  purchases_30d >=3
           and length(search_term) <> 10
         union all
         select distinct  market_place_id,seller_id,'merchant_details' crawl_object,'0' frequent
         from whde.whde.amazon_product_details
         where pt = MAX_PT ("whde.whde.amazon_product_details")
           and seller_id is not null
         union all
         select distinct  market_place_id,advertised_asin,'amazon_reviews' crawl_object,'0' frequent
         from whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
         where ds >= '20240101'
     )a
where advertised_asin is not null
;
