--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-04 11:46:07
--********************************************************************--
select * from dwd_sit_shp_amazon_seller_sites_store_df
where ds = '20240704'
;

select distinct parent_asin from adm_compain_asin_adv_info_ds_tmp
;

select* from user_info where pt = '20240701'
;

select * from dws_mkt_adv_strategy_stop_word_product_detail_ds --adm_strategy_stop_word_product_detail_ds
where ds = '20240630'
;

SELECT * from user_info
where pt ='20240629'


select ds,tenant_id,tenant_name,market_place_name,profile_id,seller_id,seller_name ,count(*)
from adm_parent_asin_adv_info_ds_tmp
group by  ds,tenant_id,tenant_name,market_place_name,profile_id,seller_id,seller_name
;


select *
from dwd_amzn_sp_advertised_product_by_advertiser_report_ds
where ds = '20240614'
;

select * from adm_parent_asin_adv_info_ds_tmp
where parent_asin = 'B08B1VNX86'
;

select * from dwd_amzn_asin_to_parent_df
where ds = '20240615'
  and parent_asin = 'B08B1VNX86'
;


select * From get_merchant_listings_all_data
where pt = '20240615'
  and asin1 in( 'B08B1VNX86','B08B1VBBNB','B08B1WJ7RF')
;





SELECT tenant_id,operation_date, count(*)
FROM whde.ods_get_ledger_detail_view_data
WHERE SUBSTR(ds,1,8) = '${bizdate}'
group by tenant_id,operation_date
;

select seller_id ,marketplace_id,count(*)
from dwd_amzn_all_orders_df
where ds = '${bizdate}'
group by seller_id,marketplace_id
;

SELECT  a.*
     ,b.parent_asin
     ,c.asin
FROM    (
            SELECT  *
            FROM    whde.amazon_crawle_object_info
            WHERE   ds = '${bizdate}'
              AND     crawl_object = 'product_details'
        ) a
            LEFT OUTER JOIN (
    SELECT  *
    FROM    dwd_amzn_asin_to_parent_df
    WHERE   ds = '${bizdate}'
      AND     TO_CHAR(data_dt,'yyyymmdd') = '${bizdate}'
) b
                            ON      a.market_place_code = b.market_place_id
                                AND     a.keyword = b.ASIN
            LEFT OUTER JOIN (
    SELECT  *
    FROM    dwd_amzn_asin_to_parent_df
    WHERE   ds = '${bizdate}'
      AND     TO_CHAR(data_dt,'yyyymmdd') = '${bizdate}'
) c
                            ON      a.market_place_code = c.market_place_id
                                AND     a.keyword = c.parent_asin
;

select a.*,COALESCE(b.parent_asin,c.parent_asin)
from (
         select distinct tenant_id,market_place_id,advertised_asin from ods_amzn_sp_advertised_product_by_advertiser_report
         where ds  = '${bizdate}'
           and tenant_id = '67354cc2df65894139011e1c4ca153c5'
     )a
         left outer join (
    select * from dwd_amzn_asin_to_parent_df where  ds = '${bizdate}')b
                         on a.advertised_asin =b.asin
                             and a.market_place_id = b.market_place_id
         left outer join (
    select * from dwd_amzn_asin_to_parent_df where  ds = '${bizdate}')c
                         on a.advertised_asin =c.parent_asin
                             and a.market_place_id = c.market_place_id
;

select tenant_id,marketplace_name,count(*) from adm_amazon_adv_sku_wide_d
where ds = '20240531'
group by tenant_id,marketplace_name
;

select * from user_info where pt = '20240531'
;


select tenant_id,market_place_id,count(*) from ods_amzn_sp_advertised_product_by_advertiser_report
where ds  = '${bizdate}'
group by tenant_id,market_place_id
;

select distinct profile_id, b.parent_asin
from (
         select * from ods_amzn_sp_advertised_product_by_advertiser_report
         where ds  = '${bizdate}'
           and profile_id in ('1149660418925642','4191703077327839','2515729133794805')
--and seller_id = 'ABI38J8YT8HUT'
     )a
         left outer join dwd_amzn_asin_to_parent_df  b
                         on a.advertised_asin = b.asin
where b.ds =  '${bizdate}'
;


select count(distinct parent_asin) from adm_parent_asin_adv_info_ds_tmp
where market_place_id =  'ATVPDKIKX0DER'
;

drop table amzn_ad_product_data
;
select ad_mode,count(*)
from adm_amazon_adv_strategy_pasin_search_term_d
where ds = '${bizdate}'
group by ad_mode
;

select report_date,count(*) from ods_amzn_sp_search_term_by_search_term_report
where ds= '20240508'
group by report_date
;

select report_date,count(*) from   whde.amzn_sp_advertised_product_by_advertiser_report
WHERE   pt = '${bizdate}'
group by report_date
;

SELECT *
from whde.dws_mkt_adv_strategy_add_word_product_detail_ds
where ds >='${bizdate}'
;

select * from dwd_itm_spu_amazon_search_keyword_info_ws
where ws = max_pt("whde.dwd_itm_spu_amazon_search_keyword_info_ws")
  and search_term in ('gafas presbicia hombre','Lesebrille')
;


select task_id,market_place_id,search_term,count(*)
from  whde.amazon_search_result
WHERE pt = '${bizdate}'
  and task_id = '20240502'
group by  task_id,market_place_id,search_term
;

SELECT  *
FROM    whde.dwd_sit_shp_amazon_seller_sites_store_df
WHERE   ds = MAX_PT('whde.dwd_sit_shp_amazon_seller_sites_store_df')
;

;

select distinct a.profile_id,a.asin,b.market_place_id
from ( select * from dwd_mkt_adv_amazon_product_log_ds
       where ds = '20240503') a
         left outer join (
    select distinct  market_place_id,advertised_asin,'product_details' crawl_object,'0' frequent
    from whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
    where ds >= '20240101') b
                         on a.asin = b.advertised_asin;

select distinct tenant_id, profile_id,market_place_id,seller_id
from whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
where ds >= '20240101'
;


select * from adm_amazon_adv_search_term_pasin_rank_df
where ds = '20240504'
;


SELECT  market_place_id
     ,COUNT(*)
FROM    whde.amazon_search_result
WHERE   pt = '${bizdate}'
  AND     TO_CHAR(data_date,'yyyymmdd') = '${bizdate}'
GROUP BY market_place_id
;



SELECT  t1.marketplace_id
     , t2.parent_asin
     , t1.asin
     ,t2.asin
     ,t3.keyword
     ,count(*)
FROM    whde.dwd_itm_sku_amazon_skw_asin_rank_info_hs t1
            LEFT JOIN   (
    SELECT  asin
         ,parent_asin
         ,market_place_id marketplace_id
    FROM    whde.dwd_amzn_asin_to_parent_df
    WHERE   ds = MAX_PT('whde.dwd_amzn_asin_to_parent_df')
) t2
                        ON      t1.asin = t2.asin
                            and t1.marketplace_id=t2.marketplace_id
            left outer join （select* from whde.amazon_crawle_object_info where ds = '20240504') t3
on t1.asin = t3.keyword
WHERE   SUBSTR(t1.hs,1,8) >= TO_CHAR(DATEADD(CURRENT_DATE(),-2,'dd'),'yyyymmdd')
  and nvl(period,'day') ='day'
  and t3.crawl_object = 'product_details'
group by   t1.marketplace_id
        , t2.parent_asin
        , t1.asin
        ,t2.asin
        ,t3.keyword
;


select * from adm_strategy_main_info_ds
where ds = '20240509'
;
