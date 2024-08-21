--@exclude_input=whde.dim_tenant_info_df
--@exclude_input=whde.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-21 18:00:59
--********************************************************************--



drop table if EXISTS adm_parent_asin_adv_info_ds_tmp1;
create table adm_parent_asin_adv_info_ds_tmp1 as

select   t1.tenant_id
     ,t1.tenant_name
     ,t1.market_place_id
     ,t1.market_place_name
     ,t2.adv_manager_id
     ,t2.adv_manager_name
     ,t2.adv_department_list_id
     ,t2.adv_department_list_name
     ,t1.seller_id
     ,t3.seller_name
     ,t1.profile_id
     ,t1.parent_asin
     ,campaign_budget_currency_code
     ,campaign_id
     ,campaign_name
     ,impressions
     ,clicks
     ,cost
     ,sale_amt_adv
     ,sale_num_adv
     ,order_num_adv
     ,CTR
     ,CVR
     ,CPC
     ,acos
     ,ds
from (
         select  a.tenant_id
              ,b.tenant_name
              ,a.market_place_id
              ,c.country_cn_name market_place_name
              ,a.seller_id
              ,profile_id
              ,d.parent_asin
              ,campaign_budget_currency_code
              ,campaign_id
              ,campaign_name
              ,sum(impressions)  impressions
              ,sum(clicks) clicks
              ,sum(cost) cost
              ,round(sum(sales_7d) ,4) sale_amt_adv
              ,round(sum(units_sold_clicks_7d) ,4) sale_num_adv
              ,round(sum(purchases_7d) ,4) order_num_adv
              ,round(case when sum(impressions) >0 then sum(clicks)/sum(impressions) else null end  ,4) CTR
              ,round(case when sum(clicks) >0 then sum(units_sold_clicks_7d)/sum(clicks) else null end ,4) CVR
              ,round(case when sum(clicks) >0 then sum(cost)/sum(clicks) else null end ,4) CPC
              ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_7d) else null end ,4) acos
              ,a.ds
         FROM     (select * from whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds WHERE  ds>='20240106')a
                      left outer join (select * from  whde.dim_tenant_info_df where is_enabled = 1) b
                                      on a.tenant_id = b.tenant_id
                      left outer join whde.dim_marketplace_info_df c
                                      on a.market_place_id =c.market_place_id
                      left outer join   (select * from  (select *  ,market_place_id marketplace_id,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                                         from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                                         where rn =1 and parent_asin is not null
         )d
                                        on a.advertised_asin = d.asin
                                            and a.market_place_id = d.market_place_id
         group by a.tenant_id
                ,b.tenant_name
                ,a.market_place_id
                ,c.country_cn_name
                ,a.seller_id
                ,profile_id
                ,d.parent_asin
                ,campaign_budget_currency_code
                ,campaign_id
                ,campaign_name
                ,a.ds ) t1
         left outer join (select * from  dim_user_permission_info_df ) t2
                         on t1.tenant_id = t2.tenant_id
                             and t1.parent_asin = t2.parent_asin
                             and t1.market_place_id =t2.market_place_id
         left outer join (select distinct tenant_id,market_place_id,seller_id,seller_name from  dim_user_permission_info_df ) t3
                         on t1.tenant_id = t3.tenant_id
                             and t1.market_place_id =t3.market_place_id
                             and t1.seller_id = t3.seller_id
;




drop table if EXISTS adm_parent_asin_adv_info_ds_tmp2;
CREATE TABLE adm_parent_asin_adv_info_ds_tmp2 AS
SELECT  a.marketplace_id
     ,a.seller_id
     ,a.currency
     ,a.purchase_time
     ,b.parent_asin
     ,SUM(sale_num_total) sale_num_total
     ,SUM(order_num_total) order_num_total
     ,sum(sale_amt_total) sale_amt_total
FROM    (
            SELECT  marketplace_id
                 ,seller_id
                 ,currency
                 ,asin
                 ,to_char(purchase_time,'yyyymmdd')   purchase_time
                 ,SUM(ordered_num) sale_num_total
                 ,COUNT(DISTINCT amazon_order_id) order_num_total
                 ,sum(ordered_num*item_amt) sale_amt_total
            FROM    whde.dwd_amzn_all_orders_df
            WHERE   ds = MAX_PT("whde.dwd_amzn_all_orders_df")
              AND     order_status <> 'Cancelled'
            GROUP BY marketplace_id
                   ,seller_id
                   ,asin
                   ,currency
                   ,to_char(purchase_time,'yyyymmdd')
        ) a
            LEFT OUTER JOIN (select * from  (select *  ,market_place_id marketplace_id,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                             from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                             where rn =1 and parent_asin is not null
) b
                            ON      a.asin = b.asin
                                and a.marketplace_id = b.market_place_id
GROUP BY a.marketplace_id
       ,a.seller_id
       ,a.purchase_time
       ,b.parent_asin
       ,a.currency
;



drop table if EXISTS adm_parent_asin_adv_info_ds_tmp3;
create table adm_parent_asin_adv_info_ds_tmp3 as
SELECT   md5(concat(a.tenant_id,a.profile_id,a.parent_asin)) id
     ,a.tenant_id
     ,a.tenant_name
     ,a.market_place_id
     ,a.market_place_name
     ,a.adv_manager_id
     ,a.adv_manager_name
     ,a.adv_department_list_id
     ,a.adv_department_list_name
     ,a.seller_id
     ,a.seller_name
     ,a.profile_id
     ,a.parent_asin
     ,'' category_list_id
     ,b.breadcrumbs_feature
     ,a.campaign_budget_currency_code
     ,a.campaign_id
     ,a.campaign_name
     ,a.impressions
     ,a.clicks
     ,a.cost
     ,a.sale_amt_adv
     ,c.sale_amt_total
     ,c.sale_amt_total- round(a.sale_amt_adv/7,2)  sale_amt_natural  --暂时替代口径，真是数据需要下载自然流量表
     ,a.order_num_adv
     ,a.sale_num_adv
     ,c.sale_num_total
     ,c.sale_num_total- round(a.sale_num_adv/7,0) sale_num_natural
     ,c.order_num_total
     ,a.CTR
     ,a.CVR
     ,a.CPC
     ,a.acos
     ,case when c.sale_amt_total >0 then a.cost/c.sale_amt_total else null end tacos
     ,a.ds
from adm_parent_asin_adv_info_ds_tmp1 a
         left outer join (
    SELECT  market_place_id
         ,parent_asin
         ,max(REPLACE(breadcrumbs_feature,'            >               ','>')) breadcrumbs_feature
    FROM    whde.amazon_product_details
    WHERE   pt = MAX_PT("whde.amazon_product_details")
    group by  market_place_id
           ,parent_asin
) b --父aisn、以及类目
                         on      a.parent_asin = b.parent_asin
                             and     a.market_place_id = b.market_place_id
         left outer join adm_parent_asin_adv_info_ds_tmp2 c
                         on      a.market_place_id = c.marketplace_id
                             AND     a.seller_id = c.seller_id
                             AND     a.parent_asin = c.parent_asin
                             and    a.ds = c.purchase_time
;


drop table if EXISTS adm_parent_asin_adv_info_ds_tmp4;
create table adm_parent_asin_adv_info_ds_tmp4 as
SELECT   market_place_id
     ,SPLIT_PART(breadcrumbs_feature,'>',1,4) breadcrumbs_feature
     ,sum(impressions)  impressions
     ,sum(clicks) clicks
     ,sum(cost) cost
     ,round(sum(sale_amt_adv) ,4) sale_amt_adv
     ,round(sum(sale_num_adv) ,4) sale_num_adv
     ,round(sum(order_num_adv) ,4) order_num_adv
     ,round(case when sum(impressions) >0 then sum(clicks)/sum(impressions) else null end  ,4) CTR
     ,round(case when sum(clicks) >0 then sum(sale_num_adv)/sum(clicks) else null end ,4) CVR
     ,round(case when sum(clicks) >0 then sum(cost)/sum(clicks) else null end ,4) CPC
     ,round(case when sum(sale_amt_adv)>0 then sum(cost)/sum(sale_amt_adv) else null end ,4) acos
     ,SUM(sale_num_total) sale_num_total
     ,SUM(order_num_total) order_num_total
     ,sum(sale_amt_total) sale_amt_total
     ,case when sum(sale_amt_total) >0 then round(sum(cost)/sum(sale_amt_total),4) else null end tacos
from adm_parent_asin_adv_info_ds_tmp3
group by market_place_id,SPLIT_PART(breadcrumbs_feature,'>',1,4)
;


--drop table if EXISTS  adm_parent_asin_adv_info_ds_tmp ;
insert OVERWRITE table  adm_parent_asin_adv_info_ds_tmp
SELECT  a.id
     ,a.tenant_id
     ,a.tenant_name
     ,a.market_place_id
     ,a.market_place_name
     ,a.adv_manager_id
     ,a.adv_manager_name
     ,a.adv_department_list_id
     ,a.adv_department_list_name
     ,a.seller_id
     ,a.seller_name
     ,a.profile_id
     ,a.parent_asin
     ,a.category_list_id
     ,a.breadcrumbs_feature
     ,a.campaign_budget_currency_code
     ,sum(a.impressions) impressions
     ,sum(a.clicks) clicks
     ,sum(a.cost) cost
     ,sum(a.sale_amt_adv) sale_amt_adv
     ,sum(a.sale_amt_total) sale_amt_total
     ,sum(a.sale_amt_natural) sale_amt_natural
     ,cast(sum(a.sale_num_adv) as BIGINT) sale_num_adv
     ,cast(sum(a.sale_num_total) as BIGINT) sale_num_total
     ,cast(sum(a.sale_num_natural) as BIGINT) sale_num_natural
     ,cast(sum(a.order_num_adv) as BIGINT) order_num_adv
     ,cast(sum(a.order_num_total) as BIGINT) order_num_total
     ,round(case when sum(a.impressions) >0 then sum(a.clicks)/sum(a.impressions) else null end  ,4) CTR
     ,round(case when sum(a.clicks) >0 then sum(a.sale_num_adv)/sum(a.clicks) else null end ,4) CVR
     ,round(case when sum(a.clicks) >0 then sum(a.cost)/sum(a.clicks) else null end ,4) CPC
     ,round(case when sum(a.sale_amt_adv)>0 then sum(a.cost)/sum(a.sale_amt_adv) else null end ,4) acos
     ,case when sum(a.sale_amt_total) >0 then round(sum(a.cost)/sum(a.sale_amt_total),4) else null end tacos
     ,SPLIT_PART(a.breadcrumbs_feature,'>',1,4) compare_category
     ,b.impressions as  compare_cate_impressions
     ,b.clicks as compare_cate_clicks
     ,b.cost as compare_cate_cost
     ,b.sale_amt_adv as compare_cate_sale_amt_adv
     ,b.sale_amt_total as compare_cate_sale_amt_total
     ,b.sale_amt_total-b.sale_amt_adv  as compare_cate_sale_amt_natural
     ,b.sale_num_adv as compare_cate_sale_num_adv
     ,b.sale_num_total as compare_cate_sale_num_total
     ,b.sale_num_total-b.sale_num_adv as compare_cate_sale_num_natural
     ,b.order_num_adv as compare_cate_order_num_adv
     ,b.order_num_total as compare_cate_order_num_total
     ,b.CTR as compare_cate_CTR
     ,b.CVR as compare_cate_CVR
     ,b.CPC as compare_cate_CPC
     ,b.acos as compare_cate_acos
     ,b.tacos as compare_cate_tacos
     ,ds
     ,GETDATE()  etl_data_dt
from adm_parent_asin_adv_info_ds_tmp3 a
         left outer join adm_parent_asin_adv_info_ds_tmp4  b
                         on a. market_place_id =b.market_place_id
                             and SPLIT_PART(a.breadcrumbs_feature,'>',1,4) = SPLIT_PART(b.breadcrumbs_feature,'>',1,4)
where parent_asin is not null
group by  a.id
       ,a.tenant_id
       ,a.tenant_name
       ,a.market_place_id
       ,a.market_place_name
       ,a.adv_manager_id
       ,a.adv_manager_name
       ,a.adv_department_list_id
       ,a.adv_department_list_name
       ,a.seller_id
       ,a.seller_name
       ,a.profile_id
       ,a.parent_asin
       ,a.category_list_id
       ,a.breadcrumbs_feature
       ,a.campaign_budget_currency_code
       ,SPLIT_PART(a.breadcrumbs_feature,'>',1,4)
       ,b.impressions
       ,b.clicks
       ,b.cost
       ,b.sale_amt_adv
       ,b.sale_amt_total
       ,b.sale_amt_total-b.sale_amt_adv
       ,b.sale_num_adv
       ,b.sale_num_total
       ,b.sale_num_total-b.sale_num_adv
       ,b.order_num_adv
       ,b.order_num_total
       ,b.CTR
       ,b.CVR
       ,b.CPC
       ,b.acos
       ,b.tacos
       ,ds
;


