--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-21 18:11:48
--********************************************************************--

drop table if EXISTS  adm_compain_asin_adv_info_ds_tmp ;
create table  adm_compain_asin_adv_info_ds_tmp as
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
     ,a.campaign_id
     ,a.campaign_name
     ,a.impressions
     ,a.clicks
     ,a.cost
     ,a.sale_amt_adv
     ,a.sale_num_adv
     ,a.order_num_adv
     ,a.order_num_total
     ,a.CTR
     ,a.CVR
     ,a.CPC
     ,a.acos
     ,SPLIT_PART(a.breadcrumbs_feature,'>',1,4) compare_category
     ,b.impressions as  compare_cate_impressions
     ,b.clicks as compare_cate_clicks
     ,b.cost as compare_cate_cost
     ,b.sale_amt_adv as compare_cate_sale_amt_adv
     ,b.sale_num_adv as compare_cate_sale_num_adv
     ,b.order_num_adv as compare_cate_order_num_adv
     ,b.order_num_total as compare_cate_order_num_total
     ,b.CTR as compare_cate_CTR
     ,b.CVR as compare_cate_CVR
     ,b.CPC as compare_cate_CPC
     ,b.acos as compare_cate_acos
     ,ds
     ,GETDATE()  etl_data_dt
from adm_parent_asin_adv_info_ds_tmp3 a
         left outer join adm_parent_asin_adv_info_ds_tmp4  b
                         on a. market_place_id =b.market_place_id
                             and SPLIT_PART(a.breadcrumbs_feature,'>',1,4) = SPLIT_PART(b.breadcrumbs_feature,'>',1,4)
where parent_asin is not null
;

