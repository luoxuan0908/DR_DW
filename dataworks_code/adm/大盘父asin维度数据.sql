--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-08 19:35:01
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.adm_parent_asin_adv_info_ds
(     id	string	 comment '主键'
    ,tenant_id	string	 comment '租户ID'
    ,tenant_name	string	 comment '租户名称'
    ,market_place_id	string	 comment '市场ID'
    ,market_place_name	string	 comment '市场名称'
    ,adv_manager_id	string	 comment '广告负责人ID'
    ,adv_manager_name	string	 comment '广告负责人'
    ,adv_department_list_id	string	 comment '广告部门ID'
    ,adv_department_list_name	string	 comment '广告部门列表'
    ,seller_id	string	 comment '卖家ID'
    ,seller_name	string	 comment '卖家名称'
    ,profile_id	string	 comment '配置ID'
    ,parent_asin	string	 comment '父asin'
    ,category_list_id	string	 comment '商品类目ID'
    ,category_list_name	string	 comment '商品类目名称'
    ,currency_code	string	 comment '币种'
    ,impressions	bigint	 comment '曝光量'
    ,clicks	bigint	 comment '点击量'
    ,cost	decimal(18,6)	 comment '花费'
    ,sale_amt	decimal(18,6)	 comment '销售额'
    ,order_num	bigint	 comment '销量'
    ,ctr	decimal(18,6)	 comment 'CTR'
    ,cvr	decimal(18,6)	 comment 'CVR'
    ,cpc	decimal(18,6)	 comment 'CPC'
    ,acos	decimal(18,6)	 comment 'ACOS'
    ,compare_category_list	string	 comment '对比类目list'
    ,compare_cate_impressions	bigint	 comment '类目曝光量'
    ,compare_cate_clicks	bigint	 comment '类目点击量'
    ,compare_cate_cost	decimal(18,6)	 comment '类目花费'
    ,compare_cate_sale_amt	decimal(18,6)	 comment '类目销售额'
    ,compare_cate_order_num	bigint	 comment '类目销量'
    ,compare_cate_ctr	decimal(18,6)	 comment '类目CTR'
    ,compare_cate_cvr	decimal(18,6)	 comment '类目CVR'
    ,compare_cate_cpc	decimal(18,6)	 comment '类目CPC'
    ,compare_cate_acos	decimal(18,6)	 comment '类目ACOS'
    ,data_dt	string	 comment '数据统计日期'
    ,etl_data_dt	datetime	 comment '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊原始订单表子表')
    LIFECYCLE 365;
;




drop table if EXISTS adm_parent_asin_adv_info_ds_tmp1;
create table adm_parent_asin_adv_info_ds_tmp1 as
select  a.tenant_id
     ,b.tenant_name
     ,a.market_place_id
     ,c.country_cn_name market_place_name
     ,b.adv_manager_id
     ,b.adv_manager_name
     ,b.adv_department_list_id
     ,b.adv_department_list_name
     ,a.seller_id
     ,a.profile_id
     ,COALESCE(d.parent_asin,a.advertised_asin)  parent_asin
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

             left outer join whde.dim_marketplace_info_df c
                             on a.market_place_id =c.market_place_id
             left outer join (select * from  (select *,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                              from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                              where rn =1
)d
                             on a.advertised_asin = d.asin
                                 and a.market_place_id = d.market_place_id
             left outer join (select * from  dim_user_permission_info_df ) b
                             on a.tenant_id = b.tenant_id
                                 and d.parent_asin = b.parent_asin
                                 and a.market_place_id =b.market_place_id
group by a.tenant_id
       ,b.tenant_name
       ,a.market_place_id
       ,c.country_cn_name
       ,b.adv_manager_id
       ,b.adv_manager_name
       ,b.adv_department_list_id
       ,b.adv_department_list_name
       ,a.seller_id
       ,a.profile_id
       ,COALESCE(d.parent_asin,a.advertised_asin)
       ,campaign_budget_currency_code
       ,campaign_id
       ,campaign_name
       ,a.ds
;


drop table if EXISTS adm_parent_asin_adv_info_ds_tmp2;
CREATE TABLE adm_parent_asin_adv_info_ds_tmp2 AS
SELECT  a.marketplace_id
     ,a.seller_id
     ,a.currency
     ,a.purchase_time
     ,COALESCE(b.parent_asin,c.parent_asin) parent_asin
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
            LEFT OUTER JOIN (select * from  (select *,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                             from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                             where rn =1
) b
                            ON      a.asin = b.asin
                                and a.marketplace_id = b.market_place_id
            LEFT OUTER JOIN (
    SELECT  DISTINCT market_place_id
                   ,parent_asin
    FROM    whde.dwd_amzn_asin_to_parent_df
    WHERE   ds =  '${bizdate}'
) c
                            ON      a.asin = c.parent_asin
                                and a.marketplace_id = c.market_place_id
GROUP BY a.marketplace_id
       ,a.seller_id
       ,a.purchase_time
       ,COALESCE(b.parent_asin,c.parent_asin)
       ,a.currency
;

--select *from adm_parent_asin_adv_info_ds_tmp2;

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
     ,d.sold_by seller_name
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
         ,max(breadcrumbs_feature) breadcrumbs_feature
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
         left outer join (
    SELECT  distinct seller_id --卖家ID
                   ,sold_by
    FROM    whde.amazon_product_details
    WHERE   pt = MAX_PT("whde.amazon_product_details")
      and sold_by is not null
) d --父aisn、以及类目
                         on      a.seller_id = d.seller_id
;

--select * from adm_parent_asin_adv_info_ds_tmp3


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



--insert OVERWRITE table adm_parent_asin_adv_info_ds PARTITION (ds = '${bizdate}')

select* from adm_parent_asin_adv_info_ds_tmp;

drop table if EXISTS  adm_parent_asin_adv_info_ds_tmp ;
create table adm_parent_asin_adv_info_ds_tmp as
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
     ,a.impressions
     ,a.clicks
     ,a.cost
     ,a.sale_amt_adv
     ,a.sale_amt_total
     ,a.sale_amt_natural
     ,cast(a.sale_num_adv as BIGINT) sale_num_adv
     ,cast(a.sale_num_total as BIGINT) sale_num_total
     ,cast(a.sale_num_natural as BIGINT) sale_num_natural
     ,cast(a.order_num_adv as BIGINT) order_num_adv
     ,cast(a.order_num_total as BIGINT) order_num_total
     ,a.CTR
     ,a.CVR
     ,a.CPC
     ,a.acos
     ,a.tacos
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
;

drop table if EXISTS  adm_compain_asin_adv_info_ds_tmp ;
create table adm_compain_asin_adv_info_ds_tmp as
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
;



drop table if EXISTS adm_parent_asin_adv_info_ds_tmp;
create table adm_parent_asin_adv_info_ds_tmp as
SELECT   md5(concat(tenant_id,profile_id,advertised_asin)) id
     ,tenant_id
     ,'实验租户' tenant_name
     ,market_place_id
     ,case when market_place_id = 'APJ6JRA9NG5V4' then '意大利'
           when market_place_id = 'A1RKKUPIHCS9HS' then '西班牙'
           when market_place_id = 'A1PA6795UKMFR9' then '德国'
           when market_place_id = 'A13V1IB3VIYZZH' then '法国'
           else null end  market_place_name
     ,'10000001'adv_manager_id
     ,'追风' adv_manager_name
     ,'12345>678>321 'adv_department_list_id
     ,'某某公司>广告部>营销一组' adv_department_list_name
     ,seller_id
     ,case when market_place_id = 'APJ6JRA9NG5V4' then '意大利店铺'
           when market_place_id = 'A1RKKUPIHCS9HS' then '西班牙店铺'
           when market_place_id = 'A1PA6795UKMFR9' then '德国店铺'
           when market_place_id = 'A13V1IB3VIYZZH' then '法国店铺'
           else null end seller_name
     ,profile_id
     ,advertised_asin parent_asin
     ,'1>2>3>4>5' category_list_id
     ,'Home & Kitchen>Kitchen & Dining>Storage & Organization>Thermoses>Insulated Beverage Containers>Thermoses' category_list_name
     ,campaign_budget_currency_code
     ,sum(impressions)  impressions
     ,sum(clicks) clicks
     ,sum(cost) cost
     ,round(sum(sales_7d) ,4) sale_amt_adv
     ,round(sum(sales_30d) ,4) sale_amt_total
     ,round(sum(sales_30d-sales_7d) ,4) sale_amt_natural
     ,round(sum(units_sold_clicks_7d) ,4) order_num
     ,round(case when sum(impressions) >0 then sum(clicks)/sum(impressions) else null end  ,4) CTR
     ,round(case when sum(clicks) >0 then sum(units_sold_clicks_7d)/sum(clicks) else null end ,4) CVR
     ,round(case when sum(clicks) >0 then sum(cost)/sum(clicks) else null end ,4) CPC
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_7d) else null end ,4) acos
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_30d) else null end ,4) tacos
     ,'Home & Kitchen>Kitchen & Dining>Storage & Organization>Thermoses>Insulated Beverage Containers' compare_category_list
     ,sum(impressions)*2 compare_cate_impressions
     ,sum(clicks)*2 compare_cate_clicks
     ,sum(cost)*2 compare_cate_cost
     ,round(sum(sales_7d) ,4) compare_cate_sale_amt_adv
     ,round(sum(sales_30d) ,4) compare_cate_sale_amt_total
     ,round(sum(sales_30d-sales_7d) ,4) compare_cate_sale_amt_natural
     ,round(sum(units_sold_clicks_7d) ,4) compare_cate_order_num
     ,round(case when sum(impressions) >0 then sum(clicks)/sum(impressions) else null end  ,4) compare_cate_CTR
     ,round(case when sum(clicks) >0 then sum(units_sold_clicks_7d)/sum(clicks) else null end ,4) compare_cate_CVR
     ,round(case when sum(clicks) >0 then sum(cost)/sum(clicks) else null end ,4) compare_cate_CPC
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_7d) else null end ,4) compare_cate_acos
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_30d) else null end ,4) compare_cate_tacos
     ,ds
     ,GETDATE()  etl_data_dt
FROM     whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds  a
WHERE    ds >= '20240201' --'${bizdate}'
group by   md5(concat(tenant_id,profile_id,advertised_asin))
       ,tenant_id
       ,'实验租户'
       ,market_place_id
       ,case when market_place_id = 'APJ6JRA9NG5V4' then '意大利站'
             when market_place_id = 'A1RKKUPIHCS9HS' then '西班牙'
             when market_place_id = 'A1PA6795UKMFR9' then '德国'
             when market_place_id = 'A13V1IB3VIYZZH' then '法国'
             else null end
       ,'10000001'
       ,'追风'
       ,'12345>678>321 '
       ,'某某公司>广告部>营销一组'
       ,seller_id
       ,profile_id
       ,advertised_asin
       ,'1>2>3>4>5'
       ,'Home & Kitchen>Kitchen & Dining>Storage & Organization>Thermoses>Insulated Beverage Containers>Thermoses'
       ,campaign_budget_currency_code
       ,ds
;



drop table if EXISTS  whde.adm_compain_asin_adv_info_ds ;
CREATE TABLE IF NOT EXISTS whde.adm_compain_asin_adv_info_ds
(     id	string	 comment '主键'
    ,tenant_id	string	 comment '租户ID'
    ,tenant_name	string	 comment '租户名称'
    ,market_place_id	string	 comment '市场ID'
    ,market_place_name	string	 comment '市场名称'
    ,adv_manager_id	string	 comment '广告负责人ID'
    ,adv_manager_name	string	 comment '广告负责人'
    ,adv_department_list_id	string	 comment '广告部门ID'
    ,adv_department_list_name	string	 comment '广告部门列表'
    ,seller_id	string	 comment '卖家ID'
    ,seller_name	string	 comment '卖家名称'
    ,profile_id	string	 comment '配置ID'
    ,parent_asin	string	 comment '父asin'
    ,category_list_id	string	 comment '商品类目ID'
    ,category_list_name	string	 comment '商品类目名称'
    ,compain_id	string	comment '广告活动ID'
    ,compain_name	string	comment '广告活动名称'
    ,currency_code	string	 comment '币种'
    ,impressions	bigint	 comment '曝光量'
    ,clicks	bigint	 comment '点击量'
    ,cost	decimal(18,6)	 comment '花费'
    ,sale_amt	decimal(18,6)	 comment '销售额'
    ,order_num	bigint	 comment '销量'
    ,ctr	decimal(18,6)	 comment 'CTR'
    ,cvr	decimal(18,6)	 comment 'CVR'
    ,cpc	decimal(18,6)	 comment 'CPC'
    ,acos	decimal(18,6)	 comment 'ACOS'
    ,compare_category_list	string	 comment '对比类目list'
    ,compare_cate_impressions	bigint	 comment '类目曝光量'
    ,compare_cate_clicks	bigint	 comment '类目点击量'
    ,compare_cate_cost	decimal(18,6)	 comment '类目花费'
    ,compare_cate_sale_amt	decimal(18,6)	 comment '类目销售额'
    ,compare_cate_order_num	bigint	 comment '类目销量'
    ,compare_cate_ctr	decimal(18,6)	 comment '类目CTR'
    ,compare_cate_cvr	decimal(18,6)	 comment '类目CVR'
    ,compare_cate_cpc	decimal(18,6)	 comment '类目CPC'
    ,compare_cate_acos	decimal(18,6)	 comment '类目ACOS'
    ,data_dt	string	 comment '数据统计日期'
    ,etl_data_dt	datetime	 comment '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊原始订单表子表')
    LIFECYCLE 365;

insert OVERWRITE table adm_compain_asin_adv_info_ds PARTITION (ds = '${bizdate}')


drop table if EXISTS adm_compain_asin_adv_info_ds_tmp;
create table adm_compain_asin_adv_info_ds_tmp as
SELECT  md5(concat(tenant_id,profile_id,advertised_asin)) id
     ,tenant_id
     ,'实验租户' tenant_name
     ,market_place_id
     ,case when market_place_id = 'APJ6JRA9NG5V4' then '意大利'
           when market_place_id = 'A1RKKUPIHCS9HS' then '西班牙'
           when market_place_id = 'A1PA6795UKMFR9' then '德国'
           when market_place_id = 'A13V1IB3VIYZZH' then '法国'
           else null end  market_place_name
     ,'10000001'adv_manager_id
     ,'追风' adv_manager_name
     ,'12345>678>321 'adv_department_list_id
     ,'某某公司>广告部>营销一组' adv_department_list_name
     ,seller_id
     ,case when market_place_id = 'APJ6JRA9NG5V4' then '意大利店铺'
           when market_place_id = 'A1RKKUPIHCS9HS' then '西班牙店铺'
           when market_place_id = 'A1PA6795UKMFR9' then '德国店铺'
           when market_place_id = 'A13V1IB3VIYZZH' then '法国店铺'
           else null end seller_name
     ,profile_id
     ,advertised_asin
     ,'1>2>3>4>5' category_list_id
     ,'Home & Kitchen>Kitchen & Dining>Storage & Organization>Thermoses>Insulated Beverage Containers>Thermoses' category_list_name
     ,campaign_id
     ,campaign_name
     ,campaign_budget_currency_code
     ,sum(impressions)  impressions
     ,sum(clicks) clicks
     ,sum(cost) cost
     ,round(sum(sales_7d) ,4) sale_amt_adv
     ,round(sum(sales_30d) ,4) sale_amt_total
     ,round(sum(sales_30d-sales_7d) ,4) sale_amt_natural
     ,round(sum(units_sold_clicks_7d) ,4) order_num
     ,round(case when sum(impressions) >0 then sum(clicks)/sum(impressions) else null end  ,4) CTR
     ,round(case when sum(clicks) >0 then sum(units_sold_clicks_7d)/sum(clicks) else null end ,4) CVR
     ,round(case when sum(clicks) >0 then sum(cost)/sum(clicks) else null end ,4) CPC
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_7d) else null end ,4) acos
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_30d) else null end ,4) tacos
     ,'Home & Kitchen>Kitchen & Dining>Storage & Organization>Thermoses>Insulated Beverage Containers' compare_category_list
     ,sum(impressions)*2 compare_cate_impressions
     ,sum(clicks)*2 compare_cate_clicks
     ,sum(cost)*2 compare_cate_cost
     ,round(sum(sales_7d) ,4) compare_cate_sale_amt_adv
     ,round(sum(sales_30d) ,4) compare_cate_sale_amt_total
     ,round(sum(sales_30d-sales_7d) ,4) compare_cate_sale_amt_natural
     ,round(sum(units_sold_clicks_7d) ,4) compare_cate_order_num
     ,round(case when sum(impressions) >0 then sum(clicks)/sum(impressions) else null end  ,4) compare_cate_CTR
     ,round(case when sum(clicks) >0 then sum(units_sold_clicks_7d)/sum(clicks) else null end ,4) compare_cate_CVR
     ,round(case when sum(clicks) >0 then sum(cost)/sum(clicks) else null end ,4) compare_cate_CPC
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_7d) else null end ,4) compare_cate_acos
     ,round(case when sum(sales_7d)>0 then sum(cost)/sum(sales_30d) else null end ,4) compare_cate_tacos
     ,ds
     ,GETDATE()  etl_data_dt
FROM     whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds  a
WHERE    ds >= '20240201' --'${bizdate}'
group by   md5(concat(tenant_id,profile_id,advertised_asin))
       ,tenant_id
       ,'实验租户'
       ,market_place_id
       ,case when market_place_id = 'APJ6JRA9NG5V4' then '意大利站'
             when market_place_id = 'A1RKKUPIHCS9HS' then '西班牙'
             when market_place_id = 'A1PA6795UKMFR9' then '德国'
             when market_place_id = 'A13V1IB3VIYZZH' then '法国'
             else null end
       ,'10000001'
       ,'追风'
       ,'12345>678>321 '
       ,'某某公司>广告部>营销一组'
       ,seller_id
       ,profile_id
       ,advertised_asin
       ,'1>2>3>4>5'
       ,'Home & Kitchen>Kitchen & Dining>Storage & Organization>Thermoses>Insulated Beverage Containers>Thermoses'
       ,campaign_id
       ,campaign_name
       ,campaign_budget_currency_code
       ,ds
;