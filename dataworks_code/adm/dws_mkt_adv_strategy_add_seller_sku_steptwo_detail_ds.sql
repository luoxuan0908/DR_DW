--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:08:00
--********************************************************************--

drop table if EXISTS dws_mkt_adv_strategy_add_seller_sku_steptwo_detail_ds;
CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_add_seller_sku_steptwo_detail_ds
(
    tenant_id             STRING COMMENT '租户ID'
    ,row_id                STRING COMMENT '行级策略明细ID'
    ,strategy_id           STRING COMMENT '策略ID'
    ,profile_id            STRING COMMENT '配置ID'
    ,marketplace_id        STRING COMMENT '市场ID'
    ,marketplace_name      STRING COMMENT '市场名称'
    ,currency_code         STRING COMMENT '币种'
    ,ad_type               STRING COMMENT '广告类型'
    ,seller_id             STRING COMMENT '卖家ID'
    ,seller_name           STRING COMMENT '卖家名称(亚马逊上的店铺名称)'
    ,adv_manager_id        STRING COMMENT '广告负责人ID'
    ,adv_manager_name      STRING COMMENT '广告负责人名称'
    ,campaign_id           STRING COMMENT '广告活动ID'
    ,campaign_name         STRING COMMENT '广告活动名称'
    ,ad_group_id           STRING COMMENT '广告组ID'
    ,ad_group_name         STRING COMMENT '广告组名称'
    ,camp_budget_amt   decimal(18,6) COMMENT '广告活动预算'
    ,sku_id                STRING COMMENT 'sku_id'
    ,seller_sku            STRING COMMENT '推广sku'
    ,top_parent_asin       STRING COMMENT '父aisn'
    ,target_auto_list      STRING COMMENT '自动投放列表'
    ,target_keyword_list   STRING COMMENT '投放关键词列表'
    ,target_cat_list       STRING COMMENT '投放类目列表'
    ,target_asin_list      STRING COMMENT '投放品列表'
    ,adv_days              BIGINT COMMENT '广告天数'
    ,impressions           BIGINT COMMENT '曝光量'
    ,clicks                BIGINT COMMENT '点击量'
    ,cost                  DECIMAL(18,6) COMMENT '花费'
    ,sale_amt              DECIMAL(18,6) COMMENT '销售额'
    ,order_num             BIGINT COMMENT '销量'
    ,ctr                   DECIMAL(18,6) COMMENT 'CTR'
    ,cvr                   DECIMAL(18,6) COMMENT 'CVR'
    ,cpc                   DECIMAL(18,6) COMMENT 'CPC'
    ,cpa                   DECIMAL(18,6) COMMENT 'CPA'
    ,acos                  DECIMAL(18,6) COMMENT 'ACOS'
    ,life_cycle_label      STRING COMMENT '生命周期'
    ,goal_label            STRING COMMENT '目标标签'
    ,ad_mode_label         STRING COMMENT '广告投放类型标签'
    ,term_type_label       STRING COMMENT '操作对象类型'
    ,acos_label            STRING COMMENT 'acos标签'
    ,click_label           STRING COMMENT '点击标签'
    ,cvr_label             STRING COMMENT '转化标签'
    ,action_type           STRING COMMENT '操作类型'
    ,create_time           DATETIME COMMENT '创建时间'
    ,seller_sku_img_url STRING comment '推广品图片'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略添加推广品(广告组子表)')
    LIFECYCLE 366
;



-- alter table dws_mkt_adv_strategy_add_seller_sku_steptwo_detail_ds add columns (seller_sku_img_url STRING comment '推广品图片');


--广告组清单
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_1;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_1 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,campaign_budget_currency_code as currency_code
     ,seller_id
     ,seller_name
     ,ad_mode
     ,advertised_asin as seller_asin
     ,advertised_sku  as seller_sku
     ,top_cost_parent_asin
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,sum(impressions) as impressions
     ,sum(clicks) as clicks
     ,sum(cost) as cost
     ,sum(sale_amt) as sale_amt
     ,sum(order_num) as order_num
     ,datediff(to_date('${bizdate}','yyyymmdd'),min(report_date),'dd') + 1 as adv_days
from whde.adm_amazon_adv_sku_wide_d
where ds = '${bizdate}'
group by tenant_id
       ,profile_id
       ,marketplace_id
       ,marketplace_name
       ,campaign_budget_currency_code
       ,seller_id
       ,seller_name
       ,ad_mode
       ,advertised_asin
       ,advertised_sku
       ,top_cost_parent_asin
       ,campaign_id
       ,campaign_name
       ,ad_group_id
       ,ad_group_name
;


--投放关键词列表
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_2;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_2 as
select tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,wm_concat(distinct '_&_',keyword_text) as target_keyword_list
from whde.adm_amazon_adv_keyword_target_status_df
where ds = max_pt('whde.adm_amazon_adv_keyword_target_status_df')   and status = 'ENABLED'
group by tenant_id
       ,profile_id
       ,campaign_id
       ,ad_group_id
;

--投放品/投放类目列表
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_3;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_3 as
select tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,wm_concat(distinct '_&_',asin) as target_asin_list
     ,wm_concat(distinct '_&_',category) as target_cat_list
from whde.adm_amazon_adv_product_target_status_df
where ds = max_pt('whde.adm_amazon_adv_product_target_status_df')   and status = 'ENABLED'
group by tenant_id
       ,profile_id
       ,campaign_id
       ,ad_group_id
;


--最新广告活动预算
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_4;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_4 as
SELECT  tenant_id
     ,profile_id
     ,campaign_id
     ,budget as campaign_budget_amt
from whde.adm_amazon_adv_camp_status_df
where ds = max_pt('whde.adm_amazon_adv_camp_status_df')
;


--自动投放表达式列表
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_5;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_5 as
SELECT tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,wm_concat(distinct '_&_',target_text) as target_auto_list
from whde.adm_amazon_adv_strategy_target_d
where ds = max_pt('whde.adm_amazon_adv_strategy_target_d') and ad_mode = '自动投放'
group by tenant_id
       ,profile_id
       ,campaign_id
       ,ad_group_id
;



--广告组基础信息
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp06;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp06 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_mode
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q4.campaign_budget_amt
     ,q1.adv_days
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.cpa
     ,q1.acos
     ,q5.target_auto_list
     ,q2.target_keyword_list
     ,q3.target_cat_list
     ,q3.target_asin_list
from(
        select tenant_id
             ,profile_id
             ,marketplace_id
             ,marketplace_name
             ,currency_code
             ,seller_id
             ,seller_name
             ,ad_mode
             ,campaign_id
             ,campaign_name
             ,ad_group_id
             ,ad_group_name
             ,top_cost_parent_asin
             ,max(adv_days) as adv_days
             ,sum(impressions) as impressions
             ,sum(clicks) as clicks
             ,sum(cost) as cost
             ,sum(sale_amt) as sale_amt
             ,sum(order_num) as order_num
             ,case when sum(impressions) = 0 then null else sum(clicks)/sum(impressions) end as ctr
             ,case when sum(clicks) = 0 then null else sum(order_num)/sum(clicks) end as cvr
             ,case when sum(sale_amt) = 0 then null else sum(cost)/sum(sale_amt) end as acos
             ,case when sum(clicks) = 0 then null else sum(cost)/sum(clicks) end as cpc
             ,case when sum(order_num) = 0 then null else sum(cost)/sum(order_num) end as cpa
        from dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_1
        group by tenant_id
               ,profile_id
               ,marketplace_id
               ,marketplace_name
               ,currency_code
               ,seller_id
               ,seller_name
               ,ad_mode
               ,campaign_id
               ,campaign_name
               ,ad_group_id
               ,ad_group_name
               ,top_cost_parent_asin
    )q1
        left join dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_2 q2
                  on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id and q1.ad_group_id = q2.ad_group_id
        left join dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_3 q3
                  on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id and q1.ad_group_id = q3.ad_group_id
        left join dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_4 q4
                  on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and q1.campaign_id = q4.campaign_id
        left join dws_mkt_adv_strategy_add_seller_sku_detail_tmp05_5 q5
                  on q1.tenant_id = q5.tenant_id and q1.profile_id = q5.profile_id and q1.campaign_id = q5.campaign_id and q1.ad_group_id = q5.ad_group_id
;



--添加推广品的目标广告组
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp07;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp07 as
select q1.tenant_id
     ,q1.profile_id
     ,split(sub_group,'_/_')[0] as campaign_id
     ,split(sub_group,'_/_')[1] as ad_group_id
     ,q1.sku_id
     ,q1.seller_sku
     ,q1.seller_sku_img_url
     ,q1.top_parent_asin
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.acos_label
     ,q1.click_label
     ,q1.cvr_label
from (
         select tenant_id
              ,profile_id
              ,ad_group_id_list
              ,top_parent_asin
              ,adv_manager_id
              ,adv_manager_name
              ,sku_id
              ,seller_sku
              ,seller_sku_img_url
              ,life_cycle_label
              ,goal_label
              ,term_type_label
              ,acos_label
              ,click_label
              ,cvr_label
         from dws_mkt_adv_strategy_add_seller_sku_stepone_detail_ds
         where ds = '${nowdate}'
     )q1
    lateral view explode(split(ad_group_id_list,'_&_')) tmpTable as sub_group
;




drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_final;
create table dws_mkt_adv_strategy_add_seller_sku_detail_final as
select q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.ad_group_id,q2.sku_id,q3.strategy_id)) as row_id
     ,q3.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.campaign_budget_amt
     ,q1.adv_days
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.cpa
     ,q1.acos
     ,q1.target_auto_list
     ,q1.target_keyword_list
     ,q1.target_cat_list
     ,q1.target_asin_list
     ,q2.sku_id
     ,q2.seller_sku
     ,q2.seller_sku_img_url
     ,q2.top_parent_asin
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q2.life_cycle_label
     ,q2.goal_label
     ,q2.term_type_label
     ,q1.ad_mode as ad_mode_label
     ,q2.acos_label
     ,q2.click_label
     ,q2.cvr_label
     ,q3.action_type
     ,getdate() as create_time
from dws_mkt_adv_strategy_add_seller_sku_detail_tmp06 q1
         inner join dws_mkt_adv_strategy_add_seller_sku_detail_tmp07 q2
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id and q1.ad_group_id = q2.ad_group_id
         inner join dws_mkt_adv_strategy_main_all_df q3   --基于母表完成否词否品的标签组合筛选
                    on q2.tenant_id = q3.tenant_id
                        and q2.life_cycle_label = q3.life_cycle_label
                        and q2.goal_label = q3.goal_label
                        and q2.click_label = q3.click_label
                        and q2.cvr_label = q3.cvr_label
                        and q2.term_type_label = q3.term_type_label
                        and q2.acos_label = q3.acos_label
where q3.action_name = '添加推广品' and q2.life_cycle_label = '成熟期'
;




--支持重跑
alter table dws_mkt_adv_strategy_add_seller_sku_steptwo_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_add_seller_sku_steptwo_detail_ds partition (ds = '${nowdate}')
(
 tenant_id
,row_id
,strategy_id
,profile_id
,marketplace_id
,marketplace_name
,currency_code
,ad_type
,seller_id
,seller_name
,adv_manager_id
,adv_manager_name
,campaign_id
,campaign_name
,ad_group_id
,ad_group_name
,camp_budget_amt
,sku_id
,seller_sku
,seller_sku_img_url
,top_parent_asin
,target_auto_list
,target_keyword_list
,target_cat_list
,target_asin_list
,adv_days
,impressions
,clicks
,cost
,sale_amt
,order_num
,ctr
,cvr
,cpc
,cpa
,acos
,life_cycle_label
,goal_label
,ad_mode_label
,term_type_label
,acos_label
,click_label
,cvr_label
,action_type
,create_time
)
select  tenant_id
     ,row_id
     ,strategy_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,'商品推广' as ad_type
     ,seller_id
     ,seller_name
     ,adv_manager_id
     ,adv_manager_name
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,cast(campaign_budget_amt as decimal(18,6)) as camp_budget_amt
     ,sku_id
     ,seller_sku
     ,seller_sku_img_url
     ,top_parent_asin
     ,target_auto_list
     ,target_keyword_list
     ,target_cat_list
     ,target_asin_list
     ,adv_days
     ,impressions
     ,clicks
     ,cast(cost as decimal(18,6)) as cost
     ,cast(sale_amt as decimal(18,6)) as sale_amt
     ,order_num
     ,cast(ctr as decimal(18,6)) as ctr
     ,cast(cvr as decimal(18,6)) as cvr
     ,cast(cpc as decimal(18,6)) as cpc
     ,cast(cpa as decimal(18,6)) as cpa
     ,cast(acos as decimal(18,6)) as acos
     ,life_cycle_label
     ,goal_label
     ,ad_mode_label
     ,term_type_label
     ,acos_label
     ,click_label
     ,cvr_label
     ,action_type
     ,create_time
from dws_mkt_adv_strategy_add_seller_sku_detail_final
where adv_manager_id is not null
;
