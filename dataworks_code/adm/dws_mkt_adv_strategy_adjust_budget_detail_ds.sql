--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:11:01
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dws_mkt_adv_strategy_adjust_budget_detail_ds(
                                                                                tenant_id STRING COMMENT '租户ID',
                                                                                row_id STRING COMMENT '行级明细ID',
                                                                                strategy_id STRING COMMENT '策略ID',
                                                                                profile_id STRING COMMENT '配置ID',
                                                                                marketplace_id STRING COMMENT '市场ID',
                                                                                marketplace_name STRING COMMENT '市场名称',
                                                                                currency_code STRING COMMENT '币种',
                                                                                seller_id STRING COMMENT '卖家ID',
                                                                                seller_name STRING COMMENT '卖家名称(亚马逊上的店铺名称)',
                                                                                ad_type STRING COMMENT '广告类型',
                                                                                campaign_id STRING COMMENT '广告活动ID',
                                                                                campaign_name STRING COMMENT '广告活动名称',
                                                                                camp_budget_amt DECIMAL(18,6) COMMENT '广告活动预算',
    camp_budget_amt_new DECIMAL(18,6) COMMENT '调整后广告活动预算',
    adv_manager_id STRING COMMENT '广告负责人ID',
    adv_manager_name STRING COMMENT '广告负责人名称',
    selling_price STRING COMMENT '售价',
    main_asin_url STRING COMMENT '商品链接',
    main_img_url STRING COMMENT '商品主图',
    camp_stocklack_rate DECIMAL(18,6) COMMENT '库存不足父ASIN花费占比',
    adv_days BIGINT COMMENT '广告指标统计天数',
    top_parent_asin STRING COMMENT '父ASIN',
    stock_sale_days BIGINT COMMENT '父ASIN库存可售天数',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '广告花费',
    sale_amt DECIMAL(18,6) COMMENT '广告销售额',
    order_num BIGINT COMMENT '广告销量',
    ctr DECIMAL(18,6) COMMENT 'CTR',
    cvr DECIMAL(18,6) COMMENT 'CVR',
    cpc DECIMAL(18,6) COMMENT 'CPC',
    cpa DECIMAL(18,6) COMMENT 'CPA',
    acos DECIMAL(18,6) COMMENT 'ACOS',
    life_cycle_label STRING COMMENT '生命周期',
    goal_label STRING COMMENT '目标标签',
    term_type_label STRING COMMENT '操作对象标签',
    ad_mode_label STRING COMMENT '投放类型标签',
    stock_label STRING COMMENT '库存标签',
    action_type STRING COMMENT '操作类型',
    create_time DATETIME COMMENT '创建时间',
    n7_avg_cost DECIMAL(18,6) COMMENT '近7天平均广告花费'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告策略调整预算(子表)')
    LIFECYCLE 366;

--基础数据
drop table if exists dws_mkt_adv_strategy_adjust_budget_detail_tmp01;
create table dws_mkt_adv_strategy_adjust_budget_detail_tmp01 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,ad_type
     ,campaign_id
     ,campaign_name
     ,ad_mode
     ,campaign_budget_amt
     ,ad_group_id_list
     ,ad_group_num
     ,parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,impressions
     ,clicks
     ,cost
     ,sale_amt
     ,sale_num
     ,ctr
     ,cvr
     ,cpc
     ,acos
     ,adv_days
from    whde.adm_amazon_adv_strategy_campaign_d
where   ds = '${bizdate}'
  and     adv_days >= 14
  and     nvl(parent_asin,'') <> ''
;


--建议对象筛选
drop table if exists dws_mkt_adv_strategy_adjust_budget_detail_tmp02;
create table dws_mkt_adv_strategy_adjust_budget_detail_tmp02 as
select  tenant_id
     ,profile_id
     ,marketplace_id
     ,seller_id
     ,adv_manager_id
     ,adv_manager_name
     ,top_parent_asin
     ,fba_first_instock_time
     ,fba_first_instock_days
     ,life_cycle_label
     ,stock_sale_days
     ,stock_label
from    whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = '${bizdate}' and life_cycle_label = '成熟期'
group by tenant_id
       ,profile_id
       ,marketplace_id
       ,seller_id
       ,adv_manager_id
       ,adv_manager_name
       ,top_parent_asin
       ,fba_first_instock_time
       ,fba_first_instock_days
       ,life_cycle_label
       ,stock_sale_days
       ,stock_label
;


--只有一个父ASIN则该父ASIN库存不足则下调，多个父ASIN若所有库存不足的父体的花费占比加起来大于60%就下调
drop table if exists dws_mkt_adv_strategy_adjust_budget_detail_tmp04;
create table dws_mkt_adv_strategy_adjust_budget_detail_tmp04 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.campaign_id
     ,q1.parent_asin
     ,q2.stock_label
     ,q1.cost_rate
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q2.stock_sale_days
from (
         select tenant_id
              ,profile_id
              ,marketplace_id
              ,seller_id
              ,campaign_id
              ,parent_asin
              ,case when sum(cost) over (partition by tenant_id,profile_id,campaign_id) = 0 then null else cost / sum(cost) over (partition by tenant_id,profile_id,campaign_id) end as cost_rate
         from dws_mkt_adv_strategy_adjust_budget_detail_tmp01
     )q1
         inner join dws_mkt_adv_strategy_adjust_budget_detail_tmp02 q2
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin
;


--近7天日均广告花费
drop table if exists dws_mkt_adv_strategy_adjust_budget_detail_tmp04_1;
create table dws_mkt_adv_strategy_adjust_budget_detail_tmp04_1 as
select tenant_id
     ,profile_id
     ,campaign_id
     ,cast(avg(cost) as decimal(18,6)) as n7_avg_cost
from (
         select   tenant_id
              ,profile_id
              ,campaign_id
              ,ds
              ,sum(cost) as cost
         from    whde.dwd_mkt_adv_amazon_sp_product_ds
         where   ds > to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd'),'yyyymmdd')
         group by tenant_id
                ,profile_id
                ,campaign_id
                ,ds
     )q1
group by  tenant_id
       ,profile_id
       ,campaign_id
;


--标签整合
drop table if exists dws_mkt_adv_strategy_adjust_budget_detail_tmp05;
create table dws_mkt_adv_strategy_adjust_budget_detail_tmp05 as
select   q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_mode
     ,q1.campaign_budget_amt
     ,cast(q1.campaign_budget_amt * 0.75 as decimal(18,6)) as campaign_budget_amt_new
     ,q1.ad_group_id_list
     ,q3.adv_manager_id
     ,q3.adv_manager_name
     ,q1.parent_asin as top_parent_asin
     ,q3.stock_sale_days
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url as main_img_url
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.sale_num as order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,case when q1.sale_num = 0 then null else q1.cost / q1.sale_num end as cpa
     ,q1.acos
     ,q1.adv_days
     ,q3.cost_rate as parent_asin_cost_rate
     ,q2.cost_rate as stock_lack_cost_rate
     ,'成熟期' as life_cycle_label
     ,'利润最大化' as goal_label
     ,'广告活动' as term_type_label
     ,'ALL' as ad_mode_label
     ,q3.stock_label
     ,q4.n7_avg_cost
from dws_mkt_adv_strategy_adjust_budget_detail_tmp01 q1
         inner join (
    select tenant_id
         ,profile_id
         ,campaign_id
         ,stock_label
         ,sum(cost_rate) as cost_rate
    from dws_mkt_adv_strategy_adjust_budget_detail_tmp04
    where stock_label = '库存不足'   --库存不足的父ASIN的花费占比统计
    group by tenant_id
           ,profile_id
           ,campaign_id
           ,stock_label
    having cost_rate > 0.6
) q2  --库存不足父ASIN花费占比超过0.6的广告活动
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id
         inner join dws_mkt_adv_strategy_adjust_budget_detail_tmp04 q3  --父ASIN指标
                    on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id and q1.parent_asin = q3.parent_asin
         inner join dws_mkt_adv_strategy_adjust_budget_detail_tmp04_1 q4 --近7天日均广告花费
                    on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and q1.campaign_id = q4.campaign_id
;





--支持重跑
alter table dws_mkt_adv_strategy_adjust_budget_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_adjust_budget_detail_ds partition (ds = '${nowdate}')
(
    tenant_id
   ,row_id
   ,strategy_id
   ,profile_id
   ,marketplace_id
   ,marketplace_name
   ,currency_code
   ,seller_id
   ,seller_name
   ,ad_type
   ,campaign_id
   ,campaign_name
   ,camp_budget_amt
   ,camp_budget_amt_new
   ,adv_manager_id
   ,adv_manager_name
   ,selling_price
   ,main_asin_url
   ,main_img_url
   ,camp_stocklack_rate
   ,top_parent_asin
   ,stock_sale_days
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
   ,term_type_label
   ,ad_mode_label
   ,stock_label
   ,action_type
   ,create_time
   ,n7_avg_cost
)
select   q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.top_parent_asin,q2.strategy_id)) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q1.campaign_id
     ,q1.campaign_name
     ,cast(q1.campaign_budget_amt as decimal(18,6)) as campaign_budget_amt
     ,cast(q1.campaign_budget_amt_new as decimal(18,6)) as campaign_budget_amt_new
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
     ,cast(q1.stock_lack_cost_rate as decimal(18,6)) as stock_lack_cost_rate
     ,q1.top_parent_asin
     ,cast(q1.stock_sale_days as bigint) as stock_sale_days
     ,q1.adv_days
     ,sum(q1.impressions) as impressions
     ,sum(q1.clicks) as clicks
     ,cast(sum(q1.cost) as decimal(18,6)) as cost
     ,cast(sum(q1.sale_amt) as decimal(18,6)) as sale_amt
     ,sum(q1.order_num) as order_num
     ,cast(case when sum(q1.impressions) = 0 then null else sum(clicks)/sum(impressions) end as decimal(18,6)) as ctr
     ,cast(case when sum(q1.clicks) = 0 then null else sum(order_num)/sum(clicks) end as decimal(18,6)) as cvr
     ,cast(case when sum(q1.clicks) = 0 then null else sum(cost)/sum(clicks) end as decimal(18,6)) as cpc
     ,cast(case when sum(q1.order_num) = 0 then null else sum(cost)/sum(order_num) end as decimal(18,6)) as cpa
     ,cast(case when sum(q1.sale_amt) = 0 then null else sum(cost)/sum(sale_amt) end as decimal(18,6)) as acos
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.ad_mode_label
     ,q1.stock_label
     ,q2.action_type
     ,getdate() as create_time
     ,q1.n7_avg_cost
from dws_mkt_adv_strategy_adjust_budget_detail_tmp05 q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on  q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and nvl(q1.stock_label,' ') = nvl(q2.stock_label ,' ')
                        and q1.term_type_label = q2.term_type_label
                        and q1.ad_mode_label = q2.ad_mode_label
where q2.action_type = 'ADJ_CAMP_BUDGET' and q2.life_cycle_label = '成熟期'
group by q1.tenant_id
       ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.top_parent_asin,q2.strategy_id))
       ,q2.strategy_id
       ,q1.profile_id
       ,q1.marketplace_id
       ,q1.marketplace_name
       ,q1.currency_code
       ,q1.seller_id
       ,q1.seller_name
       ,q1.ad_type
       ,q1.campaign_id
       ,q1.campaign_name
       ,cast(q1.campaign_budget_amt as decimal(18,6))
       ,cast(q1.campaign_budget_amt_new as decimal(18,6))
       ,q1.ad_group_id_list
       ,q1.adv_manager_id
       ,q1.adv_manager_name
       ,q1.selling_price
       ,q1.main_asin_url
       ,q1.main_img_url
       ,q1.top_parent_asin
       ,cast(q1.stock_sale_days as bigint)
       ,q1.adv_days
       ,cast(q1.stock_lack_cost_rate as decimal(18,6))
       ,q1.life_cycle_label
       ,q1.goal_label
       ,q1.term_type_label
       ,q1.ad_mode_label
       ,q1.stock_label
       ,q2.action_type
       ,q1.n7_avg_cost
;

