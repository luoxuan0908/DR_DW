--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:11:28
--********************************************************************--
CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_adjust_budget_raise_detail_ds
(
    tenant_id                         STRING COMMENT '租户ID'
    ,row_id                            STRING COMMENT '行级明细ID'
    ,strategy_id                       STRING COMMENT '策略ID'
    ,profile_id                        STRING COMMENT '配置ID'
    ,marketplace_id                    STRING COMMENT '市场ID'
    ,marketplace_name                  STRING COMMENT '市场名称'
    ,currency_code                     STRING COMMENT '币种'
    ,seller_id                         STRING COMMENT '卖家ID'
    ,seller_name                       STRING COMMENT '卖家名称(亚马逊上的店铺名称)'
    ,ad_type                           STRING COMMENT '广告类型'
    ,campaign_id                       STRING COMMENT '广告活动ID'
    ,campaign_name                     STRING COMMENT '广告活动名称'
    ,camp_budget_amt                   DECIMAL(18,6) COMMENT '广告活动预算'
    ,camp_budget_amt_new               DECIMAL(18,6) COMMENT '调整后广告活动预算'
    ,ad_group_id_list                  STRING COMMENT '包含的广告组列表'
    ,adv_manager_id                    STRING COMMENT '广告负责人ID'
    ,adv_manager_name                  STRING COMMENT '广告负责人名称'
    ,top_parent_asin                   STRING COMMENT '父ASIN'
    ,selling_price                     DECIMAL(18,6) COMMENT '售价'
    ,main_asin_url                     STRING COMMENT '商品链接'
    ,main_img_url                      STRING COMMENT '商品主图'
    ,n1_camp_budget_amt                DECIMAL(18,6) COMMENT 'T-1日广告活动预算'
    ,n1_camp_budget_lack_rate          DECIMAL(18,6) COMMENT 'T-1日预算缺失率'
    ,n7_camp_budget_lack_rate          DECIMAL(18,6) COMMENT '近7天预算缺失率'
    ,n7_max_camp_budget_amt            DECIMAL(18,6) COMMENT '近7天最大广告活动预算'
    ,n7_min_camp_budget_amt            DECIMAL(18,6) COMMENT '近7天最小广告活动预算'
    ,n7_avg_cost                       DECIMAL(18,6) COMMENT '近7天平均广告花费'
    ,n7_best_camp_budget_amt           DECIMAL(18,6) COMMENT '近7天预算缺失率最小当天对应的广告活动预算'
    ,n7_best_camp_budget_lack_rate     DECIMAL(18,6) COMMENT '近7天最小的预算缺失率'
    ,adv_days                          BIGINT COMMENT '广告指标统计天数'
    ,impressions                       BIGINT COMMENT '曝光量'
    ,clicks                            BIGINT COMMENT '点击量'
    ,cost                              DECIMAL(18,6) COMMENT '广告花费'
    ,sale_amt                          DECIMAL(18,6) COMMENT '广告销售额'
    ,order_num                         BIGINT COMMENT '广告销量'
    ,ctr                               DECIMAL(18,6) COMMENT 'CTR'
    ,cvr                               DECIMAL(18,6) COMMENT 'CVR'
    ,cpc                               DECIMAL(18,6) COMMENT 'CPC'
    ,cpa                               DECIMAL(18,6) COMMENT 'CPA'
    ,acos                              DECIMAL(18,6) COMMENT 'ACOS'
    ,category                          STRING COMMENT '父ASIN所属类目'
    ,cate_ctr                          DECIMAL(18,6) COMMENT '父ASIN所属类目平均CTR'
    ,cate_cvr                          DECIMAL(18,6) COMMENT '父ASIN所属类目平均CVR'
    ,cate_cpc                          DECIMAL(18,6) COMMENT '父ASIN所属类目平均CPC'
    ,cate_cpa                          DECIMAL(18,6) COMMENT '父ASIN所属类目平均CPA'
    ,cate_acos                         DECIMAL(18,6) COMMENT '父ASIN所属类目平均ACOS'
    ,life_cycle_label                  STRING COMMENT '生命周期'
    ,goal_label                        STRING COMMENT '目标标签'
    ,term_type_label                   STRING COMMENT '操作对象标签'
    ,ad_mode_label                     STRING COMMENT '投放类型标签'
    ,stock_label                       STRING COMMENT '库存标签'
    ,click_label                       STRING COMMENT '点击标签'
    ,cvr_label                         STRING COMMENT '转化标签'
    ,acos_label                        STRING COMMENT 'acos标签'
    ,budget_label                      STRING COMMENT '预算标签'
    ,action_type                       STRING COMMENT '操作类型'
    ,create_time                       DATETIME COMMENT '创建时间'
    ,season_label string comment '季节标签'
    )
    PARTITIONED BY
(
    ds                 STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略上调预算(子表)')
    LIFECYCLE 366
;


--基础数据
drop table if exists dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp01;
create table dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp01 as
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
     ,CASE WHEN marketplace_id IN ('A1AM78C64UM0Y8','ATVPDKIKX0DER','A39IBJ37TRP1C6','A2EUQ1WTGCTBG2') then replace(replace(selling_price,',',''),'$','')
           WHEN marketplace_id IN ('A1C3SOZRARQ6R3') then replace(replace(selling_price,',','.'),'zł','')
           WHEN marketplace_id IN ('A1805IZSGTT6HS','A13V1IB3VIYZZH','APJ6JRA9NG5V4','A1PA6795UKMFR9','AMEN7PMS3EDWL','A1RKKUPIHCS9HS') then replace(replace(selling_price,',','.'),'€','')
           WHEN marketplace_id IN ('A2Q3Y263D00KWC') AND INSTR(selling_price,'.')>0 then replace(replace(replace(selling_price,'.',''),',','.'),'R$','')
           WHEN marketplace_id IN ('A2Q3Y263D00KWC') AND INSTR(selling_price,'.')=0 then replace(replace(selling_price,',','.'),'R$','')
           WHEN marketplace_id IN ('A33AVAJ2PDY3EV') AND INSTR(selling_price,'.')>0 then replace(replace(replace(selling_price,'.',''),',','.'),'TL','')
           WHEN marketplace_id IN ('A33AVAJ2PDY3EV') AND INSTR(selling_price,'.')=0  then replace(replace(selling_price,',','.'),'TL','')
           WHEN marketplace_id IN ('A1F83G8C2ARO7P') then replace(replace(selling_price,',',''),'£','')
           WHEN marketplace_id IN ('A2NODRKZP88ZB9') then replace(replace(selling_price,',',''),'kr','')
           WHEN marketplace_id IN ('A21TJRUUN4KGV') then replace(replace(selling_price,',',''),'₹','')  end selling_price
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
from whde.adm_amazon_adv_strategy_campaign_d
where ds = '${bizdate}'
;


--建议对象筛选
drop table if exists dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02;
create table dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,seller_id
     ,adv_manager_id
     ,adv_manager_name
     ,top_parent_asin
     ,stock_label
     ,life_cycle_label
     ,category
     ,season_label
from    whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = '${bizdate}'
group by tenant_id
       ,profile_id
       ,marketplace_id
       ,seller_id
       ,adv_manager_id
       ,adv_manager_name
       ,top_parent_asin
       ,stock_label
       ,life_cycle_label
       ,category
       ,season_label
;

--成熟期广告活动粒度类目指标
drop table if exists dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02_1;
create table dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02_1 as
select tenant_id
     ,category
     ,case when sum(impressions) = 0 then null else sum(clicks)/sum(impressions) end as cate_ctr
     ,case when sum(clicks) = 0 then null else sum(order_num)/sum(clicks) end as cate_cvr
     ,case when sum(clicks) = 0 then null else sum(cost)/sum(clicks) end as cate_cpc
     ,case when sum(order_num) = 0 then null else sum(cost)/sum(order_num) end as cate_cpa
     ,case when sum(sale_amt) = 0 then null else sum(cost)/sum(sale_amt) end as cate_acos
from (
         select   q1.tenant_id
              ,q1.profile_id
              ,q1.campaign_id
              ,q1.parent_asin
              ,q1.impressions
              ,q1.clicks
              ,q1.cost
              ,q1.sale_amt
              ,q1.order_num
              ,q2.category
         from (
                  select   tenant_id
                       ,profile_id
                       ,campaign_id
                       ,parent_asin
                       ,sum(impressions) as impressions
                       ,sum(clicks) as clicks
                       ,sum(cost) as cost
                       ,sum(sale_amt) as sale_amt
                       ,sum(sale_num) as order_num
                  from  dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp01
                  group by tenant_id
                         ,profile_id
                         ,campaign_id
                         ,parent_asin
              ) q1
                  inner join dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02 q2
                             on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin and q2.life_cycle_label = '成熟期'
     )s1
group by tenant_id
       ,category
;



--预算缺失指标
drop table if exists dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp03;
create table dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp03 as
select   tenant_id
     ,profile_id
     ,campaign_id
     ,lack_rate as n7_campaign_budget_lack_rate
     ,budget_label
     ,n1_max_budget as n1_campaign_budget
     ,n1_budget_rate as n1_campaign_budget_lack_rate
     ,n7_max_budget as n7_max_campaign_budget
     ,n7_min_budget as n7_min_campaign_budget
     ,n7_avg_cost
     ,n7_best_budget as n7_best_campaign_budget
     ,n7_best_lack_rate as n7_best_campaign_budget_lack_rate
     ,adj_budget as camp_budget_amt_new
from    whde.dws_mkt_adv_camp_budget_lack_rate_ds
where   ds = '${bizdate}' and adj_budget is not null
;


--标签整合
drop table if exists dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp04;
create table dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp04 as
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
     ,q1.campaign_budget_amt as camp_budget_amt
     ,q1.ad_group_id_list
     -- ,q1.ad_group_num
     ,q1.parent_asin as top_parent_asin
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
     ,q2.category
     ,q4.cate_ctr
     ,q4.cate_cvr
     ,q4.cate_cpc
     ,q4.cate_cpa
     ,q4.cate_acos
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q2.life_cycle_label
     ,'利润最大化' as goal_label
     ,'广告活动' as term_type_label
     ,ad_mode as ad_mode_label
     ,q2.stock_label
     ,case when cate_cvr = 0 or cate_cvr is null then null
           when clicks < 1/cate_cvr then '无效点击'
           when  clicks >= 1/cate_cvr then '有效点击' end as click_label
     ,case when sale_num = 0 then '不出单'
           when cvr < cate_cvr * 0.5 then '低转化'
           when cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
           when cvr >= cate_cvr then '高转化' end as cvr_label
     ,case when acos < cate_acos then '低ACOS' end as acos_label
     ,round(0.8,2) n7_campaign_budget_lack_rate
     ,'高度预算不足' budget_label
     ,10.0 n1_campaign_budget
     ,'高度预算不足' n1_campaign_budget_lack_rate
     ,20.0 n7_max_campaign_budget
     ,5.0 n7_min_campaign_budget
     ,10.0 n7_avg_cost
     ,25.0 n7_best_campaign_budget
     ,round(0.8 ,2) n7_best_campaign_budget_lack_rate
     --,least(q3.camp_budget_amt_new,q1.campaign_budget_amt * 1.5) as camp_budget_amt_new
     ,25.0 camp_budget_amt_new
     ,q2.season_label
from dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp01 q1
         inner join dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02 q2  --基础筛选
                    on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.parent_asin = q2.top_parent_asin
    --inner join dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp03 q3
--on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id
         inner join dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp02_1 q4
                    on q2.tenant_id = q4.tenant_id and q2.category = q4.category
where --q3.camp_budget_amt_new > q1.campaign_budget_amt
    q1.adv_days >= 14
  and q2.stock_label = '库存充足'
  and q2.life_cycle_label = '成熟期'
;




--剔除下调预算的
drop table if exists dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp05;
create table dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp05 as
SELECT   q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.camp_budget_amt
     ,q1.ad_group_id_list
     ,q1.top_parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
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
     ,q1.adv_days
     ,q1.category
     ,q1.cate_ctr
     ,q1.cate_cvr
     ,q1.cate_cpc
     ,q1.cate_cpa
     ,q1.cate_acos
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.ad_mode_label
     ,q1.stock_label
     ,'有效点击' click_label
     ,'高转化' cvr_label
     ,'低ACOS' acos_label
     ,q1.n7_campaign_budget_lack_rate
     ,q1.budget_label
     ,q1.n1_campaign_budget
     ,q1.n1_campaign_budget_lack_rate
     ,q1.n7_max_campaign_budget
     ,q1.n7_min_campaign_budget
     ,q1.n7_avg_cost
     ,q1.n7_best_campaign_budget
     ,q1.n7_best_campaign_budget_lack_rate
     ,q1.camp_budget_amt_new
     ,q1.season_label
from dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp04 q1
         left join (
    select tenant_id
         ,profile_id
         ,campaign_id
    from dws_mkt_adv_strategy_adjust_budget_detail_ds
    where ds = '${nowdate}'
) q2
                   on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id
where q2.campaign_id is null
;

--支持重跑
alter table dws_mkt_adv_strategy_adjust_budget_raise_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_adjust_budget_raise_detail_ds partition (ds = '${nowdate}')
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
    ,ad_group_id_list
    ,adv_manager_id
    ,adv_manager_name
    ,top_parent_asin
    ,selling_price
    ,main_asin_url
    ,main_img_url
    ,n1_camp_budget_amt
    ,n1_camp_budget_lack_rate
    ,n7_camp_budget_lack_rate
    ,n7_max_camp_budget_amt
    ,n7_min_camp_budget_amt
    ,n7_avg_cost
    ,n7_best_camp_budget_amt
    ,n7_best_camp_budget_lack_rate
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
    ,category
    ,cate_ctr
    ,cate_cvr
    ,cate_cpc
    ,cate_cpa
    ,cate_acos
    ,life_cycle_label
    ,goal_label
    ,term_type_label
    ,ad_mode_label
    ,stock_label
    ,click_label
    ,cvr_label
    ,acos_label
    ,budget_label
    ,action_type
    ,create_time
    ,season_label
)
select distinct   q1.tenant_id
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
              ,q1.camp_budget_amt --cast(q1.camp_budget_amt as decimal(18,6)) as camp_budget_amt
              ,q1.camp_budget_amt_new --cast(q1.camp_budget_amt_new as decimal(18,6)) as camp_budget_amt_new
              ,q1.ad_group_id_list
              ,q1.adv_manager_id
              ,q1.adv_manager_name
              ,q1.top_parent_asin
              ,null selling_price --cast(q1.selling_price as decimal(18,6)) as
              ,q1.main_asin_url
              ,q1.main_img_url
              ,null --cast(q1.n1_campaign_budget as decimal(18,6)) as n1_campaign_budget
              ,null --cast(q1.n1_campaign_budget_lack_rate as decimal(18,6)) as n1_campaign_budget_lack_rate
              ,null --cast(q1.n7_campaign_budget_lack_rate as decimal(18,6)) as n7_campaign_budget_lack_rate
              ,null --cast(q1.n7_max_campaign_budget as decimal(18,6)) as n7_max_campaign_budget
              ,null --cast(q1.n7_min_campaign_budget as decimal(18,6)) as n7_min_campaign_budget
              ,null --cast(q1.n7_avg_cost as decimal(18,6)) as n7_avg_cost
              ,null --cast(q1.n7_best_campaign_budget as decimal(18,6)) as n7_best_campaign_budget
              ,null --cast(q1.n7_best_campaign_budget_lack_rate as decimal(18,6)) as n7_best_campaign_budget_lack_rate
              ,q1.adv_days
              ,q1.impressions
              ,q1.clicks
              ,cast(q1.cost as decimal(18,6)) as cost
              ,cast(q1.sale_amt as decimal(18,6)) as sale_amt
              ,q1.order_num
              ,cast(q1.ctr as decimal(18,6)) as ctr
              ,cast(q1.cvr as decimal(18,6)) as cvr
              ,cast(q1.cpc as decimal(18,6)) as cpc
              ,cast(q1.cpa as decimal(18,6)) as cpa
              ,cast(q1.acos as decimal(18,6)) as acos
              ,q1.category
              ,cast(q1.cate_ctr as decimal(18,6)) as cate_ctr
              ,cast(q1.cate_cvr as decimal(18,6)) as cate_cvr
              ,cast(q1.cate_cpc as decimal(18,6)) as cate_cpc
              ,cast(q1.cate_cpa as decimal(18,6)) as cate_cpa
              ,cast(q1.cate_acos as decimal(18,6)) as cate_acos
              ,q1.life_cycle_label
              ,q1.goal_label
              ,q1.term_type_label
              ,q1.ad_mode_label
              ,q1.stock_label
              ,q1.click_label
              ,q1.cvr_label
              ,q1.acos_label
              ,q1.budget_label
              ,q2.action_type
              ,getdate() as create_time
              ,q1.season_label
from dws_mkt_adv_strategy_adjust_budget_raise_detail_tmp05 q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on  q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and q1.stock_label = q2.stock_label
                        and q1.term_type_label = q2.term_type_label
                        and q1.budget_label = q2.budget_label
                        and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                        and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and nvl(q1.acos_label,' ') = nvl(q2.acos_label,' ')
where q2.action_name = '上调预算' and q2.life_cycle_label = '成熟期'
;

select * from dws_mkt_adv_strategy_main_all_df
;
