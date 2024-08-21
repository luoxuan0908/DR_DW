--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-28 21:18:40
--********************************************************************--

CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_category_index_ds
(
    tenant_id STRING COMMENT '租户ID',
    marketplace_id STRING COMMENT '站点ID',
    marketplace_name STRING COMMENT '站点名称',
    category STRING COMMENT '类目',
    term_type STRING COMMENT '统计对象类型：搜索词、搜索品',
    life_cycle_type STRING COMMENT '生命周期：成熟期/新品期',
    cate_impressions_n30d BIGINT COMMENT '类目广告曝光量',
    cate_clicks_n30d BIGINT COMMENT '类目广告点击量',
    cate_cost_n30d DECIMAL(18,6) COMMENT '类目广告花费',
    cate_sale_amt_n30d DECIMAL(18,6) COMMENT '类目广告销售额',
    cate_order_num_n30d BIGINT COMMENT '类目广告销量',
    cate_ctr_n30d DECIMAL(18,6) COMMENT '类目CTR',
    cate_cvr_n30d DECIMAL(18,6) COMMENT '类目CVR',
    cate_cpc_n30d DECIMAL(18,6) COMMENT '类目CPC',
    cate_cpa_n30d DECIMAL(18,6) COMMENT '类目CPA',
    cate_acos_n30d DECIMAL(18,6) COMMENT '类目ACOS',
    cate_impressions_n90d BIGINT COMMENT '类目近90天广告曝光量',
    cate_clicks_n90d BIGINT COMMENT '类目近90天广告点击量',
    cate_cost_n90d DECIMAL(18,6) COMMENT '类目近90天广告花费',
    cate_sale_amt_n90d DECIMAL(18,6) COMMENT '类目近90天广告销售额',
    cate_order_num_n90d BIGINT COMMENT '类目近90天广告销量',
    cate_ctr_n90d DECIMAL(18,6) COMMENT '类目近90天CTR',
    cate_cvr_n90d DECIMAL(18,6) COMMENT '类目近90天CVR',
    cate_cpc_n90d DECIMAL(18,6) COMMENT '类目近90天CPC',
    cate_cpa_n90d DECIMAL(18,6) COMMENT '类目近90天CPA',
    cate_acos_n90d DECIMAL(18,6) COMMENT '类目近90天ACOS',
    create_time DATETIME COMMENT '创建时间'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='类目平均指标')
    LIFECYCLE 30;



------------------------------------------算类目平均>>开始--------------------------------------------

--类目平均基础数据准备
drop table if exists dws_mkt_adv_strategy_category_index_tmp00;
create table dws_mkt_adv_strategy_category_index_tmp00 as
select   q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.parent_asin
     ,q1.term_type
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q2.category
     ,q2.self_category
     ,q2.fba_first_instock_days
     ,datediff(to_date(q1.ds,'yyyymmdd'),to_date(substr(q2.fba_first_instock_time,1,10),'yyyy-mm-dd'),'dd') as fba_first_instock_from_days
     ,q1.ds
from    (
            select   s1.tenant_id
                 ,s1.profile_id
                 ,s3.marketplace_id
                 ,s3.marketplace_name
                 ,s1.currency_code
                 ,s1.seller_id
                 ,s3.parent_asin
                 ,case when length(regexp_replace(s1.search_term,'([^0-9])','')) > 0
                and length(regexp_replace(s1.search_term,'([^a-z])','')) > 0
                and instr(s1.search_term,' ') = 0
                and length(s1.search_term) = 10 then '搜索品' else '搜索词' end term_type
                 ,s1.ds
                 ,sum(impressions) as impressions
                 ,sum(clicks) as clicks
                 ,sum(cost) as cost
                 ,sum(w7d_sale_amt) as sale_amt
                 ,sum(w7d_units_sold_clicks) as order_num
            from    (
                        select   tenant_id
                             ,profile_id
                             ,seller_id
                             ,campaign_id
                             ,campaign_budget_currency_code as currency_code
                             ,ad_group_id
                             ,search_term
                             ,sum(impressions) as impressions
                             ,sum(clicks) as clicks
                             ,sum(cost) as cost
                             ,sum(w7d_sale_amt) as w7d_sale_amt
                             ,sum(w7d_units_sold_clicks) as w7d_units_sold_clicks
                             ,ds
                        -- ,campaign_status
                        from    whde.dwd_mkt_adv_amazon_sp_search_term_ds
                        where   ds between to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd'),'yyyymmdd') and '${bizdate}'
                        group by tenant_id
                               ,profile_id
                               ,seller_id
                               ,campaign_id
                               ,campaign_budget_currency_code
                               ,ad_group_id
                               ,search_term
                               ,ds
                        -- ,campaign_status
                    ) s1
                        inner join   (
                select   tenant_id
                     ,profile_id
                     ,marketplace_id
                     ,marketplace_name
                     ,seller_id
                     ,campaign_id
                     ,ad_group_id
                     ,max(top_cost_parent_asin) as parent_asin
                from    whde.adm_amazon_adv_sku_wide_d
                where   ds = max_pt('whde.adm_amazon_adv_sku_wide_d')
                group by  tenant_id
                       ,profile_id
                       ,marketplace_id
                       ,marketplace_name
                       ,seller_id
                       ,campaign_id
                       ,ad_group_id
            ) s3
                                     on s1.tenant_id = s3.tenant_id and s1.profile_id = s3.profile_id and s1.campaign_id = s3.campaign_id and s1.ad_group_id = s3.ad_group_id
            group by s1.tenant_id
                   ,s1.profile_id
                   ,s3.marketplace_id
                   ,s3.marketplace_name
                   ,s1.currency_code
                   ,s1.seller_id
                   ,s3.parent_asin
                   ,case when length(regexp_replace(s1.search_term,'([^0-9])','')) > 0
                and length(regexp_replace(s1.search_term,'([^a-z])','')) > 0
                and instr(s1.search_term,' ') = 0
                and length(s1.search_term) = 10 then '搜索品' else '搜索词' end
                   ,s1.ds
        ) q1
            inner join   (
    select   tenant_id
         ,profile_id
         ,top_parent_asin
         ,fba_first_instock_time
         ,fba_first_instock_days
         ,category
         ,self_category
    from    whde.dws_mkt_adv_strategy_parent_asin_index_ds
    where   ds = '${bizdate}' and fba_first_instock_time is not null
) q2 --父aisn、以及类目、库存
                         on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin
;




--爱思奇自带系列平均指标
--30天明细表
drop table if exists dws_mkt_adv_strategy_category_index_tmp03_1;
create table dws_mkt_adv_strategy_category_index_tmp03_1 as
select  tenant_id
     ,marketplace_id
     ,marketplace_name
     ,category self_category
     ,term_type
     ,case when fba_first_instock_from_days > 180 then 1 when fba_first_instock_from_days <= 180 then 0 end as mature_cate_label
     ,sum(impressions) as cate_impressions
     ,sum(clicks) as cate_clicks
     ,sum(cost) as cate_cost
     ,sum(sale_amt) as cate_sale_amt
     ,sum(order_num) as cate_order_num
     ,case when sum(impressions) = 0 then null else sum(clicks)/sum(impressions) end as cate_ctr
     ,case when sum(clicks) = 0 then null else sum(order_num)/sum(clicks) end as cate_cvr
     ,case when sum(clicks) = 0 then null else sum(cost)/sum(clicks) end as cate_cpc
     ,case when sum(order_num) = 0 then null else sum(cost)/sum(order_num) end as cate_cpa
     ,case when sum(sale_amt) = 0 then null else sum(cost)/sum(sale_amt) end as cate_acos
from  dws_mkt_adv_strategy_category_index_tmp00
where ds >= to_char(dateadd(to_date('${bizdate}','yyyymmdd'), -30, 'dd'),'yyyymmdd')
--and nvl(self_category,'') <> ''
group by tenant_id
       ,marketplace_id
       ,marketplace_name
       ,category
       ,term_type
       ,case when fba_first_instock_from_days > 180 then 1 when fba_first_instock_from_days <= 180 then 0 end
;

--90天明细表
drop table if exists dws_mkt_adv_strategy_category_index_tmp03_2;
create table dws_mkt_adv_strategy_category_index_tmp03_2 as
select  tenant_id
     ,marketplace_id
     ,marketplace_name
     ,category self_category
     ,term_type
     ,case when fba_first_instock_from_days > 180 then 1 when fba_first_instock_from_days <= 180 then 0 end as mature_cate_label
     ,sum(impressions) as cate_impressions
     ,sum(clicks) as cate_clicks
     ,sum(cost) as cate_cost
     ,sum(sale_amt) as cate_sale_amt
     ,sum(order_num) as cate_order_num
     ,case when sum(impressions) = 0 then null else sum(clicks)/sum(impressions) end as cate_ctr
     ,case when sum(clicks) = 0 then null else sum(order_num)/sum(clicks) end as cate_cvr
     ,case when sum(clicks) = 0 then null else sum(cost)/sum(clicks) end as cate_cpc
     ,case when sum(order_num) = 0 then null else sum(cost)/sum(order_num) end as cate_cpa
     ,case when sum(sale_amt) = 0 then null else sum(cost)/sum(sale_amt) end as cate_acos
from  dws_mkt_adv_strategy_category_index_tmp00
where ds >= to_char(dateadd(to_date('${bizdate}','yyyymmdd'), -90, 'dd'),'yyyymmdd')
--and nvl(self_category,'') <> ''
group by tenant_id
       ,marketplace_id
       ,marketplace_name
       ,category
       ,term_type
       ,case when fba_first_instock_from_days > 180 then 1 when fba_first_instock_from_days <= 180 then 0 end
;


--整合
drop table if exists dws_mkt_adv_strategy_category_index_tmp03_3;
create table dws_mkt_adv_strategy_category_index_tmp03_3 as
select  q1.tenant_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.self_category as category
     ,q1.term_type
     ,q1.mature_cate_label
     ,q2.cate_impressions
     ,q2.cate_clicks
     ,q2.cate_cost
     ,q2.cate_sale_amt
     ,q2.cate_order_num
     ,q2.cate_ctr
     ,q2.cate_cvr
     ,q2.cate_cpc
     ,q2.cate_cpa
     ,q2.cate_acos
     ,q1.cate_impressions  as cate_impressions_n90d
     ,q1.cate_clicks  as cate_clicks_n90d
     ,q1.cate_cost  as cate_cost_n90d
     ,q1.cate_sale_amt  as cate_sale_amt_n90d
     ,q1.cate_order_num  as cate_order_num_n90d
     ,q1.cate_ctr  as cate_ctr_n90d
     ,q1.cate_cvr  as cate_cvr_n90d
     ,q1.cate_cpc  as cate_cpc_n90d
     ,q1.cate_cpa  as cate_cpa_n90d
     ,q1.cate_acos  as cate_acos_n90d
from dws_mkt_adv_strategy_category_index_tmp03_2 q1
         inner join dws_mkt_adv_strategy_category_index_tmp03_1 q2
                    on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.self_category = q2.self_category and q1.term_type = q2.term_type and q1.mature_cate_label = q2.mature_cate_label
;



--支持重跑
alter table dws_mkt_adv_strategy_category_index_ds drop if exists partition (ds = '${bizdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_category_index_ds partition (ds = '${bizdate}')
(
 tenant_id
,marketplace_id
,marketplace_name
,category
,term_type
,life_cycle_type
,cate_impressions_n30d
,cate_clicks_n30d
,cate_cost_n30d
,cate_sale_amt_n30d
,cate_order_num_n30d
,cate_ctr_n30d
,cate_cvr_n30d
,cate_cpc_n30d
,cate_cpa_n30d
,cate_acos_n30d
,cate_impressions_n90d
,cate_clicks_n90d
,cate_cost_n90d
,cate_sale_amt_n90d
,cate_order_num_n90d
,cate_ctr_n90d
,cate_cvr_n90d
,cate_cpc_n90d
,cate_cpa_n90d
,cate_acos_n90d
,create_time
)
select q1.tenant_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.category
     ,q1.term_type
     ,case when q1.mature_cate_label = 1 then '成熟期' when q1.mature_cate_label = 0 then '新品期' end as mature_type
     ,q1.cate_impressions
     ,q1.cate_clicks
     ,cast(q1.cate_cost as decimal(18,6)) as cate_cost
     ,cast(q1.cate_sale_amt as decimal(18,6)) as cate_sale_amt
     ,q1.cate_order_num
     ,cast(q1.cate_ctr  as decimal(18,6)) as cate_ctr
     ,cast(q1.cate_cvr  as decimal(18,6)) as cate_cvr
     ,cast(q1.cate_cpc  as decimal(18,6)) as cate_cpc
     ,cast(q1.cate_cpa  as decimal(18,6)) as cate_cpa
     ,cast(q1.cate_acos as decimal(18,6)) as cate_acos
     ,q1.cate_impressions_n90d
     ,q1.cate_clicks_n90d
     ,cast(q1.cate_cost_n90d as decimal(18,6)) as cate_cost_n90d
     ,cast(q1.cate_sale_amt_n90d as decimal(18,6)) as cate_sale_amt_n90d
     ,q1.cate_order_num_n90d
     ,cast(q1.cate_ctr_n90d  as decimal(18,6)) as cate_ctr_n90d
     ,cast(q1.cate_cvr_n90d  as decimal(18,6)) as cate_cvr_n90d
     ,cast(q1.cate_cpc_n90d  as decimal(18,6)) as cate_cpc_n90d
     ,cast(q1.cate_cpa_n90d  as decimal(18,6)) as cate_cpa_n90d
     ,cast(q1.cate_acos_n90d as decimal(18,6)) as cate_acos_n90d
     ,getdate() as create_time
from dws_mkt_adv_strategy_category_index_tmp03_3 q1
where mature_cate_label is not null
;
