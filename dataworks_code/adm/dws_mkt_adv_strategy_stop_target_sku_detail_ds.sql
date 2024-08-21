--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:12:36
--********************************************************************--

CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_stop_target_sku_detail_ds
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
    ,target_sku            STRING COMMENT '推广sku'
    ,target_asin           STRING COMMENT '推广asin'
    ,top_parent_asin       STRING COMMENT '父aisn'
    ,selling_price         DECIMAL(18,6) COMMENT '售价'
    ,main_asin_url         STRING COMMENT '商品链接'
    ,main_img_url          STRING COMMENT '商品主图'
    ,title                 STRING COMMENT '标题'
    ,color                 STRING COMMENT '颜色'
    ,size                  STRING COMMENT '尺码'
    ,stock_sale_days       STRING COMMENT '库存可售天数'
    ,order_num_rank        BIGINT COMMENT '同一个父asin下的销量排名'
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
    ,category              STRING COMMENT '类目'
    ,cate_ctr              DECIMAL(18,6) COMMENT '类目CTR'
    ,cate_cvr              DECIMAL(18,6) COMMENT '类目CVR'
    ,cate_cpc              DECIMAL(18,6) COMMENT '类目CPC'
    ,cate_cpa              DECIMAL(18,6) COMMENT '类目CPA'
    ,cate_acos             DECIMAL(18,6) COMMENT '类目ACOS'
    ,life_cycle_label      STRING COMMENT '生命周期'
    ,goal_label            STRING COMMENT '目标标签'
    ,ad_mode_label         STRING COMMENT '广告投放类型标签'
    ,term_type_label       STRING COMMENT '操作对象类型'
    ,acos_label            STRING COMMENT 'acos标签'
    ,action_type           STRING COMMENT '操作类型'
    ,create_time           DATETIME COMMENT '创建时间'
    ,seller_sku_list STRING comment '推广sku列表'
    ,seller_sku_cnt STRING comment '推广sku数量'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略暂停推广品(子表)')
    LIFECYCLE 366
;


--30天基础数据
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp00;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp00 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.currency_code
     ,q1.ad_mode
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.advertised_asin
     ,q1.advertised_sku
     ,q1.parent_asin as top_parent_asin
     ,q1.top_cost_parent_asin
     ,q1.selling_price
     ,q1.title
     ,q1.link
     ,q1.main_image_url
     ,q1.adv_days
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,case when q1.impressions = 0 then null else q1.clicks/q1.impressions end as ctr
     ,case when q1.clicks = 0 then null else q1.order_num/q1.clicks end as cvr
     ,case when q1.clicks = 0 then null else q1.cost/q1.clicks end as cpc
     ,case when q1.order_num = 0 then null else q1.cost/q1.order_num end as cpa
     ,case when q1.sale_amt = 0 then null else q1.cost/q1.sale_amt end as acos
from (
         select   tenant_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,campaign_budget_currency_code as currency_code
              ,seller_id
              ,seller_name
              ,ad_mode
              ,campaign_id
              ,campaign_name
              ,ad_group_id
              ,ad_group_name
              ,advertised_asin
              ,advertised_sku
              ,parent_asin
              ,top_cost_parent_asin
              ,max(selling_price) as selling_price
              ,max(title) as title
              ,max(link) as link
              ,max(main_image_url) as main_image_url
              ,sum(impressions) as impressions
              ,sum(clicks) as clicks
              ,sum(cost) as cost
              ,sum(sale_amt) as sale_amt
              ,sum(order_num) as order_num
              ,datediff(to_date('${bizdate}','yyyymmdd'),min(report_date),'dd') + 1 as adv_days
         from whde.adm_amazon_adv_sku_wide_d
         where ds = '${bizdate}'
         group by   tenant_id
                ,profile_id
                ,marketplace_id
                ,marketplace_name
                ,campaign_budget_currency_code
                ,seller_id
                ,seller_name
                ,ad_mode
                ,campaign_id
                ,campaign_name
                ,ad_group_id
                ,ad_group_name
                ,advertised_asin
                ,advertised_sku
                ,parent_asin
                ,top_cost_parent_asin
     )q1
         inner join (
    select tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,sku
         ,asin
    from whde.adm_amazon_adv_pro_product_status_df
    where ds = max_pt('whde.adm_amazon_adv_pro_product_status_df') and status = 'ENABLED'  --正常状态的推广品
    group by tenant_id
           ,profile_id
           ,campaign_id
           ,ad_group_id
           ,sku
           ,asin
)q2
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id and q1.ad_group_id = q2.ad_group_id and q1.advertised_asin = q2.asin and q1.advertised_sku = q2.sku
;




--商品基本信息
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp01;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp01 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.currency_code
     ,q1.ad_mode
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.top_cost_parent_asin
     ,q1.top_parent_asin
     ,q1.advertised_asin
     ,q1.advertised_sku
     ,q1.selling_price
     ,q1.title
     ,q1.link
     ,q1.main_image_url
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
     ,'' color
     ,'' size
from dws_mkt_adv_strategy_stop_target_sku_detail_tmp00 q1
--left join (
--           select asin
--                 ,sku
--                 ,max(color) as color
--                 ,max(size) as size
--           from asq_dw.ods_zhongteng_manual_download_asin_list_d   --中腾属性表
--           where ds = max_pt('asq_dw.ods_zhongteng_manual_download_asin_list_d')
--           group by asin
--                   ,sku
--          ) q2
--on q1.advertised_asin = q2.asin and q1.advertised_sku = q2.sku
--left join (
--           select seller_sku
--                 ,asin
--                 ,parent_asin
--                 ,max(cn_color) as color
--                 ,max(size) as size
--           from asq_dw.ods_asq_jdy_asin_list   --爱思奇属性表
--           where ds = max_pt('asq_dw.ods_asq_jdy_asin_list')
--           group by seller_sku
--                   ,asin
--                   ,parent_asin
--          ) q3
--on q1.advertised_asin = q3.asin and q1.advertised_sku = q3.seller_sku and q1.top_parent_asin = q3.parent_asin
where q1.adv_days >= 14
;




--商品基本标签筛选
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp02;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp02 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,seller_id
     ,adv_manager_id
     ,adv_manager_name
     ,top_parent_asin
     ,stock_sale_days
     ,category
     ,sum(cate_impressions_n30d) as cate_impressions
     ,sum(cate_clicks_n30d) as cate_clicks
     ,sum(cate_cost_n30d) as cate_cost
     ,sum(cate_sale_amt_n30d) as cate_sale_amt
     ,sum(cate_order_num_n30d) as cate_order_num
     ,case when sum(cate_impressions_n30d) = 0 then null else sum(cate_clicks_n30d)/sum(cate_impressions_n30d) end as cate_ctr
     ,case when sum(cate_clicks_n30d) = 0 then null else sum(cate_order_num_n30d)/sum(cate_clicks_n30d) end as cate_cvr
     ,case when sum(cate_clicks_n30d) = 0 then null else sum(cate_cost_n30d)/sum(cate_clicks_n30d) end as cate_cpc
     ,case when sum(cate_order_num_n30d) = 0 then null else sum(cate_cost_n30d)/sum(cate_order_num_n30d) end as cate_cpa
     ,case when sum(cate_sale_amt_n30d) = 0 then null else sum(cate_cost_n30d)/sum(cate_sale_amt_n30d) end as cate_acos
from    whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = max_pt('whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds') and life_cycle_label = '成熟期'
group by tenant_id
       ,profile_id
       ,marketplace_id
       ,seller_id
       ,adv_manager_id
       ,adv_manager_name
       ,top_parent_asin
       ,stock_sale_days
       ,category
;



drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp01_1;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp01_1 as
select tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,top_cost_parent_asin
     ,wm_concat(distinct '_/_',advertised_sku) as seller_sku_list
     ,count(distinct advertised_sku) as seller_sku_cnt
     ,sum(impressions) as impressions
     ,sum(clicks) as clicks
     ,sum(cost) as cost
     ,sum(sale_amt) as sale_amt
     ,sum(order_num) as order_num
     ,cast(case when sum(impressions) <> 0 then sum(clicks) / sum(impressions) else null end as decimal(18,6)) as ctr
     ,cast(case when sum(clicks) <> 0 then sum(order_num) / sum(clicks) else null end as decimal(18,6)) as cvr
     ,cast(case when sum(clicks) <> 0 then sum(cost) / sum(clicks) else null end as decimal(18,6)) as cpc
     ,cast(case when sum(sale_amt) <> 0 then sum(cost) / sum(sale_amt) else null end as decimal(18,6)) as acos
     ,cast(case when sum(order_num) <> 0 then sum(cost) / sum(order_num) else null end as decimal(18,6)) as cpa
from dws_mkt_adv_strategy_stop_target_sku_detail_tmp01
group by tenant_id
       ,profile_id
       ,campaign_id
       ,ad_group_id
       ,top_cost_parent_asin
;


--先关注广告组的效果，如果整个广告组效果差，那是投放词品的问题，不一定是推广品的问题
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp01_2;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp01_2 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.campaign_id
     ,q1.ad_group_id
     ,q1.top_cost_parent_asin
     ,q1.seller_sku_list
     ,q1.seller_sku_cnt
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q1.cpa
     ,q2.cate_cpa
     ,case when q1.cpa < q2.cate_cpa * 1.5 then 1 else 0 end as if_good_group
from  dws_mkt_adv_strategy_stop_target_sku_detail_tmp01_1 q1
          inner join dws_mkt_adv_strategy_stop_target_sku_detail_tmp02 q2
                     on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.top_cost_parent_asin = q2.top_parent_asin
;


--asin库存
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp02_1;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp02_1 as
select tenant_id
     ,marketplace_id
     ,seller_id
     ,parent_asin
     ,asin
     ,case when nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) = 0 then 0               --库存为0则库存天数为0
           when nvl(sum(afnstock_n15d_avg_sale_num),0) = 0 then 999  --近15天销量为0则库存天数充足
           else (nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) ) /nvl(sum(afnstock_n15d_avg_sale_num),0) end as stock_sale_days  --库存天数
     ,row_number()over(partition by tenant_id,marketplace_id,seller_id,parent_asin order by sum(n30d_sale_num) desc) as order_num_rank --近30天总销量
from whde.dws_itm_sku_amazon_asin_index_df
where ds = '${bizdate}'
group by  tenant_id
       ,marketplace_id
       ,seller_id
       ,parent_asin
       ,asin
;


--关联属性
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp03;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp03 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.currency_code
     ,q1.ad_mode
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.top_parent_asin
     ,q1.advertised_asin
     ,q1.advertised_sku
     ,CASE WHEN q1.marketplace_id IN ('A1AM78C64UM0Y8','ATVPDKIKX0DER','A39IBJ37TRP1C6','A2EUQ1WTGCTBG2') then replace(replace(q1.selling_price,',',''),'$','')
           WHEN q1.marketplace_id IN ('A1C3SOZRARQ6R3') then replace(replace(q1.selling_price,',','.'),'zł','')
           WHEN q1.marketplace_id IN ('A1805IZSGTT6HS','A13V1IB3VIYZZH','APJ6JRA9NG5V4','A1PA6795UKMFR9','AMEN7PMS3EDWL','A1RKKUPIHCS9HS') then replace(replace(q1.selling_price,',','.'),'€','')
           WHEN q1.marketplace_id IN ('A2Q3Y263D00KWC') AND INSTR(q1.selling_price,'.')>0 then replace(replace(replace(q1.selling_price,'.',''),',','.'),'R$','')
           WHEN q1.marketplace_id IN ('A2Q3Y263D00KWC') AND INSTR(selling_price,'.')=0 then replace(replace(selling_price,',','.'),'R$','')
           WHEN q1.marketplace_id IN ('A33AVAJ2PDY3EV') AND INSTR(q1.selling_price,'.')>0 then replace(replace(replace(q1.selling_price,'.',''),',','.'),'TL','')
           WHEN q1.marketplace_id IN ('A33AVAJ2PDY3EV') AND INSTR(selling_price,'.')=0  then replace(replace(selling_price,',','.'),'TL','')
           WHEN q1.marketplace_id IN ('A1F83G8C2ARO7P') then replace(replace(q1.selling_price,',',''),'£','')
           WHEN q1.marketplace_id IN ('A2NODRKZP88ZB9') then replace(replace(q1.selling_price,',',''),'kr','')
           WHEN q1.marketplace_id IN ('A21TJRUUN4KGV') then replace(replace(q1.selling_price,',',''),'₹','')  end selling_price
     ,q1.title
     ,q1.link
     ,q1.main_image_url
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
     ,q1.color
     ,q1.size
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,cast(q3.stock_sale_days as bigint) as stock_sale_days
     ,q2.category
     ,q2.cate_ctr
     ,q2.cate_cvr
     ,q2.cate_cpc
     ,q2.cate_cpa
     ,q2.cate_acos
     ,q3.order_num_rank
     ,q4.seller_sku_list
     ,q4.seller_sku_cnt
from dws_mkt_adv_strategy_stop_target_sku_detail_tmp01 q1
         inner join dws_mkt_adv_strategy_stop_target_sku_detail_tmp02 q2
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.top_parent_asin = q2.top_parent_asin
         inner join dws_mkt_adv_strategy_stop_target_sku_detail_tmp02_1 q3
                    on q1.tenant_id = q3.tenant_id and q1.marketplace_id = q3.marketplace_id and q1.seller_id = q3.seller_id and q1.top_parent_asin = q3.parent_asin and q1.advertised_asin = q3.asin
         inner join dws_mkt_adv_strategy_stop_target_sku_detail_tmp01_2 q4
                    on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and q1.campaign_id = q4.campaign_id and q1.ad_group_id = q4.ad_group_id
where q4.if_good_group = 1
;


--生成标签
drop table if exists dws_mkt_adv_strategy_stop_target_sku_detail_tmp04;
create table dws_mkt_adv_strategy_stop_target_sku_detail_tmp04 as
select tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,seller_id
     ,seller_name
     ,currency_code
     ,ad_mode
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,top_parent_asin
     ,advertised_asin as target_asin
     ,advertised_sku as target_sku
     ,selling_price
     ,title
     ,link as main_asin_url
     ,main_image_url as main_img_url
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
     ,color
     ,size
    ,adv_manager_id
    ,adv_manager_name
    ,stock_sale_days
    ,category
    ,cate_ctr
    ,cate_cvr
    ,cate_cpc
    ,cate_cpa
    ,cate_acos
    ,'成熟期' as life_cycle_label
    ,'利润最大化' as goal_label
    ,'推广品' as term_type_label
    ,ad_mode as ad_mode_label
    ,case when acos > cate_acos * 1.3 then '高ACOS' end as acos_label
      ,order_num_rank
      ,seller_sku_list
      ,seller_sku_cnt
from dws_mkt_adv_strategy_stop_target_sku_detail_tmp03
;



--支持重跑
alter table dws_mkt_adv_strategy_stop_target_sku_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_stop_target_sku_detail_ds partition (ds = '${nowdate}')
(
     tenant_id
    ,row_id
    ,strategy_id
    ,profile_id
    ,marketplace_id
    ,marketplace_name
    ,seller_id
    ,seller_name
    ,currency_code
    ,ad_type
    ,campaign_id
    ,campaign_name
    ,ad_group_id
    ,ad_group_name
    ,top_parent_asin
    ,adv_manager_id
    ,adv_manager_name
    ,target_asin
    ,target_sku
    ,selling_price
    ,title
    ,main_asin_url
    ,main_img_url
    ,color
    ,size
    ,stock_sale_days
    ,order_num_rank
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
    ,acos_label
    ,action_type
    ,create_time
    ,seller_sku_list
    ,seller_sku_cnt
)
select  q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.ad_group_id,q1.top_parent_asin,q1.target_asin,q1.target_sku,q2.strategy_id)) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.currency_code
     ,'商品推广' as ad_type
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.top_parent_asin
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.target_asin
     ,q1.target_sku
     ,cast(9.61 as decimal(18,6)) as selling_price
     ,q1.title
     ,q1.main_asin_url
     ,q1.main_img_url
     ,q1.color
     ,q1.size
     ,q1.stock_sale_days
     ,q1.order_num_rank
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
     ,q1.acos_label
     ,q2.action_type
     ,getdate() as create_time
     ,q1.seller_sku_list
     ,q1.seller_sku_cnt
from dws_mkt_adv_strategy_stop_target_sku_detail_tmp04 q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and q1.term_type_label = q2.term_type_label
                        and q1.ad_mode_label = q2.ad_mode_label
                        and q1.acos_label = q2.acos_label
where q2.action_name = '暂停推广品' and q2.life_cycle_label = '成熟期'
;
