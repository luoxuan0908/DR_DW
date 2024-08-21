--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:07:13
--********************************************************************--

CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_add_seller_sku_stepone_detail_ds
(
    sku_id                STRING COMMENT 'hash(租户站点店铺sku)'
    ,strategy_id           STRING COMMENT '策略ID'
    ,tenant_id             STRING COMMENT '租户ID'
    ,profile_id            STRING COMMENT '配置ID'
    ,marketplace_id        STRING COMMENT '市场ID'
    ,marketplace_name      STRING COMMENT '市场名称'
    ,currency_code         STRING COMMENT '币种'
    ,seller_id             STRING COMMENT '卖家ID'
    ,seller_name           STRING COMMENT '卖家名称(亚马逊上的店铺名称)'
    ,adv_manager_id        STRING COMMENT '广告负责人ID'
    ,adv_manager_name      STRING COMMENT '广告负责人名称'
    ,seller_sku            STRING COMMENT '推广sku'
    ,seller_asin           STRING COMMENT '推广asin'
    ,top_parent_asin       STRING COMMENT '父aisn'
    ,selling_price         DECIMAL(18,6) COMMENT '售价'
    ,main_asin_url         STRING COMMENT '商品链接'
    ,main_img_url          STRING COMMENT '商品主图'
    ,title                 STRING COMMENT '标题'
    ,color                 STRING COMMENT '颜色'
    ,size                  STRING COMMENT '尺码'
    ,stock_sale_days       BIGINT COMMENT '库存可售天数'
    ,order_num_rank        BIGINT COMMENT '同一个父asin下的销量排名'
    ,adv_days              BIGINT COMMENT '广告天数'
    ,ad_group_cnt          BIGINT COMMENT '涉及广告组个数'
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
    ,term_type_label       STRING COMMENT '操作对象类型'
    ,acos_label            STRING COMMENT 'acos标签'
    ,click_label           STRING COMMENT '点击标签'
    ,cvr_label             STRING COMMENT '转化标签'
    ,create_time           DATETIME COMMENT '创建时间'
    ,seller_sku_img_url STRING comment '推广品图片'
    , ad_group_id_list STRING comment '广告组列表'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略添加推广品(推广品子表)')
    LIFECYCLE 366
;


--------------------------------------------------推广品处理--------------------------------------------------

--推广品30天基础数据
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp00;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp00 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.currency_code
     ,q1.advertised_asin
     ,q1.advertised_sku
     ,q1.parent_asin as top_parent_asin
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
     ,q1.ad_group_cnt
from (
         select   tenant_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,campaign_budget_currency_code as currency_code
              ,seller_id
              ,seller_name
              ,advertised_asin
              ,advertised_sku
              ,parent_asin
              ,count(distinct ad_group_id) as ad_group_cnt
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
         group by tenant_id
                ,profile_id
                ,marketplace_id
                ,marketplace_name
                ,campaign_budget_currency_code
                ,seller_id
                ,seller_name
                ,advertised_asin
                ,advertised_sku
                ,parent_asin
     )q1
;




--商品基本标签筛选
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp01;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp01 as
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



--asin库存
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp02;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp02 as
select tenant_id
     ,marketplace_id
     ,seller_id
     ,parent_asin
     ,asin
     ,max(main_image_url) as seller_sku_img_url
     ,case when nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) = 0 then 0               --库存为0则库存天数为0
           when nvl(sum(afnstock_n15d_avg_sale_num),0) = 0 then 999  --近15天销量为0则库存天数充足
           else (nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) ) /nvl(sum(afnstock_n15d_avg_sale_num),0) end as stock_sale_days  --库存天数                             --近30天总销量
from whde.dws_itm_sku_amazon_asin_index_df
where ds = '${bizdate}'
group by  tenant_id
       ,marketplace_id
       ,seller_id
       ,parent_asin
       ,asin
having stock_sale_days >= 15
;


--关联商品基本信息
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp03;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp03 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.currency_code
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
     ,q1.ad_group_cnt
     ,'' as color
     ,'' as size
      ,'' en_color
      ,q7.seller_sku_img_url
      ,q6.adv_manager_id
      ,q6.adv_manager_name
      ,q7.stock_sale_days
      ,q6.category
      ,q6.cate_ctr
      ,q6.cate_cvr
      ,q6.cate_cpc
      ,q6.cate_cpa
      ,q6.cate_acos
from dws_mkt_adv_strategy_add_seller_sku_detail_tmp00 q1
--left join (
--           select asin
--                 ,sku
--                 ,max(color) as color
--                 ,max(size) as size
--           from whde.ods_zhongteng_manual_download_asin_list_d   --中腾属性表
--           where ds = max_pt('whde.ods_zhongteng_manual_download_asin_list_d')
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
--                 ,max(color) as en_color
--           from whde.ods_asq_jdy_asin_list   --爱思奇属性表
--           where ds = max_pt('whde.ods_asq_jdy_asin_list')
--           group by seller_sku
--                   ,asin
--                   ,parent_asin
--          ) q3
--on q1.advertised_asin = q3.asin and q1.advertised_sku = q3.seller_sku and q1.top_parent_asin = q3.parent_asin
inner join dws_mkt_adv_strategy_add_seller_sku_detail_tmp01 q6
on q1.tenant_id = q6.tenant_id and q1.profile_id = q6.profile_id and q1.top_parent_asin = q6.top_parent_asin
inner join dws_mkt_adv_strategy_add_seller_sku_detail_tmp02 q7
on q1.tenant_id = q7.tenant_id and q1.marketplace_id = q7.marketplace_id and q1.seller_id = q7.seller_id and q1.top_parent_asin = q7.parent_asin and q1.advertised_asin = q7.asin
where q1.adv_days >= 14
;



--爱斯奇限制高CTR推广品
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp03_1;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp03_1 as
select tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,seller_id
     ,seller_name
     ,currency_code
     ,top_parent_asin
     ,advertised_asin
     ,advertised_sku
     ,selling_price
     ,title
     ,link
     ,main_image_url
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
     ,ad_group_cnt
     ,color
     ,size
    ,en_color
    ,seller_sku_img_url
    ,adv_manager_id
    ,adv_manager_name
    ,stock_sale_days
    ,category
    ,cate_ctr
    ,cate_cvr
    ,cate_cpc
    ,cate_cpa
    ,cate_acos
    from dws_mkt_adv_strategy_add_seller_sku_detail_tmp03
;


--生成标签
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp04;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp04 as
select tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,seller_id
     ,seller_name
     ,currency_code
     ,top_parent_asin
     ,advertised_asin as seller_asin
     ,advertised_sku as seller_sku
     ,hash(concat(tenant_id,profile_id,advertised_sku)) as sku_id
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
    ,en_color
    ,seller_sku_img_url
    ,ad_group_cnt
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
    ,case when acos < cate_acos then '低ACOS' end as acos_label
      ,case when cate_cvr = 0 or cate_cvr is null then null
            when  clicks < 1/cate_cvr then '无效点击'
            when  clicks >= 1/cate_cvr then '有效点击'  end as click_label --点击标签
      ,case when cvr < cate_cvr * 0.5 then '低转化'
            when cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
            when cvr >= cate_cvr then '高转化' end as cvr_label --转化标签
      ,case when cpc < cate_cpc then '低CPC' when cpc >= cate_cpc then '高CPC' end as cpc_label  --CPC标签
      ,row_number()over(partition by tenant_id,profile_id,top_parent_asin order by order_num desc) as order_num_rank
from dws_mkt_adv_strategy_add_seller_sku_detail_tmp03_1
;


--------------------------------------------------目标广告组处理--------------------------------------------------
--筛选效果不差的广告组作为目标广告组
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_0;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_0 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.campaign_id
     ,q1.ad_group_id
     ,case when q1.ctr > q2.cate_ctr then 1 end as if_high_ctr   --条件2
from (
         select   tenant_id
              ,profile_id
              ,campaign_id
              ,ad_group_id
              ,top_cost_parent_asin
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
         from whde.adm_amazon_adv_sku_wide_d
         where ds = '${bizdate}'
         group by tenant_id
                ,profile_id
                ,campaign_id
                ,ad_group_id
                ,top_cost_parent_asin
         having order_num >= 2    --条件1
     )q1
         left join dws_mkt_adv_strategy_add_seller_sku_detail_tmp01 q2
                   on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.top_cost_parent_asin = q2.top_parent_asin
;



--获取广告组清单
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_1;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_1 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.parent_asin
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.seller_asin
     ,q1.seller_sku
     ,case when q1.ad_group_name like '%捡漏%' then 1 else 0 end as if_pick     --捡漏的广告组
from (
         select   tenant_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,seller_id
              ,seller_name
              ,parent_asin
              ,campaign_id
              ,campaign_name
              ,ad_group_id
              ,ad_group_name
              ,advertised_asin as seller_asin
              ,advertised_sku  as seller_sku
         from whde.adm_amazon_adv_sku_wide_d
         where ds = '${bizdate}'
         group by tenant_id
                ,profile_id
                ,marketplace_id
                ,marketplace_name
                ,seller_id
                ,seller_name
                ,parent_asin
                ,campaign_id
                ,campaign_name
                ,ad_group_id
                ,ad_group_name
                ,advertised_asin
                ,advertised_sku
     )q1
         --left join (
--           select marketplace_id
--                 ,seller_id
--                 ,sku
--                 ,top_parent_asin
--                 ,asin
--                 ,tenant_id
--           from whde.dwd_asq_parent_asin_manager_ds
--           where ds = max_pt('whde.dwd_asq_parent_asin_manager_ds') and sku_tag in ('爆款','旺款','特大爆款') --爆款所在组不推荐进来
--           group by marketplace_id
--                 ,seller_id
--                 ,sku
--                 ,top_parent_asin
--                 ,asin
--          )q2
--on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.seller_id = q2.seller_id and q1.parent_asin = q2.top_parent_asin and q1.seller_asin = q2.asin and q1.seller_sku = q2.sku
         inner join dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_0 q3  --本身效果不差的广告组
                    on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id and q1.ad_group_id = q3.ad_group_id
where --q2.sku is null and
      q3.if_high_ctr = 1
;

--目标推广品对应父asin所在的没有该推广品的广告组
drop table if exists dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_2;
create table dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_2 as
select q2.sku_id
     ,wm_concat(distinct '_&_',concat(q1.campaign_id,'_/_',q1.ad_group_id)) as ad_group_list
from (
         select tenant_id
              ,profile_id
              ,marketplace_id
              ,campaign_id
              ,ad_group_id
              ,parent_asin
         from dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_1
         where if_pick = 0  --非捡漏广告组
         group by tenant_id
                ,profile_id
                ,marketplace_id
                ,campaign_id
                ,ad_group_id
                ,parent_asin
     ) q1
         inner join (
    select tenant_id
         ,marketplace_id
         ,top_parent_asin
         ,seller_sku
         ,sku_id
    from dws_mkt_adv_strategy_add_seller_sku_detail_tmp04
    where acos_label = '低ACOS' and click_label = '有效点击' and cvr_label in ('一般转化','高转化')  --符合基础标签的推广品
    group by tenant_id
           ,marketplace_id
           ,top_parent_asin
           ,seller_sku
           ,sku_id
)q2
                    on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.parent_asin = q2.top_parent_asin
         left join dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_1 q3
                   on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id and q1.ad_group_id = q3.ad_group_id and q1.parent_asin = q3.parent_asin and q2.seller_sku = q3.seller_sku
where q3.seller_sku is null
group by q2.sku_id
;



--支持重跑
alter table dws_mkt_adv_strategy_add_seller_sku_stepone_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_add_seller_sku_stepone_detail_ds partition (ds = '${nowdate}')
(
 sku_id
,strategy_id
,tenant_id
,profile_id
,marketplace_id
,marketplace_name
,currency_code
,seller_id
,seller_name
,adv_manager_id
,adv_manager_name
,seller_sku
,seller_asin
,top_parent_asin
,selling_price
,main_asin_url
,main_img_url
,title
,color
,size
,seller_sku_img_url
,stock_sale_days
,order_num_rank
,adv_days
,ad_group_cnt
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
,acos_label
,click_label
,cvr_label
,create_time
,ad_group_id_list
)
select  q1.sku_id
     ,q2.strategy_id
     ,q1.tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,adv_manager_id
     ,adv_manager_name
     ,seller_sku
     ,seller_asin
     ,top_parent_asin
     ,cast(24.99   as  decimal(18,6))   selling_price
     ,main_asin_url
     ,main_img_url
     ,title
     ,color
     ,size
     ,seller_sku_img_url
     ,cast(stock_sale_days as bigint) as stock_sale_days
     ,order_num_rank
     ,adv_days
     ,ad_group_cnt
     ,impressions
     ,clicks
     ,cast(cost as decimal(18,6)) as cost
     ,cast(sale_amt as decimal(18,6)) as  sale_amt
     ,order_num
     ,cast(ctr as decimal(18,6)) as  ctr
     ,cast(cvr as decimal(18,6)) as  cvr
     ,cast(cpc as decimal(18,6)) as  cpc
     ,cast(cpa  as decimal(18,6)) as cpa
     ,cast(acos as decimal(18,6)) as acos
     ,category
     ,cast(cate_ctr as decimal(18,6)) as cate_ctr
     ,cast(cate_cvr as decimal(18,6)) as cate_cvr
     ,cast(cate_cpc as decimal(18,6)) as cate_cpc
     ,cast(cate_cpa as decimal(18,6)) as cate_cpa
     ,cast(cate_acos as decimal(18,6)) as cate_acos
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.acos_label
     ,q1.click_label
     ,q1.cvr_label
     ,getdate() as create_time
     ,q3.ad_group_list
from dws_mkt_adv_strategy_add_seller_sku_detail_tmp04 q1
    inner join dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
on q1.tenant_id = q2.tenant_id
    and q1.life_cycle_label = q2.life_cycle_label
    and q1.goal_label = q2.goal_label
    and q1.click_label = q2.click_label
    and q1.cvr_label = q2.cvr_label
    and q1.term_type_label = q2.term_type_label
    and q1.acos_label = q2.acos_label
    inner join dws_mkt_adv_strategy_add_seller_sku_detail_tmp04_2 q3
    on q1.sku_id = q3.sku_id
where q2.action_name = '添加推广品' and q2.life_cycle_label = '成熟期'
;