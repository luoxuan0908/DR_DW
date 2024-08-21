--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 21:02:37
--********************************************************************--

--目标标签、生命周期、投放类型标签、库存标签、操作对象标签、自然坑位排名标签、点击标签、转化标签、广告坑位排名标签、关联坑位排名标签、垄断标签
CREATE TABLE IF NOT EXISTS whde.dws_mkt_adv_strategy_neg_word_product_detail_ds(
    cost DECIMAL(18,6) COMMENT '花费',
    sale_amt DECIMAL(18,6) COMMENT '销售额',
    order_num BIGINT COMMENT '销量',
    cvr DECIMAL(18,6) COMMENT 'CVR',
    cpc DECIMAL(18,6) COMMENT 'CPC',
    acos DECIMAL(18,6) COMMENT 'ACOS',
    category STRING COMMENT '类目',
    cate_cvr DECIMAL(18,6) COMMENT '类目CVR',
    cate_cpc DECIMAL(18,6) COMMENT '类目CPC',
    cate_acos DECIMAL(18,6) COMMENT '类目ACOS',
    aba_rank BIGINT COMMENT 'aba排名',
    aba_date DATETIME COMMENT 'aba日期',
    norm_rank BIGINT COMMENT '自然坑位排名',
    adv_rank BIGINT COMMENT '广告坑位排名',
    adv_days BIGINT COMMENT '广告天数',
    ad_type STRING COMMENT '广告类型',
    life_cycle_label STRING COMMENT '生命周期标签',
    goal_label STRING COMMENT '目标标签',
    ad_mode_label STRING COMMENT '投放类型',
    stock_label STRING COMMENT '库存标签',
    term_type_label STRING COMMENT '搜索对象标签',
    norm_rank_label STRING COMMENT '自然搜索坑位排名标签',
    adv_rank_label STRING COMMENT '广告搜索坑位排名标签',
    relation_rank_label STRING COMMENT '关联广告坑位排名标签',
    click_label STRING COMMENT '点击标签',
    cvr_label STRING COMMENT '转化标签',
    sale_monopoly_label STRING COMMENT '垄断标签',
    action_type STRING COMMENT '操作类型',
    create_time DATETIME COMMENT '创建时间',
    search_asin_img_url STRING COMMENT '搜索品图片',
    ctr DECIMAL(18,6) COMMENT 'ctr',
    cate_ctr DECIMAL(18,6) COMMENT '类目ctr'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告策略否词否品(子表)')
    LIFECYCLE 366;

--标记广告活动的CPA
drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03;
create table dws_mkt_adv_strategy_neg_word_product_detail_tmp03 as
select tenant_id
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
     ,ad_group_num
     ,ad_group_id_list
     ,parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,term_type
     ,search_term
     ,impressions
     ,clicks
     ,cost
     ,sale_amt
     ,order_num
     ,ctr
     ,cvr
     ,cpc
     ,acos
     ,category_list
     ,cate_impressions
     ,cate_clicks
     ,cate_cost
     ,cate_sale_amt
     ,cate_order_num
     ,cate_ctr
     ,cate_cvr
     ,cate_cpc
     ,cate_acos
     ,aba_rank
     ,aba_date
     ,adv_days
     ,case when order_num = 0 then 999 else cost / order_num end as cpa
from  whde.adm_amazon_adv_strategy_search_term_d
where ds ='${bizdate}' --T日
  and adv_days >= 14  and nvl(parent_asin,'') <> ''
;


--关联坑位标签
drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03_1;
create table dws_mkt_adv_strategy_neg_word_product_detail_tmp03_1 as
select marketplace_id as market_place_id
     ,search_term as child_asin                         --详情页asin
     ,top_parent_asin as similar_parent_asin --关联坑位父asin
     ,nvl(adv_rank_label,0) as relation_label
from whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds
where ds = max_pt('whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds') and term_type = '搜索品'
;


--asin详情爬虫清单
drop table if exists dws_mkt_adv_strategy_search_term_product_corw_list;
create table dws_mkt_adv_strategy_search_term_product_corw_list as
select market_place_id as marketplace_id
     ,child_asin as search_term
from dws_mkt_adv_strategy_neg_word_product_detail_tmp03_1
group by market_place_id
       ,child_asin
;



--历史否词否品，不重复推荐
drop table if exists dws_mkt_adv_strategy_neg_word_his_tmp;
create table dws_mkt_adv_strategy_neg_word_his_tmp as
select  profile_id
     ,campaign_id
     ,keyword_text as search_term
     ,tenant_id
from whde.adm_amazon_adv_neg_keyword_status_df
where ds ='${bizdate}'   and status = 'ENABLED'
group by profile_id
       ,campaign_id
       ,keyword_text
       ,tenant_id

union all

select  profile_id
     ,campaign_id
     ,asin
     ,tenant_id
from whde.adm_amazon_adv_neg_product_status_df
where ds ='${bizdate}'   and status = 'ENABLED'
group by profile_id
       ,campaign_id
       ,asin
       ,tenant_id
;



--搜索词排名
drop table if exists dws_mkt_adv_strategy_search_term_rank_tmp;
create table dws_mkt_adv_strategy_search_term_rank_tmp as
select  top_parent_asin as parent_asin
     ,search_term
     ,norm_rank_label as p1_norm_label
     ,adv_rank_label as p1_adv_label
     ,cast(adv_rank as bigint) as adv_rank
     ,cast(norm_rank as bigint) as norm_rank
     ,marketplace_id
from whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds
where ds = max_pt('whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds') and term_type = '搜索词'
;


--搜索词爬虫清单
drop table if exists dws_mkt_adv_strategy_search_term_corw_list;
create table dws_mkt_adv_strategy_search_term_corw_list as
select marketplace_id
     ,search_term
from dws_mkt_adv_strategy_search_term_rank_tmp
group by marketplace_id
       ,search_term
;


--搜索词垄断标签
drop table if exists dws_mkt_adv_strategy_itm_egd_amazon_manual_dl_tmp;
create table dws_mkt_adv_strategy_itm_egd_amazon_manual_dl_tmp as
select  marketplace_id
     ,keyword as search_term
     ,case when sale_monopoly_rate > 0.1 then 1 else 0 end sale_monopoly_label
from whde.dws_mkt_adv_strategy_keyword_index_ds
where ds = max_pt('whde.dws_mkt_adv_strategy_keyword_index_ds')
;


--库存充足
drop table if exists dws_mkt_adv_strategy_stock_tmp;
create table dws_mkt_adv_strategy_stock_tmp as
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
     ,category
     ,term_type
     ,cate_impressions_n30d  as cate_impressions
     ,cate_clicks_n30d  as cate_clicks
     ,cate_cost_n30d  as cate_cost
     ,cate_sale_amt_n30d  as cate_sale_amt
     ,cate_order_num_n30d  as cate_order_num
     ,cate_ctr_n30d  as cate_ctr
     ,cate_cvr_n30d  as cate_cvr
     ,cate_cpc_n30d  as cate_cpc
     ,cate_cpa_n30d  as cate_cpa
     ,cate_acos_n30d  as cate_acos
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
from    whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = '${bizdate}' and life_cycle_label = '成熟期' and stock_label = '库存充足'
;


--90天出单数
drop table if exists dws_mkt_adv_strategy_90_order_num_tmp;
create table dws_mkt_adv_strategy_90_order_num_tmp as
select   tenant_id
     ,profile_id
     ,seller_id
     ,campaign_id
     ,search_term
     ,sum(impressions) as impressions
     ,sum(clicks) as clicks
     ,sum(cost) as cost
     ,sum(w7d_sale_amt) as sale_amt
     ,sum(w7d_units_sold_clicks) as order_num
     ,case when sum(impressions) = 0 then null else sum(clicks)/sum(impressions) end as ctr
     ,case when sum(clicks) = 0 then null else sum(w7d_units_sold_clicks)/sum(clicks) end as cvr
     ,case when sum(w7d_sale_amt) = 0 then null else sum(cost)/sum(w7d_sale_amt) end as acos
     ,case when sum(w7d_units_sold_clicks) = 0 then null else sum(cost)/sum(w7d_units_sold_clicks) end as cpa
from    whde.dwd_mkt_adv_amazon_sp_search_term_ds
where   ds >= to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-90,'dd'),'yyyymmdd')
group by tenant_id
       ,profile_id
       ,seller_id
       ,campaign_id
       ,search_term
;

--asin图片
drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp_asinimg;
create table dws_mkt_adv_strategy_neg_word_product_detail_tmp_asinimg as
select marketplace_id
     ,asin
     ,main_image_url as search_asin_img_url
from whde.dws_itm_sku_amazon_asin_index_df
where ds = max_pt('whde.dws_itm_sku_amazon_asin_index_df')
;

--关联属性
drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp05_3;
create table dws_mkt_adv_strategy_neg_word_product_detail_tmp05_3 as
select  q1.tenant_id
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
     ,q1.ad_group_num
     ,q1.ad_group_id_list
     ,q1.parent_asin
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.term_type
     ,q1.search_term
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q2.category
     ,q2.cate_impressions
     ,q2.cate_clicks
     ,q2.cate_cost
     ,q2.cate_sale_amt
     ,q2.cate_order_num
     ,q2.cate_ctr
     ,q2.cate_cvr
     ,q2.cate_cpc
     ,q2.cate_acos
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_days
     ,q2.stock_label
     ,nvl(q3.p1_norm_label,0)  p1_norm_label
     ,nvl(q3.p1_adv_label,0)  p1_adv_label
     ,nvl(q3.norm_rank,999)  norm_rank
     ,nvl(q3.adv_rank,999)  adv_rank
     ,q6.sale_monopoly_label
     ,nvl(q7.relation_label,0)  relation_label
     ,q10.order_num as n90d_order_num
     ,q10.clicks as n90d_clicks
     ,q2.cate_cvr_n90d
     ,case when q1.cpa > 1.3 * q2.cate_cpa then 1 else 0 end as if_neg
from dws_mkt_adv_strategy_neg_word_product_detail_tmp03 q1
         inner join dws_mkt_adv_strategy_stock_tmp q2  --库存标签判断条件
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin and q1.term_type = q2.term_type
         left join dws_mkt_adv_strategy_search_term_rank_tmp q3 --自然排名和广告排名
                   on q1.marketplace_id = q3.marketplace_id and tolower(q1.search_term) = tolower(q3.search_term) and q1.parent_asin = q3.parent_asin
         left join dws_mkt_adv_strategy_neg_word_his_tmp q4 --否词记录表，否过的词就不要再推荐了
                   on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and q1.campaign_id = q4.campaign_id and tolower(q1.search_term) = tolower(q4.search_term)
         left join dws_mkt_adv_strategy_itm_egd_amazon_manual_dl_tmp q6 --垄断标签
                   on tolower(q1.search_term) = tolower(q6.search_term) and q1.marketplace_id = q6.marketplace_id
         left join dws_mkt_adv_strategy_neg_word_product_detail_tmp03_1 q7  --关联坑位标签
                   on q1.marketplace_id = q7.market_place_id and tolower(q1.search_term) = tolower(q7.child_asin) and q1.parent_asin = q7.similar_parent_asin
         left join dws_mkt_adv_strategy_90_order_num_tmp q10       --近90天出单数
                   on q1.tenant_id = q10.tenant_id and q1.profile_id = q10.profile_id and q1.campaign_id = q10.campaign_id and tolower(q1.search_term) = tolower(q10.search_term)
where  q4.search_term is null
;


----需要剔除的核心词
--drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp05_4;
--create table dws_mkt_adv_strategy_neg_word_product_detail_tmp05_4 as
--select search_term as keyword
--from whde.ods_asq_jdy_target_keyword
--where ds = max_pt('asq_dw.ods_asq_jdy_target_keyword') and (_widget_1699953513777 like '%大词%' or _widget_1699953513777 like '%核心%' or _widget_1699953513777 like '%类目词%')
--group by _widget_1699953513778
--;


--标签生产
drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_asq_final;
create table dws_mkt_adv_strategy_neg_word_product_detail_asq_final as
select  tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,adv_manager_id
     ,adv_manager_name
     ,ad_type
     ,campaign_id
     ,campaign_name
     ,ad_mode
     ,ad_group_name
     ,ad_group_id_list
     ,parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,term_type
     ,search_term
     ,impressions
     ,clicks
     ,cost
     ,sale_amt
     ,order_quantity
     ,ctr
     ,cvr
     ,cpc
     ,acos
     ,cat_key
     ,cate_impressions
     ,cate_clicks
     ,cate_cost
     ,cate_sale_amt
     ,cate_order_num
     ,cate_ctr
     ,cvr_cat
     ,cpc_cat
     ,acos_cat
     ,search_frequency_rank
     ,aba_date
     ,adv_days
     ,norm_rank
     ,adv_rank
     ,life_cycle
     ,goal_label
     ,stock_label
     ,norm_rank_label
     ,case when term_type = '搜索词' and stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and cvr_label = '不出单' then 'ALL' else adv_rank_label end as adv_rank_label
     ,case when term_type = '搜索品' and stock_label = '库存充足' and cvr_label = '不出单' then 'ALL' else relation_rank_label end as relation_rank_label
     ,click_label
     ,cvr_label
     ,sale_monopoly_label
from (
         select  tenant_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,currency_code
              ,seller_id
              ,seller_name
              ,adv_manager_id
              ,adv_manager_name
              ,ad_type
              ,campaign_id
              ,campaign_name
              ,ad_mode
              ,cast(null as string) as ad_group_name
              ,ad_group_id_list
              ,parent_asin
              ,selling_price
              ,main_asin_url
              ,main_image_url
              ,term_type
              ,case when term_type = '搜索品' then toupper(search_term) else search_term end as search_term
              ,impressions
              ,clicks
              ,cost
              ,sale_amt
              ,order_num as order_quantity
              ,ctr
              ,cvr
              ,cpc
              ,acos
              ,category as cat_key
              ,cate_impressions
              ,cate_clicks
              ,cate_cost
              ,cate_sale_amt
              ,cate_order_num
              ,cate_ctr
              ,cast(cate_cvr  as decimal(18,6)) as cvr_cat
              ,cast(cate_cpc  as decimal(18,6)) as cpc_cat
              ,cast(cate_acos as decimal(18,6)) as acos_cat
              ,aba_rank  as search_frequency_rank
              ,aba_date
              ,adv_days
              ,case when term_type = '搜索品' then null else norm_rank end as norm_rank
              ,case when term_type = '搜索品' then null else adv_rank end as adv_rank
              ,'成熟期' as life_cycle    --生命周期
              ,'利润最大化' as goal_label --目标标签
              ,stock_label     --库存标签
              ,case when term_type = '搜索品' then null when p1_norm_label = 1 then '自然排名第一页' when p1_norm_label = 0 then '自然排名非第一页' end as norm_rank_label         --自然搜索坑位排名标签
              ,case when term_type = '搜索品' then null when p1_adv_label = 1 then '广告排名第一页' when p1_adv_label = 0 then '广告排名非第一页' end as adv_rank_label           --广告搜索坑位排名标签
              ,case when term_type = '搜索词' then null when relation_label = 1 then '广告排名第一页' when relation_label = 0 then '广告排名非第一页' end as relation_rank_label    --关联广告坑位排名
              ,case when n90d_clicks < (1/cate_cvr_n90d) * 1.2 then '无效点击' when clicks >= (1/cate_cvr) * 1.2 then '有效点击' end as click_label --点击标签
              ,case when n90d_order_num = 0 then '不出单'
                    when cvr < cate_cvr * 0.5 then '低转化'
                    when cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
                    when cvr >= cate_cvr then '高转化' end as cvr_label --转化标签
              ,case when term_type = '搜索品' then null when sale_monopoly_label = 1 then '销售垄断' end as sale_monopoly_label
         from dws_mkt_adv_strategy_neg_word_product_detail_tmp05_3
         where cate_cvr <> 0 and if_neg = 1 and cate_cvr_n90d <> 0
     )q1
--left join dws_mkt_adv_strategy_neg_word_product_detail_tmp05_4 q2  --过滤核心词
--on toupper(q1.search_term) = toupper(q2.keyword)
--where q2.keyword is null
;


--支持重跑
alter table dws_mkt_adv_strategy_neg_word_product_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_neg_word_product_detail_ds partition (ds = '${nowdate}')
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
        ,adv_manager_id
        ,adv_manager_name
        ,campaign_id
        ,campaign_name
        ,ad_group_name_list
        ,ad_group_id_list
        ,top_parent_asin
        ,main_asin_url
        ,main_img_url
        ,search_term
        ,clicks
        ,cost
        ,sale_amt
        ,order_num
        ,cvr
        ,cpc
        ,acos
        ,category
        ,cate_cvr
        ,cate_cpc
        ,cate_acos
        ,aba_rank
        ,aba_date
        ,norm_rank
        ,adv_rank
        ,adv_days
        ,ad_type
        ,life_cycle_label
        ,goal_label
        ,ad_mode_label
        ,stock_label
        ,term_type_label
        ,norm_rank_label
        ,adv_rank_label
        ,relation_rank_label
        ,click_label
        ,cvr_label
        ,sale_monopoly_label
        ,action_type
        ,create_time
        ,search_asin_img_url
        ,ctr
        ,cate_ctr
)
select  q1.tenant_id
     ,abs(hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.parent_asin,q1.search_term,q2.strategy_id))) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_name
     ,q1.ad_group_id_list
     ,q1.parent_asin
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.search_term
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_quantity
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q1.cat_key
     ,q1.cvr_cat
     ,q1.cpc_cat
     ,q1.acos_cat
     ,q1.search_frequency_rank
     ,q1.aba_date
     ,q1.norm_rank
     ,q1.adv_rank
     ,q1.adv_days
     ,q1.ad_type
     ,q1.life_cycle
     ,q1.goal_label
     ,q1.ad_mode
     ,q1.stock_label
     ,q1.term_type
     ,q1.norm_rank_label
     ,q1.adv_rank_label
     ,q1.relation_rank_label
     ,q1.click_label
     ,q1.cvr_label
     ,q1.sale_monopoly_label
     ,q2.action_type
     ,getdate() as gmt_create
     ,q3.search_asin_img_url
     ,q1.ctr
     ,q1.cate_ctr
from dws_mkt_adv_strategy_neg_word_product_detail_asq_final q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and nvl(q1.stock_label,' ') = nvl(q2.stock_label,' ')
                        and nvl(q1.norm_rank_label,' ') = nvl(q2.norm_rank_label,' ')
                        and nvl(q1.adv_rank_label,' ') = nvl(q2.adv_rank_label,' ')
                        and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                        and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and nvl(q1.sale_monopoly_label,' ') = nvl(q2.sale_monopoly_label,' ')
                        and q1.term_type = q2.term_type_label
                        and q1.ad_mode = q2.ad_mode_label
                        and nvl(q1.relation_rank_label,' ') = nvl(q2.relation_rank_label,' ')
         left join dws_mkt_adv_strategy_neg_word_product_detail_tmp_asinimg q3
                   on q1.marketplace_id = q3.marketplace_id and tolower(q1.search_term) = tolower(q3.asin)
where q2.action_name = '精准否定' and q2.life_cycle_label = '成熟期' --and concat(q1.click_label,'_',q1.cvr_label,'_',q1.ad_mode) <> '无效点击_不出单_自动投放'
;




--删除临时表
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp01;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp02;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03_1;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03_2;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03_3;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp03_4;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp04_1;
-- drop table if exists dws_mkt_adv_strategy_neg_word_product_detail_tmp04_2;