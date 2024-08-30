--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:09:56
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dws_mkt_adv_strategy_add_word_product_detail_ds(
    tenant_id STRING COMMENT '租户ID',
    row_id STRING COMMENT '行级策略明细唯一ID',
    strategy_id STRING COMMENT '策略ID',
    profile_id STRING COMMENT '配置ID',
    marketplace_id STRING COMMENT '站点ID',
    marketplace_name STRING COMMENT '站点名称',
    currency_code STRING COMMENT '币种',
    ad_type STRING COMMENT '广告类型',
    seller_id STRING COMMENT '卖家ID',
    seller_name STRING COMMENT '卖家名称',
    adv_manager_id STRING COMMENT '广告负责人ID',
    adv_manager_name STRING COMMENT '广告负责人名称',
    camp_id_list STRING COMMENT '原广告活动id列表',
    camp_name_list STRING COMMENT '原广告活动名称列表',
    campaign_name_new STRING COMMENT '目标广告活动名称',
    ad_group_name_new STRING COMMENT '目标广告组名称',
    target_sku_list STRING COMMENT '推广的sku列表',
    match_type_new STRING COMMENT '目标匹配类型',
    top_parent_asin STRING COMMENT '父aisn',
    selling_price STRING COMMENT '售价',
    main_asin_url STRING COMMENT '商品链接',
    main_img_url STRING COMMENT '商品主图',
    search_term STRING COMMENT '搜索词/品',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '广告花费',
    sale_amt DECIMAL(18,6) COMMENT '广告销售额',
    order_num BIGINT COMMENT '广告销量',
    ctr DECIMAL(18,6) COMMENT 'CTR',
    cvr DECIMAL(18,6) COMMENT 'CVR',
    cpc DECIMAL(18,6) COMMENT 'CPC',
    acos DECIMAL(18,6) COMMENT 'ACOS',
    category STRING COMMENT '父ASIN所属类目',
    cate_ctr DECIMAL(18,6) COMMENT '父ASIN所属类目平均CTR',
    cate_cvr DECIMAL(18,6) COMMENT '父ASIN所属类目平均CVR',
    cate_cpc DECIMAL(18,6) COMMENT '父ASIN所属类目平均CPC',
    cate_acos DECIMAL(18,6) COMMENT '父ASIN所属类目平均ACOS',
    aba_rank BIGINT COMMENT 'aba排名',
    aba_date DATETIME COMMENT 'aba日期',
    norm_rank BIGINT COMMENT '自然搜索排名',
    adv_rank BIGINT COMMENT '广告搜索排名',
    adv_days BIGINT COMMENT '广告指标统计天数',
    life_cycle_label STRING COMMENT '生命周期',
    goal_label STRING COMMENT '目标标签',
    stock_label STRING COMMENT '库存标签',
    ad_mode_label STRING COMMENT '投放类型标签',
    term_type_label STRING COMMENT '投放对象类型',
    norm_rank_label STRING COMMENT '自然搜索排名标签',
    click_label STRING COMMENT '点击标签',
    cvr_label STRING COMMENT '转化标签',
    adv_rank_label STRING COMMENT '广告搜索排名标签',
    relation_rank_label STRING COMMENT '关联坑位排名标签',
    flow_label STRING COMMENT '流量标签',
    cpc_label STRING COMMENT 'cpc标签',
    action_type STRING COMMENT '操作类型',
    create_time DATETIME COMMENT '创建时间',
    ad_group_cnt BIGINT COMMENT '已投放广告组个数',
    ad_group_name_list STRING COMMENT '已投放广告组列表',
    sale_monopoly_rate DECIMAL(18,6) COMMENT 'TOP3商品转化份额',
    chn_seller_rate DECIMAL(18,6) COMMENT '搜索词前三页中国卖家占比',
    search_asin_img_url STRING COMMENT '搜索品图片'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告策略添加大小词、投放品(子表)')
    LIFECYCLE 366;

--无效点击的场景下限制出单数>=2
--词推荐精准匹配的时候都放出中国卖家占比、销售垄断比例
--加词加品偶然性剔除：点击量>=3

--基础数据有匹配类型需往上汇一层
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp01;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp01 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,ad_type
     ,ad_mode
     ,parent_asin
     ,concat_ws('_&_',array_distinct(split(wm_concat(distinct '_&_',camp_name_list),'_&_'))) as camp_name_list
     ,concat_ws('_&_',array_distinct(split(wm_concat(distinct '_&_',camp_id_list),'_&_'))) as camp_id_list
     ,max(selling_price) as selling_price
     ,max(main_asin_url) as main_asin_url
     ,max(main_image_url) as main_image_url
     ,term_type
     ,search_term
     ,sum(impressions) as impressions
     ,sum(clicks) as clicks
     ,sum(cost) as cost
     ,sum(sale_amt) as sale_amt
     ,sum(sale_num) as sale_num
     ,cast(case when sum(impressions) <> 0 then sum(clicks) / sum(impressions) else null end as decimal(18,6)) as ctr
     ,cast(case when sum(clicks) <> 0 then sum(sale_num) / sum(clicks) else null end as decimal(18,6)) as cvr
     ,cast(case when sum(clicks) <> 0 then sum(cost) / sum(clicks) else null end as decimal(18,6)) as cpc
     ,cast(case when sum(sale_amt) <> 0 then sum(cost) / sum(sale_amt) else null end as decimal(18,6)) as acos
     ,cast(case when sum(sale_num) <> 0 then sum(cost) / sum(sale_num) else null end as decimal(18,6)) as cpa
     ,min(aba_rank) as aba_rank
     ,min(aba_date) as aba_date
     ,max(adv_days) as adv_days
from whde.adm_amazon_adv_strategy_pasin_search_term_d
where ds = '${bizdate}' --T日
  and adv_days >= 14
  and nvl(parent_asin,'') <> ''
  and ad_mode = '自动投放'
group by tenant_id
       ,profile_id
       ,marketplace_id
       ,marketplace_name
       ,currency_code
       ,seller_id
       ,seller_name
       ,ad_type
       ,ad_mode
       ,parent_asin
       ,term_type
       ,search_term
;

--建议对象筛选
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp02;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp02 as
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

select * from dws_mkt_adv_strategy_add_word_product_detail_tmp02 where ds = '20240722';




--关联坑位标签
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp03;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp03 as
select marketplace_id as market_place_id
     ,search_term as child_asin                         --详情页asin
     ,top_parent_asin as similar_parent_asin --关联坑位父asin
     ,adv_rank_label as relation_label
from whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds
where ds = max_pt('whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds') and term_type = '搜索品'
;


--搜索品爬取清单
drop table if exists dws_mkt_adv_strategy_add_word_product_corw_list_tmp;
create table dws_mkt_adv_strategy_add_word_product_corw_list_tmp as
select market_place_id as marketplace_id
     ,child_asin as search_term
from dws_mkt_adv_strategy_add_word_product_detail_tmp03
group by market_place_id
       ,child_asin
;


--历史投放词品，不重复推荐
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp04;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp04 as
select tenant_id
     ,profile_id
     ,parent_asin
     ,search_term
from(
        select   profile_id
             ,parent_asin
             ,keyword_text as search_term
             ,tenant_id
        from    whde.adm_amazon_adv_keyword_target_status_df
        where   ds = '${bizdate}'-- max_pt ('whde.adm_amazon_adv_keyword_target_status_df')
          and     status = 'ENABLED'
        union all
        select   profile_id
             ,parent_asin
             ,asin
             ,tenant_id
        from    whde.adm_amazon_adv_product_target_status_df
        where   ds = '${bizdate}'-- max_pt ('whde.adm_amazon_adv_product_target_status_df')
          and     status = 'ENABLED'
    )
group by tenant_id
       ,profile_id
       ,parent_asin
       ,search_term
;



--搜索词排名
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp05;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp05 as
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
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp05_1;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp05_1 as
select  search_term
     ,marketplace_id
from   dws_mkt_adv_strategy_add_word_product_detail_tmp05
group by search_term
       ,marketplace_id
;



--大小词标签、搜索词的垄断和中国卖家占比指标
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_bigterm_tmp;
create table dws_mkt_adv_strategy_add_word_product_detail_bigterm_tmp as
select  marketplace_id
     ,keyword as search_term
     ,case when aba_rank < 50000 then 1 else 0 end as if_big
     ,case when aba_rank < 100000 then 1 else 0 end as if_big_asq
     ,cast(sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
--  ,cast(chn_seller_rate as decimal(18,6)) as chn_seller_rate
from whde.dws_mkt_adv_strategy_keyword_index_ds
where ds = max_pt('whde.dws_mkt_adv_strategy_keyword_index_ds')
;


--搜索词的垄断和中国卖家占比指标
-- drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp06;
-- create table dws_mkt_adv_strategy_add_word_product_detail_tmp06 as
-- select marketplace_id
--       ,keyword as search_term
--       ,cast(sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
--       ,cast(chn_seller_rate as decimal(18,6)) as chn_seller_rate
-- from whde.dws_mkt_adv_strategy_keyword_index_ds
-- where ds = '${bizdate}'
-- ;


--asin图片
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp07;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp07 as
select marketplace_id
     ,asin
     ,main_image_url as search_asin_img_url
from whde.dws_itm_sku_amazon_asin_index_df
where ds = max_pt('whde.dws_itm_sku_amazon_asin_index_df')
;

--确定推广品(销量TOP10)
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp08_0;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp08_0 as
select  tenant_id
     ,profile_id
     ,parent_asin
     ,wm_concat(distinct '_/_',advertised_sku) sku_list
from (
         select   tenant_id
              ,profile_id
              ,parent_asin
              ,advertised_sku
              ,row_number() over (partition by tenant_id,profile_id,parent_asin order by order_num desc ) rn
         from (
                  select   tenant_id
                       ,profile_id
                       ,parent_asin
                       ,advertised_sku
                       ,sum(order_num) order_num
                  from    whde.adm_amazon_adv_sku_wide_d
                  where   ds = max_pt('whde.adm_amazon_adv_sku_wide_d')  --and asin_fba_stock_num >= 5
                  group by tenant_id
                         ,profile_id
                         ,parent_asin
                         ,advertised_sku
                  having order_num > 0
              )
     )
where  rn <= 10
group by  tenant_id
       ,profile_id
       ,parent_asin
;

drop table if exists dws_mkt_adv_strategy_add_word_product_detail_tmp08_1;
create table dws_mkt_adv_strategy_add_word_product_detail_tmp08_1 as
select  q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q1.ad_mode
     ,q1.parent_asin
     ,q6.adv_manager_id
     ,q6.adv_manager_name
     ,q1.camp_name_list
     ,q1.camp_id_list
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.term_type
     ,q1.search_term
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.sale_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q6.category as category_list
     ,q6.cate_impressions
     ,q6.cate_clicks
     ,q6.cate_cost
     ,q6.cate_sale_amt
     ,q6.cate_order_num
     ,q6.cate_ctr
     ,q6.cate_cvr
     ,q6.cate_cpc
     ,q6.cate_acos
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_days
     ,q6.stock_label
     ,case when q8.search_term is null then null when q8.search_term is not null then nvl(q3.p1_norm_label,0) end as p1_norm_label
     ,case when q8.search_term is null then null when q8.search_term is not null then nvl(q3.p1_adv_label,0) end as p1_adv_label
     ,case when q8.search_term is null then null when q8.search_term is not null then nvl(q3.norm_rank,999) end as norm_rank
     ,case when q8.search_term is null then null when q8.search_term is not null then nvl(q3.adv_rank,999) end as adv_rank
     ,case when q9.search_term is null then null when q9.search_term is not null then nvl(q5.relation_label,0) end as relation_label
     ,nvl(q2.if_big,0) as if_big_term
     ,q7.sku_list
from dws_mkt_adv_strategy_add_word_product_detail_tmp01 q1
         left join dws_mkt_adv_strategy_add_word_product_detail_bigterm_tmp q2
                   on q1.marketplace_id = q2.marketplace_id and tolower(q1.search_term) = tolower(q2.search_term)
         left join dws_mkt_adv_strategy_add_word_product_detail_tmp05 q3 --自然排名和广告排名
                   on q1.marketplace_id = q3.marketplace_id and tolower(q1.search_term) = tolower(q3.search_term) and q1.parent_asin = q3.parent_asin
         left join dws_mkt_adv_strategy_add_word_product_detail_tmp04 q4 --加词加品记录表，不重复推荐
                   on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and tolower(q1.search_term) = tolower(q4.search_term)
         left join dws_mkt_adv_strategy_add_word_product_detail_tmp03 q5  --关联坑位标签
                   on q1.marketplace_id = q5.market_place_id and tolower(q1.search_term) = tolower(q5.child_asin) and q1.parent_asin = q5.similar_parent_asin
         inner join dws_mkt_adv_strategy_add_word_product_detail_tmp02 q6  --库存标签判断条件
                    on q1.tenant_id = q6.tenant_id and q1.profile_id = q6.profile_id and q1.parent_asin = q6.top_parent_asin and q1.term_type = q6.term_type
         inner join dws_mkt_adv_strategy_add_word_product_detail_tmp08_0 q7 --推广品列表
                    on q1.tenant_id = q7.tenant_id and q1.profile_id = q7.profile_id and q1.parent_asin = q7.parent_asin
         left join dws_mkt_adv_strategy_add_word_product_detail_tmp05_1 q8  --爬虫清单
                   on q1.marketplace_id = q8.marketplace_id and tolower(q1.search_term) = tolower(q8.search_term)
         left join dws_mkt_adv_strategy_add_word_product_corw_list_tmp q9 --搜索品爬虫清单
                   on q1.marketplace_id = q9.marketplace_id and tolower(q1.search_term) = tolower(q9.search_term)
where q4.search_term is null
;


--标签
drop table if exists dws_mkt_adv_strategy_add_word_product_detail_zt_final;
create table dws_mkt_adv_strategy_add_word_product_detail_zt_final as
select  tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,ad_type
     ,ad_mode_label
     ,top_parent_asin
     ,camp_name_list
     ,camp_id_list
     ,new_camp_name
     ,new_group_name
     ,new_match_type
     ,sku_list
     ,adv_manager_id
     ,adv_manager_name
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,term_type_label
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
     ,category
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
     ,norm_rank
     ,adv_rank
     ,life_cycle_label
     ,goal_label
     ,stock_label
     ,norm_rank_label
     ,case when term_type_label = '搜索词' and stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '无效点击' and cvr_label <> '不出单' then 'ALL'
           when term_type_label = '搜索词' and stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '有效点击' and cvr_label = '高转化' then 'ALL'  else adv_rank_label end as adv_rank_label
     ,case when term_type_label = '搜索品' and stock_label = '库存充足' and click_label = '无效点击' and cvr_label <> '不出单' then 'ALL'
           when term_type_label = '搜索品' and stock_label = '库存充足' and click_label = '有效点击' and cvr_label = '高转化' then 'ALL' else relation_rank_label end as relation_rank_label
     ,click_label
     ,case when term_type_label = '搜索词' and stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '无效点击' and order_num >= 1 then '出单'
           when term_type_label = '搜索品' and stock_label = '库存充足' and click_label = '无效点击' and order_num >= 1 then '出单'
           when term_type_label = '搜索品' and stock_label = '库存充足' and click_label = '无效点击' and order_num =0  then '不出单'
           else cvr_label end as cvr_label
     ,flow_label
     ,case when term_type_label = '搜索词' and stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '有效点击' and cvr_label = '一般转化' and adv_rank_label = '广告排名第一页' then cpc_label
           when term_type_label = '搜索品' and stock_label = '库存充足' and click_label = '有效点击' and cvr_label = '一般转化' and relation_rank_label = '广告排名第一页' then cpc_label else null end as cpc_label
from (
         select  tenant_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,currency_code
              ,seller_id
              ,seller_name
              ,ad_type
              ,ad_mode as ad_mode_label
              ,parent_asin as top_parent_asin
              ,camp_name_list
              ,camp_id_list
              ,case when term_type = '搜索词' and if_big_term = 1 then concat('SP_AI_添加投放大词_手动宽泛_',seller_name,'_',parent_asin,'_',replace(substr(search_term,1,50),' ','_'))  --大词单独一个广告活动
                    when term_type = '搜索词' and if_big_term = 0 then concat('SP_AI_添加投放小词_手动精准_',seller_name,'_',parent_asin)  --小词共享一个广告活动
                    when term_type = '搜索品' then concat('SP_AI_添加投放品_手动精准_',seller_name,'_',parent_asin) END as new_camp_name   --投放品共享一个广告活动
              ,case when term_type = '搜索词' then concat('SP_AI_添加投放词_',replace(substr(search_term,1,50),' ','_'))
                    when term_type = '搜索品' then concat('SP_AI_添加投放品_',toupper(replace(substr(search_term,1,50),' ','_'))) END new_group_name
              ,case when term_type = '搜索词' and if_big_term = 1 THEN '广泛'
                    when term_type = '搜索词' and if_big_term = 0 THEN '精准'
                    when term_type = '搜索品' then '精准' END new_match_type
              ,sku_list
              ,adv_manager_id
              ,adv_manager_name
              ,selling_price
              ,main_asin_url
              ,main_image_url
              ,term_type as term_type_label
              ,case when term_type = '搜索品' then toupper(search_term) else search_term end as search_term
              ,impressions
              ,clicks
              ,cost
              ,sale_amt
              ,sale_num as order_num
              ,ctr
              ,cvr
              ,cpc
              ,acos
              ,category_list as category
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
              ,case when term_type = '搜索品' then null else norm_rank end as norm_rank
              ,case when term_type = '搜索品' then null else adv_rank end as adv_rank
              ,'成熟期' as life_cycle_label    --生命周期
              ,'利润最大化' as goal_label --目标标签
              ,stock_label     --库存标签
              ,case when term_type = '搜索品' then null when p1_norm_label = 1 then '自然排名第一页' else '自然排名非第一页' end as norm_rank_label         --自然搜索坑位排名标签
              ,case when term_type = '搜索品' then null when p1_adv_label = 1 then '广告排名第一页'else '广告排名非第一页' end as adv_rank_label           --广告搜索坑位排名标签
              ,case when term_type = '搜索词' then null when relation_label = 1 then '广告排名第一页' else '广告排名非第一页' end as relation_rank_label    --关联广告坑位排名标签
              ,case when clicks < 1/cate_cvr then '无效点击' when clicks >= 1/cate_cvr then '有效点击' end as click_label --点击标签
              ,case when  cvr < cate_cvr * 0.5 then '低转化'
                    when  cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
                    when  cvr >= cate_cvr then '高转化'
             end as cvr_label --转化标签
              ,case when term_type = '搜索品' then null when if_big_term = 1 then '大词' when if_big_term = 0 then '小词' end as flow_label  --流量标签
              ,case when cpc < cate_cpc then '低CPC' when cpc >= cate_cpc then '高CPC' end as cpc_label
         from dws_mkt_adv_strategy_add_word_product_detail_tmp08_1
         where cate_cvr <> 0 and clicks >= 3
     ) q1
;




--支持重跑
alter table dws_mkt_adv_strategy_add_word_product_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_add_word_product_detail_ds partition (ds = '${nowdate}')
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
        ,camp_id_list
        ,camp_name_list
        ,campaign_name_new
        ,ad_group_name_new
        ,target_sku_list
        ,match_type_new
        ,top_parent_asin
        ,selling_price
        ,main_asin_url
        ,main_img_url
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
        ,category
        ,cate_ctr
        ,cate_cvr
        ,cate_cpc
        ,cate_acos
        ,aba_rank
        ,aba_date
        ,norm_rank
        ,adv_rank
        ,adv_days
        ,life_cycle_label
        ,goal_label
        ,stock_label
        ,ad_mode_label
        ,term_type_label
        ,norm_rank_label
        ,click_label
        ,cvr_label
        ,adv_rank_label
        ,relation_rank_label
        ,flow_label
        ,cpc_label
        ,action_type
        ,create_time
        ,sale_monopoly_rate
        ,chn_seller_rate
        ,search_asin_img_url
)

select q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.new_camp_name,q1.new_group_name,q1.new_match_type,q1.top_parent_asin,q1.search_term,q2.strategy_id)) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.ad_type
     ,q1.seller_id
     ,q1.seller_name
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.camp_id_list
     ,q1.camp_name_list
     ,q1.new_camp_name
     ,q1.new_group_name
     ,q1.sku_list
     ,q1.new_match_type
     ,q1.top_parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.search_term
     ,q1.impressions
     ,q1.clicks
     ,cast(q1.cost  as decimal(18,6)) as cost
     ,cast(q1.sale_amt   as decimal(18,6)) as sale_amt
     ,q1.order_num
     ,cast(q1.ctr   as decimal(18,6)) as ctr
     ,cast(q1.cvr   as decimal(18,6)) as cvr
     ,cast(q1.cpc   as decimal(18,6)) as cpc
     ,cast(q1.acos   as decimal(18,6)) as acos
     ,q1.category
     ,cast(q1.cate_ctr   as decimal(18,6))  as cate_ctr
     ,cast(q1.cate_cvr   as decimal(18,6))  as cate_cvr
     ,cast(q1.cate_cpc   as decimal(18,6))  as cate_cpc
     ,cast(q1.cate_acos   as decimal(18,6)) as cate_acos
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.norm_rank
     ,q1.adv_rank
     ,q1.adv_days
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.stock_label
     ,q1.ad_mode_label
     ,q1.term_type_label
     ,q1.norm_rank_label
     ,q1.click_label
     ,q1.cvr_label
     ,q1.adv_rank_label
     ,q1.relation_rank_label
     ,q1.flow_label
     ,q1.cpc_label
     ,q2.action_type
     ,getdate() as create_time
     ,q3.sale_monopoly_rate
     ,0 --q3.chn_seller_rate
     ,q4.search_asin_img_url
from dws_mkt_adv_strategy_add_word_product_detail_zt_final q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and nvl(q1.stock_label,' ') = nvl(q2.stock_label,' ')
                        and nvl(q1.norm_rank_label,' ') = nvl(q2.norm_rank_label,' ')
                        and nvl(q1.adv_rank_label,' ') = nvl(q2.adv_rank_label,' ')
                        and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                        and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and nvl(q1.flow_label,' ') = nvl(q2.flow_label,' ')
                        and q1.term_type_label = q2.term_type_label
                        and q1.ad_mode_label = q2.ad_mode_label
                        and nvl(q1.cpc_label,' ') = nvl(q2.cpc_label,' ')
                        and nvl(q1.relation_rank_label,' ') = nvl(q2.relation_rank_label,' ')
         left join dws_mkt_adv_strategy_add_word_product_detail_bigterm_tmp q3
                   on q1.marketplace_id = q3.marketplace_id and tolower(q1.search_term) = tolower(q3.search_term)
         left join dws_mkt_adv_strategy_add_word_product_detail_tmp07 q4
                   on q1.marketplace_id = q4.marketplace_id and tolower(q1.search_term) = tolower(q4.asin)
where q2.action_name in ('添加投放大词','添加投放小词','添加投放品') and q2.life_cycle_label = '成熟期'
;

