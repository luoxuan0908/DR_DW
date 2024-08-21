--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 15:26:25
--********************************************************************--

create table if not exists dws_mkt_adv_strategy_neg_root_word_ds
(
    tenant_id         string comment '租户ID'
    ,stem_id           string comment '词根ID'
    ,strategy_id       string comment '策略ID(标签自由组合hashid)'
    ,profile_id        string comment '配置ID'
    ,marketplace_id    string comment '站点ID'
    ,marketplace_name  string comment '站点名称'
    ,currency_code     string comment '币种'
    ,ad_type           string comment '广告类型'
    ,seller_id         string comment '卖家ID'
    ,seller_name       string comment '卖家名称'
    ,adv_manager_id    string comment '广告负责人id'
    ,adv_manager_name  string comment '广告负责人名称'
    ,top_parent_asin   string comment '父aisn'
    ,selling_price     string comment '售价'
    ,main_asin_url     string comment '商品链接'
    ,main_img_url      string comment '商品主图'
    ,stem              string comment '词根'
    ,word              string comment '单词（前端展示）'
    ,clicks            bigint comment '点击量'
    ,cost              decimal(18,6) comment '广告花费'
    ,sale_amt          decimal(18,6) comment '广告销售额'
    ,order_num         bigint comment '广告销量'
    ,ctr               decimal(18,6) comment 'ctr'
    ,cvr               decimal(18,6) comment 'cvr'
    ,cpc               decimal(18,6) comment 'cpc'
    ,acos              decimal(18,6) comment 'acos'
    ,cate_ctr          decimal(18,6) comment '类目平均ctr'
    ,cate_cvr          decimal(18,6) comment '类目平均cvr'
    ,cate_cpc          decimal(18,6) comment '类目平均cpc'
    ,cate_acos         decimal(18,6) comment '类目平均acos'
    ,adv_days          bigint comment '广告天数'
    ,term_cnt          bigint comment '搜索词数量'
    ,search_term_list  string comment '搜索词列表(通过_/_拼接)'
    ,camp_id_list      string comment '广告活动id列表(通过_/_拼接)'
    ,camp_cnt          bigint comment '广告活动数量'
    ,life_cycle_label  string comment '生命周期标签'
    ,goal_label        string comment '目标标签'
    ,ad_mode_label     string comment '投放类型标签'
    ,term_type_label   string comment '操作对象标签'
    ,stock_label       string comment '库存标签'
    ,click_label       string comment '点击标签'
    ,cvr_label         string comment '转化标签'
    ,action_type       string comment '操作类型'
    ,create_time       datetime comment '创建时间'
    )
    partitioned by
(
    ds                 string
)
    stored as aliorc
    tblproperties ('comment' = '中腾爱思奇广告策略否词根子表(站点、店铺、父asin、词根)')
    lifecycle 366
;


--建议对象筛选
drop table if exists dws_mkt_adv_strategy_neg_root_word_tmp03;
create table dws_mkt_adv_strategy_neg_root_word_tmp03 as
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
     ,aba_rank
     ,aba_date
     ,adv_days
     ,case when order_num = 0 then cost else cost/order_num end as cpa
from  WHDE.adm_amazon_adv_strategy_search_term_d
where ds = '${bizdate}' --T日
  and adv_days >= 14  and nvl(parent_asin,'') <> '' and term_type = '搜索词'
;

--历史否词
drop table if exists dws_mkt_adv_strategy_neg_root_word_his_tmp;
create table dws_mkt_adv_strategy_neg_root_word_his_tmp as
select  profile_id
     ,campaign_id
     ,keyword_text as search_term
     ,tenant_id
from WHDE.adm_amazon_adv_neg_keyword_status_df
where ds = '${nowdate}'  and status = 'ENABLED'
group by profile_id
       ,campaign_id
       ,keyword_text
       ,tenant_id
;


--过滤掉已经否定掉的词
drop table if exists dws_mkt_adv_strategy_neg_root_word_rest_tmp;
create table dws_mkt_adv_strategy_neg_root_word_rest_tmp as
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
     ,q1.ad_group_num
     ,q1.ad_group_id_list
     ,q1.parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.term_type
     ,q1.search_term
     ,q1.word
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_days
from    (
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
                 ,ad_group_num
                 ,ad_group_id_list
                 ,parent_asin
                 ,selling_price
                 ,main_asin_url
                 ,main_image_url
                 ,term_type
                 ,search_term
                 ,word
                 ,impressions
                 ,clicks
                 ,cost
                 ,sale_amt
                 ,order_num
                 ,ctr
                 ,cvr
                 ,cpc
                 ,acos
                 ,aba_rank
                 ,aba_date
                 ,adv_days
            from dws_mkt_adv_strategy_neg_root_word_tmp03
                     lateral view explode(split(search_term,' ')) adtable as word
            where term_type = '搜索词'
        ) q1
            left join dws_mkt_adv_strategy_neg_root_word_his_tmp q2 --否词记录表，否过的词就不要再推荐了
                      on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id and tolower(q1.word) = tolower(q2.search_term)
where q2.search_term is null
;


--基础筛选
drop table if exists dws_mkt_adv_strategy_neg_root_word_stock_tmp04;
create table dws_mkt_adv_strategy_neg_root_word_stock_tmp04 as
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
where   ds = '${bizdate}' and life_cycle_label = '成熟期' and stock_label = '库存充足' and term_type = '搜索词'
;
;


--统计词根对应单词的花费，取最大的花费做映射
set odps.sql.python.version = cp37;

drop table if exists dws_mkt_adv_strategy_neg_root_word_max_cost;
create table dws_mkt_adv_strategy_neg_root_word_max_cost as
select   tenant_id
     ,profile_id
     ,ad_type
     ,parent_asin
     ,stem
     ,word
from(
        select tenant_id
             ,profile_id
             ,ad_type
             ,parent_asin
             ,stem
             ,word
             ,row_number() over (partition by tenant_id,profile_id,ad_type,parent_asin,stem order by cost desc ) rn
        from(
                select   tenant_id
                     ,profile_id
                     ,ad_type
                     ,parent_asin
                     --,zby_bi.udf_word2stem(word) stem
                     ,word stem
                     ,word
                     ,sum(cost) cost
                from    dws_mkt_adv_strategy_neg_root_word_rest_tmp
                group by tenant_id
                       ,profile_id
                       ,ad_type
                       ,parent_asin
                       ,stem
                       ,word
            )q1
    )s1
where   rn = 1
;


--统计父aisn下每个词根的效果
drop table if exists dws_mkt_adv_strategy_neg_root_word_stem;
create table dws_mkt_adv_strategy_neg_root_word_stem as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,ad_type
     ,parent_asin
     --,zby_bi.udf_word2stem(word)
     ,word as stem
     ,wm_concat(distinct '_/_',campaign_id) camp_id_list
     -- ,wm_concat(distinct '_/_',campaign_name) camp_name_list
     ,count(distinct campaign_id) as camp_cnt
     ,max(selling_price) as selling_price
     ,max(main_asin_url) as main_asin_url
     ,max(main_image_url) as main_img_url
     ,wm_concat(distinct '_/_',search_term) as search_term_list
     ,count(distinct search_term) as term_cnt
     ,sum(impressions) as impressions
     ,sum(clicks) as clicks
     ,sum(cost) as cost
     ,sum(sale_amt) as sale_amt
     ,sum(order_num) as order_num
     ,cast(case when sum(impressions) <> 0 then sum(clicks) / sum(impressions) else null end as decimal(18,6)) as ctr
     ,cast(case when sum(clicks) <> 0 then sum(order_num) / sum(clicks) else null end as decimal(18,6)) as cvr
     ,cast(case when sum(clicks) <> 0 then sum(cost) / sum(clicks) else null end as decimal(18,6)) as cpc
     ,cast(case when sum(sale_amt) <> 0 then sum(cost) / sum(sale_amt) else null end as decimal(18,6)) as acos
     ,max(adv_days) as adv_days
from    dws_mkt_adv_strategy_neg_root_word_rest_tmp
group by tenant_id
       ,profile_id
       ,marketplace_id
       ,marketplace_name
       ,currency_code
       ,seller_id
       ,seller_name
       ,ad_type
       ,parent_asin
       ,stem
;



--标签开发
drop table if exists dws_mkt_adv_strategy_neg_root_word_df_asq_final;
create table dws_mkt_adv_strategy_neg_root_word_df_asq_final as
select   q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q1.parent_asin
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q1.stem
     ,q3.word
     ,q1.camp_id_list
     ,q1.camp_cnt
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
     ,q1.search_term_list
     ,q1.term_cnt
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q1.adv_days
     ,'利润最大化' as goal_label --目标标签
     ,'成熟期' as life_cycle --生命周期
     ,'ALL' as ad_mode
     ,'词根' as term_type
     ,q2.stock_label --库存标签
     ,case when q1.clicks >= (1/q2.cate_cvr) * 1.2 then '有效点击' end as click_label --点击标签
     ,case when q1.cvr < q2.cate_cvr * 0.5 then '低转化' end as cvr_label --转化标签
     ,q2.cate_ctr
     ,q2.cate_cvr
     ,q2.cate_cpc
     ,q2.cate_acos
from dws_mkt_adv_strategy_neg_root_word_stem q1
         left join dws_mkt_adv_strategy_neg_root_word_stock_tmp04 q2    --库存充足条件
                   on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin
         inner join dws_mkt_adv_strategy_neg_root_word_max_cost q3   --花费最大的词
                    on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.ad_type = q3.ad_type and q1.parent_asin = q3.parent_asin and q1.stem = q3.stem
where  q2.cate_cvr <> 0
;


----过滤否词根词库(核心词、介词)
--set odps.sql.python.version=cp37;
--drop table if exists dws_mkt_adv_strategy_neg_root_word_df_asq_final_recom;
--create table dws_mkt_adv_strategy_neg_root_word_df_asq_final_recom as
--select s1.tenant_id
--      ,s1.profile_id
--      ,s1.marketplace_id
--      ,s1.marketplace_name
--      ,s1.currency_code
--      ,s1.seller_id
--      ,s1.seller_name
--      ,s1.ad_type
--      ,s1.parent_asin
--      ,s1.adv_manager_id
--      ,s1.adv_manager_name
--      ,s1.stem
--      ,s1.word
--      ,s1.camp_id_list
--      ,s1.camp_cnt
--      ,s1.selling_price
--      ,s1.main_asin_url
--      ,s1.main_img_url
--      ,s1.search_term_list
--      ,s1.term_cnt
--      ,s1.impressions
--      ,s1.clicks
--      ,s1.cost
--      ,s1.sale_amt
--      ,s1.order_num
--      ,s1.ctr
--      ,s1.cvr
--      ,s1.cpc
--      ,s1.acos
--      ,s1.adv_days
--      ,s1.goal_label
--      ,s1.life_cycle
--      ,s1.ad_mode
--      ,s1.term_type
--      ,s1.stock_label
--      ,s1.click_label
--      ,s1.cvr_label
--      ,s1.cate_ctr
--      ,s1.cate_cvr
--      ,s1.cate_cpc
--      ,s1.cate_acos
--      ,zby_bi.udf_is_ok_stem(s1.word,s2.text_final,'word') as if_recom
--from dws_mkt_adv_strategy_neg_root_word_df_asq_final s1
--cross join (
--            select concat_ws(' ',array_distinct(split(concat(test_1,' ',test_2,' ',test_3),' '))) as text_final
--            from (
--                  select wm_concat(distinct ' ',split(_widget_1699953513777,'｜')[1]) as test_1
--                        ,concat_ws(' ',array_distinct(split(wm_concat(distinct ' ',_widget_1699953513778),' '))) as test_2
--                        ,'about above across after against along amid among around as at before behind below beneath beside besides between beyond but by concerning considering despite down during except for from in inside into like near of off on onto out outside over past regarding round since through throughout till to toward under underneath unlike until unto up upon with within without' as test_3
--                  from whde.ods_asq_jdy_target_keyword
--                  where ds = max_pt('whde.ods_asq_jdy_target_keyword') and (_widget_1699953513777 like '%大词%' or _widget_1699953513777 like '%核心%' or _widget_1699953513777 like '%类目词%') and _widget_1699953513777 not like '%长尾词%'
--                 ) q1
--            )s2
--on 1 = 1
--;

----------------------------------------------插入结果表----------------------------------------------
--标签限制
insert overwrite table dws_mkt_adv_strategy_neg_root_word_ds partition (ds = '${bizdate}')
select q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.ad_type,q1.parent_asin,q1.stem,'${bizdate}')) as stem_id
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
     ,q1.parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
     ,q1.stem
     ,q1.word
     ,q1.clicks
     ,cast(q1.cost as decimal(18,6)) as cost
     ,cast(q1.sale_amt as decimal(18,6)) as sale_amt
     ,q1.order_num
     ,cast(q1.ctr as decimal(18,6)) as ctr
     ,cast(q1.cvr as decimal(18,6)) as cvr
     ,cast(q1.cpc as decimal(18,6)) as cpc
     ,cast(q1.acos as decimal(18,6)) as acos
     ,cast(q1.cate_ctr as decimal(18,6)) as cate_ctr
     ,cast(q1.cate_cvr as decimal(18,6)) as cate_cvr
     ,cast(q1.cate_cpc as decimal(18,6)) as cate_cpc
     ,cast(q1.cate_acos as decimal(18,6)) as cate_acos
     ,q1.adv_days
     ,q1.term_cnt
     ,q1.search_term_list
     ,q1.camp_id_list
     ,q1.camp_cnt
     ,q1.life_cycle --生命周期
     ,q1.goal_label --目标标签
     ,q1.ad_mode
     ,q1.term_type
     ,q1.stock_label --库存标签
     ,q1.click_label --点击标签
     ,q1.cvr_label
     ,q2.action_type
     ,getdate() as create_time
from dws_mkt_adv_strategy_neg_root_word_df_asq_final q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and q1.stock_label = q2.stock_label
                        and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                        and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and q1.term_type = q2.term_type_label
                        and q1.ad_mode = q2.ad_mode_label
where q2.action_name = '词根否定' and q2.life_cycle_label = '成熟期' --and q1.if_recom = 1
;

