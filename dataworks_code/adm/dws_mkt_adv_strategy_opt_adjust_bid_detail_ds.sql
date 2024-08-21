--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-30 00:39:02
--********************************************************************--
--@exclude_input=asq_dw.dws_mkt_adv_strategy_search_term_pasin_rank_new_ds
--@exclude_input=asq_dw.dws_mkt_adv_strategy_keyword_index_ds
--@exclude_input=open_dw.adm_amazon_adv_search_term_pasin_rank_df
--@exclude_input=asq_dw.dws_mkt_adv_strategy_search_term_adjust_bid_param_hf
--@exclude_output=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_asq_final
--@exclude_output=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_zt_final
--@exclude_output=asq_dw.dws_mkt_adv_strategy_adjust_bid_detail_his
--@exclude_output=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06
--@exclude_output=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05
--@exclude_output=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp02
--@exclude_output=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01
--@exclude_input=asq_dw.dws_mkt_adv_strategy_main_all_df
--@exclude_input=asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_param_ds
--@exclude_input=asq_dw.dws_mkt_adv_strategy_adjust_bid_param_hf
--@exclude_input=open_dw.dwd_itm_spu_amazon_search_keyword_info_ws
--@exclude_input=asq_dw.dws_mkt_adv_skw_parent_asin_rank_hf
--odps sql
--********************************************************************--
--author:王敏佳
--create time:2024-01-10 14:06:46
--********************************************************************--
--odps sql
--********************************************************************--
--author:王敏佳
--create time:2024-01-10 14:06:46
--********************************************************************--
CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_opt_adjust_bid_detail_ds
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
    ,top_parent_asin       STRING COMMENT '父aisn'
    ,selling_price         STRING COMMENT '售价'
    ,main_asin_url         STRING COMMENT '商品链接'
    ,main_img_url          STRING COMMENT '商品主图'
    ,target_term           STRING COMMENT '投放词/品'
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
    ,aba_rank              BIGINT COMMENT 'aba排名'
    ,aba_date              DATETIME COMMENT 'aba日期'
    ,norm_rank             BIGINT COMMENT '自然坑位排名'
    ,adv_rank              BIGINT COMMENT '广告坑位排名'
    ,life_cycle_label      STRING COMMENT '生命周期'
    ,goal_label            STRING COMMENT '目标标签'
    ,ad_mode_label         STRING COMMENT '广告投放类型标签'
    ,stock_label           STRING COMMENT '库存标签'
    ,term_type_label       STRING COMMENT '操作对象类型'
    ,norm_rank_label       STRING COMMENT '自然坑位排名标签'
    ,click_label           STRING COMMENT '点击标签'
    ,cvr_label             STRING COMMENT '转化标签'
    ,cpa_label             STRING COMMENT 'CPA标签'
    ,adv_rank_label        STRING COMMENT '广告坑位排名标签'
    ,flow_label            STRING COMMENT '流量标签'
    ,action_type           STRING COMMENT '操作类型'
    ,create_time           DATETIME COMMENT '创建时间'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '爱思奇中腾广告策略投放词寻优(子表)')
    LIFECYCLE 366
;


-- alter table asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_ds add columns (sale_monopoly_rate decimal(18,6) comment 'TOP3商品转化份额', chn_seller_rate decimal(18,6) comment '搜索词前三页中国卖家占比' );
-- alter table asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_detail_ds add columns (bid_adjust_rate decimal(18,6) comment '竞价调整幅度', bid decimal(18,6) comment '当前竞价' );


--基础数据
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01 as
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
     ,ad_group_id
     ,ad_group_name
     ,parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,target_type
     ,target_text
     ,target_id_list
     ,impressions
     ,clicks
     ,cost
     ,sale_amt
     ,sale_num
     ,ctr
     ,cvr
     ,cpc
     ,case when sale_num = 0 then null else cost / sale_num end as cpa
     ,acos
     ,adv_days
     ,aba_rank
     ,aba_date
from  open_dw.adm_amazon_adv_strategy_target_d
where ds = '${nowdate}'  --T日
  and tenant_id in ('1714548493239062529','1555073968741003270') and adv_days >= 14 and nvl(parent_asin,'') <> '' and ad_mode = '手动投放' and target_type = '投放词'
;


--精准匹配的投放词
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01_1;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01_1 as
select tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,keyword_id
     ,keyword_text
     ,bid
     ,0.1 as bid_adjust_rate
from open_dw.adm_amazon_adv_keyword_target_status_df
where ds = max_pt('open_dw.adm_amazon_adv_keyword_target_status_df') and match_type = 'EXACT' and status = 'ENABLED'
;


--成熟期、库存充足、有类目平均筛选
drop table if exists  dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp02;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp02 as
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
     ,replace(term_type,'搜索','投放') as term_type
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
     ,craw_post_code as post_code
from    asq_dw.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = max_pt('asq_dw.dws_mkt_adv_strategy_parent_asin_base_index_new_ds') and life_cycle_label = '成熟期' and stock_label = '库存充足' and term_type = '搜索词'
;

-------------算库存标签>>结束--------------

--大小词标签
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05 as
select  marketplace_id
     ,keyword as search_term
     ,case when aba_rank < 50000 then 1 else 0 end as if_big
     ,case when aba_rank < 100000 then 1 else 0 end as if_big_asq
     ,cast(sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
     ,cast(chn_seller_rate as decimal(18,6)) as chn_seller_rate
from asq_dw.dws_mkt_adv_strategy_keyword_index_ds
where ds = max_pt('asq_dw.dws_mkt_adv_strategy_keyword_index_ds')
;


--搜索词排名
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06 as
select  top_parent_asin as parent_asin
     ,search_term
     ,norm_rank_label as p1_norm_label
     ,adv_rank_label as p1_adv_label
     ,cast(adv_rank as bigint) as adv_rank
     ,cast(norm_rank as bigint) as norm_rank
     ,marketplace_id
     ,post_code
from asq_dw.dws_mkt_adv_strategy_search_term_pasin_rank_new_ds
where ds = max_pt('asq_dw.dws_mkt_adv_strategy_search_term_pasin_rank_new_ds') and term_type = '搜索词'
;


--搜索词爬虫清单
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06_1;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06_1 as
select marketplace_id
     ,search_term
     ,post_code
from dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06
group by marketplace_id
       ,search_term
       ,post_code
;


--正在守坑的词
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_his;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_his as
select   tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,top_parent_asin
     ,target_term
from    (
            select tenant_id
                 ,profile_id
                 ,campaign_id
                 ,ad_group_id
                 ,top_parent_asin
                 ,target_term
            from asq_dw.dws_mkt_adv_strategy_adjust_bid_param_hf --每小时跑
            where hs = (select max(hs) from asq_dw.dws_mkt_adv_strategy_adjust_bid_param_hf where hs is not null) and adv_manager_id <> '1'

            union all

            select   tenant_id
                 ,profile_id
                 ,campaign_id
                 ,ad_group_id
                 ,top_parent_asin
                 ,target_term
            from asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_param_ds  --寻优
            where ds = (select max(ds) from asq_dw.dws_mkt_adv_strategy_opt_adjust_bid_param_ds where ds is not null) and adv_manager_id <> '1'

            union all

            select  tenant_id
                 ,profile_id
                 ,campaign_id
                 ,ad_group_id
                 ,top_parent_asin
                 ,target_term
            from asq_dw.dws_mkt_adv_strategy_search_term_adjust_bid_param_hf --搜索词守坑
            where hs = (select max(hs) from asq_dw.dws_mkt_adv_strategy_search_term_adjust_bid_param_hf where hs is not null) and adv_manager_id <> '1'
        )
group by  tenant_id
       ,profile_id
       ,campaign_id
       ,ad_group_id
       ,top_parent_asin
       ,target_term
;

--搜索词的垄断和中国卖家占比指标
-- drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp07;
-- create table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp07 as
-- select marketplace_id
--       ,keyword as search_term
--       ,cast(sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
--       ,cast(chn_seller_rate as decimal(18,6)) as chn_seller_rate
-- from asq_dw.dws_mkt_adv_strategy_keyword_index_ds
-- where ds = max_pt('asq_dw.dws_mkt_adv_strategy_keyword_index_ds')
-- ;


-----------------------------------------中腾关联属性-----------------------------------------
drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_zt_final;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_zt_final as
select tenant_id
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
     ,ad_group_id
     ,ad_group_name
     ,top_parent_asin
     ,selling_price
     ,main_asin_url
     ,main_img_url
     ,target_term
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
     ,aba_rank
     ,aba_date
     ,adv_rank
     ,norm_rank
     ,life_cycle_label
     ,goal_label
     ,term_type_label
     ,ad_mode_label
     ,stock_label
     ,flow_label
     ,cpa_label
     ,ctr_label
     ,norm_rank_label
     ,adv_rank_label
     ,click_label
     ,case when norm_rank_label = '自然排名非第一页' and adv_rank_label = '广告排名非第一页' and click_label = '有效点击' and cpa_label = '低CPA' and order_num > 0 then '出单' else cvr_label end as cvr_label
     ,bid_adjust_rate
     ,bid
from(
        select tenant_id
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
             ,ad_group_id
             ,ad_group_name
             ,top_parent_asin
             ,selling_price
             ,main_asin_url
             ,main_img_url
             ,target_term
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
             ,aba_rank
             ,aba_date
             ,adv_rank
             ,norm_rank
             ,'成熟期' as life_cycle_label
             ,'利润最大化' as goal_label
             ,term_type as term_type_label
             ,ad_mode as ad_mode_label
             ,stock_label
             ,case when if_big = 1 then '大词' else '小词' end as flow_label
             ,case when cpa < cate_cpa then '低CPA' when cpa >= cate_cpa then '高CPA' end as cpa_label
             ,case when ctr < cate_ctr then '低CTR' end as ctr_label
             ,case when p1_norm_label = 1 then '自然排名第一页' when p1_norm_label = 0 then '自然排名非第一页' end as norm_rank_label
             ,case when p1_adv_label = 1 then '广告排名第一页' when p1_adv_label = 0 then '广告排名非第一页' end as adv_rank_label
             ,case when clicks >= 1/cate_cvr then '有效点击' end as click_label
             ,case when order_num = 0 then '不出单'
                   when cvr < cate_cvr * 0.5 then '低转化'
                   when cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
                   when cvr > cate_cvr then '高转化' end as cvr_label
             ,bid_adjust_rate
             ,bid
        from (
                 select   q1.tenant_id
                      ,q1.profile_id
                      ,q1.marketplace_id
                      ,q1.marketplace_name
                      ,q1.currency_code
                      ,q1.seller_id
                      ,q1.seller_name
                      ,q2.adv_manager_id
                      ,q2.adv_manager_name
                      ,q1.ad_type
                      ,q1.campaign_id
                      ,q1.campaign_name
                      ,q1.ad_mode
                      ,q1.ad_group_id
                      ,q1.ad_group_name
                      ,q1.parent_asin as top_parent_asin
                      ,q1.selling_price
                      ,q1.main_asin_url
                      ,q1.main_image_url as main_img_url
                      ,q1.target_type as term_type
                      ,q1.target_text as target_term
                      ,q1.impressions
                      ,q1.clicks
                      ,q1.cost
                      ,q1.sale_amt
                      ,q1.sale_num as order_num
                      ,q1.ctr
                      ,q1.cvr
                      ,q1.cpc
                      ,q1.cpa
                      ,q1.acos
                      ,q2.category
                      ,q2.cate_ctr
                      ,q2.cate_cvr
                      ,q2.cate_cpc
                      ,q2.cate_cpa
                      ,q2.cate_acos
                      ,q1.aba_rank
                      ,q1.aba_date
                      ,case when q7.search_term is not null then nvl(q4.p1_norm_label,0) end as p1_norm_label
                      ,case when q7.search_term is not null then nvl(q4.p1_adv_label,0) end as p1_adv_label
                      ,case when q7.search_term is not null then nvl(q4.norm_rank,999) end as norm_rank
                      ,case when q7.search_term is not null then nvl(q4.adv_rank,999) end as adv_rank
                      ,q1.adv_days
                      ,q2.stock_label
                      ,nvl(q3.if_big,0) as if_big
                      ,q6.bid_adjust_rate
                      ,q6.bid
                 from dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01 q1
                          inner join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp02 q2  --基础筛选
                                     on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin and q1.target_type = q2.term_type
                          inner join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01_1 q6  --精准匹配的投放词
                                     on q1.tenant_id = q6.tenant_id and q1.profile_id = q6.profile_id and q1.campaign_id = q6.campaign_id and q1.ad_group_id = q6.ad_group_id and tolower(q1.target_text) = tolower(q6.keyword_text)
                          left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05 q3 --大小词
                                    on q1.marketplace_id = q3.marketplace_id and tolower(q1.target_text) = tolower(q3.search_term)
                          left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06 q4 --投放词排名
                                    on q1.marketplace_id = q4.marketplace_id and tolower(q1.target_text) = tolower(q4.search_term) and q1.parent_asin = q4.parent_asin and q2.post_code = q4.post_code
                          left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06_1 q7  --搜索词爬虫清单
                                    on q1.marketplace_id = q7.marketplace_id and tolower(q1.target_text) = tolower(q7.search_term) and q2.post_code = q7.post_code
                          left join dws_mkt_adv_strategy_opt_adjust_bid_detail_his q5 --剔除已经在守坑、寻优的
                                    on q1.tenant_id = q5.tenant_id and q1.profile_id = q5.profile_id and q1.campaign_id = q5.campaign_id and q1.parent_asin = q5.top_parent_asin and q1.ad_group_id = q5.ad_group_id and tolower(q1.target_text) = tolower(q5.target_term)
                 where q1.tenant_id = '1714548493239062529' and q5.target_term is null and q2.cate_cvr <> 0
             )s1
    )p1
;



-----------------------------------------爱思奇关联属性-----------------------------------------

drop table if exists dws_mkt_adv_strategy_opt_adjust_bid_detail_asq_final;
create table dws_mkt_adv_strategy_opt_adjust_bid_detail_asq_final as
select tenant_id
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
     ,ad_group_id
     ,ad_group_name
     ,top_parent_asin
     ,selling_price
     ,main_asin_url
     ,main_img_url
     ,target_term
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
     ,aba_rank
     ,aba_date
     ,adv_rank
     ,norm_rank
     ,life_cycle_label
     ,goal_label
     ,term_type_label
     ,ad_mode_label
     ,stock_label
     ,flow_label
     ,cpa_label
     ,ctr_label
     ,norm_rank_label
     ,adv_rank_label
     ,click_label
     ,case when norm_rank_label = '自然排名非第一页' and adv_rank_label = '广告排名非第一页' and click_label = '有效点击' and cpa_label = '低CPA' and order_num > 0 then '出单' else cvr_label end as cvr_label
     ,bid_adjust_rate
     ,bid
from (
         select tenant_id
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
              ,ad_group_id
              ,ad_group_name
              ,top_parent_asin
              ,selling_price
              ,main_asin_url
              ,main_img_url
              ,target_term
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
              ,aba_rank
              ,aba_date
              ,adv_rank
              ,norm_rank
              ,'成熟期' as life_cycle_label
              ,'利润最大化' as goal_label
              ,term_type as term_type_label
              ,ad_mode as ad_mode_label
              ,stock_label
              ,case when if_big = 1 then '大词' else '小词' end as flow_label
              ,case when cpa < cate_cpa then '低CPA' when cpa >= cate_cpa then '高CPA' end as cpa_label
              ,case when ctr < cate_ctr then '低CTR' end as ctr_label
              ,case when p1_norm_label = 1 then '自然排名第一页' when p1_norm_label = 0 then '自然排名非第一页' end as norm_rank_label
              ,case when p1_adv_label = 1 then '广告排名第一页' when p1_adv_label = 0 then '广告排名非第一页' end as adv_rank_label
              ,case when clicks >= (1/cate_cvr) * 1.2 then '有效点击' end as click_label
              ,case when order_num = 0 then '不出单'
                    when cvr < cate_cvr * 0.5 then '低转化'
                    when cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
                    when cvr > cate_cvr then '高转化' end as cvr_label
              ,bid_adjust_rate
              ,bid
         from (
                  select   q1.tenant_id
                       ,q1.profile_id
                       ,q1.marketplace_id
                       ,q1.marketplace_name
                       ,q1.currency_code
                       ,q1.seller_id
                       ,q1.seller_name
                       ,q2.adv_manager_id
                       ,q2.adv_manager_name
                       ,q1.ad_type
                       ,q1.campaign_id
                       ,q1.campaign_name
                       ,q1.ad_mode
                       ,q1.ad_group_id
                       ,q1.ad_group_name
                       ,q1.parent_asin as top_parent_asin
                       ,q1.selling_price
                       ,q1.main_asin_url
                       ,q1.main_image_url as main_img_url
                       ,q1.target_type as term_type
                       ,q1.target_text as target_term
                       ,q1.impressions
                       ,q1.clicks
                       ,q1.cost
                       ,q1.sale_amt
                       ,q1.sale_num as order_num
                       ,q1.ctr
                       ,q1.cvr
                       ,q1.cpc
                       ,q1.cpa
                       ,q1.acos
                       ,q2.category
                       ,q2.cate_ctr
                       ,q2.cate_cvr
                       ,q2.cate_cpc
                       ,q2.cate_cpa
                       ,q2.cate_acos
                       ,q1.aba_rank
                       ,q1.aba_date
                       ,case when q7.search_term is not null then nvl(q4.p1_norm_label,0) end as p1_norm_label
                       ,case when q7.search_term is not null then nvl(q4.p1_adv_label,0) end as p1_adv_label
                       ,case when q7.search_term is not null then nvl(q4.norm_rank,999) end as norm_rank
                       ,case when q7.search_term is not null then nvl(q4.adv_rank,999) end as adv_rank
                       ,q1.adv_days
                       ,q2.stock_label
                       ,nvl(q3.if_big_asq,0) as  if_big
                       ,q6.bid_adjust_rate
                       ,q6.bid
                  from dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01 q1
                           inner join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp02 q2  --基础筛选
                                      on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin and q1.target_type = q2.term_type
                           inner join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp01_1 q6  --精准匹配的投放词
                                      on q1.tenant_id = q6.tenant_id and q1.profile_id = q6.profile_id and q1.campaign_id = q6.campaign_id and q1.ad_group_id = q6.ad_group_id and tolower(q1.target_text) = tolower(q6.keyword_text)
                           left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05 q3 --大小词
                                     on q1.marketplace_id = q3.marketplace_id and tolower(q1.target_text) = tolower(q3.search_term)
                           left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06 q4 --投放词排名
                                     on q1.marketplace_id = q4.marketplace_id and tolower(q1.target_text) = tolower(q4.search_term) and q1.parent_asin = q4.parent_asin and q2.post_code = q4.post_code
                           left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp06_1 q7  --搜索词爬虫清单
                                     on q1.marketplace_id = q7.marketplace_id and tolower(q1.target_text) = tolower(q7.search_term) and q2.post_code = q7.post_code
                           left join dws_mkt_adv_strategy_opt_adjust_bid_detail_his q5 --剔除已经在守坑的
                                     on q1.tenant_id = q5.tenant_id and q1.profile_id = q5.profile_id and q1.campaign_id = q5.campaign_id and q1.parent_asin = q5.top_parent_asin and q1.ad_group_id = q5.ad_group_id and tolower(q1.target_text) = tolower(q5.target_term)
                  where q1.tenant_id = '1555073968741003270' and q5.target_term is null and q2.cate_cvr <> 0
              )s1
     )p1
;


--支持重跑
alter table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds drop if exists partition (ds = '${nowdate}');


--插入最新数据
insert into table dws_mkt_adv_strategy_opt_adjust_bid_detail_ds partition (ds = '${nowdate}')
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
   ,ad_type
   ,campaign_id
   ,campaign_name
   ,ad_group_id
   ,ad_group_name
   ,top_parent_asin
   ,selling_price
   ,main_asin_url
   ,main_img_url
   ,target_term
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
   ,aba_rank
   ,aba_date
   ,adv_rank
   ,norm_rank
   ,life_cycle_label
   ,goal_label
   ,term_type_label
   ,ad_mode_label
   ,stock_label
   ,norm_rank_label
   ,click_label
   ,cvr_label
   ,cpa_label
   ,adv_rank_label
   ,flow_label
   ,action_type
   ,create_time
   ,sale_monopoly_rate
   ,chn_seller_rate
   ,bid_adjust_rate
   ,bid
)
select q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.ad_group_id,q1.top_parent_asin,q1.target_term,q2.strategy_id)) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.ad_type
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.top_parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
     ,q1.target_term
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
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_rank
     ,q1.norm_rank
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.ad_mode_label
     ,q1.stock_label
     ,q1.norm_rank_label
     ,q1.click_label
     ,q1.cvr_label
     ,q1.cpa_label
     ,q1.adv_rank_label
     ,q1.flow_label
     ,q2.action_type
     ,getdate() as create_time
     ,q3.sale_monopoly_rate
     ,q3.chn_seller_rate
     ,q1.bid_adjust_rate
     ,q1.bid
from dws_mkt_adv_strategy_opt_adjust_bid_detail_asq_final q1
         inner join dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and nvl(q1.stock_label,' ') = nvl(q2.stock_label,' ')
                        and nvl(q1.norm_rank_label,' ') = nvl(q2.norm_rank_label,' ')
                        and nvl(q1.adv_rank_label,' ') = nvl(q2.adv_rank_label,' ')
                        and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                        and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and nvl(q1.ad_mode_label,' ') = nvl(q2.ad_mode_label,' ')
                        and nvl(q1.term_type_label,' ') = nvl(q2.term_type_label,' ')
                        and nvl(q1.cpa_label,' ') = nvl(q2.cpa_label,' ')
         left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05 q3
                   on q1.marketplace_id = q3.marketplace_id and tolower(q1.target_term) = tolower(q3.search_term)
where q2.action_name = '投放词寻优' and q2.life_cycle_label = '成熟期' and q1.term_type_label = '投放词' and (q1.cpa_label = '低CPA' or (q1.cpa_label = '高CPA' and q1.ctr_label = '低CTR'))


union all

select q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.ad_group_id,q1.top_parent_asin,q1.target_term,q2.strategy_id)) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.ad_type
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.top_parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
     ,q1.target_term
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
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_rank
     ,q1.norm_rank
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.ad_mode_label
     ,q1.stock_label
     ,q1.norm_rank_label
     ,q1.click_label
     ,q1.cvr_label
     ,q1.cpa_label
     ,q1.adv_rank_label
     ,q1.flow_label
     ,q2.action_type
     ,getdate() as create_time
     ,q3.sale_monopoly_rate
     ,q3.chn_seller_rate
     ,q1.bid_adjust_rate
     ,q1.bid
from dws_mkt_adv_strategy_opt_adjust_bid_detail_zt_final q1
         inner join dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        and nvl(q1.stock_label,' ') = nvl(q2.stock_label,' ')
                        and nvl(q1.norm_rank_label,' ') = nvl(q2.norm_rank_label,' ')
                        and nvl(q1.adv_rank_label,' ') = nvl(q2.adv_rank_label,' ')
                        and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                        and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and nvl(q1.ad_mode_label,' ') = nvl(q2.ad_mode_label,' ')
                        and nvl(q1.term_type_label,' ') = nvl(q2.term_type_label,' ')
                        and nvl(q1.cpa_label,' ') = nvl(q2.cpa_label,' ')
         left join dws_mkt_adv_strategy_opt_adjust_bid_detail_ds_tmp05 q3
                   on q1.marketplace_id = q3.marketplace_id and tolower(q1.target_term) = tolower(q3.search_term)
where q2.action_name = '投放词寻优' and q2.life_cycle_label = '成熟期' and q1.term_type_label = '投放词' and (q1.cpa_label = '低CPA' or (q1.cpa_label = '高CPA' and q1.ctr_label = '低CTR'))
;
