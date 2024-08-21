--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:13:16
--********************************************************************--

CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_stop_word_product_detail_ds
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
    ,target_id             STRING COMMENT '投放对象ID'
    ,target_term           STRING COMMENT '投放词/品'
    ,match_type            STRING COMMENT '匹配类型'
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
    ,norm_rank             BIGINT COMMENT '投放词自然坑位排名'
    ,adv_rank              BIGINT COMMENT '投放词广告坑位排名'
    ,life_cycle_label      STRING COMMENT '生命周期'
    ,goal_label            STRING COMMENT '目标标签'
    ,ad_mode_label         STRING COMMENT '广告投放类型标签'
    ,stock_label           STRING COMMENT '库存标签'
    ,term_type_label       STRING COMMENT '操作对象类型'
    ,click_label           STRING COMMENT '点击标签'
    ,cvr_label             STRING COMMENT '转化标签'
    ,action_type           STRING COMMENT '操作类型'
    ,create_time           DATETIME COMMENT '创建时间'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略暂停投放(子表)')
    LIFECYCLE 366
;



--30天汇总数据
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp00;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp00 as
select  a.tenant_id
     ,a.profile_id
     ,a.marketplace_id
     ,a.marketplace_name
     ,a.currency_code
     ,a.seller_id
     ,a.seller_name
     ,'商品推广' ad_type
     ,a.campaign_id
     ,campaign_name
     ,ad_mode
     ,a.ad_group_id
     ,ad_group_name
     ,a.parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,target_type
     ,a.targeting as target_term
     ,keyword_id as target_id
     ,match_type
     ,impressions
     ,clicks
     ,cast(cost as decimal(18,6)) cost
     ,cast(sale_amt as decimal(18,6)) sale_amt
     ,sale_num as order_num
     ,cast(case when impressions <> 0 then clicks / impressions end as decimal(18,6)) ctr
     ,cast(case when clicks <> 0 then sale_num / clicks end as decimal(18,6)) cvr
     ,cast(case when clicks <> 0 then cost / clicks end as decimal(18,6)) cpc
     ,cast(case when sale_amt <> 0 then cost / sale_amt end as decimal(18,6)) acos
     ,aba_rank
     ,aba_date
     ,datediff(getdate(),min_report_date,'dd') + 1 adv_days
from    (
            select   s1.tenant_id
                 ,s1.profile_id
                 ,s3.marketplace_id
                 ,s3.marketplace_name
                 ,s3.seller_id
                 ,s3.seller_name
                 ,s1.campaign_id
                 ,s3.campaign_name
                 ,currency_code
                 ,s1.ad_group_id
                 ,s3.ad_group_name
                 ,ad_mode
                 ,parent_asin
                 ,target_type
                 ,targeting
                 ,keyword_id
                 ,match_type
                 ,sum(impressions) impressions
                 ,sum(clicks) clicks
                 ,sum(cost) cost
                 ,sum(w7d_sale_amt) sale_amt
                 ,sum(w7d_units_sold_clicks) sale_num
                 ,min(report_date) min_report_date
            from    (
                        select  tenant_id
                             ,profile_id
                             ,seller_id
                             ,campaign_id
                             ,campaign_budget_currency_code currency_code
                             ,ad_group_id
                             ,case when instr(targeting,'asin') >= 1 then '投放品'
                                   when instr(targeting,'category') >= 1 then '投放类目'
                                   else '投放词'
                            end target_type
                             ,case when instr(targeting,'asin') >= 1 then replace(split_part(targeting,'=',2),'"','')
                                   when instr(targeting,'category') >= 1 then split_part(replace(split_part(targeting,'=',2),'"',''),'price',1)
                                   else targeting
                            end targeting
                             ,keyword_id
                             ,match_type
                             ,impressions
                             ,clicks
                             ,cost
                             ,w7d_sale_amt
                             ,w7d_units_sold_clicks
                             ,report_date
                        from    whde.dwd_mkt_adv_amazon_sp_target_ds
                        where   ds >= to_char(dateadd(to_date('${nowdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
                          and campaign_status = 'ENABLED'
                    ) s1
                        left join   (
                select  tenant_id
                     ,profile_id
                     ,marketplace_id
                     ,seller_id
                     ,campaign_id
                     ,ad_group_id
                     ,top_cost_parent_asin parent_asin
                     ,max(marketplace_name) marketplace_name
                     ,max(seller_name) seller_name
                     ,max(campaign_name) campaign_name
                     ,max(ad_group_name) ad_group_name
                     ,max(ad_mode) ad_mode
                     ,max(campaign_status)campaign_status
                from    whde.adm_amazon_adv_sku_wide_d
                where   ds = '${bizdate}'
                group by tenant_id
                       ,profile_id
                       ,marketplace_id
                       ,seller_id
                       ,campaign_id
                       ,ad_group_id
                       ,top_cost_parent_asin
            ) s3
                                    on      s1.tenant_id = s3.tenant_id
                                        and     s1.profile_id = s3.profile_id
                                        and     s1.seller_id = s3.seller_id
                                        and     s1.campaign_id = s3.campaign_id
                                        and     s1.ad_group_id = s3.ad_group_id
            where s3.campaign_status='已启动'
            group by s1.tenant_id
                   ,s1.profile_id
                   ,s3.marketplace_id
                   ,s3.marketplace_name
                   ,s3.seller_id
                   ,s3.seller_name
                   ,s1.campaign_id
                   ,s3.campaign_name
                   ,currency_code
                   ,s1.ad_group_id
                   ,s3.ad_group_name
                   ,ad_mode
                   ,parent_asin
                   ,target_type
                   ,targeting
                   ,keyword_id
                   ,match_type
        ) a
            left join   (
    select   tenant_id
         ,marketplace_id
         ,seller_id
         ,parent_asin
         ,link as main_asin_url
         ,main_image_url
         ,selling_price
    from    whde.dws_itm_spu_amazon_parent_asin_index_df
    where   ds = max_pt('whde.dws_itm_spu_amazon_parent_asin_index_df')
) b --父aisn、以及类目、库存
                        on      a.tenant_id = b.tenant_id
                            and     a.marketplace_id = b.marketplace_id
                            and     a.seller_id = b.seller_id
                            and     a.parent_asin = b.parent_asin
            left join   (
    select   marketplace_id
         ,keyword as search_term
         ,min(cast(aba_rank as bigint)) aba_rank
         ,max(aba_date) aba_date
    from   whde.dws_mkt_adv_strategy_keyword_index_ds
    where ds = max_pt('whde.dws_mkt_adv_strategy_keyword_index_ds')
    group by marketplace_id
           ,keyword
) e --aba搜索排名
                        on tolower(a.targeting) = tolower(e.search_term) and a.marketplace_id = e.marketplace_id
;


--基础宽表
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp01 ;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp01 as
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
     ,ad_group_id
     ,ad_group_name
     ,parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,case when target_type = '投放类目' then '投放品' else target_type end as target_type
     ,target_term
     ,target_id
     ,match_type
     ,impressions
     ,clicks
     ,cost
     ,sale_amt
     ,order_num
     ,ctr
     ,cvr
     ,cpc
     ,case when order_num = 0 then null else cost / order_num end as cpa
     ,acos
     ,adv_days
     ,aba_rank
     ,aba_date
from  dws_mkt_adv_strategy_stop_word_product_detail_tmp00
where --adv_days >= 14  and
    nvl(parent_asin,'') <> '' and ad_mode = '手动投放'
;


--基础筛选
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp02;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp02 as
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
where   ds ='${bizdate}'   and life_cycle_label = '成熟期' --and stock_label = '库存充足'
;


--正常状态的投放词
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp03;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp03 as
select tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,keyword_id as target_id
from whde.adm_amazon_adv_keyword_target_status_df
where ds = '${bizdate}' and status = 'ENABLED'
union all
select tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,target_id
from whde.adm_amazon_adv_product_target_status_df
where ds = '${bizdate}' and status = 'ENABLED'
;

--排名数据
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp03_1;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp03_1 as
select  top_parent_asin
     ,search_term
     ,cast(adv_rank as bigint) as adv_rank
     ,cast(norm_rank as bigint) as norm_rank
     ,marketplace_id
     ,replace(term_type,'搜索','投放') as term_type
from whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds
where ds = max_pt('whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds')
;



--关联属性
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp04;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp04 as
select q1.tenant_id
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
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.target_type
     ,q1.target_term
     ,q1.match_type
     ,q1.target_id
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
     ,q1.aba_rank
     ,q1.aba_date
     ,q2.category
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q2.stock_sale_days
     ,q2.stock_label
     ,q2.cate_ctr
     ,q2.cate_cvr
     ,q2.cate_cpc
     ,q2.cate_cpa
     ,q2.cate_acos
     ,case when q1.cpa > q2.cate_cpa * 1.3 then 1 else 0 end if_high_cpa
     ,case when q1.clicks >= (1/q2.cate_cvr) * 1.2 then '有效点击' end as click_label
     ,case when q1.cvr < q2.cate_cvr * 0.5 then '低转化' end as cvr_label
     ,q3.adv_rank
     ,q3.norm_rank
from dws_mkt_adv_strategy_stop_word_product_detail_tmp01 q1
         inner join dws_mkt_adv_strategy_stop_word_product_detail_tmp02 q2
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin and q1.target_type = q2.term_type
         left join dws_mkt_adv_strategy_stop_word_product_detail_tmp03_1 q3
                   on q1.marketplace_id = q3.marketplace_id and q1.parent_asin = q3.top_parent_asin and tolower(q1.target_term) = tolower(q3.search_term) and q1.target_type = q3.term_type
where q2.cate_cvr <> 0
;

--生成标签
drop table if exists dws_mkt_adv_strategy_stop_word_product_detail_tmp05;
create table dws_mkt_adv_strategy_stop_word_product_detail_tmp05 as
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
     ,ad_group_id
     ,ad_group_name
     ,parent_asin as top_parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url as main_img_url
     ,target_term
     ,target_id
     ,match_type
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
     ,adv_days
     ,aba_rank
     ,aba_date
     ,category
     ,adv_manager_id
     ,adv_manager_name
     ,cate_ctr
     ,cate_cvr
     ,cate_cpc
     ,cate_cpa
     ,cate_acos
     ,'成熟期' as life_cycle_label
     ,'利润最大化' as goal_label
     ,ad_mode as ad_mode_label
     ,case when target_type = '投放类目' then '投放品' else target_type end as term_type_label
     ,stock_label
     ,click_label
     ,'低转化' cvr_label
     ,adv_rank
     ,norm_rank
from dws_mkt_adv_strategy_stop_word_product_detail_tmp04
--where --if_high_cpa = 1 and
--click_label = '有效点击' --and cvr_label = '低转化'
--sale_amt = 0
;


--支持重跑
alter table dws_mkt_adv_strategy_stop_word_product_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_stop_word_product_detail_ds partition (ds = '${nowdate}')
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
    ,campaign_id
    ,campaign_name
    ,ad_group_id
    ,ad_group_name
    ,top_parent_asin
    ,adv_manager_id
    ,adv_manager_name
    ,selling_price
    ,main_asin_url
    ,main_img_url
    ,target_id
    ,target_term
    ,match_type
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
    ,norm_rank
    ,adv_rank
    ,life_cycle_label
    ,goal_label
    ,ad_mode_label
    ,term_type_label
    ,stock_label
    ,click_label
    ,cvr_label
    ,action_type
    ,create_time
)
select  distinct q1.tenant_id
               ,hash(concat(q1.tenant_id,q1.profile_id,q1.campaign_id,q1.ad_group_id,q1.top_parent_asin,q1.target_id,q2.strategy_id)) as row_id
               ,q2.strategy_id
               ,q1.profile_id
               ,q1.marketplace_id
               ,q1.marketplace_name
               ,q1.currency_code
               ,q1.ad_type
               ,q1.seller_id
               ,q1.seller_name
               ,q1.campaign_id
               ,q1.campaign_name
               ,q1.ad_group_id
               ,q1.ad_group_name
               ,q1.top_parent_asin
               ,q1.adv_manager_id
               ,q1.adv_manager_name
               ,q1.selling_price
               ,q1.main_asin_url
               ,q1.main_img_url
               ,q1.target_id
               ,q1.target_term
               ,case when q1.term_type_label <> '投放词' then null
                     when q1.match_type = 'EXACT' then '精准匹配' when q1.match_type = 'BROAD' then '宽泛匹配' when q1.match_type = 'PHRASE' then '词组匹配' end as match_type
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
               ,q1.ad_mode_label
               ,q1.term_type_label
               ,q1.stock_label
               ,q1.click_label
               ,q1.cvr_label
               ,q2.action_type
               ,getdate() as create_time
from dws_mkt_adv_strategy_stop_word_product_detail_tmp05 q1
         inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                    on q1.tenant_id = q2.tenant_id
                        and q1.life_cycle_label = q2.life_cycle_label
                        and q1.goal_label = q2.goal_label
                        --and nvl(q1.stock_label,' ') = nvl(q2.stock_label,' ')
--and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
--and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                        and q1.term_type_label = q2.term_type_label
                        and q1.ad_mode_label = q2.ad_mode_label
--inner join dws_mkt_adv_strategy_stop_word_product_detail_tmp03 q3
--on q1.tenant_id = q2.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id and q1.ad_group_id = q3.ad_group_id and q1.target_id = q3.target_id
where q2.action_name in ('暂停投放品','暂停投放词') and q2.life_cycle_label = '成熟期'
;
