--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:14:15
--********************************************************************--


CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_word_upgrade_detail_ds
(
    tenant_id             STRING COMMENT '租户ID'
    ,row_id                STRING COMMENT '行级策略明细唯一ID'
    ,strategy_id           STRING COMMENT '策略ID'
    ,profile_id            STRING COMMENT '配置ID'
    ,marketplace_id        STRING COMMENT '站点ID'
    ,marketplace_name      STRING COMMENT '站点名称'
    ,currency_code         STRING COMMENT '币种'
    ,ad_type               STRING COMMENT '广告类型'
    ,seller_id             STRING COMMENT '卖家ID'
    ,seller_name           STRING COMMENT '卖家名称'
    ,adv_manager_id        STRING COMMENT '广告负责人ID'
    ,adv_manager_name      STRING COMMENT '广告负责人名称'
    ,campaign_id           STRING COMMENT '广告活动id'
    ,campaign_name         STRING COMMENT '广告活动名称'
    ,campaign_name_new     STRING COMMENT '目标广告活动名称'
    ,ad_group_name_new     STRING COMMENT '目标广告组名称'
    ,target_sku_list       STRING COMMENT '推广品列表'
    ,match_type            STRING COMMENT '原搜索词匹配类型'
    ,match_type_new        STRING COMMENT '目标投放词匹配类型'
    ,top_parent_asin       STRING COMMENT '父aisn'
    ,selling_price         STRING COMMENT '售价'
    ,main_asin_url         STRING COMMENT '商品链接'
    ,main_img_url          STRING COMMENT '商品主图'
    ,search_term           STRING COMMENT '投放词'
    ,impressions           BIGINT COMMENT '曝光量'
    ,clicks                BIGINT COMMENT '点击量'
    ,cost                  DECIMAL(18,6) COMMENT '广告花费'
    ,sale_amt              DECIMAL(18,6) COMMENT '广告销售额'
    ,order_num             BIGINT COMMENT '广告销量'
    ,ctr                   DECIMAL(18,6) COMMENT 'CTR'
    ,cvr                   DECIMAL(18,6) COMMENT 'CVR'
    ,cpc                   DECIMAL(18,6) COMMENT 'CPC'
    ,acos                  DECIMAL(18,6) COMMENT 'ACOS'
    ,category              STRING COMMENT '父ASIN所属类目'
    ,cate_ctr              DECIMAL(18,6) COMMENT '父ASIN所属类目平均CTR'
    ,cate_cvr              DECIMAL(18,6) COMMENT '父ASIN所属类目平均CVR'
    ,cate_cpc              DECIMAL(18,6) COMMENT '父ASIN所属类目平均CPC'
    ,cate_acos             DECIMAL(18,6) COMMENT '父ASIN所属类目平均ACOS'
    ,aba_rank              BIGINT COMMENT 'aba排名'
    ,aba_date              DATETIME COMMENT 'aba日期'
    ,norm_rank             BIGINT COMMENT '自然搜索排名'
    ,adv_rank              BIGINT COMMENT '广告搜索排名'
    ,adv_days              BIGINT COMMENT '广告指标统计天数'
    ,life_cycle_label      STRING COMMENT '生命周期'
    ,goal_label            STRING COMMENT '目标标签'
    ,stock_label           STRING COMMENT '库存标签'
    ,ad_mode_label         STRING COMMENT '投放类型标签'
    ,term_type_label       STRING COMMENT '投放对象类型'
    ,norm_rank_label       STRING COMMENT '自然搜索排名标签'
    ,click_label           STRING COMMENT '点击标签'
    ,cvr_label             STRING COMMENT '转化标签'
    ,adv_rank_label        STRING COMMENT '广告搜索排名标签'
    ,flow_label            STRING COMMENT '流量标签'
    ,cpc_label             STRING COMMENT 'cpc标签'
    ,match_type_label      STRING COMMENT '匹配类型标签'
    ,action_type           STRING COMMENT '操作类型'
    ,create_time           DATETIME COMMENT '创建时间'
    ,sale_monopoly_rate decimal(18,6) comment 'TOP3商品转化份额'
    ,chn_seller_rate decimal(18,6) comment '搜索词前三页中国卖家占比'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略好词晋升(子表)')
    LIFECYCLE 366
;


--基础数据
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp01;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp01 as
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
     ,camp_name_list
     ,camp_id_list
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,term_type
     ,search_term
     ,match_type
     ,impressions
     ,clicks
     ,cost
     ,sale_amt
     ,sale_num
     ,ctr
     ,cvr
     ,cpc
     ,acos
     ,aba_rank
     ,aba_date
     ,adv_days
from whde.adm_amazon_adv_strategy_pasin_search_term_d
where ds = '${bizdate}' --T日
  and adv_days >= 14
  and nvl(parent_asin,'') <> ''
  and ad_mode = '手动投放' and term_type = '搜索词' and match_type in ('PHRASE','BROAD')  --宽泛匹配和词组匹配
;


--建议对象筛选：库存充足、成熟期
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp02;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp02 as
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
     ,'库存充足' stock_label
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
     ,create_time
     ,ds
from    whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = '${bizdate}' and life_cycle_label = '成熟期'  and term_type = '搜索词' --and stock_label = '库存充足'
;



--父asin在广告活动上的花费
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp03_1;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp03_1 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.campaign_id
     ,q2.campaign_name
     ,q1.parent_asin
     ,q1.search_term
     ,q1.match_type
     ,q2.cost
from (
         select tenant_id
              ,profile_id
              ,campaign_id
              ,parent_asin
              ,search_term
              ,match_type
         from dws_mkt_adv_strategy_word_upgrade_detail_tmp01
                  lateral view explode(split(camp_id_list,'_&_')) tmpTable as campaign_id
         group by tenant_id
                 ,profile_id
                 ,campaign_id
                 ,parent_asin
                 ,search_term
                 ,match_type
     )q1
         inner join (
    select tenant_id
         ,profile_id
         ,parent_asin
         ,campaign_id
         ,campaign_name
         ,sum(cost) as cost
    from whde.adm_amazon_adv_strategy_search_term_d
    where ds = '${bizdate}'  and nvl(parent_asin,'') <> ''
    group by tenant_id
           ,profile_id
           ,parent_asin
           ,campaign_id
           ,campaign_name
)q2
                    on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.parent_asin and q1.campaign_id = q2.campaign_id
;


--父ASIN在广告活动上花费的排序
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp03_2;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp03_2 as
select tenant_id
     ,profile_id
     ,parent_asin
     ,campaign_id
     ,campaign_name
     ,search_term
     ,match_type
     ,cost
     ,rank()over(partition by tenant_id,profile_id,parent_asin order by cost desc) as rk   --不连续的排序
from dws_mkt_adv_strategy_word_upgrade_detail_tmp03_1
;


--父ASIN花费最大的广告活动下投放词的现状
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp03_3;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp03_3 as
select q1.tenant_id
     ,q1.profile_id
     ,q1.parent_asin
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.search_term
     ,q1.match_type as search_term_match_type
     ,case when q1.match_type = 'BROAD' then 1 when q1.match_type = 'PHRASE' then 2 end as search_term_match_level
     ,case when q3.match_type = 1 then 'BROAD' when q3.match_type = 2 then 'PHRASE'  when q3.match_type = 3 then 'EXACT' end as keyword_match_type
     ,nvl(q3.match_type,0) as keyword_match_level
from (
         select tenant_id
              ,profile_id
              ,parent_asin
              ,campaign_id
              ,campaign_name
              ,search_term
              ,match_type
         from dws_mkt_adv_strategy_word_upgrade_detail_tmp03_2
         where rk = 1
     )q1
         left join (
    select tenant_id
         ,profile_id
         ,campaign_id
         ,keyword_text
         ,max(case when match_type = 'BROAD' then 1 when match_type = 'PHRASE' then 2 when match_type = 'EXACT' then 3 end) as match_type
    from whde.adm_amazon_adv_keyword_target_status_df
    where ds = '${bizdate}' and status = 'ENABLED'
    group by tenant_id
           ,profile_id
           ,campaign_id
           ,keyword_text
)q3
                   on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id and tolower(q1.search_term) = tolower(q3.keyword_text)
;




--搜索词排名
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp05;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp05 as
select  top_parent_asin as parent_asin
     ,search_term
     ,norm_rank_label as p1_norm_label
     ,adv_rank_label as p1_adv_label
     ,cast(adv_rank as bigint) as adv_rank
     ,cast(norm_rank as bigint) as norm_rank
     ,marketplace_id
from whde.dws_mkt_adv_strategy_search_term_pasin_rank_ds
where ds = '${bizdate}' and term_type = '搜索词'
;



--搜索词爬虫清单
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp05_1;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp05_1 as
select  search_term
     ,marketplace_id
from   dws_mkt_adv_strategy_word_upgrade_detail_tmp05
group by search_term
       ,marketplace_id
;


--搜索词的垄断和中国卖家占比指标
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_tmp06;
create table dws_mkt_adv_strategy_word_upgrade_detail_tmp06 as
select marketplace_id
     ,keyword as search_term
     ,cast(sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
-- ,cast(chn_seller_rate as decimal(18,6)) as chn_seller_rate
from whde.dws_mkt_adv_strategy_keyword_index_ds
where ds = '${bizdate}'
;


--确定推广品(销量TOP10)
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp00;
create table dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp00 as
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
                  where   ds = max_pt('whde.adm_amazon_adv_sku_wide_d') and asin_fba_stock_num >= 5
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



--关联数据
drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp01;
create table dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp01 as
select  q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q1.camp_name_list
     ,q1.camp_id_list
     ,q1.parent_asin
     ,q3.adv_manager_id
     ,q3.adv_manager_name
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
     ,q1.term_type
     ,q1.search_term
     ,q1.match_type
     ,q1.impressions
     ,q1.clicks
     ,q1.cost
     ,q1.sale_amt
     ,q1.sale_num as order_num
     ,q1.ctr
     ,q1.cvr
     ,q1.cpc
     ,q1.acos
     ,q3.category as category_list
     ,q3.cate_impressions
     ,q3.cate_clicks
     ,q3.cate_cost
     ,q3.cate_sale_amt
     ,q3.cate_order_num
     ,q3.cate_ctr
     ,q3.cate_cvr
     ,q3.cate_cpc
     ,q3.cate_acos
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_days
     ,q3.stock_label
     ,case when q5.search_term is null then null when q5.search_term is not null then nvl(q2.p1_norm_label,0) end as p1_norm_label
     ,case when q5.search_term is null then null when q5.search_term is not null then nvl(q2.p1_adv_label,0) end as p1_adv_label
     ,case when q5.search_term is null then null when q5.search_term is not null then nvl(q2.norm_rank,999) end as norm_rank
     ,case when q5.search_term is null then null when q5.search_term is not null then nvl(q2.adv_rank,999) end as adv_rank
     ,q4.sku_list
from dws_mkt_adv_strategy_word_upgrade_detail_tmp01 q1
         left join dws_mkt_adv_strategy_word_upgrade_detail_tmp05 q2 --自然排名和广告排名
                   on q1.marketplace_id = q2.marketplace_id and tolower(q1.search_term) = tolower(q2.search_term) and q1.parent_asin = q2.parent_asin
         inner join dws_mkt_adv_strategy_word_upgrade_detail_tmp02 q3 --库存标签判断条件
                    on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.parent_asin = q3.top_parent_asin and q1.term_type = q3.term_type
         inner join dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp00 q4  --推广品
                    on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and q1.parent_asin = q4.parent_asin
         left join dws_mkt_adv_strategy_word_upgrade_detail_tmp05_1 q5  --爬虫清单
                   on q1.marketplace_id = q5.marketplace_id and tolower(q1.search_term) = tolower(q5.search_term)
;



drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp02;
create table dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp02 as
select tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,ad_type
     ,campaign_id
     ,parent_asin as top_parent_asin
     ,adv_manager_id
     ,adv_manager_name
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,search_term
     ,match_type
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
     ,cate_ctr
     ,cate_cvr
     ,cate_cpc
     ,cate_acos
     ,aba_rank
     ,aba_date
     ,adv_days
     ,norm_rank
     ,adv_rank
     ,sku_list
     ,life_cycle_label
     ,goal_label
     ,ad_mode_label
     ,term_type_label
     ,stock_label
     ,norm_rank_label
     ,case when stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '有效点击' and cvr_label = '高转化' then 'ALL' else adv_rank_label end as adv_rank_label
     ,click_label
     ,case when stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '无效点击' and order_num >=2 then '出单' else cvr_label end as cvr_label
     ,case when stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '无效点击' and order_num >=2 and adv_rank_label = '广告排名非第一页' then flow_label else null end flow_label
     ,case when stock_label = '库存充足' and norm_rank_label = '自然排名非第一页' and click_label = '有效点击' and cvr_label = '一般转化' and adv_rank_label = '广告排名第一页' then cpc_label else null end cpc_label
     ,match_type_label
from (
         select  tenant_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,currency_code
              ,seller_id
              ,seller_name
              ,ad_type
              ,campaign_id
              ,parent_asin
              ,adv_manager_id
              ,adv_manager_name
              ,selling_price
              ,main_asin_url
              ,main_image_url
              ,search_term
              ,match_type
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
              ,norm_rank
              ,adv_rank
              ,sku_list
              ,'成熟期' as life_cycle_label    --生命周期
              ,'利润最大化' as goal_label --目标标签
              ,'手动投放' as ad_mode_label
              ,'搜索词' as term_type_label
              ,stock_label                             --库存标签
              ,case when p1_norm_label = 1 then '自然排名第一页' when p1_norm_label = 0 then '自然排名非第一页' end as norm_rank_label         --自然搜索坑位排名标签
              ,case when p1_adv_label = 1 then '广告排名第一页' when p1_adv_label = 0 then '广告排名非第一页' end as adv_rank_label            --广告搜索坑位排名标签
              ,case when clicks < 1/cate_cvr then '无效点击'
                    when clicks >= 1/cate_cvr then '有效点击' end as click_label    --点击标签
              ,case when order_num = 0 then '不出单'
                    when cvr < cate_cvr * 0.5 then '低转化'
                    when cvr between cate_cvr * 0.5 and cate_cvr then '一般转化'
                    when cvr >= cate_cvr then '高转化' end as cvr_label --转化标签
              ,case when cpc < cate_cpc then '低CPC' when cpc >= cate_cpc then '高CPC' end as cpc_label  --CPC标签
              ,'ALL' as flow_label
              ,case when match_type = 'BROAD' then '宽泛匹配' when match_type = 'PHRASE' then '词组匹配' else match_type end as match_type_label
         from dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp01
                  lateral view explode(split(camp_id_list,'_&_')) tmpTable as campaign_id
         where cate_cvr > 0
     )q1
;

drop table if exists dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp03;
create table dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp03 as
select q1.tenant_id
     ,hash(concat(q1.tenant_id,q1.profile_id,q3.campaign_id,q1.top_parent_asin,q1.search_term,q1.match_type_label,q2.strategy_id)) as row_id
     ,q2.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.seller_id
     ,q1.seller_name
     ,q1.ad_type
     ,q3.campaign_id
     ,q3.campaign_name
     ,q1.top_parent_asin
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_image_url
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
     ,q1.category_list
     ,q1.cate_ctr
     ,q1.cate_cvr
     ,q1.cate_cpc
     ,q1.cate_acos
     ,q1.aba_rank
     ,q1.aba_date
     ,q1.adv_days
     ,q1.norm_rank
     ,q1.adv_rank
     ,q1.sku_list
     ,q1.life_cycle_label
     ,q1.goal_label
     ,q1.ad_mode_label
     ,q1.term_type_label
     ,q1.stock_label
     ,q1.norm_rank_label
     ,q1.adv_rank_label
     ,q1.click_label
     ,q1.cvr_label
     ,q1.flow_label
     ,q1.cpc_label
     ,q1.match_type_label
     ,q3.keyword_match_type
     ,q3.keyword_match_level
     ,q2.action_type
     ,cast(q4.sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
-- ,cast(q4.chn_seller_rate as decimal(18,6)) as chn_seller_rate
from  dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp02 q1
          inner join whde.dws_mkt_adv_strategy_main_all_df q2   --基于母表完成否词否品的标签组合筛选
                     on q1.tenant_id = q2.tenant_id
                         and q1.life_cycle_label = q2.life_cycle_label
                         and q1.goal_label = q2.goal_label
                         and q1.stock_label = q2.stock_label
                         --and nvl(q1.norm_rank_label,' ') = nvl(q2.norm_rank_label,' ')
--and nvl(q1.adv_rank_label,' ') = nvl(q2.adv_rank_label,' ')
                         and nvl(q1.click_label,' ') = nvl(q2.click_label,' ')
                         and nvl(q1.cvr_label,' ') = nvl(q2.cvr_label,' ')
                         and nvl(q1.cpc_label,' ') = nvl(q2.cpc_label,' ')
                         and nvl(q1.flow_label,' ') = nvl(q2.flow_label,' ')
                         and nvl(q1.match_type_label,' ') = nvl(q2.match_type_label,' ')
                         and nvl(q1.ad_mode_label,' ') = nvl(q2.ad_mode_label,' ')
                         and nvl(q1.term_type_label,' ') = nvl(q2.term_type_label,' ')
          inner join dws_mkt_adv_strategy_word_upgrade_detail_tmp03_3 q3
                     on q1.tenant_id = q3.tenant_id and q1.campaign_id = q3.campaign_id and q1.top_parent_asin = q3.parent_asin and q1.search_term = q3.search_term
          left join dws_mkt_adv_strategy_word_upgrade_detail_tmp06 q4
                    on q1.marketplace_id = q4.marketplace_id and tolower(q1.search_term) = tolower(q4.search_term)
where ((q1.match_type_label = '宽泛匹配' and q3.keyword_match_level <= 1) or (q1.match_type_label = '词组匹配' and q3.keyword_match_level <= 2)) and q2.action_type = 'POS_TERM_UPGRADE' and q2.life_cycle_label = '成熟期'
group by q1.tenant_id
       ,hash(concat(q1.tenant_id,q1.profile_id,q3.campaign_id,q1.top_parent_asin,q1.search_term,q1.match_type_label,q2.strategy_id))
       ,q2.strategy_id
       ,q1.profile_id
       ,q1.marketplace_id
       ,q1.marketplace_name
       ,q1.currency_code
       ,q1.seller_id
       ,q1.seller_name
       ,q1.ad_type
       ,q3.campaign_id
       ,q3.campaign_name
       ,q1.top_parent_asin
       ,q1.adv_manager_id
       ,q1.adv_manager_name
       ,q1.selling_price
       ,q1.main_asin_url
       ,q1.main_image_url
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
       ,q1.category_list
       ,q1.cate_ctr
       ,q1.cate_cvr
       ,q1.cate_cpc
       ,q1.cate_acos
       ,q1.aba_rank
       ,q1.aba_date
       ,q1.adv_days
       ,q1.norm_rank
       ,q1.adv_rank
       ,q1.sku_list
       ,q1.life_cycle_label
       ,q1.goal_label
       ,q1.ad_mode_label
       ,q1.term_type_label
       ,q1.stock_label
       ,q1.norm_rank_label
       ,q1.adv_rank_label
       ,q1.click_label
       ,q1.cvr_label
       ,q1.flow_label
       ,q1.cpc_label
       ,q1.match_type_label
       ,q3.keyword_match_type
       ,q3.keyword_match_level
       ,q2.action_type
       ,cast(q4.sale_monopoly_rate as decimal(18,6))
-- ,cast(q4.chn_seller_rate as decimal(18,6))
;

--支持重跑
alter table dws_mkt_adv_strategy_word_upgrade_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_word_upgrade_detail_ds partition (ds = '${nowdate}')
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
      ,campaign_name_new
      ,ad_group_name_new
      ,top_parent_asin
      ,selling_price
      ,main_asin_url
      ,main_img_url
      ,search_term
      ,match_type
      ,match_type_new
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
      ,adv_days
      ,norm_rank
      ,adv_rank
      ,target_sku_list
      ,life_cycle_label
      ,goal_label
      ,ad_mode_label
      ,term_type_label
      ,stock_label
      ,norm_rank_label
      ,adv_rank_label
      ,click_label
      ,cvr_label
      ,flow_label
      ,cpc_label
      ,match_type_label
      ,action_type
      ,create_time
      ,sale_monopoly_rate
      ,chn_seller_rate
)
select tenant_id
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
     ,campaign_name as campaign_name_new
     ,case when match_type_label = '宽泛匹配' then  concat('SP_AI_词组匹配_',search_term)
           when match_type_label = '词组匹配' then  concat('SP_AI_精准匹配_',search_term) end as group_name
     ,top_parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,search_term
     ,match_type_label
     ,case when match_type_label = '词组匹配' then '精准' when match_type_label = '宽泛匹配' then '词组' end as match_type_new
     ,impressions
     ,clicks
     ,cast(cost as decimal(18,6)) as cost
     ,cast(sale_amt as decimal(18,6)) as sale_amt
     ,order_num
     ,cast(ctr as decimal(18,6)) as ctr
     ,cast(cvr as decimal(18,6)) as cvr
     ,cast(cpc as decimal(18,6)) as cpc
     ,cast(acos as decimal(18,6)) as acos
     ,category_list
     ,cast(cate_ctr as decimal(18,6)) as cate_ctr
     ,cast(cate_cvr as decimal(18,6)) as cate_cvr
     ,cast(cate_cpc as decimal(18,6)) as cate_cpc
     ,cast(cate_acos as decimal(18,6)) as cate_acos
     ,aba_rank
     ,aba_date
     ,adv_days
     ,norm_rank
     ,adv_rank
     ,sku_list
     ,life_cycle_label
     ,goal_label
     ,ad_mode_label
     ,term_type_label
     ,stock_label
     ,norm_rank_label
     ,adv_rank_label
     ,click_label
     ,cvr_label
     ,flow_label
     ,cpc_label
     ,match_type_label
     ,action_type
     ,getdate() as create_time
     ,sale_monopoly_rate
     ,0 chn_seller_rate
from  dws_mkt_adv_strategy_word_upgrade_detail_zt_tmp03
;