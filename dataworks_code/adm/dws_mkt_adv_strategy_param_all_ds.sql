--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:11:56
--********************************************************************--
CREATE TABLE IF NOT EXISTS asq_dw.dws_mkt_adv_strategy_param_all_ds(
    tenant_id STRING COMMENT '租户ID',
    operate_type STRING COMMENT '操作类型英文',
    operate_name STRING COMMENT '操作类型中文',
    row_id STRING COMMENT '策略行级明细ID',
    profile_id STRING COMMENT '广告配置ID',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    ad_group_id STRING COMMENT '广告组ID',
    ad_group_name STRING COMMENT '广告组名称',
    match_type STRING COMMENT '搜索词匹配类型英文:NEGATIVE_EXACT/NEGATIVE_PHRASE/NEGATIVE_BROAD',
    search_term STRING COMMENT '具体的词',
    expression_type STRING COMMENT '搜索品匹配类型英文:ASIN_SAME_AS/ASIN_BRAND_SAME_AS',
    expression_value STRING COMMENT '具体品ID/品牌ID',
    campaign_portfolio_id STRING COMMENT '亚马逊广告分组',
    campaign_targeting_type STRING COMMENT '广告投放类型',
    campaign_state STRING COMMENT '新建广告活动后设置初始状态',
    campaign_start_date DATETIME COMMENT '广告活动开始日期',
    campaign_end_date DATETIME COMMENT '广告活动结束日期',
    campaign_dynamicbidding_strategy STRING COMMENT '竞价策略',
    campaign_dynamicbidding_placementbidding STRING COMMENT '根据广告位调整竞价',
    campaign_budget DECIMAL(18,6) COMMENT '广告活动预算',
    ad_group_state STRING COMMENT '新建广告组后设置初始状态',
    ad_group_defaultbid DECIMAL(18,6) COMMENT '若不单独设置竞价金额，词或品取这个值作为默认竞价值',
    ad_product_state STRING COMMENT '新建推广品后设置初始状态',
    ad_product_sku STRING COMMENT '推广品SKU名',
    ad_product_asin STRING COMMENT '推广品ASIN名',
    targeting_product_state STRING COMMENT '新建投放品后设置初始状态',
    targeting_product_expression STRING COMMENT '投放品投放策略',
    targeting_product_bid DECIMAL(18,6) COMMENT '投放品竞价金额',
    targeting_product_expression_type STRING COMMENT '投放品投放类型',
    keyword_state STRING COMMENT '新建投放词后设置初始状态',
    keyword_bid DECIMAL(18,6) COMMENT '投放词竞价金额',
    campaign_budget_type STRING COMMENT '广告活动预算类型',
    campaign_budget_effective DECIMAL(18,6) COMMENT '广告活动有效预算',
    keyword_match_type STRING COMMENT '投放词匹配类型',
    targeting_product_id STRING COMMENT '投放品id',
    keyword_id STRING COMMENT '投放词id'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='爱思奇中腾广告策略参数表')
    LIFECYCLE 366;

-- create table if not exists dws_mkt_adv_strategy_param_all_ds
-- (
--    tenant_id  string  comment'租户ID'
--   ,operate_type string  comment'操作类型英文'
--   ,operate_name string  comment'操作类型中文'
--   ,row_id string  comment'策略行级明细ID'
--   ,profile_id string  comment'广告配置ID'
--   ,campaign_id string  comment'广告活动ID'
--   ,campaign_name string  comment'广告活动名称'
--   ,ad_group_id string  comment'广告组ID'
--   ,ad_group_name string  comment'广告组名称'
--   ,match_type string  comment'搜索词匹配类型英文:NEGATIVE_EXACT/NEGATIVE_PHRASE/NEGATIVE_BROAD'
--   ,search_term string  comment'具体的词'
--   ,expression_type string  comment'搜索品匹配类型英文:ASIN_SAME_AS/ASIN_BRAND_SAME_AS'
--   ,expression_value string  comment'具体品ID/品牌ID'
-- )
-- comment '爱思奇中腾广告策略参数表'
-- partitioned by
-- (
--     ds  string
-- )
-- lifecycle 366
-- ;


-- alter table dws_mkt_adv_strategy_param_all_ds add columns (targeting_product_id STRING comment '投放品id', keyword_id STRING comment '投放词id' );


alter table dws_mkt_adv_strategy_param_all_ds drop if exists partition (ds = '${bizdate}');

--否词否品
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_id
  ,campaign_name
  ,ad_group_id
  ,ad_group_name
  ,match_type
  ,search_term
  ,expression_type
  ,expression_value
)
select tenant_id
     ,operate_type
     ,operate_name
     ,row_id
     ,profile_id
     ,campaign_id
     ,campaign_name
     ,split(sub_group,'_/_')[1] as ad_group_id
     ,ad_group_name
     ,match_type
     ,search_term
     ,expression_type
     ,expression_value
from (
         select  tenant_id
              ,case when term_type_label = '搜索词' then 'add_neg_keyword_operate' else 'add_neg_product_operate' end as operate_type
              ,case when term_type_label = '搜索词' then '添加否词' else '添加否品' end as operate_name
              ,row_id
              ,profile_id
              ,campaign_id
              ,campaign_name
              ,null as ad_group_name
              ,ad_group_id_list
              ,case when term_type_label = '搜索词' then 'NEGATIVE_EXACT' else null end as match_type
              ,case when term_type_label = '搜索词' then search_term else null end as search_term
              ,case when term_type_label = '搜索品' then 'ASIN_SAME_AS' else null end as expression_type
              ,case when term_type_label = '搜索品' then search_term else null end as expression_value
         from dws_mkt_adv_strategy_neg_word_product_detail_ds
         where ds = '${bizdate}'  --T日数据
     )
         lateral view explode(split(ad_group_id_list,'_&_')) tmpTable as sub_group    --通过"_&_"分割多个拼接
;

--否词根
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_id
  ,campaign_name
  ,ad_group_id
  ,ad_group_name
  ,match_type
  ,search_term
)
select tenant_id
     ,operate_type
     ,operate_name
     ,row_id
     ,profile_id
     ,campaign_id
     ,campaign_name
     ,split(sub_group,'_/_')[1] as ad_group_id
     ,ad_group_name
     ,match_type
     ,search_term
from (
         select  tenant_id
              ,'add_neg_stem_operate' as operate_type
              ,'添加否词根' as operate_name
              ,row_id
              ,profile_id
              ,campaign_id
              ,campaign_name
              ,null as ad_group_name
              ,ad_group_id_list
              ,'NEGATIVE_PHRASE' as match_type
              ,word as search_term
         from dws_mkt_adv_strategy_neg_adv_word_ds
         where ds = '${bizdate}'  --T日数据
     )
         lateral view explode(split(ad_group_id_list,'_&_')) tmpTable as sub_group    --通过"_&_"分割多个拼接
;

--爱思奇添加投放大小词、添加投放品
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_name
  ,ad_group_name
  ,keyword_match_type
  ,search_term
  ,targeting_product_expression
  ,targeting_product_expression_type
)
select tenant_id
     ,case when term_type_label = '搜索词' then 'add_keyword_operate' else 'add_product_operate' end as operate_type
     ,case when term_type_label = '搜索词' then '添加投放词' else '添加投放品' end as operate_name
     ,row_id
     ,profile_id
     ,campaign_name_new
     ,ad_group_name_new
     ,case when term_type_label = '搜索品' then null
           when match_type_new = '精准' then 'EXACT'
           when match_type_new = '广泛' then 'BROAD' end as keyword_match_type
     ,case when term_type_label = '搜索品' then null else search_term end as search_term
     ,case when term_type_label = '搜索词' then null else concat('ASIN_SAME_AS_/_',search_term) end as targeting_product_expression
     ,case when term_type_label = '搜索词' then null else 'MANUAL' end as targeting_product_expression_type
from dws_mkt_adv_strategy_add_word_product_detail_ds
where ds = '${bizdate}' and tenant_id = '1555073968741003270'
;



--中腾添加投放大小词、添加投放品
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_name
  ,campaign_targeting_type
  ,campaign_state
  ,campaign_start_date
  ,campaign_dynamicbidding_strategy
  ,campaign_budget
  ,campaign_budget_type
  ,campaign_budget_effective
  ,ad_group_name
  ,ad_group_state
  ,ad_group_defaultbid
  ,search_term
  ,ad_product_state
  ,ad_product_sku
  ,targeting_product_state
  ,targeting_product_expression
  ,targeting_product_bid
  ,targeting_product_expression_type
  ,keyword_state
  ,keyword_bid
  ,keyword_match_type
)
select tenant_id
     ,case when term_type_label = '搜索词' then 'add_keyword_operate' else 'add_product_operate' end as operate_type
     ,case when term_type_label = '搜索词' then '添加投放词' else '添加投放品' end as operate_name
     ,row_id
     ,profile_id
     ,campaign_name_new
     ,'MANUAL' as campaign_targeting_type
     ,'ENABLED' as campaign_state
     ,getdate() as campaign_start_date
     ,'MANUAL' as campaign_dynamicbidding_strategy
     ,10 as campaign_budget
     ,'DAILY' as campaign_budget_type
     ,cast(null as decimal(18,6)) as campaign_budget_effective
     ,ad_group_name_new
     ,'ENABLED' as ad_group_state
     ,0.6 as ad_group_defaultbid
     ,case when term_type_label = '搜索品' then null else search_term end as search_term
     ,'ENABLED' as ad_product_state
     ,target_sku_list as ad_product_sku
     ,'ENABLED' as targeting_product_state
     ,case when term_type_label = '搜索词' then null else concat('ASIN_SAME_AS_/_',search_term) end as targeting_product_expression
     ,cast(case when term_type_label = '搜索词' then null else 1.25 * nvl(CPC,cate_cpc) end as decimal(18,6)) as targeting_product_bid
     ,'MANUAL' as targeting_product_expression_type
     ,'ENABLED' as keyword_state
     ,cast(case when term_type_label = '搜索品' then null else 1.25 * nvl(CPC,cate_cpc) end as decimal(18,6)) as keyword_bid
     ,case when term_type_label = '搜索品' then null
           when match_type_new = '精准' then 'EXACT'
           when match_type_new = '广泛' then 'BROAD' end as keyword_match_type
from dws_mkt_adv_strategy_add_word_product_detail_ds
where ds = '${bizdate}' and tenant_id = '1714548493239062529'
;



--中腾/爱思奇调整预算
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_id
  ,campaign_budget
  ,campaign_budget_type
)
select tenant_id
     ,'adjust_campaign_budget_operate' as operate_type
     ,'调整预算' as operate_name
     ,row_id
     ,profile_id
     ,campaign_id
     ,camp_budget_amt_new as campaign_budget
     ,'DAILY' as campaign_budget_type
from dws_mkt_adv_strategy_adjust_budget_detail_ds
where ds = '${bizdate}'
;


--爱思奇中腾好词晋升参数
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_id
  ,campaign_name
  ,campaign_state
  ,ad_group_name
  ,ad_group_state
  ,ad_group_defaultbid
  ,ad_product_state
  ,ad_product_sku
  ,keyword_state
  ,keyword_bid
  ,keyword_match_type
  ,search_term
)
select tenant_id
     ,'add_keyword_upgrade_operate' as operate_type
     ,'好词晋升' as operate_name
     ,row_id
     ,profile_id
     ,campaign_id
     ,campaign_name
     ,'ENABLED' as campaign_state
     ,ad_group_name_new
     ,case when tenant_id = '1714548493239062529' then 'ENABLED' end as ad_group_state
     ,case when tenant_id = '1714548493239062529' then 0.6 end as ad_group_defaultbid
     ,case when tenant_id = '1714548493239062529' then 'ENABLED' end as ad_product_state
     ,case when tenant_id = '1714548493239062529' then target_sku_list end as ad_product_sku
     ,case when tenant_id = '1714548493239062529' then 'ENABLED' end as keyword_state
     ,case when tenant_id = '1714548493239062529' then cast(1.25 * nvl(cpc,cate_cpc) as decimal(18,6)) end as keyword_bid
     ,case when match_type_new = '精准' then 'EXACT'
           when match_type_new = '词组' then 'PHRASE' end as keyword_match_type
     ,search_term
from dws_mkt_adv_strategy_word_upgrade_detail_ds
where ds = '${bizdate}'
;


--暂停投放
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,keyword_id
  ,keyword_state
  ,targeting_product_id
  ,targeting_product_state
)
select tenant_id
     ,case when term_type_label = '投放词' then 'pause_keyword_operate' else 'pause_product_operate' end as operate_type
     ,case when term_type_label = '投放词' then '暂停投放词' else '暂停投放品' end as operate_name
     ,row_id
     ,profile_id
     ,case when term_type_label = '投放词' then target_id end as keyword_id
     ,case when term_type_label = '投放词' then 'PAUSED' end as keyword_state
     ,case when term_type_label = '投放品' then target_id end as targeting_product_id
     ,case when term_type_label = '投放品' then 'PAUSED' end as targeting_product_state
from dws_mkt_adv_strategy_stop_word_product_detail_ds
where ds = '${bizdate}'
;


--爱思奇搜索词守坑，在已有的广告活动广告组中添加投放词
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_name
  ,ad_group_name
  ,keyword_match_type
  ,keyword_bid
  ,search_term
)
select tenant_id
     ,'term_location_operate' as operate_type
     ,'搜索词守坑' as operate_name
     ,row_id
     ,profile_id
     ,campaign_name_new
     ,ad_group_name_new
     ,'EXACT' as keyword_match_type
     ,bid_new as keyword_bid
     ,search_term
from dws_mkt_adv_strategy_search_term_adjust_bid_detail_ds
where ds = '${bizdate}' and tenant_id = '1555073968741003270'
;



--中腾搜索词守坑，新建广告活动广告组中添加投放词
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_name
  ,campaign_targeting_type
  ,campaign_state
  ,campaign_start_date
  ,campaign_dynamicbidding_strategy
  ,campaign_budget
  ,campaign_budget_type
  ,campaign_budget_effective
  ,ad_group_name
  ,ad_group_state
  ,ad_group_defaultbid
  ,search_term
  ,ad_product_state
  ,ad_product_sku
  ,keyword_state
  ,keyword_bid
  ,keyword_match_type
)
select tenant_id
     ,'term_location_operate' as operate_type
     ,'搜索词守坑' as operate_name
     ,row_id
     ,profile_id
     ,campaign_name_new
     ,'MANUAL' as campaign_targeting_type
     ,'ENABLED' as campaign_state
     ,getdate() as campaign_start_date
     ,'LEGACY_FOR_SALES' as campaign_dynamicbidding_strategy
     ,campaign_budget_new as campaign_budget
     ,'DAILY' as campaign_budget_type
     ,cast(null as decimal(18,6)) as campaign_budget_effective
     ,ad_group_name_new
     ,'ENABLED' as ad_group_state
     ,0.6 as ad_group_defaultbid
     ,search_term
     ,'ENABLED' as ad_product_state
     ,seller_sku_list as ad_product_sku
     ,'ENABLED' as keyword_state
     ,bid_new as keyword_bid
     ,'EXACT' as keyword_match_type
from dws_mkt_adv_strategy_search_term_adjust_bid_detail_ds
where ds = '${bizdate}' and tenant_id = '1714548493239062529'
;


--中腾/爱思奇上调预算
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_id
  ,campaign_budget
  ,campaign_budget_type
)
select tenant_id
     ,'adjust_campaign_budget_operate' as operate_type
     ,'调整预算' as operate_name
     ,row_id
     ,profile_id
     ,campaign_id
     ,camp_budget_amt_new as campaign_budget
     ,'DAILY' as campaign_budget_type
from dws_mkt_adv_strategy_adjust_budget_raise_detail_ds
where ds = '${bizdate}'
;


-------------------------------------------------新品策略-------------------------------------------------
--否词否品
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_id
  ,campaign_name
  ,ad_group_id
  ,ad_group_name
  ,match_type
  ,search_term
  ,expression_type
  ,expression_value
)
select tenant_id
     ,operate_type
     ,operate_name
     ,row_id
     ,profile_id
     ,campaign_id
     ,campaign_name
     ,split(sub_group,'_/_')[1] as ad_group_id
     ,ad_group_name
     ,match_type
     ,search_term
     ,expression_type
     ,expression_value
from (
         select  tenant_id
              ,case when term_type_label = '搜索词' then 'add_neg_keyword_operate' else 'add_neg_product_operate' end as operate_type
              ,case when term_type_label = '搜索词' then '添加否词' else '添加否品' end as operate_name
              ,row_id
              ,profile_id
              ,campaign_id
              ,campaign_name
              ,null as ad_group_name
              ,ad_group_id_list
              ,case when term_type_label = '搜索词' then 'NEGATIVE_EXACT' else null end as match_type
              ,case when term_type_label = '搜索词' then search_term else null end as search_term
              ,case when term_type_label = '搜索品' then 'ASIN_SAME_AS' else null end as expression_type
              ,case when term_type_label = '搜索品' then search_term else null end as expression_value
         from dws_mkt_adv_strategy_np_neg_word_product_detail_ds
         where ds = '${bizdate}'  --T日数据
     )
         lateral view explode(split(ad_group_id_list,'_&_')) tmpTable as sub_group    --通过"_&_"分割多个拼接
;


--爱思奇添加投放大小词、添加投放品
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,campaign_name
  ,ad_group_name
  ,keyword_match_type
  ,search_term
  ,targeting_product_expression
  ,targeting_product_expression_type
)
select tenant_id
     ,case when term_type_label = '搜索词' then 'add_keyword_operate' else 'add_product_operate' end as operate_type
     ,case when term_type_label = '搜索词' then '添加投放词' else '添加投放品' end as operate_name
     ,row_id
     ,profile_id
     ,campaign_name_new
     ,ad_group_name_new
     ,case when term_type_label = '搜索品' then null
           when match_type_new = '精准' then 'EXACT'
           when match_type_new = '广泛' then 'BROAD' end as keyword_match_type
     ,case when term_type_label = '搜索品' then null else search_term end as search_term
     ,case when term_type_label = '搜索词' then null else concat('ASIN_SAME_AS_/_',search_term) end as targeting_product_expression
     ,case when term_type_label = '搜索词' then null else 'MANUAL' end as targeting_product_expression_type
from dws_mkt_adv_strategy_np_add_word_product_detail_ds
where ds = '${bizdate}' and tenant_id = '1555073968741003270'
;


--暂停投放
insert into table dws_mkt_adv_strategy_param_all_ds partition (ds = '${bizdate}')
(
   tenant_id
  ,operate_type
  ,operate_name
  ,row_id
  ,profile_id
  ,keyword_id
  ,keyword_state
  ,targeting_product_id
  ,targeting_product_state
)
select tenant_id
     ,case when term_type_label = '投放词' then 'pause_keyword_operate' else 'pause_product_operate' end as operate_type
     ,case when term_type_label = '投放词' then '暂停投放词' else '暂停投放品' end as operate_name
     ,row_id
     ,profile_id
     ,case when term_type_label = '投放词' then target_id end as keyword_id
     ,case when term_type_label = '投放词' then 'PAUSED' end as keyword_state
     ,case when term_type_label = '投放品' then target_id end as targeting_product_id
     ,case when term_type_label = '投放品' then 'PAUSED' end as targeting_product_state
from dws_mkt_adv_strategy_np_stop_word_product_detail_ds
where ds = '${bizdate}'
;