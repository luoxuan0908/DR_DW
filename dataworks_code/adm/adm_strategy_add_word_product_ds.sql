--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-07 00:47:42
--********************************************************************--


CREATE TABLE IF NOT EXISTS whde.adm_strategy_add_word_product_ds(

                                                                    row_id	string
    ,tenant_id string comment '租户ID'
    ,tenant_name string comment '租户名称'
    ,marketplace_id string comment '市场ID'
    ,marketplace_name string comment '市场名称'
    ,adv_manager_id string comment '广告负责人ID'
    ,adv_manager_name string comment '广告负责人'
    ,adv_department_list_id string comment '广告部门ID'
    ,adv_department_list_name string comment '广告部门列表'
    ,seller_id string comment '卖家ID'
    ,seller_name string comment '卖家名称'
    ,profile_id string comment '配置ID'
    ,top_parent_asin string comment '父asin'
    ,category_id string comment '商品类目ID'
    ,category_name string comment '商品类目名称'
    ,currency_code string comment '币种'
    ,strategy_id string comment '策略ID'
    ,strategy_name string comment '策略名称'
    ,action_id string comment '操作动作ID'
    ,action_name string comment '操作动作'
    ,object_term string comment '操作对象'
    ,orig_campaign_name_list string comment '广告活动名称'
    ,orig_campaign_match_type string comment '原始最高匹配类型'
    ,new_campaign_name string comment '目标广告活动名称'
    ,new_group_name string comment '目标广告组名称'
    ,target_sku_list string comment '推广list'
    ,new_campaign_match_type string comment '目标匹配类型'
    ,new_cpc string comment '新建的投放词竞价'
    ,new_bid_strategy string comment '新建的竞价策略'
    ,new_campaign_budget_amt string comment '新建的广告活动预算'
    ,statistic_days string comment '统计天数'
    ,clicks string comment '点击量'
    ,cost string comment '广告花费'
    ,sale_amt string comment '销售额'
    ,sale_num string comment '销售量'
    ,order_num string comment '订单量'
    ,cpc string comment '降价'
    ,acos string comment 'acos'
    ,cvr string comment '转化率'
    ,cate_cvr string comment '类目CVR'
    ,cate_cpc string comment '类目CPC'
    ,cate_acos string comment '类目ACOS'
    ,adv_search_rank string comment '广告坑位排名'
    ,natural_search_rank string comment '自然坑位排名'
    ,aba_rank string comment 'ABA搜索排名'
    ,aba_rank_week_to_week string comment 'aba搜索排名周环比'
    ,data_date string comment '日期'
)
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告策略添加大小词、投放品(子表)')
    LIFECYCLE 366;


select * from dws_mkt_adv_strategy_add_word_product_detail_ds where ds = '${bizadate}';


insert OVERWRITE   table adm_strategy_add_word_product_ds PARTITION (ds = '${nowdate}')
select distinct
    row_id
              ,a.tenant_id
              ,d.tenant_name
              ,a.marketplace_id
              ,a.marketplace_name
              ,d.adv_manager_id
              ,d.adv_manager_name
              ,d.adv_department_list_id
              ,d.adv_department_list_name
              ,d.seller_id
              ,d.seller_name
              ,a.profile_id
              ,a.top_parent_asin
              ,hash(category) category_id
              ,a.category category_name
              ,a.currency_code currency_code
              ,a.strategy_id strategy_id
              ,b.strategy_name strategy_name
              ,hash(action_type) action_id
              ,case when action_type = 'POS_TERM_SMALL' then '添加小词'
                    when  action_type= 'POS_PRODUCT_SMALL' then '添加投放品'
                    else '添加大词' end as action_name
              ,search_term object_term
              ,camp_name_list orig_campaign_name_list
              ,'紧密匹配' orig_campaign_match_type
              ,campaign_name_new new_campaign_name
              ,ad_group_name_new new_group_name
              ,target_sku_list target_sku_list
              ,'精准匹配' new_campaign_match_type
              ,cpc*1.2 new_cpc
              ,'只下降' new_bid_strategy
              ,15 new_campaign_budget_amt
              ,adv_days statistic_days
              ,clicks clicks
              ,cost cost
              ,sale_amt sale_amt
              ,order_num sale_num
              ,order_num order_num
              ,cpc cpc
              ,acos acos
              ,cvr cvr
              ,cate_cvr cate_cvr
              ,cate_cpc cate_cpc
              ,cate_acos cate_acos
              ,adv_rank adv_search_rank
              ,norm_rank natural_search_rank
              ,aba_rank aba_rank
              ,to_char(aba_date,'yyyymmdd')  aba_rank_week_to_week
              ,'${nowdate}' data_date
from whde.dws_mkt_adv_strategy_add_word_product_detail_ds a
         left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )b
                         on a.strategy_id = b.strategy_id
         left outer join (select * from  dim_user_permission_info_df ) d
                         on a.tenant_id = d.tenant_id
                             and a.top_parent_asin = d.parent_asin
                             and a.marketplace_id =d.market_place_id
where a.ds ='${nowdate}'

union all

select  DISTINCT
    row_id
               ,a.tenant_id
               ,d.tenant_name
               ,a.marketplace_id
               ,a.marketplace_name
               ,d.adv_manager_id
               ,d.adv_manager_name
               ,d.adv_department_list_id
               ,d.adv_department_list_name
               ,d.seller_id
               ,d.seller_name
               ,a.profile_id
               ,a.top_parent_asin
               ,hash(category) category_id
               ,category category_name
               ,currency_code currency_code
               ,a.strategy_id strategy_id
               ,b.strategy_name strategy_name
               ,hash('好词晋升') action_id
               ,'好词晋升' as action_name
               ,search_term object_term
               ,campaign_name orig_campaign_name_list
               ,match_type orig_campaign_match_type
               ,campaign_name_new new_campaign_name
               ,ad_group_name_new new_group_name
               ,target_sku_list target_sku_list
               ,match_type_new new_campaign_match_type
               ,cpc*1.2 new_cpc
               ,'只下降' new_bid_strategy
               ,15 new_campaign_budget_amt
               ,adv_days statistic_days
               ,clicks clicks
               ,cost cost
               ,sale_amt sale_amt
               ,order_num sale_num
               ,order_num order_num
               ,cpc cpc
               ,acos acos
               ,cvr cvr
               ,cate_cvr cate_cvr
               ,cate_cpc cate_cpc
               ,cate_acos cate_acos
               ,adv_rank adv_search_rank
               ,norm_rank natural_search_rank
               ,aba_rank aba_rank
               ,to_char(aba_date,'yyyymmdd') aba_rank_week_to_week
               ,'${nowdate}' data_date
from dws_mkt_adv_strategy_word_upgrade_detail_ds  A
         left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )b
                         on a.strategy_id = b.strategy_id
         left outer join (select * from  dim_user_permission_info_df ) d
                         on a.tenant_id = d.tenant_id
                             and a.top_parent_asin = d.parent_asin
                             and a.marketplace_id =d.market_place_id
where a.ds ='${nowdate}'
;


