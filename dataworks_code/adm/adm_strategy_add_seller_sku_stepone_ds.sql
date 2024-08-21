--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-13 21:30:48
--********************************************************************--


CREATE TABLE IF NOT EXISTS adm_strategy_add_seller_sku_stepone_ds
(
    row_id string comment '主键'
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
    ,action_id string comment '操作动作ID：action维表化'
    ,action_name string comment '操作动作'
    ,term_type_label string comment '操作具体对象'
    ,object_term string comment '操作对象'
    ,campaign_id string comment '广告活动id'
    ,campaign_name string comment '广告活动名称'
    ,ad_group_id           STRING COMMENT '广告组ID'
    ,ad_group_name         STRING COMMENT '广告组名称'
    ,statistic_days string comment '统计天数'
    ,clicks string comment '点击量'
    ,cost string comment '广告花费'
    ,sale_amt string comment '销售额'
    ,sale_num string comment '销售量'
    ,order_num string comment '订单量'
    ,cpc string comment '竞价'
    ,acos string comment 'acos'
    ,cvr string comment '转化率'
    ,cate_cvr string comment '类目CVR'
    ,cate_cpc string comment '类目CPC'
    ,cate_acos string comment '类目ACOS'
    ,data_date string comment '日期'
)
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略添加推广品(推广品子表)')
    LIFECYCLE 366
;


insert OVERWRITE table adm_strategy_add_seller_sku_stepone_ds PARTITION (ds= '${nowdate}')
select  distinct
    b.row_id
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
               ,hash(a.category) category_id
               ,a.category category_name
               ,a.currency_code currency_code
               ,a.strategy_id strategy_id
               ,c.strategy_name strategy_name
               ,hash('添加推广品') action_id
               ,'添加推广品' as action_name
               ,'推广品' term_type_label
               ,a.seller_sku object_term
               ,b.campaign_id
               ,b.campaign_name
               ,b.ad_group_id
               ,b.ad_group_name
               ,a.adv_days
               ,a.clicks
               ,a.cost
               ,a.sale_amt
               ,a.order_num
               ,a.order_num
               ,a.cpc
               ,a.acos
               ,a.cvr
               ,a.cate_cvr
               ,a.cate_cpc
               ,a.cate_acos
               ,'${nowdate}'
from dws_mkt_adv_strategy_add_seller_sku_stepone_detail_ds  a
         left outer join dws_mkt_adv_strategy_add_seller_sku_steptwo_detail_ds b
                         on a.tenant_id = b.tenant_id
                             and a.profile_id = b.profile_id
                             and a.sku_id= b.sku_id
                             and a.strategy_id = b.strategy_id
         left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )c
                         on a.strategy_id = c.strategy_id
         left outer join (select * from  dim_user_permission_info_df ) d
                         on a.tenant_id = d.tenant_id
                             and a.top_parent_asin = d.parent_asin
                             and a.marketplace_id =d.market_place_id
where a.ds = '${nowdate}'
  and b.ds = '${nowdate}'

union all

select  distinct
    a.row_id
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
               ,hash(a.category) category_id
               ,a.category category_name
               ,a.currency_code currency_code
               ,a.strategy_id strategy_id
               ,c.strategy_name strategy_name
               ,hash('暂停推广品') action_id
               ,'暂停推广品' as action_name
               ,'推广品' term_type_label
               ,a.target_sku object_term
               ,a.campaign_id
               ,a.campaign_name
               ,a.ad_group_id
               ,a.ad_group_name
               ,a.adv_days
               ,a.clicks
               ,a.cost
               ,a.sale_amt
               ,a.order_num
               ,a.order_num
               ,a.cpc
               ,a.acos
               ,a.cvr
               ,a.cate_cvr
               ,a.cate_cpc
               ,a.cate_acos
               ,'${nowdate}'
from dws_mkt_adv_strategy_stop_target_sku_detail_ds  a
         left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )c
                         on a.strategy_id = c.strategy_id
         left outer join (select * from  dim_user_permission_info_df ) d
                         on a.tenant_id = d.tenant_id
                             and a.top_parent_asin = d.parent_asin
                             and a.marketplace_id =d.market_place_id
where a.ds = '${nowdate}'
;

