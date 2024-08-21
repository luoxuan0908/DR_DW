--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-09 21:51:41
--********************************************************************--

drop table if EXISTS adm_strategy_adjust_budget_raise_detail_ds;
CREATE TABLE IF NOT EXISTS adm_strategy_adjust_budget_raise_detail_ds
(
    row_id	string comment '主键ID'
    ,tenant_id  string comment '租户ID'
    ,tenant_name  string comment '租户名称'
    ,marketplace_id  string comment '市场ID'
    ,marketplace_name  string comment '市场名称'
    ,adv_manager_id  string comment '广告负责人ID'
    ,adv_manager_name  string comment '广告负责人'
    ,adv_department_list_id  string comment '广告部门ID'
    ,adv_department_list_name  string comment '广告部门列表'
    ,seller_id  string comment '卖家ID'
    ,seller_name  string comment '卖家名称'
    ,profile_id  string comment '配置ID'
    ,top_parent_asin  string comment '父asin'
    ,category_id  string comment '商品类目ID'
    ,category_name  string comment '商品类目名称'
    ,currency_code  string comment '币种'
    ,strategy_id  string comment '策略ID'
    ,strategy_name  string comment '策略名称'
    ,action_id  string comment '操作动作ID：action维表化'
    ,action_name  string comment '操作动作'
    ,campaign_cnt  string comment '广告活动个数'
    ,campaign_name_list  string comment '广告活动名称list'
    ,before_campaign_budget_amt  string comment '调整前预算'
    ,new_campaign_budget_amt  string comment '调整后预算'
    ,avg_campaign_budget_amt_7d  string comment '近7天平均每天花费'
    ,afn_warehouse_quantity  string comment '当前库存量'
    ,DOS  string comment '库存可售天数'
    ,best_asin_dos  string comment '热卖子asin可售天数'
    ,top_cost_asin_dos  string comment '广告花费Top子asin可售天数'
    ,avg_tacos_7d  string comment '父asin近7天tacos'
    ,statistic_days  string comment '统计天数'
    ,clicks  string comment '点击量'
    ,cost  string comment '广告花费'
    ,sale_amt  string comment '销售额'
    ,sale_num  string comment '销售量'
    ,order_num  string comment '订单量'
    ,cpc  string comment '降价'
    ,acos  string comment 'acos'
    ,cvr  string comment '转化率'
    ,cate_cvr  string comment '类目CVR'
    ,cate_cpc  string comment '类目CPC'
    ,cate_acos  string comment '类目ACOS'
    ,cate_tacos  string comment '类目TACOS'
    ,data_date string

)
    PARTITIONED BY
(
    ds                 STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略上调预算(子表)')
    LIFECYCLE 366
;

insert OVERWRITE table adm_strategy_adjust_budget_raise_detail_ds PARTITION (ds = '${nowdate}')
select distinct
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
              ,hash(category) category_id
              ,category category_name
              ,currency_code currency_code
              ,a.strategy_id strategy_id
              ,b.strategy_name strategy_name
              ,hash('预算调整') action_id
              ,'预算调整' as action_name
              ,1 campaign_cnt
              ,campaign_name campaign_name_list

              ,10
              ,20
              ,10
              ,100
              ,60
              ,50
              ,40
              ,0.15
              ,adv_days
              ,clicks
              ,cost
              ,sale_amt
              ,order_num
              ,order_num
              ,cpc
              ,acos
              ,cvr
              ,cate_cvr
              ,cate_cpc
              ,cate_acos
              ,0.1
              ,'${nowdate}'
from  dws_mkt_adv_strategy_adjust_budget_raise_detail_ds a
          left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )b
                          on a.strategy_id = b.strategy_id
          left outer join (select * from  dim_user_permission_info_df ) d
                          on a.tenant_id = d.tenant_id
                              and a.top_parent_asin = d.parent_asin
                              and a.marketplace_id =d.market_place_id
where a.ds ='${nowdate}'
;