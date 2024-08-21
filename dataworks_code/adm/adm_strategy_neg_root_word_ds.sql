--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-06 21:12:42
--********************************************************************--


create table if not exists adm_strategy_neg_root_word_ds
(
    row_id	string
    ,tenant_id string  comment '租户ID'
    ,tenant_name string  comment '租户名称'
    ,marketplace_id string  comment '市场ID'
    ,marketplace_name string  comment '市场名称'
    ,adv_manager_id string  comment '广告负责人ID'
    ,adv_manager_name string  comment '广告负责人'
    ,adv_department_list_id string  comment '广告部门ID'
    ,adv_department_list_name string  comment '广告部门列表'
    ,seller_id string  comment '卖家ID'
    ,seller_name string  comment '卖家名称'
    ,profile_id string  comment '配置ID'
    ,top_parent_asin string  comment '父asin'
    ,category_id string  comment '商品类目ID'
    ,category_name string  comment '商品类目名称'
    ,currency_code string  comment '币种'
    ,strategy_id string  comment '策略ID'
    ,strategy_name string  comment '策略名称'
    ,action_id string  comment '操作动作ID'
    ,action_name string  comment 'Action动作'
    ,word string  comment '词根'
    ,campaign_cnt string  comment '广告活动个数'
    ,campaign_name_list string  comment '广告活动名称list'
    ,search_term_cnt string  comment '搜索词个数'
    ,search_term_list string  comment '搜索词list'
    ,statistic_days string  comment '统计天数'
    ,impressions string  comment '曝光量'
    ,clicks string  comment '点击量'
    ,cost string  comment '广告花费'
    ,sale_amt string  comment '销售额'
    ,sale_num string  comment '销售量'
    ,order_num string  comment '订单量'
    ,cpc string  comment '竞价'
    ,acos string  comment 'acos'
    ,cvr string  comment '转化率'
    ,cate_cvr string  comment '类目CVR'
    ,cate_cpc string  comment '类目CPC'
    ,cate_acos string  comment '类目ACOS'
    ,data_date string  comment '日期'
)
    partitioned by
(
    ds                 string
)
    stored as aliorc
    tblproperties ('comment' = '广告策略否词根子表(站点、店铺、父asin、广告活动、词根)')
    lifecycle 366
;

insert OVERWRITE table adm_strategy_neg_root_word_ds PARTITION (ds = '${nowdate}')
select distinct
    abs(hash(a.tenant_id,a.profile_id,a.stem,'否词根',a.camp_id_list))
              ,a.tenant_id
              ,d.tenant_name
              ,marketplace_id
              ,marketplace_name
              ,d.adv_manager_id
              ,d.adv_manager_name
              ,d.adv_department_list_id
              ,d.adv_department_list_name
              ,d.seller_id
              ,d.seller_name
              ,a.profile_id
              ,a.top_parent_asin
              ,hash(b.breadcrumbs_feature)
              ,b.breadcrumbs_feature
              ,a.currency_code
              ,a.strategy_id
              ,c.strategy_name
              ,hash('否词根')
              ,'否词根' action_name
              ,stem              -- comment '词根'
              ,camp_cnt
              ,camp_id_list
              ,term_cnt
              ,search_term_list
              ,adv_days
              ,cast(clicks/0.05 as bigint)
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
              ,'${nowdate}'
from  dws_mkt_adv_strategy_neg_root_word_ds a
          left outer join (select  market_place_id, parent_asin,max(replace(breadcrumbs_feature,'            >                ','>')) breadcrumbs_feature from whde.amazon_product_details where pt = '${bizdate}'group by market_place_id, parent_asin)  b
                          on a.top_parent_asin = b.parent_asin
                              and a.marketplace_id = b.market_place_id
          left outer  join  ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )c
                            on a.strategy_id = c.strategy_id
          left outer join (select * from  dim_user_permission_info_df ) d
                          on a.tenant_id = d.tenant_id
                              and a.top_parent_asin = d.parent_asin
                              and a.marketplace_id =d.market_place_id
where a.ds ='${bizdate}'
;
