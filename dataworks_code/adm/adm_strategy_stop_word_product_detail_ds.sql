--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-12 14:07:25
--********************************************************************--

CREATE TABLE IF NOT EXISTS adm_strategy_stop_word_product_detail_ds
(
    row_id STRING COMMENT '主键'
    ,tenant_id STRING COMMENT '租户ID'
    ,tenant_name STRING COMMENT '租户名称'
    ,marketplace_id STRING COMMENT '市场ID'
    ,marketplace_name STRING COMMENT '市场名称'
    ,adv_manager_id STRING COMMENT '广告负责人ID'
    ,adv_manager_name STRING COMMENT '广告负责人'
    ,adv_department_list_id STRING COMMENT '广告部门ID'
    ,adv_department_list_name STRING COMMENT '广告部门列表'
    ,seller_id STRING COMMENT '卖家ID'
    ,seller_name STRING COMMENT '卖家名称'
    ,profile_id STRING COMMENT '配置ID'
    ,top_parent_asin STRING COMMENT '父asin'
    ,category_id STRING COMMENT '商品类目ID'
    ,category_name STRING COMMENT '商品类目名称'
    ,currency_code STRING COMMENT '币种'
    ,strategy_id STRING COMMENT '策略ID'
    ,strategy_name STRING COMMENT '策略名称'
    ,action_id STRING COMMENT '操作动作ID：action维表化'
    ,action_name STRING COMMENT '操作动作'
    ,term_type_label STRING COMMENT '操作具体对象'
    ,object_term STRING COMMENT '操作对象'
    ,campaign_id STRING COMMENT '广告活动id'
    ,campaign_name STRING COMMENT '广告活动名称'
    ,statistic_days STRING COMMENT '统计天数'
    ,clicks STRING COMMENT '点击量'
    ,cost STRING COMMENT '广告花费'
    ,sale_amt STRING COMMENT '销售额'
    ,sale_num STRING COMMENT '销售量'
    ,order_num STRING COMMENT '订单量'
    ,cpc STRING COMMENT '竞价'
    ,acos STRING COMMENT 'acos'
    ,cvr STRING COMMENT '转化率'
    ,cate_cvr STRING COMMENT '类目CVR'
    ,cate_cpc STRING COMMENT '类目CPC'
    ,cate_acos STRING COMMENT '类目ACOS'
    ,adv_search_rank STRING COMMENT '广告坑位排名'
    ,natural_search_rank STRING COMMENT '自然坑位排名'
    ,aba_rank STRING COMMENT 'ABA搜索排名'
    ,aba_rank_week_to_week STRING COMMENT 'aba搜索排名周环比'
    ,data_date STRING COMMENT '日期'

)
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略暂停投放(子表)')
    LIFECYCLE 366
;

insert OVERWRITE table adm_strategy_stop_word_product_detail_ds PARTITION (ds = '${nowdate}')

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
              ,hash(case when term_type_label = '投放品' then '暂停投放品'
                         when  term_type_label= '投放词' then '暂停投放词'  end) action_id
              ,case when term_type_label = '投放品' then '暂停投放品'
                    when  term_type_label= '投放词' then '暂停投放词'  end as action_name
              ,term_type_label
              ,target_term
              ,campaign_id
              ,campaign_name
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
              ,adv_rank adv_search_rank
              ,norm_rank natural_search_rank
              ,aba_rank aba_rank
              ,to_char(aba_date,'yyyymmdd') aba_rank_week_to_week --存错数据了！！！
              ,'${nowdate}' data_date
from whde.dws_mkt_adv_strategy_stop_word_product_detail_ds a
         left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )b
                         on a.strategy_id = b.strategy_id
         left outer join (select * from  dim_user_permission_info_df ) d
                         on a.tenant_id = d.tenant_id
                             and a.top_parent_asin = d.parent_asin
                             and a.marketplace_id =d.market_place_id
where a.ds ='${nowdate}'
;
