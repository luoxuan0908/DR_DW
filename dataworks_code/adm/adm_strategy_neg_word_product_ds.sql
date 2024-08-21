--@exclude_input=whde.dws_mkt_adv_strategy_main_all_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-05 20:08:27
--********************************************************************--

CREATE TABLE IF NOT EXISTS adm_strategy_neg_word_product_ds
(
    row_id  string comment '主键'
    ,tenant_id  string comment '租户ID'
    ,tenant_name  string comment '租户名称'
    ,marketplace_id  string comment '市场ID'
    ,marketplace_name  string comment '市场名称'
    ,adv_manager_id  string comment '广告负责人ID'
    ,adv_manager_name  string comment '广告负责人名称'
    ,adv_department_list_id  string comment '广告部门ID'
    ,adv_department_list_name  string comment '广告部门列表'
    ,seller_id  string comment '卖家ID'
    ,seller_name  string comment '卖家名称'
    ,profile_id  string comment '配置ID'
    ,top_parent_asin  string comment '父asin'
    ,category_id  string comment '商品类目ID'
    ,category  string comment '商品类目名称'
    ,currency_code  string comment '币种'
    ,strategy_id  string comment '策略ID'
    ,strategy_name  string comment '策略名称'
    ,action_id  string comment '操作动作ID：action维表化'
    ,action_name  string comment '操作动作'
    ,search_term  string comment '搜索词'
    ,campaign_id  string comment '广告活动ID'
    ,campaign_name  string comment '广告活动名称'
    ,ad_group_name_list  string comment '广告组名称'
    ,ad_group_id_list  string comment '广告组ID'
    ,statistic_days  bigint comment '统计天数'
    ,clicks  bigint comment '点击量'
    ,cost  decimal(18,6) comment '广告花费'
    ,sale_amt  decimal(18,6)  comment '销售额'
    ,sale_num  bigint comment '销售量'
    ,order_num  bigint comment '订单量'
    ,cpc  decimal(18,6) comment '竞价'
    ,acos  decimal(18,6) comment 'acos'
    ,cvr  string comment '转化率'
    ,cate_cvr  decimal(18,6) comment '类目CVR'
    ,cate_cpc  decimal(18,6) comment '类目CPC'
    ,cate_acos  decimal(18,6) comment '类目ACOS'
    ,norm_rank  bigint comment '自然坑位排名'
    ,adv_rank  bigint comment '广告坑位排名'
    ,aba_rank  bigint comment 'aba排名'
    ,data_date string comment '数据时间'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略否词否品子表(新）')
    LIFECYCLE 60
;


insert OVERWRITE table adm_strategy_neg_word_product_ds PARTITION (ds ='${bizdate}' )
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
               ,hash(category)
               ,a.category
               ,a.currency_code
               ,a.strategy_id
               ,b.strategy_name
               ,hash(b.action_name)
               ,case when a.action_type = 'NEG_PRODUCT' then '精准否品' when  a.action_type = 'NEG_TERM' then '精准否词' else null end as  action_name
               ,search_term
               ,campaign_id
               ,campaign_name
               ,ad_group_name_list
               ,ad_group_id_list
               ,adv_days statistic_days
               ,clicks
               ,cost
               ,sale_amt
               ,order_num sale_num
               ,order_num
               ,cpc
               ,acos
               ,cvr
               ,cate_cvr
               ,cate_cpc
               ,cate_acos
               ,norm_rank
               ,adv_rank
               ,aba_rank
               ,'${bizdate}'
from (select *  from whde.dws_mkt_adv_strategy_neg_word_product_detail_ds where ds ='${bizdate}')a
         left outer join ( select distinct strategy_id ,strategy_name,action_name from whde.dws_mkt_adv_strategy_main_all_df )b
                         on a.strategy_id = b.strategy_id
         left outer join (select * from  dim_user_permission_info_df ) d
                         on a.tenant_id = d.tenant_id
                             and a.top_parent_asin = d.parent_asin
                             and a.marketplace_id =d.market_place_id
;


