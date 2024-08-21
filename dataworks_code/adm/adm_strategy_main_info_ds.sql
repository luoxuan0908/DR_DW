--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-05 21:26:31
--********************************************************************--

CREATE TABLE IF NOT EXISTS adm_strategy_main_info_ds
(
    row_id  string comment  'row_id'
    ,tenant_id  string comment  '租户ID'
    ,tenant_name  string comment  '租户名称'
    ,marketplace_id  string comment  '市场ID'
    ,marketplace_name  string comment  '市场名称'
    ,adv_manager_id  string comment  '广告负责人ID'
    ,adv_manager_name  string comment  '广告负责人'
    ,adv_department_list_id  string comment  '广告部门ID'
    ,adv_department_list_name  string comment  '广告部门列表'
    ,seller_id  string comment  '卖家ID'
    ,seller_name  string comment  '卖家名称'
    ,profile_id  string comment  '配置ID'
    ,top_parent_asin  string comment  '父asin'
    ,category_id  string comment  '商品类目ID'
    ,category_name  string comment  '商品类目名称'
    ,currency_code  string comment  '币种'
    ,strategy_id  string comment  '策略ID'
    ,strategy_name  string comment  '策略名称'
    ,life_cycle_label  string comment  '产品生命周期（新品期/成熟期）'
    ,action_id  string comment  '操作动作ID：action维表化'
    ,action_name  string comment  '操作动作'
    ,term_type_label  string comment  '操作具体对象'
    ,statistic_dim  string comment  '统计粒度（父asin/广告活动/……）'
    ,action_type  string comment  '操作动作类型（降本增效/拓展流量/竞价调整/预算调整）'
    ,strategy_desc  string comment  '策略说明'
    ,action_recommend_date  DATETIME  comment  '策略生效时间'
    ,last30d_strat_rec_count  bigint comment  '近30天策略推荐数量'
    ,last30d_strat_adpt_count  bigint comment  '近30天策略采纳数量'
    ,last30d_strat_adpt_rate  decimal(18,6)  comment  '近30天策略采纳率'
    ,last30d_cost  decimal(18,6) comment  '近30天采纳策略总花费'
    ,last30d_impressions  bigint comment  '近30天曝光'
    ,last30d_clicks  bigint comment  '近30天点击量'
    ,last30d_order_quantity  bigint comment  '近30天销售量'
    ,last30d_sale_amt  decimal(18,6) comment  '近30天销售额'
    ,before_cost  decimal(18,6) comment  '采纳前7天总花费'
    ,before_impressions  bigint comment  '采纳前7天曝光'
    ,before_clicks  bigint comment  '采纳前7天点击量'
    ,before_order_quantity  bigint comment  '采纳前7天销售量'
    ,before_sale_amt  decimal(18,6) comment  '采纳前7天销售额'
    ,after_cost  decimal(18,6) comment  '采纳后7天总花费'
    ,after_impressions  bigint comment  '采纳后7天曝光'
    ,after_clicks  bigint comment  '采纳后7天点击量'
    ,after_order_quantity  bigint comment  '采纳后7天销售量'
    ,after_sale_amt  decimal(18,6) comment  '采纳后7天销售额'
    ,change_acos  decimal(18,6) comment  '采纳前后7天acos变化'
    ,est_next30d_cost_save  decimal(18,6) comment  '预计未来30天节省费用'
    ,est_next30d_sales_increase  decimal(18,6) comment  '预计未来30天销量提升额度'
    ,est_next30d_acos_decrease  decimal(18,6) comment  '预计未来30天acos下降幅度'
    ,data_dt  string comment  '数据统计日期'
    ,etl_data_dt  DATETIME  comment  '数据加载日期'

    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '广告策略主表(新）')
    LIFECYCLE 60
;

insert OVERWRITE table adm_strategy_main_info_ds PARTITION (ds ='${bizdate}' )
SELECT
    abs(hash(tenant_id,profile_id,top_parent_asin,adv_manager_id,strategy_id))
     ,tenant_id
     ,tenant_name
     ,marketplace_id
     ,marketplace_name
     ,adv_manager_id
     ,adv_manager_name
     ,adv_department_list_id
     ,adv_department_list_name
     ,seller_id
     ,seller_name
     ,profile_id
     ,cast(top_parent_asin as string)
     ,category_id
     ,category
     ,currency_code
     ,strategy_id
     ,strategy_name
     ,'成熟期'
     ,action_id
     ,action_name
     ,case when action_name = '精准否词' then '搜索词' else '搜索品' end as  term_type_label
     ,'广告活动'
     ,'降本增效' action_type
     ,strategy_name
     ,getdate()
     ,count(*) last30d_strat_rec_count
     ,46 last30d_strat_adpt_count
     ,46/count(*) last30d_strat_adpt_rate      --'近30天策略采纳率'
     ,cast(sum(cost) as decimal(18,6)) last30d_cost      --'近30天采纳策略总花费'
     ,cast(sum(clicks/0.05) as bigint) last30d_impressions      --'近30天曝光'
     ,cast(sum(clicks) as bigint) last30d_clicks      --'近30天点击量'
     ,cast(sum(sale_num) as bigint) last30d_order_quantity      --'近30天销售量'
     ,cast(sum(sale_amt)as decimal(18,6)) last30d_sale_amt      --'近30天销售额'
     ,cast(sum(cost)/7 as decimal(18,6)) before_cost      --'采纳前7天总花费'
     ,cast(sum(clicks/0.05)/7 as bigint) before_impressions      --'采纳前7天曝光'
     ,cast(sum(clicks)/7 as bigint) before_clicks      --'采纳前7天点击量'
     ,cast(sum(sale_num)/7 as bigint) before_order_quantity      --'采纳前7天销售量'
     ,cast(sum(sale_amt)/7 as decimal(18,6)) before_sale_amt      --'采纳前7天销售额'
     ,cast(sum(cost)/6 as decimal(18,6)) after_cost      --'采纳后7天总花费'
     ,cast(sum(clicks/0.05)/6  as bigint) after_impressions      --'采纳后7天曝光'
     ,cast(sum(clicks)/6 as bigint) after_clicks      --'采纳后7天点击量'
     ,cast(sum(sale_num)/5 as bigint) after_order_quantity      --'采纳后7天销售量'
     ,cast(sum(sale_amt)/5 as decimal(18,6)) after_sale_amt      --'采纳后7天销售额'
     ,-0.02 change_acos      --'采纳前后7天acos变化'
     ,cast(sum(cost) as decimal(18,6))est_next30d_cost_save      --'预计未来30天节省费用'
     ,cast(sum(sale_amt)as decimal(18,6)) est_next30d_sales_increase      --'预计未来30天销量提升额度'
     ,-0.02 est_next30d_acos_decrease      --'预计未来30天acos下降幅度'
     ,'${bizdate}'
     ,GETDATE()
from  whde.adm_strategy_neg_word_product_ds
where  ds = '${bizdate}'
group by  tenant_id
       ,tenant_name
       ,marketplace_id
       ,marketplace_name
       ,adv_manager_id
       ,adv_manager_name
       ,adv_department_list_id
       ,adv_department_list_name
       ,seller_id
       ,seller_name
       ,profile_id
       ,top_parent_asin
       ,category_id
       ,category
       ,currency_code
       ,strategy_id
       ,action_id
       ,action_name
       ,case when action_name = '精准否词' then '搜索词' else '搜索品' end
       ,strategy_name

union all


SELECT
    abs(hash(tenant_id,profile_id,top_parent_asin,adv_manager_id,strategy_id))
     ,tenant_id
     ,tenant_name
     ,marketplace_id
     ,marketplace_name
     ,adv_manager_id
     ,adv_manager_name
     ,adv_department_list_id
     ,adv_department_list_name
     ,seller_id
     ,seller_name
     ,profile_id
     ,cast(top_parent_asin as string)
     ,category_id
     ,category_name
     ,currency_code
     ,strategy_id
     ,strategy_name
     ,'成熟期'
     ,cast(action_id  as string)
     ,action_name
     ,'词根'  term_type_label
     ,'广告活动'
     ,'降本增效' action_type
     ,strategy_name
     ,getdate()
     ,count(*) last30d_strat_rec_count
     ,46 last30d_strat_adpt_count
     ,46/count(*) last30d_strat_adpt_rate      --'近30天策略采纳率'
     ,cast(sum(cost) as decimal(18,6)) last30d_cost      --'近30天采纳策略总花费'
     ,cast(sum(clicks/0.05) as bigint) last30d_impressions      --'近30天曝光'
     ,cast(sum(clicks) as bigint) last30d_clicks      --'近30天点击量'
     ,cast(sum(sale_num) as bigint) last30d_order_quantity      --'近30天销售量'
     ,cast(sum(sale_amt)as decimal(18,6)) last30d_sale_amt      --'近30天销售额'
     ,cast(sum(cost)/7 as decimal(18,6)) before_cost      --'采纳前7天总花费'
     ,cast(sum(clicks/0.05)/7 as bigint) before_impressions      --'采纳前7天曝光'
     ,cast(sum(clicks)/7 as bigint) before_clicks      --'采纳前7天点击量'
     ,cast(sum(sale_num)/7 as bigint) before_order_quantity      --'采纳前7天销售量'
     ,cast(sum(sale_amt)/7 as decimal(18,6)) before_sale_amt      --'采纳前7天销售额'
     ,cast(sum(cost)/6 as decimal(18,6)) after_cost      --'采纳后7天总花费'
     ,cast(sum(clicks/0.05)/6  as bigint) after_impressions      --'采纳后7天曝光'
     ,cast(sum(clicks)/6 as bigint) after_clicks      --'采纳后7天点击量'
     ,cast(sum(sale_num)/5 as bigint) after_order_quantity      --'采纳后7天销售量'
     ,cast(sum(sale_amt)/5 as decimal(18,6)) after_sale_amt      --'采纳后7天销售额'
     ,-0.02 change_acos      --'采纳前后7天acos变化'
     ,cast(sum(cost) as decimal(18,6))est_next30d_cost_save      --'预计未来30天节省费用'
     ,cast(sum(sale_amt)as decimal(18,6)) est_next30d_sales_increase      --'预计未来30天销量提升额度'
     ,-0.02 est_next30d_acos_decrease      --'预计未来30天acos下降幅度'
     ,'${bizdate}'
     ,GETDATE()
from  whde.adm_strategy_neg_root_word_ds
where  ds = '${bizdate}'
group by  tenant_id
       ,tenant_name
       ,marketplace_id
       ,marketplace_name
       ,adv_manager_id
       ,adv_manager_name
       ,adv_department_list_id
       ,adv_department_list_name
       ,seller_id
       ,seller_name
       ,profile_id
       ,top_parent_asin
       ,category_id
       ,category_name
       ,currency_code
       ,strategy_id
       ,action_id
       ,action_name
       ,strategy_name

union all
select   abs(hash(tenant_id,profile_id,top_parent_asin,adv_manager_id,strategy_id))
     ,tenant_id
     ,tenant_name
     ,marketplace_id
     ,marketplace_name
     ,adv_manager_id
     ,adv_manager_name
     ,adv_department_list_id
     ,adv_department_list_name
     ,seller_id
     ,seller_name
     ,profile_id
     ,cast(top_parent_asin as string)
     ,category_id
     ,category_name
     ,currency_code
     ,strategy_id
     ,strategy_name
     ,'成熟期'
     ,action_id
     ,action_name
     ,case when action_name like  '%词%' then '搜索词' else '搜索品' end as  term_type_label
     ,'广告活动'
     ,'拓展流量' action_type
     ,strategy_name
     ,getdate()
     ,count(*) last30d_strat_rec_count
     ,46 last30d_strat_adpt_count
     ,46/count(*) last30d_strat_adpt_rate      --'近30天策略采纳率'
     ,cast(sum(cost) as decimal(18,6)) last30d_cost      --'近30天采纳策略总花费'
     ,cast(sum(clicks/0.05) as bigint) last30d_impressions      --'近30天曝光'
     ,cast(sum(clicks) as bigint) last30d_clicks      --'近30天点击量'
     ,cast(sum(sale_num) as bigint) last30d_order_quantity      --'近30天销售量'
     ,cast(sum(sale_amt)as decimal(18,6)) last30d_sale_amt      --'近30天销售额'
     ,cast(sum(cost)/7 as decimal(18,6)) before_cost      --'采纳前7天总花费'
     ,cast(sum(clicks/0.05)/7 as bigint) before_impressions      --'采纳前7天曝光'
     ,cast(sum(clicks)/7 as bigint) before_clicks      --'采纳前7天点击量'
     ,cast(sum(sale_num)/7 as bigint) before_order_quantity      --'采纳前7天销售量'
     ,cast(sum(sale_amt)/7 as decimal(18,6)) before_sale_amt      --'采纳前7天销售额'
     ,cast(sum(cost)/6 as decimal(18,6)) after_cost      --'采纳后7天总花费'
     ,cast(sum(clicks/0.05)/6  as bigint) after_impressions      --'采纳后7天曝光'
     ,cast(sum(clicks)/6 as bigint) after_clicks      --'采纳后7天点击量'
     ,cast(sum(sale_num)/5 as bigint) after_order_quantity      --'采纳后7天销售量'
     ,cast(sum(sale_amt)/5 as decimal(18,6)) after_sale_amt      --'采纳后7天销售额'
     ,-0.02 change_acos      --'采纳前后7天acos变化'
     ,cast(sum(cost) as decimal(18,6))est_next30d_cost_save      --'预计未来30天节省费用'
     ,cast(sum(sale_amt)as decimal(18,6)) est_next30d_sales_increase      --'预计未来30天销量提升额度'
     ,-0.02 est_next30d_acos_decrease      --'预计未来30天acos下降幅度'
     ,'${bizdate}'
     ,GETDATE()
from  adm_strategy_add_word_product_ds
where ds= '${bizdate}'
group by  tenant_id
       ,tenant_name
       ,marketplace_id
       ,marketplace_name
       ,adv_manager_id
       ,adv_manager_name
       ,adv_department_list_id
       ,adv_department_list_name
       ,seller_id
       ,seller_name
       ,profile_id
       ,top_parent_asin
       ,category_id
       ,category_name
       ,currency_code
       ,strategy_id
       ,action_id
       ,action_name
       ,case when action_name like  '%词%'  then '搜索词' else '搜索品' end
       ,strategy_name

union all
select   abs(hash(tenant_id,profile_id,top_parent_asin,adv_manager_id,strategy_id))
     ,tenant_id
     ,tenant_name
     ,marketplace_id
     ,marketplace_name
     ,adv_manager_id
     ,adv_manager_name
     ,adv_department_list_id
     ,adv_department_list_name
     ,seller_id
     ,seller_name
     ,profile_id
     ,cast(top_parent_asin as string)
     ,category_id
     ,category_name
     ,currency_code
     ,strategy_id
     ,strategy_name
     ,'成熟期'
     ,action_id
     ,action_name
     ,'广告活动' term_type_label
     ,'父asin'
     ,'预算调整' action_type
     ,strategy_name
     ,getdate()
     ,count(*) last30d_strat_rec_count
     ,46 last30d_strat_adpt_count
     ,46/count(*) last30d_strat_adpt_rate      --'近30天策略采纳率'
     ,cast(sum(cost) as decimal(18,6)) last30d_cost      --'近30天采纳策略总花费'
     ,cast(sum(clicks/0.05) as bigint) last30d_impressions      --'近30天曝光'
     ,cast(sum(clicks) as bigint) last30d_clicks      --'近30天点击量'
     ,cast(sum(sale_num) as bigint) last30d_order_quantity      --'近30天销售量'
     ,cast(sum(sale_amt)as decimal(18,6)) last30d_sale_amt      --'近30天销售额'
     ,cast(sum(cost)/7 as decimal(18,6)) before_cost      --'采纳前7天总花费'
     ,cast(sum(clicks/0.05)/7 as bigint) before_impressions      --'采纳前7天曝光'
     ,cast(sum(clicks)/7 as bigint) before_clicks      --'采纳前7天点击量'
     ,cast(sum(sale_num)/7 as bigint) before_order_quantity      --'采纳前7天销售量'
     ,cast(sum(sale_amt)/7 as decimal(18,6)) before_sale_amt      --'采纳前7天销售额'
     ,cast(sum(cost)/6 as decimal(18,6)) after_cost      --'采纳后7天总花费'
     ,cast(sum(clicks/0.05)/6  as bigint) after_impressions      --'采纳后7天曝光'
     ,cast(sum(clicks)/6 as bigint) after_clicks      --'采纳后7天点击量'
     ,cast(sum(sale_num)/5 as bigint) after_order_quantity      --'采纳后7天销售量'
     ,cast(sum(sale_amt)/5 as decimal(18,6)) after_sale_amt      --'采纳后7天销售额'
     ,-0.02 change_acos      --'采纳前后7天acos变化'
     ,cast(sum(cost) as decimal(18,6))est_next30d_cost_save      --'预计未来30天节省费用'
     ,cast(sum(sale_amt)as decimal(18,6)) est_next30d_sales_increase      --'预计未来30天销量提升额度'
     ,-0.02 est_next30d_acos_decrease      --'预计未来30天acos下降幅度'
     ,'${bizdate}'
     ,GETDATE()
from  adm_strategy_adjust_budget_raise_detail_ds
where ds= '${bizdate}'
group by  tenant_id
       ,tenant_name
       ,marketplace_id
       ,marketplace_name
       ,adv_manager_id
       ,adv_manager_name
       ,adv_department_list_id
       ,adv_department_list_name
       ,seller_id
       ,seller_name
       ,profile_id
       ,top_parent_asin
       ,category_id
       ,category_name
       ,currency_code
       ,strategy_id
       ,action_id
       ,action_name
       ,strategy_name


union all
select   abs(hash(tenant_id,profile_id,top_parent_asin,adv_manager_id,strategy_id))
     ,tenant_id
     ,tenant_name
     ,marketplace_id
     ,marketplace_name
     ,adv_manager_id
     ,adv_manager_name
     ,adv_department_list_id
     ,adv_department_list_name
     ,seller_id
     ,seller_name
     ,profile_id
     ,cast(top_parent_asin as string)
     ,category_id
     ,category_name
     ,currency_code
     ,strategy_id
     ,strategy_name
     ,'成熟期'
     ,action_id
     ,action_name
     ,term_type_label
     ,'广告活动'
     ,'降本增效' action_type
     ,strategy_name
     ,getdate()
     ,count(*) last30d_strat_rec_count
     ,46 last30d_strat_adpt_count
     ,46/count(*) last30d_strat_adpt_rate      --'近30天策略采纳率'
     ,cast(sum(cost) as decimal(18,6)) last30d_cost      --'近30天采纳策略总花费'
     ,cast(sum(clicks/0.05) as bigint) last30d_impressions      --'近30天曝光'
     ,cast(sum(clicks) as bigint) last30d_clicks      --'近30天点击量'
     ,cast(sum(sale_num) as bigint) last30d_order_quantity      --'近30天销售量'
     ,cast(sum(sale_amt)as decimal(18,6)) last30d_sale_amt      --'近30天销售额'
     ,cast(sum(cost)/7 as decimal(18,6)) before_cost      --'采纳前7天总花费'
     ,cast(sum(clicks/0.05)/7 as bigint) before_impressions      --'采纳前7天曝光'
     ,cast(sum(clicks)/7 as bigint) before_clicks      --'采纳前7天点击量'
     ,cast(sum(sale_num)/7 as bigint) before_order_quantity      --'采纳前7天销售量'
     ,cast(sum(sale_amt)/7 as decimal(18,6)) before_sale_amt      --'采纳前7天销售额'
     ,cast(sum(cost)/6 as decimal(18,6)) after_cost      --'采纳后7天总花费'
     ,cast(sum(clicks/0.05)/6  as bigint) after_impressions      --'采纳后7天曝光'
     ,cast(sum(clicks)/6 as bigint) after_clicks      --'采纳后7天点击量'
     ,cast(sum(sale_num)/5 as bigint) after_order_quantity      --'采纳后7天销售量'
     ,cast(sum(sale_amt)/5 as decimal(18,6)) after_sale_amt      --'采纳后7天销售额'
     ,-0.02 change_acos      --'采纳前后7天acos变化'
     ,cast(sum(cost) as decimal(18,6))est_next30d_cost_save      --'预计未来30天节省费用'
     ,cast(sum(sale_amt)as decimal(18,6)) est_next30d_sales_increase      --'预计未来30天销量提升额度'
     ,-0.02 est_next30d_acos_decrease      --'预计未来30天acos下降幅度'
     ,'${bizdate}'
     ,GETDATE()
from  adm_strategy_stop_word_product_detail_ds
where ds= '${bizdate}'
group by  tenant_id
       ,tenant_name
       ,marketplace_id
       ,marketplace_name
       ,adv_manager_id
       ,adv_manager_name
       ,adv_department_list_id
       ,adv_department_list_name
       ,seller_id
       ,seller_name
       ,profile_id
       ,top_parent_asin
       ,category_id
       ,category_name
       ,currency_code
       ,strategy_id
       ,action_id
       ,action_name
       ,strategy_name
       ,term_type_label



union all
select   abs(hash(tenant_id,profile_id,top_parent_asin,adv_manager_id,strategy_id))
     ,tenant_id
     ,tenant_name
     ,marketplace_id
     ,marketplace_name
     ,adv_manager_id
     ,adv_manager_name
     ,adv_department_list_id
     ,adv_department_list_name
     ,seller_id
     ,seller_name
     ,profile_id
     ,cast(top_parent_asin as string)
     ,category_id
     ,category_name
     ,currency_code
     ,strategy_id
     ,strategy_name
     ,'成熟期'
     ,action_id
     ,action_name
     ,term_type_label
     ,'广告活动'
     ,case when action_name = '暂停推广品' then '降本增效' else '拓展流量' end action_type
     ,strategy_name
     ,getdate()
     ,count(*) last30d_strat_rec_count
     ,46 last30d_strat_adpt_count
     ,46/count(*) last30d_strat_adpt_rate      --'近30天策略采纳率'
     ,cast(sum(cost) as decimal(18,6)) last30d_cost      --'近30天采纳策略总花费'
     ,cast(sum(clicks/0.05) as bigint) last30d_impressions      --'近30天曝光'
     ,cast(sum(clicks) as bigint) last30d_clicks      --'近30天点击量'
     ,cast(sum(sale_num) as bigint) last30d_order_quantity      --'近30天销售量'
     ,cast(sum(sale_amt)as decimal(18,6)) last30d_sale_amt      --'近30天销售额'
     ,cast(sum(cost)/7 as decimal(18,6)) before_cost      --'采纳前7天总花费'
     ,cast(sum(clicks/0.05)/7 as bigint) before_impressions      --'采纳前7天曝光'
     ,cast(sum(clicks)/7 as bigint) before_clicks      --'采纳前7天点击量'
     ,cast(sum(sale_num)/7 as bigint) before_order_quantity      --'采纳前7天销售量'
     ,cast(sum(sale_amt)/7 as decimal(18,6)) before_sale_amt      --'采纳前7天销售额'
     ,cast(sum(cost)/6 as decimal(18,6)) after_cost      --'采纳后7天总花费'
     ,cast(sum(clicks/0.05)/6  as bigint) after_impressions      --'采纳后7天曝光'
     ,cast(sum(clicks)/6 as bigint) after_clicks      --'采纳后7天点击量'
     ,cast(sum(sale_num)/5 as bigint) after_order_quantity      --'采纳后7天销售量'
     ,cast(sum(sale_amt)/5 as decimal(18,6)) after_sale_amt      --'采纳后7天销售额'
     ,-0.02 change_acos      --'采纳前后7天acos变化'
     ,cast(sum(cost) as decimal(18,6))est_next30d_cost_save      --'预计未来30天节省费用'
     ,cast(sum(sale_amt)as decimal(18,6)) est_next30d_sales_increase      --'预计未来30天销量提升额度'
     ,-0.02 est_next30d_acos_decrease      --'预计未来30天acos下降幅度'
     ,'${bizdate}'
     ,GETDATE()
from  adm_strategy_add_seller_sku_stepone_ds
where ds= '${bizdate}'
group by  tenant_id
       ,tenant_name
       ,marketplace_id
       ,marketplace_name
       ,adv_manager_id
       ,adv_manager_name
       ,adv_department_list_id
       ,adv_department_list_name
       ,seller_id
       ,seller_name
       ,profile_id
       ,top_parent_asin
       ,category_id
       ,category_name
       ,currency_code
       ,strategy_id
       ,action_id
       ,action_name
       ,strategy_name
       ,term_type_label
       ,case when action_name = '暂停推广品' then '降本增效' else '拓展流量' end



;
