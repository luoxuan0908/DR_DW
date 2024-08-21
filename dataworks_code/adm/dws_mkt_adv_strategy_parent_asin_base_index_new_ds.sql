--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-27 23:31:50
--********************************************************************--


CREATE TABLE IF NOT EXISTS whde.dws_mkt_adv_strategy_parent_asin_base_index_new_ds(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置ID',
    marketplace_id STRING COMMENT '市场ID',
    seller_id STRING COMMENT '卖家ID',
    adv_manager_id STRING COMMENT '广告负责人ID',
    adv_manager_name STRING COMMENT '广告负责人名称',
    top_parent_asin STRING COMMENT '父aisn',
    fba_first_instock_time DATETIME COMMENT 'FBA首次入库时间',
    fba_first_instock_days BIGINT COMMENT 'FBA首次入库距今天数',
    life_cycle_label STRING COMMENT '生命周期：成熟期/新品期',
    stock_sale_days BIGINT COMMENT '父ASIN可售库存天数',
    stock_label STRING COMMENT '库存标签：库存充足/库存不足',
    category STRING COMMENT '类目',
    term_type STRING COMMENT '统计对象类型：搜索词/搜索品',
    cate_impressions_n30d BIGINT COMMENT '类目近30天广告曝光量',
    cate_clicks_n30d BIGINT COMMENT '类目近30天广告点击量',
    cate_cost_n30d DECIMAL(18,6) COMMENT '类目近30天广告花费',
    cate_sale_amt_n30d DECIMAL(18,6) COMMENT '类目近30天广告销售额',
    cate_order_num_n30d BIGINT COMMENT '类目近30天广告销量',
    cate_ctr_n30d DECIMAL(18,6) COMMENT '类目近30天CTR',
    cate_cvr_n30d DECIMAL(18,6) COMMENT '类目近30天CVR',
    cate_cpc_n30d DECIMAL(18,6) COMMENT '类目近30天CPC',
    cate_cpa_n30d DECIMAL(18,6) COMMENT '类目近30天CPA',
    cate_acos_n30d DECIMAL(18,6) COMMENT '类目近30天ACOS',
    cate_impressions_n90d BIGINT COMMENT '类目近90天广告曝光量',
    cate_clicks_n90d BIGINT COMMENT '类目近90天广告点击量',
    cate_cost_n90d DECIMAL(18,6) COMMENT '类目近90天广告花费',
    cate_sale_amt_n90d DECIMAL(18,6) COMMENT '类目近90天广告销售额',
    cate_order_num_n90d BIGINT COMMENT '类目近90天广告销量',
    cate_ctr_n90d DECIMAL(18,6) COMMENT '类目近90天CTR',
    cate_cvr_n90d DECIMAL(18,6) COMMENT '类目近90天CVR',
    cate_cpc_n90d DECIMAL(18,6) COMMENT '类目近90天CPC',
    cate_cpa_n90d DECIMAL(18,6) COMMENT '类目近90天CPA',
    cate_acos_n90d DECIMAL(18,6) COMMENT '类目近90天ACOS',
    create_time DATETIME COMMENT '创建时间',
    season_label STRING COMMENT '季节标签',
    adv_start_date STRING COMMENT '首次广告日期',
    adv_days BIGINT COMMENT '首次广告至今天数',
    adv_weeks DECIMAL(18,6) COMMENT '首次广告至今周数',
    craw_post_code STRING COMMENT '商品对应的关键词爬取邮编'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='父ASIN基础指标')
    LIFECYCLE 30;


--支持重跑
alter table dws_mkt_adv_strategy_parent_asin_base_index_new_ds drop if exists partition (ds = '${bizdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_parent_asin_base_index_new_ds partition (ds = '${bizdate}')
(
 tenant_id
,profile_id
,marketplace_id
,seller_id
,adv_manager_id
,adv_manager_name
,top_parent_asin
,fba_first_instock_time
,fba_first_instock_days
,life_cycle_label
,stock_label
,stock_sale_days
,category
,term_type
,cate_impressions_n30d
,cate_clicks_n30d
,cate_cost_n30d
,cate_sale_amt_n30d
,cate_order_num_n30d
,cate_ctr_n30d
,cate_cvr_n30d
,cate_cpc_n30d
,cate_cpa_n30d
,cate_acos_n30d
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
,season_label
,craw_post_code
,adv_start_date
,adv_days
,adv_weeks
)
select q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.seller_id
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.top_parent_asin
     ,q1.fba_first_instock_time
     ,q1.fba_first_instock_days
     ,q1.life_cycle_label
     ,q1.stock_label
     ,q1.stock_sale_days
     ,q2.category
     ,q2.term_type
     ,q2.cate_impressions_n30d
     ,q2.cate_clicks_n30d
     ,q2.cate_cost_n30d
     ,q2.cate_sale_amt_n30d
     ,q2.cate_order_num_n30d
     ,q2.cate_ctr_n30d
     ,q2.cate_cvr_n30d
     ,q2.cate_cpc_n30d
     ,q2.cate_cpa_n30d
     ,q2.cate_acos_n30d
     ,q2.cate_impressions_n90d
     ,q2.cate_clicks_n90d
     ,q2.cate_cost_n90d
     ,q2.cate_sale_amt_n90d
     ,q2.cate_order_num_n90d
     ,q2.cate_ctr_n90d
     ,q2.cate_cvr_n90d
     ,q2.cate_cpc_n90d
     ,q2.cate_cpa_n90d
     ,q2.cate_acos_n90d
     ,getdate() as create_time
     ,q1.season_label
     ,q1.craw_post_code
     ,q1.adv_start_date
     ,q1.adv_days
     ,q1.adv_weeks
from (
         select tenant_id
              ,profile_id
              ,marketplace_id
              ,seller_id
              ,adv_manager_id
              ,adv_manager_name
              ,top_parent_asin
              ,fba_first_instock_time
              ,fba_first_instock_days
              ,case when stock_label = 1 then '库存充足' when stock_label = 0 then '库存不足' end
              ,stock_sale_days
              ,self_category
              ,case when mature_label = 1 then '成熟期' when mature_label = 0 then '新品期' end as life_cycle_label
              ,season_label
              ,craw_post_code
              ,adv_start_date
              ,adv_days
              ,adv_weeks
         from whde.dws_mkt_adv_strategy_parent_asin_index_ds
         where ds = '${bizdate}'  --and mature_label is not null
     )q1
         inner join (
    select   tenant_id
         ,marketplace_id
         ,marketplace_name
         ,category
         ,term_type
         ,'成熟期' life_cycle_type
         ,cate_impressions_n30d
         ,cate_clicks_n30d
         ,cate_cost_n30d
         ,cate_sale_amt_n30d
         ,cate_order_num_n30d
         ,cate_ctr_n30d
         ,cate_cvr_n30d
         ,cate_cpc_n30d
         ,cate_cpa_n30d
         ,cate_acos_n30d
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
    from    whde.dws_mkt_adv_strategy_category_index_ds
    where   ds = '${bizdate}' and life_cycle_type is not null
)q2
                    on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.self_category = q2.category and q1.life_cycle_label = q2.life_cycle_type
;

