--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-30 00:36:10
--********************************************************************--
--@exclude_input=asq_dw.dws_mkt_adv_strategy_parent_asin_index_ds
--@exclude_input=open_dw.adm_amazon_adv_keyword_target_status_df
--@exclude_input=open_dw.dwd_mkt_adv_amazon_sp_target_ds
--@exclude_input=open_dw.dwd_mkt_adv_quickbi_rollback_log_df
--@exclude_input=asq_dw.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
--@exclude_input=asq_dw.dws_mkt_adv_strategy_parent_asin_base_index_ds
--odps sql
--********************************************************************--
--author:王敏佳
--create time:2024-01-03 18:03:25
--********************************************************************--

CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_adjust_bid_param_hf
(
    tenant_id        STRING COMMENT '租户ID'
    ,row_id           STRING COMMENT '策略明细ID'
    ,profile_id       STRING COMMENT '配置ID'
    ,marketplace_id   STRING COMMENT '站点ID'
    ,adv_manager_id   STRING COMMENT '操作人ID'
    ,adv_manager_name STRING COMMENT '操作人名称'
    ,campaign_id      STRING COMMENT '广告活动ID'
    ,campaign_name    STRING COMMENT '广告活动名称'
    ,ad_group_id      STRING COMMENT '广告组ID'
    ,ad_group_name    STRING COMMENT '广告组名称'
    ,top_parent_asin  STRING COMMENT '父aisn'
    ,term_type        STRING COMMENT '投放词类型'
    ,target_id        STRING COMMENT '投放词ID'
    ,target_term      STRING COMMENT '投放词/品'
    ,match_type       STRING COMMENT '匹配类型'
    ,clicks           BIGINT COMMENT '点击量'
    ,cost             DECIMAL(18,6) COMMENT '花费'
    ,sale_amt         DECIMAL(18,6) COMMENT '销售额'
    ,order_num        BIGINT COMMENT '销量'
    ,cpa              DECIMAL(18,6) COMMENT 'cpa'
    ,cvr              DECIMAL(18,6) COMMENT 'cvr'
    ,acos             DECIMAL(18,6) COMMENT 'acos'
    ,cate_acos        DECIMAL(18,6) COMMENT 'cate_acos'
    ,cate_cpa         DECIMAL(18,6) COMMENT 'cate_cpa'
    ,cate_cvr         DECIMAL(18,6) COMMENT 'cate_cvr'
    ,norm_rank        BIGINT COMMENT '自然搜索排名'
    ,adv_rank         BIGINT COMMENT '广告搜索排名'
    ,bid_adjust_label STRING COMMENT '竞价调整类型'
    ,data_dt          STRING COMMENT '数据日期'
    ,etl_data_dt      DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY
(
    hs             STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '爱思奇中腾广告策略调整竞价参数表')
    LIFECYCLE 366
;


--上一个小时的守坑明细投放词和历史的合并
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp1_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_temp1_${hs} as
select   tenant_id
     ,item_id
     ,profile_id
     ,marketplace_id
     ,operator_id
     ,operator_name
     ,parent_asin
     ,campaign_id
     ,ad_group_id
     ,target_text
from    (
            select   tenant_id
                 ,row_id as item_id
                 ,profile_id
                 ,marketplace_id
                 ,operator_id
                 ,operator_name
                 ,top_parent_asin as parent_asin
                 ,campaign_id
                 ,ad_group_id
                 ,target_term as target_text
            from    asq_dw.dwd_mkt_adv_strategy_adjust_bid_pool_hs
            where   substr(hs,1,8) = '${nowdate}'  --T日所有的应用都拿进来
              and     action_name = '投放词守坑'
              and     term_type_label = '投放词'
              and     nvl(profile_id,'') <> ''
              and     nvl(target_term,'') <> ''
              and     nvl(campaign_id,'') <> ''
              and     nvl(ad_group_id,'') <> ''

            union all

            select   tenant_id
                 ,row_id
                 ,profile_id
                 ,marketplace_id
                 ,adv_manager_id
                 ,adv_manager_name
                 ,top_parent_asin
                 ,campaign_id
                 ,ad_group_id
                 ,target_term
            from    asq_dw.dws_mkt_adv_strategy_adjust_bid_param_hf
            where   hs = (select max(hs) from asq_dw.dws_mkt_adv_strategy_adjust_bid_param_hf where hs is not null) and adv_manager_id <> '1'
        )
group by  tenant_id
       ,item_id
       ,profile_id
       ,marketplace_id
       ,operator_id
       ,operator_name
       ,parent_asin
       ,campaign_id
       ,ad_group_id
       ,target_text
;


--获取待守坑对象的当下指标
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp1_1_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_temp1_1_${hs} as
select   a.tenant_id
     ,a.item_id
     ,a.profile_id
     ,a.marketplace_id
     ,a.operator_id
     ,a.operator_name
     ,a.parent_asin
     ,a.campaign_id
     ,campaign_name
     ,a.ad_group_id
     ,ad_group_name
     ,target_id
     ,match_type
     ,a.target_text
     ,sale_amt
     ,sale_num
     ,cost
     ,clicks
     ,nvl(c.post_code,'10040') as postcode
from (
         select   tenant_id
              ,item_id
              ,profile_id
              ,marketplace_id
              ,operator_id
              ,operator_name
              ,parent_asin
              ,campaign_id
              ,ad_group_id
              ,target_text
         from dws_mkt_adv_strategy_adjust_bid_param_df_temp1_${hs}
     ) a
         left join(
    select   tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,keyword_id as target_id
         ,case when instr(targeting,'asin') >= 1 then replace(split_part(targeting,'=',2),'"','')
               when instr(targeting,'category') >= 1 then split_part(replace(split_part(targeting,'=',2),'"',''),'price',1) else targeting end as targeting
         ,match_type
         ,sum(w7d_sale_amt) as sale_amt
         ,sum(w7d_units_sold_clicks) as sale_num
         ,sum(cost) as cost
         ,sum(clicks) as clicks
         ,max(campaign_name) as campaign_name
         ,max(ad_group_name) as ad_group_name
    from    open_dw.dwd_mkt_adv_amazon_sp_target_ds   --投放词14天效果
    where   ds >= to_char(dateadd(to_date(substr('${hs}',1,8),'yyyymmdd'),-14,'dd'),'yyyymmdd')
      and     tenant_id in ('1714548493239062529','1555073968741003270')
      and     nvl(keyword_id,'')<>''
      and     match_type = 'EXACT'
    group by  tenant_id
           ,profile_id
           ,campaign_id
           ,ad_group_id
           ,keyword_id
           ,targeting
           ,match_type
) b
                  on a.tenant_id = b.tenant_id and a.profile_id = b.profile_id and a.campaign_id = b.campaign_id and a.ad_group_id = b.ad_group_id and a.target_text = b.targeting
         left join (
    select tenant_id
         ,marketplace_id
         ,top_parent_asin
         ,craw_post_code as post_code
    from asq_dw.dws_mkt_adv_strategy_parent_asin_index_ds
    where ds = max_pt('asq_dw.dws_mkt_adv_strategy_parent_asin_index_ds')
)c
                   on a.tenant_id = c.tenant_id and a.marketplace_id = c.marketplace_id and a.parent_asin = c.top_parent_asin
;


--取最近广告排名中位数
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp2_1_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_temp2_1_${hs} as
select parent_asin
     ,search_term
     ,marketplace_id
     ,postcode
     ,cast(percentile((page - 1) * 60 + search_rank,0.5) as bigint ) as adv_rank  --中位数
from open_dw.adm_amazon_adv_skw_asin_rank_info_h
where parent_asin is not null and is_sponsored = 1 and substr(hs,1,10) >= to_char(dateadd(to_date('${hs}','yyyymmddhh'),-2,'hh'),'yyyymmddhh')  --最近3小时
group by parent_asin
       ,search_term
       ,marketplace_id
       ,postcode
;


--取最近自然排名中位数
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp2_2_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_temp2_2_${hs} as
select parent_asin
     ,search_term
     ,marketplace_id
     ,postcode
     ,cast(percentile((page - 1) * 60 + search_rank,0.5) as bigint )  as norm_rank  --中位数
from open_dw.adm_amazon_adv_skw_asin_rank_info_h
where parent_asin is not null and is_sponsored = 0 and substr(hs,1,10) >= to_char(dateadd(to_date('${hs}','yyyymmddhh'),-2,'hh'),'yyyymmddhh')  --最近3小时
group by parent_asin
       ,search_term
       ,marketplace_id
       ,postcode
;



--取最新的排名数据
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp2_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_temp2_${hs} as
select   q1.tenant_id
     ,q1.item_id as row_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.operator_id
     ,q1.operator_name
     ,q1.parent_asin
     ,q1.campaign_id
     ,q1.campaign_name
     ,q1.ad_group_id
     ,q1.ad_group_name
     ,q1.target_id
     ,q1.match_type
     ,q1.target_text
     ,sum(q1.sale_amt) as sale_amt
     ,sum(q1.sale_num) as sale_num
     ,sum(q1.clicks) as clicks
     ,sum(q1.cost) as cost
     ,cast(case when sum(q1.clicks) = 0 then null else sum(q1.sale_num) / sum(q1.clicks) end as decimal(18,6)) as cvr
     ,cast(case when sum(q1.sale_num) = 0 then null else sum(q1.cost) / sum(q1.sale_num) end as decimal(18,6)) as cpa
     ,cast(case when sum(q1.sale_amt) = 0 then null else sum(q1.cost) / sum(q1.sale_amt) end as decimal(18,6)) as acos
     ,min(nvl(q2.adv_rank,999)) adv_rank    --一定是加在小时爬虫池子的,为空说明不在前三页
     ,min(nvl(q3.norm_rank,999)) norm_rank
from  dws_mkt_adv_strategy_adjust_bid_param_df_temp1_1_${hs} q1
          left join dws_mkt_adv_strategy_adjust_bid_param_df_temp2_1_${hs} q2 --广告排名
                    on q1.marketplace_id = q2.marketplace_id and  q1.parent_asin = q2.parent_asin and q1.target_text = q2.search_term and q1.postcode = q2.postcode
          left join dws_mkt_adv_strategy_adjust_bid_param_df_temp2_2_${hs} q3 --自然排名
                    on q1.marketplace_id = q3.marketplace_id and  q1.parent_asin = q3.parent_asin and q1.target_text = q3.search_term and q1.postcode = q3.postcode
group by q1.tenant_id
       ,q1.item_id
       ,q1.profile_id
       ,q1.marketplace_id
       ,q1.operator_id
       ,q1.operator_name
       ,q1.parent_asin
       ,q1.campaign_id
       ,q1.campaign_name
       ,q1.ad_group_id
       ,q1.ad_group_name
       ,q1.target_id
       ,q1.match_type
       ,q1.target_text
;



--类目平均
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp3_1_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_temp3_1_${hs} as
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
     ,stock_label
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
from    asq_dw.dws_mkt_adv_strategy_parent_asin_base_index_new_ds
where   ds = max_pt('asq_dw.dws_mkt_adv_strategy_parent_asin_base_index_new_ds') and life_cycle_label = '成熟期' and stock_label = '库存充足' and term_type = '搜索词'
;


--中腾守坑基础数据
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_zt_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_zt_${hs} as
select  s1.tenant_id
     ,s1.row_id
     ,s1.profile_id
     ,marketplace_id
     ,operator_id as adv_manager_id
     ,operator_name as adv_manager_name
     ,parent_asin as top_parent_asin
     ,s1.campaign_id
     ,campaign_name
     ,s1.ad_group_id
     ,ad_group_name
     ,s1.target_id
     ,match_type
     ,target_text
     ,clicks
     ,cost
     ,sale_amt
     ,sale_num as order_num
     ,cpa
     ,cvr
     ,acos
     ,cate_acos
     ,cate_cpa
     ,cate_cvr
     ,case when cpa < cate_cpa then '低CPA' end as cpa_label
     ,case when acos_label = '高ACOS' then '下调竞价'
           when acos_label = '非高ACOS' and norm_rank > 60 then '上调竞价'
           when acos_label = '非高ACOS' and norm_rank <= 30 and adv_rank <= 30 then '下调竞价'
           when acos_label = '非高ACOS' and norm_rank > 30 and adv_rank > 30 then '上调竞价' else '竞价不变' end as bid_adjust_label
     ,case when cate_cvr = 0 then null when  clicks >= 1/cate_cvr then '有效点击' end as click_label
     ,case when cvr >= cate_cvr * 0.5 then '非低转化' end as cvr_label
     ,norm_rank
     ,adv_rank
from (
         select   q1.tenant_id
              ,q1.row_id
              ,q1.profile_id
              ,q1.marketplace_id
              ,q1.operator_id
              ,q1.operator_name
              ,q1.parent_asin
              ,q1.campaign_id
              ,q1.campaign_name
              ,q1.ad_group_id
              ,q1.ad_group_name
              ,q1.target_id
              ,q1.match_type
              ,q1.target_text
              ,q1.sale_amt
              ,q1.sale_num
              ,q1.clicks
              ,q1.cost
              ,q1.cpa
              ,q1.cvr
              ,q1.acos
              ,q1.adv_rank
              ,q1.norm_rank
              ,q2.cate_cpa
              ,q2.cate_acos
              ,q2.cate_cvr
              ,case when q1.acos >= 1.3 * q2.cate_acos then '高ACOS' else '非高ACOS' end as acos_label
         from dws_mkt_adv_strategy_adjust_bid_param_df_temp2_${hs} q1
                  left join dws_mkt_adv_strategy_adjust_bid_param_df_temp3_1_${hs} q2
                            on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.parent_asin = q2.top_parent_asin
         where q1.tenant_id = '1714548493239062529'
     ) s1
         left join (
    select tenant_id
         ,row_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,target_id
    from open_dw.dwd_mkt_adv_quickbi_rollback_log_df   --策略撤回踢出
    where ds = (select max(ds) from open_dw.dwd_mkt_adv_quickbi_rollback_log_df where ds is not null)
)s2
                   on s1.tenant_id = s2.tenant_id and s1.row_id = s2.row_id and s1.profile_id = s2.profile_id and s1.campaign_id = s2.campaign_id and s1.ad_group_id = s2.ad_group_id and s1.target_id = s2.target_id
         inner join (
    select tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,keyword_id
         ,keyword_text
    from open_dw.adm_amazon_adv_keyword_target_status_df
    where ds = max_pt('open_dw.adm_amazon_adv_keyword_target_status_df') and status = 'ENABLED'   --正常的投放词
)s3
                    on s1.tenant_id = s3.tenant_id and s1.profile_id = s3.profile_id and s1.campaign_id = s3.campaign_id and s1.ad_group_id = s3.ad_group_id and s1.target_id = s3.keyword_id
where s2.target_id is null
;


--爱思奇守坑基础数据
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_asq_${hs};
create table dws_mkt_adv_strategy_adjust_bid_param_df_asq_${hs} as
select  s1.tenant_id
     ,s1.row_id
     ,s1.profile_id
     ,marketplace_id
     ,operator_id as adv_manager_id
     ,operator_name as adv_manager_name
     ,parent_asin as top_parent_asin
     ,s1.campaign_id
     ,campaign_name
     ,s1.ad_group_id
     ,ad_group_name
     ,s1.target_id
     ,match_type
     ,target_text
     ,clicks
     ,cost
     ,sale_amt
     ,sale_num as order_num
     ,cpa
     ,cvr
     ,acos
     ,cate_acos
     ,cate_cpa
     ,cate_cvr
     ,case when cpa < cate_cpa then '低CPA' end as cpa_label
     ,case when acos_label = '高ACOS' then '下调竞价'
           when acos_label = '非高ACOS' and norm_rank > 60 then '上调竞价'
           when acos_label = '非高ACOS' and norm_rank <= 30 and adv_rank <= 30 then '下调竞价'
           when acos_label = '非高ACOS' and norm_rank > 30 and adv_rank > 30 then '上调竞价' else '竞价不变' end as bid_adjust_label
     ,case when cate_cvr = 0 then null when clicks >= (1/cate_cvr) * 1.2 then '有效点击' end as click_label
     ,case when cvr >= cate_cvr * 0.5 then '非低转化' end as cvr_label
     ,norm_rank
     ,adv_rank
from (
         select   q1.tenant_id
              ,q1.row_id
              ,q1.profile_id
              ,q1.marketplace_id
              ,q1.operator_id
              ,q1.operator_name
              ,q1.parent_asin
              ,q1.campaign_id
              ,q1.campaign_name
              ,q1.ad_group_id
              ,q1.ad_group_name
              ,q1.target_id
              ,q1.match_type
              ,q1.target_text
              ,q1.sale_amt
              ,q1.sale_num
              ,q1.clicks
              ,q1.cost
              ,q1.cpa
              ,q1.cvr
              ,q1.acos
              ,q1.adv_rank
              ,q1.norm_rank
              ,q2.cate_cpa
              ,q2.cate_acos
              ,q2.cate_cvr
              ,case when q1.acos >= 1.3 * q2.cate_acos then '高ACOS' else '非高ACOS' end as acos_label
         from dws_mkt_adv_strategy_adjust_bid_param_df_temp2_${hs} q1
                  left join dws_mkt_adv_strategy_adjust_bid_param_df_temp3_1_${hs} q2
                            on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.parent_asin = q2.top_parent_asin
         where q1.tenant_id = '1555073968741003270'
     ) s1
         left join (
    select tenant_id
         ,row_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,target_id
    from open_dw.dwd_mkt_adv_quickbi_rollback_log_df   --策略撤回踢出
    where ds = (select max(ds) from open_dw.dwd_mkt_adv_quickbi_rollback_log_df where ds is not null)
)s2
                   on s1.tenant_id = s2.tenant_id and s1.row_id = s2.row_id and s1.profile_id = s2.profile_id and s1.campaign_id = s2.campaign_id and s1.ad_group_id = s2.ad_group_id and s1.target_id = s2.target_id
         inner join (
    select tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,keyword_id
         ,keyword_text
    from open_dw.adm_amazon_adv_keyword_target_status_df
    where ds = max_pt('open_dw.adm_amazon_adv_keyword_target_status_df') and status = 'ENABLED'   --正常的投放词
)s3
                    on s1.tenant_id = s3.tenant_id and s1.profile_id = s3.profile_id and s1.campaign_id = s3.campaign_id and s1.ad_group_id = s3.ad_group_id and s1.target_id = s3.keyword_id
where s2.target_id is null
;


--支持重跑
alter table dws_mkt_adv_strategy_adjust_bid_param_hf drop if exists partition (hs = '${hs}');

--插入最新数据
insert into table dws_mkt_adv_strategy_adjust_bid_param_hf partition (hs = '${hs}')
(
    tenant_id
   ,row_id
   ,profile_id
   ,marketplace_id
   ,adv_manager_id
   ,adv_manager_name
   ,campaign_id
   ,campaign_name
   ,ad_group_id
   ,ad_group_name
   ,top_parent_asin
   ,target_id
   ,match_type
   ,term_type
   ,target_term
   ,clicks
   ,cost
   ,sale_amt
   ,order_num
   ,cpa
   ,cvr
   ,acos
   ,cate_acos
   ,cate_cpa
   ,cate_cvr
   ,norm_rank
   ,adv_rank
   ,bid_adjust_label
   ,data_dt
   ,etl_data_dt
)
select  tenant_id
     ,row_id
     ,profile_id
     ,marketplace_id
     ,adv_manager_id
     ,adv_manager_name
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,top_parent_asin
     ,target_id
     ,match_type
     ,case when length(regexp_replace(target_text,'([^0-9])','')) > 0 and length(regexp_replace(target_text,'([^a-z])','')) > 0 and length(target_text) = 10 then '投放品' else '投放词' end term_type
     ,target_text
     ,clicks
     ,cast(cost as decimal(18,6)) as cost
     ,cast(sale_amt as decimal(18,6)) as sale_amt
     ,order_num
     ,cast(cpa as decimal(18,6)) as cpa
     ,cast(cvr as decimal(18,6)) as cvr
     ,cast(acos as decimal(18,6)) as acos
     ,cast(cate_acos as decimal(18,6)) as cate_acos
     ,cast(cate_cpa as decimal(18,6)) as cate_cpa
     ,cast(cate_cvr as decimal(18,6)) as cate_cvr
     ,norm_rank
     ,adv_rank
     ,bid_adjust_label
     ,substr('${hs}',1,8) as data_dt
     ,getdate() as etl_data_dt
from dws_mkt_adv_strategy_adjust_bid_param_df_zt_${hs}

union all

select  tenant_id
     ,row_id
     ,profile_id
     ,marketplace_id
     ,adv_manager_id
     ,adv_manager_name
     ,campaign_id
     ,campaign_name
     ,ad_group_id
     ,ad_group_name
     ,top_parent_asin
     ,target_id
     ,match_type
     ,case when length(regexp_replace(target_text,'([^0-9])','')) > 0 and length(regexp_replace(target_text,'([^a-z])','')) > 0 and length(target_text) = 10 then '投放品' else '投放词' end term_type
     ,target_text
     ,clicks
     ,cast(cost as decimal(18,6)) as cost
     ,cast(sale_amt as decimal(18,6)) as sale_amt
     ,order_num
     ,cast(cpa as decimal(18,6)) as cpa
     ,cast(cvr as decimal(18,6)) as cvr
     ,cast(acos as decimal(18,6)) as acos
     ,cast(cate_acos as decimal(18,6)) as cate_acos
     ,cast(cate_cpa as decimal(18,6)) as cate_cpa
     ,cast(cate_cvr as decimal(18,6)) as cate_cvr
     ,norm_rank
     ,adv_rank
     ,bid_adjust_label
     ,substr('${hs}',1,8) as data_dt
     ,getdate() as etl_data_dt
from dws_mkt_adv_strategy_adjust_bid_param_df_asq_${hs}
;





drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp1_${hs};
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp1_1_${hs};
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp2_1_${hs};
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp2_2_${hs};
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp2_${hs};

drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_temp3_1_${hs};

drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_zt_${hs};
drop table if exists dws_mkt_adv_strategy_adjust_bid_param_df_asq_${hs};
