--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 23:19:35
--********************************************************************--
CREATE TABLE IF NOT EXISTS dws_mkt_adv_camp_budget_lack_rate_ds
(
    tenant_id          STRING        COMMENT '租户ID',
    profile_id         STRING        COMMENT '配置id',
    marketplace_id     STRING        COMMENT '站点id',
    seller_id          STRING        COMMENT '卖家id',
    campaign_id        STRING        COMMENT '广告活动id',
    cnt_total          BIGINT        COMMENT '近7天有效统计小时数',
    lack_cnt           BIGINT        COMMENT '近7天预算缺失小时数',
    lack_rate          DECIMAL(18,6) COMMENT '近7天预算缺失比例',
    budget_label       STRING        COMMENT '预算标签',
    n1_max_budget      DECIMAL(18,6) COMMENT '近1天最大广告预算',
    n1_budget_rate     DECIMAL(18,6) COMMENT '近1天预算缺失率',
    n7_max_budget      DECIMAL(18,6) COMMENT '近7天最大广告预算',
    n7_min_budget      DECIMAL(18,6) COMMENT '近7天最小广告预算',
    if_keep            BIGINT        COMMENT '近7天广告预算不变',
    n7_avg_cost        DECIMAL(18,6) COMMENT '近7天平均广告花费',
    n7_best_budget     DECIMAL(18,6) COMMENT '近7天预算缺失占比最小当天的最大广告预算',
    n7_best_lack_rate  DECIMAL(18,6) COMMENT '近7天预算缺失占比最小的比例',
    n15_best_budget    DECIMAL(18,6) COMMENT '近15天预算缺失占比最小当天的最大广告预算',
    n15_best_lack_rate DECIMAL(18,6) COMMENT '近15天预算缺失占比最小的比例',
    latest_budget      DECIMAL(18,6) COMMENT '最新的数据下的广告活动预算',
    adj_budget         DECIMAL(18,6) COMMENT '建议调整的预算',
    create_time        DATETIME      COMMENT '创建时间'
    )
    COMMENT '广告活动预算缺失相关指标'
    PARTITIONED BY
(
    ds                 STRING
)
    LIFECYCLE 7;


--基础数据，广告活动花费和预算
drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp2;
create table if not exists dws_mkt_adv_camp_budget_lack_rate_ds_temp2 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,seller_id
     ,campaign_id
     ,report_date
     ,update_time_row
     ,update_time_zh
     ,case when campaign_budget_amount - cost <= 1 then 1 else 0 end is_lack_budget --差额在1美元以内都算缺失，暂跟佳奇一样
     ,cost
     ,cast(campaign_budget_amount as decimal(18,6)) as campaign_budget_amount
from (
         select   tenant_id
              ,profile_id
              ,marketplace_id
              ,seller_id
              ,campaign_id
              ,to_date(report_date,'yyyy-mm-dd') as report_date
              ,update_time_row
              ,update_time_zh
              ,sum(cost) cost
              ,max(campaign_budget_amount) as campaign_budget_amount
         from    whde.dwd_mkt_adv_amazon_sp_product_ms
         where replace(report_date,'-','')  = substr(update_time_row,1,8)
           and substr(ms,1,8)<='${bizdate}'
           and substr(ms,1,8)>='${bizdate1}'
         group by  tenant_id
                ,profile_id
                ,marketplace_id
                ,seller_id
                ,campaign_id
                ,to_date(report_date,'yyyy-mm-dd')
                ,update_time_row
                ,update_time_zh
     ) q1
;


--近15天每天的预算缺失率和最大广告活动预算
drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp3;
create table dws_mkt_adv_camp_budget_lack_rate_ds_temp3 as
select  tenant_id
     ,profile_id
     ,seller_id
     ,campaign_id
     ,campaign_budget_amount
     ,report_date
     ,budget_rate
     ,rank()over(partition by tenant_id,profile_id,campaign_id order by budget_rate) as rk
from (
         select   tenant_id
              ,profile_id
              ,seller_id
              ,campaign_id
              ,report_date
              ,max(campaign_budget_amount) as campaign_budget_amount
              ,sum(is_lack_budget) / count(1) as budget_rate
         from  dws_mkt_adv_camp_budget_lack_rate_ds_temp2
         where report_date < to_date('${nowdate}','yyyymmdd') and substr(update_time_row,9,2) >= '07' and substr(update_time_row,9,2) <= '22'
         group by  tenant_id
                ,profile_id
                ,seller_id
                ,campaign_id
                ,report_date
     )q1
;


--近7天每天的预算缺失率和最大广告活动预算
drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp4;
create table dws_mkt_adv_camp_budget_lack_rate_ds_temp4 as
select  tenant_id
     ,profile_id
     ,seller_id
     ,campaign_id
     ,campaign_budget_amount
     ,report_date
     ,budget_rate
     ,rank()over(partition by tenant_id,profile_id,campaign_id order by budget_rate) as rk
from (
         select   tenant_id
              ,profile_id
              ,seller_id
              ,campaign_id
              ,report_date
              ,max(campaign_budget_amount) as campaign_budget_amount
              ,sum(is_lack_budget) / count(1) as budget_rate
         from  dws_mkt_adv_camp_budget_lack_rate_ds_temp2
         where report_date > dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and report_date < to_date('${nowdate}','yyyymmdd') and substr(update_time_row,9,2) >= '07' and substr(update_time_row,9,2) <= '22'
         group by  tenant_id
                ,profile_id
                ,seller_id
                ,campaign_id
                ,report_date
     )q1
;


--最新数据及预算
drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp5;
create table dws_mkt_adv_camp_budget_lack_rate_ds_temp5 as
select  tenant_id
     ,profile_id
     ,update_time_zh
     ,campaign_id
     ,campaign_budget_amount
from (
         select   tenant_id
              ,profile_id
              ,campaign_id
              ,update_time_zh
              ,campaign_budget_amount
              ,row_number()over(partition by tenant_id,profile_id,campaign_id order by update_time_zh desc) as rk
         from  dws_mkt_adv_camp_budget_lack_rate_ds_temp2
         group by  tenant_id
                ,profile_id
                ,campaign_id
                ,update_time_zh
                ,campaign_budget_amount
     )q1
where rk = 1
;


--近7天平均广告花费
drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp6;
create table dws_mkt_adv_camp_budget_lack_rate_ds_temp6 as
select  tenant_id
     ,profile_id
     ,campaign_id
     ,avg(cost) as n7_avg_cost
from (
         select   tenant_id
              ,profile_id
              ,campaign_id
              ,report_date
              ,max(cost) as cost
         from  dws_mkt_adv_camp_budget_lack_rate_ds_temp2
         where report_date > dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and report_date < to_date('${nowdate}','yyyymmdd')
         group by  tenant_id
                ,profile_id
                ,campaign_id
                ,report_date
     )q1
group by tenant_id
       ,profile_id
       ,campaign_id
;


--近1天预算缺失率和最大广告活动预算
drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp7;
create table dws_mkt_adv_camp_budget_lack_rate_ds_temp7 as
select   tenant_id
     ,profile_id
     ,campaign_id
     ,max(campaign_budget_amount) as campaign_budget_amount
     ,sum(is_lack_budget) / count(1) as budget_rate
from  dws_mkt_adv_camp_budget_lack_rate_ds_temp2
where report_date > dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd') and report_date < to_date('${nowdate}','yyyymmdd') and substr(update_time_row,9,2) >= '07' and substr(update_time_row,9,2) <= '22'
group by  tenant_id
       ,profile_id
       ,campaign_id
;


drop table if exists dws_mkt_adv_camp_budget_lack_rate_ds_temp8;
create table dws_mkt_adv_camp_budget_lack_rate_ds_temp8 as
select   q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.seller_id
     ,q1.campaign_id
     ,q1.total_cnt
     ,q1.lack_cnt
     ,q1.budget_rate as n7_budget_rate
     ,case when q1.budget_rate = 0 then '预算充足'
           when q1.budget_rate > 0 and q1.budget_rate < 0.3 then '一般预算不足'
           when q1.budget_rate >= 0.3 then '高度预算不足' end as budget_label
     ,q1.n7_max_budget
     ,q1.n7_min_budget
     ,q1.if_keep
     ,q5.n7_avg_cost
     ,q3.n7_best_budget
     ,q3.n7_best_budget_rate
     ,q2.n15_best_budget
     ,q2.n15_best_budget_rate
     ,q6.campaign_budget_amount as n1_max_budget
     ,q6.budget_rate as n1_budget_rate
     -- ,q4.update_time_zh as latest_data_hour
     ,q4.campaign_budget_amount as latest_budget
     ,getdate() as create_time
from (
         select   tenant_id
              ,profile_id
              ,marketplace_id
              ,seller_id
              ,campaign_id
              ,count(1) as total_cnt
              ,sum(is_lack_budget) as lack_cnt
              ,sum(is_lack_budget) / count(1) as budget_rate
              ,max(campaign_budget_amount) as n7_max_budget
              ,min(campaign_budget_amount) as n7_min_budget
              ,case when max(campaign_budget_amount) = min(campaign_budget_amount) then 1 else 0 end as if_keep
         from  dws_mkt_adv_camp_budget_lack_rate_ds_temp2
         where report_date > dateadd(to_date('${bizdate}','yyyymmdd'),-7,'dd') and report_date < to_date('${nowdate}','yyyymmdd') and substr(update_time_row,9,2) >= '07' and substr(update_time_row,9,2) <= '22'
         group by  tenant_id
                ,profile_id
                ,marketplace_id
                ,seller_id
                ,campaign_id
     ) q1
         left join (
    select   tenant_id
         ,profile_id
         ,campaign_id
         ,min(budget_rate) as n15_best_budget_rate
         ,avg(campaign_budget_amount) as n15_best_budget
    from  dws_mkt_adv_camp_budget_lack_rate_ds_temp3
    where rk = 1
    group by  tenant_id
           ,profile_id
           ,campaign_id

) q2
                   on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.campaign_id = q2.campaign_id
         left join (
    select   tenant_id
         ,profile_id
         ,campaign_id
         ,min(budget_rate) as n7_best_budget_rate
         ,avg(campaign_budget_amount) as n7_best_budget
    from  dws_mkt_adv_camp_budget_lack_rate_ds_temp4
    where rk = 1
    group by  tenant_id
           ,profile_id
           ,campaign_id

) q3
                   on q1.tenant_id = q3.tenant_id and q1.profile_id = q3.profile_id and q1.campaign_id = q3.campaign_id
         left join dws_mkt_adv_camp_budget_lack_rate_ds_temp5 q4
                   on q1.tenant_id = q4.tenant_id and q1.profile_id = q4.profile_id and q1.campaign_id = q4.campaign_id
         left join dws_mkt_adv_camp_budget_lack_rate_ds_temp6 q5
                   on q1.tenant_id = q5.tenant_id and q1.profile_id = q5.profile_id and q1.campaign_id = q5.campaign_id
         left join dws_mkt_adv_camp_budget_lack_rate_ds_temp7 q6
                   on q1.tenant_id = q6.tenant_id and q1.profile_id = q6.profile_id and q1.campaign_id = q6.campaign_id
;


insert OVERWRITE  table whde.dws_mkt_adv_camp_budget_lack_rate_ds partition (ds = '${bizdate}')
select tenant_id
     ,profile_id
     ,marketplace_id
     ,seller_id
     ,campaign_id
     ,total_cnt
     ,lack_cnt
     ,cast(n7_budget_rate as decimal(18,6)) as budget_rate
     ,budget_label
     ,cast(n1_max_budget as decimal(18,6)) as n1_max_budget
     ,cast(n1_budget_rate as decimal(18,6)) as n1_budget_rate
     ,cast(n7_max_budget as decimal(18,6)) as n7_max_budget
     ,cast(n7_min_budget as decimal(18,6)) as n7_min_budget
     ,if_keep
     ,cast(n7_avg_cost as decimal(18,6)) as n7_avg_cost
     ,cast(n7_best_budget as decimal(18,6)) as  n7_best_budget
     ,cast(n7_best_budget_rate as decimal(18,6)) as n7_best_budget_rate
     ,cast(n15_best_budget as decimal(18,6)) as n15_best_budget
     ,cast(n15_best_budget_rate as decimal(18,6)) as n15_best_budget_rate
     ,cast(latest_budget as decimal(18,6)) as latest_budget
     ,cast(case when if_keep = 1 then nvl(n1_max_budget,n7_max_budget) * (1 + least(n7_budget_rate,nvl(n1_budget_rate,n7_budget_rate)))
                when if_keep = 0 and n1_budget_rate is null then least(n7_best_budget,n7_max_budget)
                when if_keep = 0 and n1_budget_rate = 0  then n1_max_budget
                when if_keep = 0 and n1_budget_rate <> 0  and n1_max_budget >= n7_max_budget then n1_max_budget * (1 + least(n7_budget_rate,n1_budget_rate))
                when if_keep = 0 and n1_budget_rate <> 0  and n1_max_budget < n7_max_budget and n1_max_budget < n7_best_budget and n7_best_budget = 0 then n7_best_budget
                when if_keep = 0 and n1_budget_rate <> 0  and n1_max_budget < n7_max_budget and n1_max_budget < n7_best_budget and n7_best_budget <> 0 then n7_best_budget * (1 + least(n7_best_budget_rate,n1_budget_rate))
                when if_keep = 0 and n1_budget_rate <> 0  and n1_max_budget < n7_max_budget and n1_max_budget >= n7_best_budget then least(n7_max_budget,n1_max_budget*(1 + least(n7_budget_rate,n1_budget_rate))) end as decimal(18,6)) as adj_budget
     ,getdate() as create_time
from dws_mkt_adv_camp_budget_lack_rate_ds_temp8
where budget_label <> '预算充足'
;