--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 15:25:36
--********************************************************************--


create table if not exists dws_mkt_adv_strategy_neg_adv_word_ds
(
    tenant_id         string comment '租户ID'
    ,row_id            string comment 'ID'
    ,stem_id           string comment '词根ID'
    ,strategy_id       string comment '策略ID'
    ,profile_id        string comment '配置ID'
    ,marketplace_id    string comment '站点ID'
    ,marketplace_name  string comment '站点名称'
    ,currency_code     string comment '币种'
    ,ad_type           string comment '广告类型'
    ,seller_id         string comment '卖家ID'
    ,seller_name       string comment '卖家名称'
    ,adv_manager_id    string comment '广告负责人ID'
    ,adv_manager_name  string comment '广告负责人名称'
    ,campaign_id       string comment '广告活动ID'
    ,campaign_name     string comment '广告活动名称'
    ,ad_group_num      bigint comment '广告组数量'
    ,ad_group_id_list  string comment '广告组id列表(通过_/_拼接)'
    ,top_parent_asin   string comment '父aisn'
    ,selling_price     string comment '售价'
    ,main_asin_url     string comment '商品链接'
    ,main_img_url      string comment '商品主图'
    ,stem              string comment '词根'
    ,word              string comment '单词（前端展示）'
    ,clicks            bigint comment '点击量'
    ,cost              decimal(18,6) comment '广告花费'
    ,sale_amt          decimal(18,6) comment '广告销售额'
    ,order_num         bigint comment '广告销量'
    ,ctr               decimal(18,6) comment 'CTR'
    ,cvr               decimal(18,6) comment 'CVR'
    ,cpc               decimal(18,6) comment 'CPC'
    ,acos              decimal(18,6) comment 'ACOS'
    ,adv_days          bigint comment '广告天数'
    ,term_cnt          bigint comment '搜索词数量'
    ,search_term_list  string comment '搜索词列表(通过_/_拼接)'
    ,life_cycle_label  string comment '生命周期'
    ,goal_label        string comment '目标标签'
    ,ad_mode_label     string comment '投放类型标签'
    ,term_type_label   string comment '操作对象标签'
    ,stock_label       string comment '库存标签'
    ,click_label       string comment '点击标签'
    ,cvr_label         string comment '转化标签'
    ,action_type       string comment '操作类型'
    ,create_time       datetime comment '创建时间'
    ,cpa decimal(18,6) comment 'cpa'
    )
    partitioned by
(
    ds                 string
)
    stored as aliorc
    tblproperties ('comment' = '广告策略否词根子表(站点、店铺、父asin、广告活动、词根)')
    lifecycle 366
;


--父aisn+广告活动下的表现
--set odps.sql.python.version = cp37;
drop table if exists dws_mkt_adv_strategy_neg_adv_word_ds_tmp01;
create table dws_mkt_adv_strategy_neg_adv_word_ds_tmp01 as
select   tenant_id
     ,profile_id
     ,marketplace_id
     ,marketplace_name
     ,currency_code
     ,seller_id
     ,seller_name
     ,ad_type
     ,campaign_id
     ,campaign_name
     ,ad_mode
     ,parent_asin
     ,trim(word) as stem --udf_word2stem(word)
     ,max(ad_group_num) ad_group_num
     ,concat_ws('_&_',array_distinct(split(wm_concat(distinct '_&_',ad_group_id_list),'_&_'))) as ad_group_id_list
     ,wm_concat(distinct '_/_',search_term) as search_term_list
     ,count(distinct search_term) as term_cnt
     ,sum(impressions) as impressions
     ,sum(clicks) as clicks
     ,sum(cost) as cost
     ,sum(sale_amt) as sale_amt
     ,sum(order_num) as order_num
     ,cast(case when sum(impressions) <> 0 then sum(clicks) / sum(impressions) else null end as decimal(18,6)) as ctr
     ,cast(case when sum(clicks) <> 0 then sum(order_num) / sum(clicks) else null end as decimal(18,6)) as cvr
     ,cast(case when sum(clicks) <> 0 then sum(cost) / sum(clicks) else null end as decimal(18,6)) as cpc
     ,cast(case when sum(sale_amt) <> 0 then sum(cost) / sum(sale_amt) else null end as decimal(18,6)) as acos
     ,cast(case when sum(order_num) <> 0 then sum(cost) / sum(order_num) else null end as decimal(18,6)) as cpa
     ,max(adv_days) as adv_days
from whde.adm_amazon_adv_strategy_search_term_d
         lateral view explode(split(search_term,' ')) adtable as word
where  ds = '${bizdate}' and adv_days >= 14 and nvl(parent_asin,'') <> '' and term_type = '搜索词'
group by tenant_id
         ,profile_id
         ,marketplace_id
         ,marketplace_name
         ,currency_code
         ,seller_id
         ,seller_name
         ,ad_type
         ,campaign_id
         ,campaign_name
         ,ad_mode
         ,parent_asin
         ,stem
;



--否定掉的词根下的表现
drop table if exists dws_mkt_adv_strategy_neg_adv_word_ds_tmp02;
create table dws_mkt_adv_strategy_neg_adv_word_ds_tmp02 as
select   q1.tenant_id
     ,q1.stem_id
     ,q1.strategy_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.currency_code
     ,q1.ad_type
     ,q1.seller_id
     ,q1.seller_name
     ,q1.adv_manager_id
     ,q1.adv_manager_name
     ,q1.top_parent_asin
     ,q1.selling_price
     ,q1.main_asin_url
     ,q1.main_img_url
     ,q1.stem
     ,q1.word
     ,q2.campaign_id
     ,q2.campaign_name
     ,q2.ad_mode
     ,q2.ad_group_num
     ,q2.ad_group_id_list
     ,q2.search_term_list
     ,q2.term_cnt
     ,q2.impressions
     ,q2.clicks
     ,q2.cost
     ,q2.sale_amt
     ,q2.order_num
     ,q2.ctr
     ,q2.cvr
     ,q2.cpc
     ,q2.cpa
     ,q2.acos
     ,q2.adv_days
     ,q1.goal_label
     ,q1.term_type_label
     ,q1.life_cycle_label
     ,q1.stock_label
     ,q1.click_label
     ,q1.cvr_label
     ,q1.action_type
from    (
            select   tenant_id
                 ,stem_id
                 ,strategy_id
                 ,profile_id
                 ,marketplace_id
                 ,marketplace_name
                 ,currency_code
                 ,ad_type
                 ,seller_id
                 ,seller_name
                 ,adv_manager_id
                 ,adv_manager_name
                 ,top_parent_asin
                 ,selling_price
                 ,main_asin_url
                 ,main_img_url
                 ,stem
                 ,word
                 ,camp_id
                 ,goal_label
                 ,ad_mode_label
                 ,term_type_label
                 ,life_cycle_label
                 ,stock_label
                 ,click_label
                 ,cvr_label
                 ,action_type
            from dws_mkt_adv_strategy_neg_root_word_ds
                     lateral view explode(split(camp_id_list,'_/_')) adtable as camp_id
            where ds = '${bizdate}'
        ) q1
            left join dws_mkt_adv_strategy_neg_adv_word_ds_tmp01 q2
                      on q1.tenant_id = q2.tenant_id and q1.top_parent_asin = q2.parent_asin and q1.stem = q2.stem and q1.camp_id = q2.campaign_id
;




--支持重跑
alter table dws_mkt_adv_strategy_neg_adv_word_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_neg_adv_word_ds partition (ds = '${nowdate}')
(
     tenant_id
    ,row_id
    ,stem_id
    ,strategy_id
    ,profile_id
    ,marketplace_id
    ,marketplace_name
    ,currency_code
    ,ad_type
    ,seller_id
    ,seller_name
    ,adv_manager_id
    ,adv_manager_name
    ,campaign_id
    ,campaign_name
    ,ad_group_num
    ,ad_group_id_list
    ,top_parent_asin
    ,selling_price
    ,main_asin_url
    ,main_img_url
    ,stem
    ,word
    ,clicks
    ,cost
    ,sale_amt
    ,order_num
    ,ctr
    ,cvr
    ,cpc
    ,cpa
    ,acos
    ,adv_days
    ,term_cnt
    ,search_term_list
    ,life_cycle_label
    ,goal_label
    ,ad_mode_label
    ,term_type_label
    ,stock_label
    ,click_label
    ,cvr_label
    ,action_type
    ,create_time
)
select distinct  tenant_id
              ,hash(concat(tenant_id,profile_id,campaign_id,top_parent_asin,stem,strategy_id)) as row_id
              ,stem_id
              ,strategy_id
              ,profile_id
              ,marketplace_id
              ,marketplace_name
              ,currency_code
              ,ad_type
              ,seller_id
              ,seller_name
              ,adv_manager_id
              ,adv_manager_name
              ,campaign_id
              ,campaign_name
              ,ad_group_num
              ,ad_group_id_list
              ,top_parent_asin
              ,selling_price
              ,main_asin_url
              ,main_img_url
              ,stem
              ,word
              ,clicks
              ,cast(cost as decimal(18,6)) as cost
              ,cast(sale_amt as decimal(18,6)) as sale_amt
              ,order_num
              ,cast(ctr as decimal(18,6)) as ctr
              ,cast(cvr as decimal(18,6)) as cvr
              ,cast(cpc as decimal(18,6)) as cpc
              ,cast(cpa as decimal(18,6)) as cpa
              ,cast(acos as decimal(18,6)) as acos
              ,adv_days
              ,term_cnt
              ,search_term_list
              ,life_cycle_label
              ,goal_label
              ,ad_mode
              ,term_type_label
              ,stock_label
              ,click_label
              ,cvr_label
              ,action_type
              ,getdate() as create_time
from  dws_mkt_adv_strategy_neg_adv_word_ds_tmp02
;
