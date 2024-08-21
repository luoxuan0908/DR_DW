--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-06 21:59:21
--********************************************************************--


create table if not exists adm_strategy_neg_root_word_detail_ds
(
    row_id string comment '主键'
    ,campaign_id string comment '广告活动id'
    ,campaign_name string comment '广告活动名称'
    ,ad_mode string comment '广告活动类型'
    ,word string comment '词根'
    ,search_term_list string comment '搜索词list'
    ,search_term_cnt string comment '搜索词个数'
    ,clicks string comment '点击量'
    ,cost string comment '广告花费'
    ,sale_amt string comment '销售额'
    ,sale_num string comment '销售量'
    ,order_num string comment '订单量'
    ,cpc string comment '降价'
    ,acos string comment 'acos'
    ,cvr string comment '转化率'
)
    partitioned by
(
    ds                 string
)
    stored as aliorc
    tblproperties ('comment' = '广告策略否词根子表(站点、店铺、父asin、广告活动、词根)')
    lifecycle 366
;

insert OVERWRITE table adm_strategy_neg_root_word_detail_ds PARTITION (ds ='${nowdate}')
select   abs(hash(tenant_id,profile_id,stem,'否词根',top_parent_asin))
     ,campaign_id
     ,campaign_name
     ,ad_mode_label
     ,word
     ,search_term_list
     ,term_cnt
     ,clicks
     ,cost
     ,sale_amt
     ,order_num
     ,order_num
     ,cpc
     ,acos
     ,cvr
from  dws_mkt_adv_strategy_neg_adv_word_ds
where ds = '${nowdate}'
;
