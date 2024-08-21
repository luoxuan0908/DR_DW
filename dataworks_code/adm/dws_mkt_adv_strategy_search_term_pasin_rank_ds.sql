--@exclude_output=whde.dws_mkt_adv_strategy_search_term_pasin_rank_tmp02
--@exclude_output=whde.dws_mkt_adv_strategy_search_term_pasin_rank_tmp01
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 22:52:07
--********************************************************************--


CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_search_term_pasin_rank_ds
(
    marketplace_id        STRING COMMENT '站点ID'
    ,marketplace_name      STRING COMMENT '站点名称'
    ,search_term           STRING COMMENT '搜索词/品'
    ,term_type             STRING COMMENT '搜索对象类型'
    ,post_code             STRING COMMENT '爬虫邮编'
    ,top_parent_asin       STRING COMMENT '父asin'
    ,adv_rank_label        STRING COMMENT '广告坑位排名标签'
    ,norm_rank_label       STRING COMMENT '自然坑位排名标签'
    ,adv_rank              BIGINT COMMENT '广告坑位排名'
    ,norm_rank             BIGINT COMMENT '自然坑位排名'
    ,create_time           DATETIME COMMENT '创建时间'
)
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '前置爬虫指标表')
    LIFECYCLE 30
;



--关联坑位排名
drop table if exists dws_mkt_adv_strategy_search_term_pasin_rank_tmp01;
create table dws_mkt_adv_strategy_search_term_pasin_rank_tmp01 as
select q1.marketplace_id
     ,q1.asin as search_asin          --详情页asin
     ,q2.parent_asin as similar_parent_asin --关联坑位父asin
     ,max(q1.adv_rank_label) as adv_rank_label
     ,max(q1.norm_rank_label) as norm_rank_label
     ,min(q1.adv_rank) as adv_rank
     ,min(q1.norm_rank) as norm_rank
from (
         select marketplace_id
              ,asin
              ,similar_asin
              ,min(case when is_sponsored = 1 then position_index end) as adv_rank
              ,min(case when is_sponsored = 0 then position_index end) as norm_rank
              ,max(case when is_sponsored = 1 and (position_index/8) > 1 then 0 when is_sponsored = 1 and (position_index/8) <= 1 then 1 end) as adv_rank_label
              ,max(case when is_sponsored = 0 and (position_index/8) > 1 then 0 when is_sponsored = 0 and (position_index/8) <= 1 then 1 end) as norm_rank_label
         from whde.dwd_itm_sku_amazon_similar_asin_info_ds
         where ds = '${bizdate}' and position_index <> 0
         group by marketplace_id
                ,asin
                ,similar_asin
     )q1
         inner join(select * from  (select *,market_place_id marketplace_id,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                    from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                    where rn =1
)q2
                   on q1.marketplace_id = q2.marketplace_id and q1.similar_asin = q2.asin
group by q1.marketplace_id
       ,q1.asin
       ,q2.parent_asin
;


drop table if exists dws_mkt_adv_strategy_search_term_pasin_rank_tmp02;
create table dws_mkt_adv_strategy_search_term_pasin_rank_tmp02 as
select  parent_asin as top_parent_asin
     ,search_term
     ,p1_norm_label
     ,p1_adv_label
     ,rank_norm_abs as norm_rank
     ,rank_adv_abs as adv_rank
     ,marketplace_id
from   whde.adm_amazon_adv_search_term_pasin_rank_df
where  ds = '${bizdate}'
;



--支持重跑
alter table dws_mkt_adv_strategy_search_term_pasin_rank_ds drop if exists partition (ds = '${bizdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_search_term_pasin_rank_ds partition (ds = '${bizdate}')
(
 marketplace_id
,search_term
,top_parent_asin
,adv_rank_label
,norm_rank_label
,adv_rank
,norm_rank
,term_type
,create_time
)
select marketplace_id
     ,search_asin as search_term
     ,similar_parent_asin as top_parent_asin
     ,adv_rank_label
     ,norm_rank_label
     ,adv_rank
     ,norm_rank
     ,'搜索品' as term_type
     ,getdate() as create_time
from dws_mkt_adv_strategy_search_term_pasin_rank_tmp01

union all

select marketplace_id
     ,search_term
     ,top_parent_asin
     ,p1_adv_label
     ,p1_norm_label
     ,adv_rank
     ,norm_rank
     ,'搜索词' as term_type
     ,getdate() as create_time
from dws_mkt_adv_strategy_search_term_pasin_rank_tmp02
;