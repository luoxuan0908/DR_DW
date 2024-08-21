--@exclude_output=whde.dws_mkt_adv_strategy_chinese_seller_rate_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 23:21:31
--********************************************************************--

--create table if not exists dws_mkt_adv_strategy_chinese_seller_index_ds
--(
--    marketplace_id string comment '站点id',
--    search_term string comment '搜索词',
--    asin string comment '搜索结果asin',
--    parent_asin string comment '搜索结果asin对应的父asin',
--    page bigint comment '搜索结果asin所在结果页',
--    search_rank bigint comment '搜索结果asin所在坑位',
--    seller_id string comment '搜索结果asin对应的卖家',
--    seller_information string comment '卖家信息',
--    if_chn_seller bigint comment '是否中国卖家：1/0',
--    create_time datetime comment '创建时间'
--)
--partitioned by (ds string)
--stored as aliorc
--tblproperties ('comment'='关键词搜索结果前三页的中国卖家标识')
--lifecycle 366;
--
--
----搜索词前三页
--drop table if exists dws_mkt_adv_strategy_chinese_seller_index_tmp00;
--create table dws_mkt_adv_strategy_chinese_seller_index_tmp00 as
--select   ''  marketplace_id
--        ,search_term
--        ,page
--        ,search_rank
--        ,data_asin as asin
--        ,parent_asin
--        ,row_number()over(partition by search_term,data_asin,parent_asin order by updated_at desc) as rk
--from      whde.amazon_search_result
--        WHERE pt = '${bizdate}' --and period = 'day'
--         and page <= 3  and is_sponsored = 0
--;
--
--
----asin信息表
--drop table if exists dws_mkt_adv_strategy_chinese_seller_index_tmp01;
--create table dws_mkt_adv_strategy_chinese_seller_index_tmp01 as
--select marketplace_id
--      ,asin
--      ,parent_asin
--      ,seller_id
--from asq_dw.dwd_mkt_adv_strategy_asin_base_info_craw_ds
--where ds = max_pt('asq_dw.dwd_mkt_adv_strategy_asin_base_info_craw_ds') and seller_id <> ''  and seller_id is not null
--;
--
--
----支持重跑
--alter table dws_mkt_adv_strategy_chinese_seller_index_ds drop if exists partition (ds = '${bizdate}');
--
----插入最新数据
--insert into table dws_mkt_adv_strategy_chinese_seller_index_ds partition (ds = '${bizdate}')
--(
--   marketplace_id
--  ,search_term
--  ,page
--  ,search_rank
--  ,asin
--  ,parent_asin
--  ,seller_id
--  ,seller_information
--  ,if_chn_seller
--  ,create_time
--)
--select q1.marketplace_id
--      ,q1.search_term
--      ,cast(q1.page as bigint) as page
--      ,cast(q1.search_rank as bigint) as search_rank
--      ,q1.asin
--      ,q1.parent_asin
--      ,q2.seller_id
--      ,q3.seller_information
--      ,cast(q3.if_chn_seller as bigint) as  if_chn_seller
--      ,getdate() as create_time
--from (
--      select marketplace_id
--            ,search_term
--            ,page
--            ,search_rank
--            ,asin
--            ,parent_asin
--      from dws_mkt_adv_strategy_chinese_seller_index_tmp00
--      where rk = 1
--     )q1
--left join dws_mkt_adv_strategy_chinese_seller_index_tmp01 q2
--on q1.marketplace_id = q2.marketplace_id and q1.asin = q2.asin and q1.parent_asin = q2.parent_asin
--left join (
--           select seller_id
--                 ,market_place_id as marketplace_id
--                 ,seller_information
--                 ,case when INSTR(seller_information,'
--CN') > 0 then 1 else 0 end if_chn_seller
--           from zby_dw.ods_amazon_data_amazon_seller_information  --全量
--           where ds = max_pt('zby_dw.ods_amazon_data_amazon_seller_information') and seller_information is not null
--          )q3
--on q2.marketplace_id = q3.marketplace_id and q2.seller_id = q3.seller_id
--;



--create table if not exists dws_mkt_adv_strategy_chinese_seller_rate_df
--(
--    marketplace_id string comment '站点id',
--    search_term string comment '搜索词',
--    search_date string comment '爬取日期',
--    chn_seller_nums bigint comment '搜索结果前三页中国卖家数量',
--    all_seller_nums bigint comment '搜索结果前三页所有卖家数量',
--    chn_seller_rate decimal(18,6) comment '搜索结果前三页中国卖家占比',
--    create_time datetime comment '创建时间'
--)
--partitioned by (ds string)
--stored as aliorc
--tblproperties ('comment'='关键词搜索结果前三页中国卖家占比')
--lifecycle 30;



-- insert overwrite table dws_mkt_adv_strategy_chinese_seller_rate_df partition (ds = '20240220')
-- select marketplace_id
--       ,search_term
--       ,ds as search_date
--       ,cast(sum(if_chn_seller) as bigint) as chn_seller_nums
--       ,cast(count(1) as bigint) as all_seller_nums
--       ,cast(sum(if_chn_seller)/count(1) as decimal(18,6)) as chn_seller_rate
--       ,getdate() as create_time
-- from (
--       select marketplace_id
--             ,search_term
--             ,page
--             ,search_rank
--             ,asin
--             ,parent_asin
--             ,seller_id
--             ,seller_information
--             ,if_chn_seller
--             ,ds
--             ,rank()over(partition by marketplace_id,search_term order by ds desc) as rk
--       from asq_dw.dws_mkt_adv_strategy_chinese_seller_index_ds
--       where ds <= '20240220'
--      ) q1
-- where rk = 1
-- group by marketplace_id
--         ,search_term
--         ,ds
-- ;


--drop table if exists dws_mkt_adv_strategy_chinese_seller_rate_tmp02;
--create table dws_mkt_adv_strategy_chinese_seller_rate_tmp02 as
--select marketplace_id
--      ,search_term
--      ,search_date
--      ,chn_seller_nums
--      ,all_seller_nums
--      ,chn_seller_rate
--      ,ds
--from dws_mkt_adv_strategy_chinese_seller_rate_df
--where ds = to_char(dateadd(to_date('${bizdate}','yyyymmdd'),-1,'dd'),'yyyymmdd')
--
--union all
--
--select marketplace_id
--      ,search_term
--      ,ds as search_date
--      ,sum(if_chn_seller) as chn_seller_nums_1d
--      ,count(1) as all_seller_nums_1d
--      ,sum(if_chn_seller)/count(1) as chn_seller_rate_1d
--      ,ds
--from dws_mkt_adv_strategy_chinese_seller_index_ds
--where ds = '${bizdate}'
--group by marketplace_id
--        ,search_term
--        ,ds
--;
--
--
----支持重跑
--alter table dws_mkt_adv_strategy_chinese_seller_rate_df drop if exists partition (ds = '${bizdate}');
--
----插入最新数据
--insert into table dws_mkt_adv_strategy_chinese_seller_rate_df partition (ds = '${bizdate}')
--(
--       marketplace_id
--      ,search_term
--      ,search_date
--      ,chn_seller_nums
--      ,all_seller_nums
--      ,chn_seller_rate
--      ,create_time
--
--)
--select marketplace_id
--      ,search_term
--      ,search_date
--      ,chn_seller_nums
--      ,all_seller_nums
--      ,cast(chn_seller_rate as decimal(18,6)) as chn_seller_rate
--      ,getdate() as create_time
--from (
--      select marketplace_id
--            ,search_term
--            ,search_date
--            ,chn_seller_nums
--            ,all_seller_nums
--            ,chn_seller_rate
--            ,row_number()over(partition by marketplace_id,search_term order by ds desc) as rk
--      from dws_mkt_adv_strategy_chinese_seller_rate_tmp02
--     )
--where rk = 1
--;




CREATE TABLE IF NOT EXISTS dws_mkt_adv_strategy_keyword_index_ds
(
    marketplace_id        STRING COMMENT '站点ID'
    ,marketplace_name      STRING COMMENT '站点名称'
    ,keyword               STRING COMMENT '关键词'
    ,aba_rank              BIGINT COMMENT '搜索词ABA周榜排名'
    ,aba_date              DATETIME COMMENT 'ABA榜单日期'
    ,sale_monopoly_rate    DECIMAL(18,6) COMMENT 'TOP3商品转化份额'
    --  ,chn_seller_rate       DECIMAL(18,6) COMMENT '搜索词前三页中国卖家占比'
    ,create_time           DATETIME COMMENT '创建时间'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '关键词指标表')
    LIFECYCLE 30
;

--支持重跑
alter table dws_mkt_adv_strategy_keyword_index_ds drop if exists partition (ds = '${bizdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_keyword_index_ds partition (ds = '${bizdate}')
(
 marketplace_id
,keyword
,aba_rank
,aba_date
,sale_monopoly_rate
--,chn_seller_rate
,create_time
)
select marketplace_id
     ,q1.search_term
     ,cast(q1.aba_rank as bigint) as aba_rank
     ,q1.aba_date
     ,cast(q1.sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
     --,cast(q2.chn_seller_rate as decimal(18,6)) as chn_seller_rate
     ,getdate() as create_time
from (
         select  marketplace_id
              ,search_term
              ,search_frequency_rank as aba_rank
              ,rank_date as aba_date
              ,(top1_product_conversion_share + top2_product_conversion_share + top3_product_conversion_share)/100 as sale_monopoly_rate
         from whde.dwd_itm_spu_amazon_search_keyword_info_ws
         where ws = max_pt('whde.dwd_itm_spu_amazon_search_keyword_info_ws')
     )q1
--full join (
--           select marketplace_id
--                 ,search_term
--                 ,chn_seller_rate
--           from   whde.dws_mkt_adv_strategy_chinese_seller_rate_df
--           where  ds = '${bizdate}'
--          )q2
--on q1.marketplace_id = q2.marketplace_id and q1.search_term = q2.search_term
;




