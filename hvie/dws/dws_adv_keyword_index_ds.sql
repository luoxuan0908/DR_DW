
CREATE TABLE IF NOT EXISTS amz.dws_adv_keyword_index_ds
(
    marketplace_id        STRING COMMENT '站点ID'
    ,marketplace_name      STRING COMMENT '站点名称'
    ,keyword               STRING COMMENT '关键词'
    ,aba_rank              BIGINT COMMENT '搜索词ABA周榜排名'
    ,aba_date              timestamp COMMENT 'ABA榜单日期'
    ,sale_monopoly_rate    DECIMAL(18,6) COMMENT 'TOP3商品转化份额'
    --  ,chn_seller_rate       DECIMAL(18,6) COMMENT '搜索词前三页中国卖家占比'
    ,create_time           timestamp COMMENT '创建时间'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '关键词指标表')

;

--支持重跑
alter table amz.dws_adv_keyword_index_ds drop if exists partition (ds = '${bizdate}');

--插入最新数据
insert into table amz.dws_adv_keyword_index_ds partition (ds = '${bizdate}')
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
     ,current_date() as create_time
from (
         select  marketplace_id
              ,search_term
              ,search_frequency_rank as aba_rank
              ,rank_date as aba_date
              ,(top1_product_conversion_share + top2_product_conversion_share + top3_product_conversion_share)/100 as sale_monopoly_rate
         from amz.mid_itm_spu_amazon_search_keyword_info_ws
         where ws = '20240827'-- max_pt('whde.dwd_itm_spu_amazon_search_keyword_info_ws')
     )q1
;




