drop table if exists amz.dwd_adv_keyword_index_ds;
CREATE TABLE IF NOT EXISTS amz.dwd_adv_keyword_index_ds
(
    marketplace_id        STRING COMMENT '站点ID'
    ,marketplace_name      STRING COMMENT '站点名称'
    ,keyword               STRING COMMENT '关键词'
    ,aba_rank              BIGINT COMMENT '搜索词ABA周榜排名'
    ,aba_date              date COMMENT 'ABA榜单日期'
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
--
-- --支持重跑
-- alter table dws_mkt_adv_strategy_keyword_index_ds drop if exists partition (ds = '${bizdate}')

--插入最新数据
insert overwrite table amz.dwd_adv_keyword_index_ds partition (ds = '20240822')
select marketplace_id
       ,null as marketplace_name
     ,t1.search_term
     ,cast(t1.aba_rank as bigint) as aba_rank
     ,t1.aba_date
     ,cast(t1.sale_monopoly_rate as decimal(18,6)) as sale_monopoly_rate
     --,cast(q2.chn_seller_rate as decimal(18,6)) as chn_seller_rate
     ,current_timestamp() as create_time
from (
         select  marketplace_id
              ,search_term
              ,search_frequency_rank as aba_rank
              ,rank_date as aba_date
              ,(top1_product_conversion_share + top2_product_conversion_share + top3_product_conversion_share)/100 as sale_monopoly_rate
         from amz.mid_itm_spu_amazon_search_keyword_info_ws
         where ws = '20240822'
     )t1
    left join (
        select * from dim.dim_base_marketplace_info_df where ds = '20240822'
) t2 on t1.marketplace_id = t2.market_place_id
;




