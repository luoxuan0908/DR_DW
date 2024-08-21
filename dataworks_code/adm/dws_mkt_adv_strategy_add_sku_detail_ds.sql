--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:09:29
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dws_mkt_adv_strategy_add_sku_detail_ds(
                                                                          tenant_id STRING COMMENT '租户ID',
                                                                          row_id STRING COMMENT '行级策略明细唯一ID',
                                                                          profile_id STRING COMMENT '配置ID',
                                                                          target_sku STRING COMMENT '推广的sku',
                                                                          asin STRING COMMENT 'aisn',
                                                                          title STRING COMMENT '商品标题',
                                                                          main_asin_url STRING COMMENT '商品链接',
                                                                          main_img_url STRING COMMENT '商品主图',
                                                                          sell_price DECIMAL(18,6) COMMENT '售价',
    currency_code STRING COMMENT '币种',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '花费',
    sale_amt DECIMAL(18,6) COMMENT '销售额',
    order_num BIGINT COMMENT '销量',
    ctr DECIMAL(18,6) COMMENT 'CTR',
    cvr DECIMAL(18,6) COMMENT 'CVR',
    cpc DECIMAL(18,6) COMMENT 'CPC',
    acos DECIMAL(18,6) COMMENT 'ACOS',
    create_time DATETIME COMMENT '创建时间',
    ratings_num BIGINT COMMENT '评论数量',
    ratings_stars DECIMAL(18,6) COMMENT '评分',
    target_asin STRING COMMENT '投放asin',
    target_asin_url STRING COMMENT '投放asin链接',
    target_asin_img_url STRING COMMENT '投放asin主图',
    target_asin_sell_price DECIMAL(18,6) COMMENT '投放品售价',
    target_asin_ratings_num BIGINT COMMENT '投放品评论数量',
    target_asin_ratings_stars DECIMAL(18,6) COMMENT '投放品评分'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='广告策略添加大小词、投放品,新建广告活动的sku信息')
    LIFECYCLE 366;


drop table if exists dws_mkt_adv_strategy_add_sku_detail_tmp01;
create table dws_mkt_adv_strategy_add_sku_detail_tmp01 as
select marketplace_id
     ,asin
     ,main_asin_url
     ,main_image_url
     ,sell_price
     ,title
     ,ratings_num
     ,ratings_stars
from (
         select   marketplace_id
              ,asin
              ,link as main_asin_url
              ,main_image_url as main_image_url
              ,CASE WHEN marketplace_id IN ('A1AM78C64UM0Y8','ATVPDKIKX0DER','A39IBJ37TRP1C6','A2EUQ1WTGCTBG2') then replace(replace(selling_price,',',''),'$','')
                    WHEN marketplace_id IN ('A1C3SOZRARQ6R3') then replace(replace(selling_price,',','.'),'zł','')
                    WHEN marketplace_id IN ('A1805IZSGTT6HS','A13V1IB3VIYZZH','APJ6JRA9NG5V4','A1PA6795UKMFR9','AMEN7PMS3EDWL','A1RKKUPIHCS9HS') then replace(replace(selling_price,',','.'),'€','')
                    WHEN marketplace_id IN ('A2Q3Y263D00KWC') AND INSTR(selling_price,'.')>0 then replace(replace(replace(selling_price,'.',''),',','.'),'R$','')
                    WHEN marketplace_id IN ('A2Q3Y263D00KWC') AND INSTR(selling_price,'.')=0 then replace(replace(selling_price,',','.'),'R$','')
                    WHEN marketplace_id IN ('A33AVAJ2PDY3EV') AND INSTR(selling_price,'.')>0 then replace(replace(replace(selling_price,'.',''),',','.'),'TL','')
                    WHEN marketplace_id IN ('A33AVAJ2PDY3EV') AND INSTR(selling_price,'.')=0  then replace(replace(selling_price,',','.'),'TL','')
                    WHEN marketplace_id IN ('A1F83G8C2ARO7P') then replace(replace(selling_price,',',''),'£','')
                    WHEN marketplace_id IN ('A2NODRKZP88ZB9') then replace(replace(selling_price,',',''),'kr','')
                    WHEN marketplace_id IN ('A21TJRUUN4KGV') then replace(replace(selling_price,',',''),'₹','')  end as sell_price
              ,title
              ,ratings_num
              ,ratings_stars
         from whde.dws_itm_sku_amazon_asin_index_df
         where ds = max_pt('whde.dws_itm_sku_amazon_asin_index_df')
     )q1
;


--支持重跑
alter table dws_mkt_adv_strategy_add_sku_detail_ds drop if exists partition (ds = '${nowdate}');

--插入最新数据
insert into table dws_mkt_adv_strategy_add_sku_detail_ds partition (ds = '${nowdate}')
(
         tenant_id
        ,row_id
        ,profile_id
        ,target_sku
        ,asin
        ,title
        ,main_asin_url
        ,main_img_url
        ,sell_price
        ,currency_code
        ,impressions
        ,clicks
        ,cost
        ,sale_amt
        ,order_num
        ,ctr
        ,cvr
        ,cpc
        ,acos
        ,create_time
        ,ratings_num
        ,ratings_stars
        ,target_asin
        ,target_asin_url
        ,target_asin_img_url
        ,target_asin_sell_price
        ,target_asin_ratings_num
        ,target_asin_ratings_stars
)
select  q1.tenant_id
     ,q1.row_id
     ,q1.profile_id
     ,q1.sku
     ,q2.asin
     ,q3.title
     ,q3.main_asin_url
     ,q3.main_image_url
     ,cast(q3.sell_price as decimal(18,6)) as  sell_price
     ,q1.currency_code
     ,nvl(q2.impressions,0) as impressions
     ,nvl(q2.clicks,0) as clicks
     ,nvl(cast(q2.cost as decimal(18,6)),0) as cost
     ,nvl(cast(q2.sale_amt as decimal(18,6)),0) as sale_amt
     ,nvl(q2.order_num,0) as order_num
     ,cast(case when q2.impressions <> 0 then q2.clicks / q2.impressions else null end as decimal(18,6)) ctr
     ,cast(case when q2.clicks <> 0 then q2.order_num / q2.clicks else null end as decimal(18,6)) cvr
     ,cast(case when q2.clicks <> 0 then q2.cost / q2.clicks else null end as decimal(18,6)) cpc
     ,cast(case when q2.sale_amt <> 0 then q2.cost / q2.sale_amt else null end as decimal(18,6)) acos
     ,getdate() as create_time
     ,cast(q3.ratings_num as bigint) as ratings_num
     ,cast(q3.ratings_stars as decimal(18,6)) as ratings_stars
     ,q4.asin as target_asin
     ,q4.main_asin_url as target_asin_url
     ,q4.main_image_url as target_asin_img_url
     ,cast(q4.sell_price as decimal(18,6)) as target_asin_sell_price
     ,cast(q4.ratings_num as bigint) as target_asin_ratings_num
     ,cast(q4.ratings_stars as decimal(18,6)) as target_asin_ratings_stars
from(
        select tenant_id
             ,row_id
             ,profile_id
             ,marketplace_id
             ,search_term
             ,sku
             ,currency_code
        from whde.dws_mkt_adv_strategy_add_word_product_detail_ds    --添加大小词，新建广告组的sku
                 lateral view explode(split(target_sku_list,'_/_')) adtable as sku
        where ds='${nowdate}'
        group by tenant_id
                ,row_id
                ,profile_id
                ,marketplace_id
                ,search_term
                ,sku
                ,currency_code

        union all

        select tenant_id
                ,row_id
                ,profile_id
                ,marketplace_id
                ,search_term
                ,sku
                ,currency_code
        from whde.dws_mkt_adv_strategy_word_upgrade_detail_ds --好词晋升，新建广告组的sku
            lateral view explode(split(target_sku_list,'_/_')) adtable as sku
        where ds='${nowdate}'
        group by tenant_id
                ,row_id
                ,profile_id
                ,marketplace_id
                ,search_term
                ,sku
                ,currency_code

        --  union all

        --  select tenant_id
        --        ,row_id
        --        ,profile_id
        --        ,marketplace_id
        --        ,search_term
        --        ,sku
        --        ,currency_code
        --  from whde.dws_mkt_adv_strategy_search_term_adjust_bid_detail_ds --搜索词守坑，新建广告组的sku
        --  lateral view explode(split(seller_sku_list,'_/_')) adtable as sku
        --  where ds='${nowdate}'
        --  group by tenant_id
        --          ,row_id
        --          ,profile_id
        --          ,marketplace_id
        --          ,search_term
        --          ,sku
        --          ,currency_code
    )q1
        left join (
    select tenant_id
         ,profile_id
         ,marketplace_id
         ,advertised_sku as seller_sku
         ,max(advertised_asin) as asin
         ,sum(sale_amt) as sale_amt
         ,sum(order_num) as order_num
         ,sum(cost) as cost
         ,sum(clicks) as clicks
         ,sum(impressions) as impressions
    from WHDE.adm_amazon_adv_sku_wide_d
    where ds = '${bizdate}'
    group by profile_id
           ,tenant_id
           ,marketplace_id
           ,advertised_sku
)q2
                  on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id and q1.sku = q2.seller_sku
        left join dws_mkt_adv_strategy_add_sku_detail_tmp01 q3  --商品信息
                  on q2.marketplace_id = q3.marketplace_id and q2.asin = q3.asin
        left join dws_mkt_adv_strategy_add_sku_detail_tmp01 q4  --商品信息
                  on q1.marketplace_id = q4.marketplace_id and toupper(q1.search_term) = q4.asin
;
