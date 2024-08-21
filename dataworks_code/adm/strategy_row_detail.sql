--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-05 21:05:21
--********************************************************************--

CREATE TABLE IF NOT EXISTS adm_strategy_row_detail_df
(
    row_id  string comment '主键'
    ,weeks  string comment '租户ID'
    ,clicks  bigint comment '点击量'
    ,cost  decimal(18,6) comment '广告花费'
    ,sale_amt  decimal(18,6)  comment '销售额'
    ,order_quantity  bigint comment '销售量'
    ,aba_rank  bigint comment 'aba排名'
    ,data_date string comment '数据时间'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '否词否品跳转表(新）')
    LIFECYCLE 60
;

insert OVERWRITE table adm_strategy_row_detail_df PARTITION (ds ='${nowdate}'  )

select distinct a.row_id
              ,b.ds
              ,b.clicks
              ,b.cost
              ,b.w7d_sale_amt
              ,b.w7d_units_sold_clicks
              ,9999999
              ,'${nowdate}'
from  whde.dws_mkt_adv_strategy_neg_word_product_detail_ds a
          left outer join whde.dwd_mkt_adv_amazon_sp_search_term_ds b
                          on a.tenant_id = b.tenant_id
                              and a.profile_id = b.profile_id
                              and a.campaign_id =b.campaign_id
                              and a.search_term = b.search_term
where a.ds = '${nowdate}'
  and b.ds >= TO_CHAR(DATEADD(TO_DATE('${nowdate}','yyyymmdd'),-90,'dd'),'yyyymmdd')
union all
SELECT  cast(abs(HASH(a.tenant_id,a.profile_id,a.stem,'否词根',a.top_parent_asin)) as string )
     ,cast(b.ds as string )
     ,cast(SUM(b.clicks) as bigint )
     ,cast(SUM(b.cost) as decimal(18,6) )
     ,cast(SUM(b.sale_amt) as decimal(18,6) )
     ,cast(SUM(b.order_num) as bigint )
     ,9999999
     ,'${nowdate}'
FROM    whde.dws_mkt_adv_strategy_neg_adv_word_ds a
            LEFT OUTER JOIN (
    SELECT  tenant_id
         ,profile_id
         ,campaign_id
         ,ds
         ,TRIM(word) AS stem --udf_word2stem(word)
         ,SUM(impressions) AS impressions
         ,SUM(clicks) AS clicks
         ,SUM(cost) AS cost
         ,SUM(w7d_sale_amt) AS sale_amt
         ,SUM(w7d_units_sold_clicks) AS order_num
    FROM    whde.dwd_mkt_adv_amazon_sp_search_term_ds
                LATERAL VIEW EXPLODE(SPLIT(search_term,' ')) adtable AS word
    WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${nowdate}','yyyymmdd'),-90,'dd'),'yyyymmdd')
    GROUP BY tenant_id
            ,profile_id
            ,campaign_id
            ,ds
            ,stem
) b
                            ON      a.tenant_id = b.tenant_id
                                AND     a.profile_id = b.profile_id
                                AND     a.campaign_id = b.campaign_id
                                AND     a.stem = b.stem
WHERE   a.ds = '${nowdate}'
GROUP BY abs(HASH(a.tenant_id,a.profile_id,a.stem,'否词根',a.top_parent_asin))
       ,b.ds
;