
CREATE TABLE IF NOT EXISTS amz.mid_amzn_asin_to_parent_df
(   id string
    ,market_place_id        STRING COMMENT '站点ID'
    ,market_place_name  STRING COMMENT '站点名称'
    ,parent_asin           STRING
    ,asin                  STRING
    ,category_list  STRING
    ,category_name   STRING
    ,seller_id  STRING
    ,seller_name  STRING
    ,data_dt              date
    ,etl_data_dt          date

)
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '父子asin映射表')
;




INSERT OVERWRITE TABLE amz.mid_amzn_asin_to_parent_df PARTITION (ds = '${last_day}')
select  t1.id
     ,t1.market_place_id
     ,t1.market_place_name
     ,t1.parent_asin
     ,t1.asin
     ,t1.breadcrumbs_feature_id
     ,t1.breadcrumbs_feature
     ,t1.seller_id
     ,COALESCE(t2.seller_name,t1.sold_by)
     ,created_at
     ,etl_data_dt
from(
        SELECT   ROW_NUMBER() OVER(PARTITION BY  1 ORDER BY a.market_place_id,a.parent_asin,a.asin,created_at) AS id
        ,a.market_place_id
             ,concat(d.country_cn_name,'站') market_place_name
             ,a.parent_asin
             ,a.asin
             ,abs(hash(replace(b.breadcrumbs_feature,'            >                 ','>'))) breadcrumbs_feature_id
             ,case when b.breadcrumbs_feature is null or b.breadcrumbs_feature = '' then '未知' else  replace(b.breadcrumbs_feature,'            >                 ','>') end breadcrumbs_feature
             ,COALESCE(e.seller_id,b.seller_id) seller_id
             ,b.sold_by
             ,created_at
             ,current_date() etl_data_dt
        FROM    (
                    SELECT  *
                         ,ROW_NUMBER() OVER (PARTITION BY market_place_id,parent_asin,asin ORDER BY created_at DESC ) AS rn
                    FROM    ods.ods_crawler_amazon_asin_master_slave_df
                    WHERE   ds = '${last_day}'
                      AND     market_place_id <> ''
                ) a
                    LEFT OUTER JOIN (
            SELECT  *
            FROM    (
                        SELECT  parent_asin
                             ,breadcrumbs_feature
                             ,seller_id
                             ,sold_by
                             ,market_place_id
                             ,ROW_NUMBER() OVER (PARTITION BY market_place_id,parent_asin ORDER BY data_date DESC ) AS rn
                        FROM    ods.ods_crawler_amazon_product_details_df
                        WHERE   ds = '${last_day}'
                          AND     breadcrumbs_feature IS NOT NULL
                    ) t
            WHERE   rn = 1
        ) b
                                    ON      a.market_place_id = b.market_place_id
                                        AND     a.parent_asin = b.parent_asin
                    left outer join ( select * from dim.dim_base_marketplace_info_df where ds ='${last_day}') d
                                    on a.market_place_id = d.market_place_id
                    left outer join (select distinct marketplace_id,asin1,seller_id from  ods.ods_report_get_merchant_listings_all_data_df where ds = '${last_day}')  e
                                    on  a.asin = e.asin1
                                        and a.market_place_id = e.marketplace_id
        WHERE   a.rn = 1
    ) t1
        left outer join (select * from ods.ods_report_authorization_account_df where ds = '${last_day}') t2
                        on t1.market_place_id = t2.marketplace_id
                            and t1.seller_id = t2.seller_id
;
