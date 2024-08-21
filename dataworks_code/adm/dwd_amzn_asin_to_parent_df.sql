--@exclude_input=whde.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:40:22
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dwd_amzn_asin_to_parent_df
(   id string
    ,market_place_id        STRING COMMENT '站点ID'
    ,market_place_name  STRING COMMENT '站点名称'
    ,parent_asin           STRING
    ,asin                  STRING
    ,category_list  STRING
    ,category_name   STRING
    ,seller_id  STRING
    ,seller_name  STRING
    ,data_dt              DATETIME
    ,etl_data_dt          DATETIME

)
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '父子asin映射表')
    LIFECYCLE 7
;

INSERT OVERWRITE TABLE whde.dwd_amzn_asin_to_parent_df PARTITION (ds = '${bizdate}')
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
             ,GETDATE() etl_data_dt
        FROM    (
                    SELECT  *
                         ,ROW_NUMBER() OVER (PARTITION BY market_place_id,parent_asin,asin ORDER BY created_at DESC ) AS rn
                    FROM    whde.amazon_asin_master_slave
                    WHERE   pt = MAX_PT("whde.amazon_asin_master_slave")
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
                        FROM    amazon_product_details
                        WHERE   pt = '${bizdate}'
                          AND     breadcrumbs_feature IS NOT NULL
                    )
            WHERE   rn = 1
        ) b
                                    ON      a.market_place_id = b.market_place_id
                                        AND     a.parent_asin = b.parent_asin
                    left outer join whde.dim_marketplace_info_df d
                                    on a.market_place_id = d.market_place_id
                    left outer join (select distinct marketplace_id,asin1,seller_id from  get_merchant_listings_all_data where pt = '${bizdate}')  e
                                    on  a.asin = e.asin1
                                        and a.market_place_id = e.marketplace_id
        WHERE   a.rn = 1
    ) t1
        left outer join (select * from authorization_account where pt = '${bizdate}') t2
                        on t1.market_place_id = t2.marketplace_id
                            and t1.seller_id = t2.seller_id
;
