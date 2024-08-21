drop table if EXISTS dwd.dwd_prd_parent_asin_index_df;

CREATE TABLE IF NOT EXISTS dwd.dwd_prd_parent_asin_index_df (
                                                                id STRING COMMENT '主键',
                                                                top_parent_asin STRING COMMENT '父asin',
                                                                tenant_id STRING COMMENT '租户id',
                                                                profile_id STRING COMMENT '配置id',
                                                                marketplace_id STRING COMMENT '站点id',
                                                                marketplace_name STRING COMMENT '站点名称',
                                                                seller_id STRING COMMENT '卖家id',
                                                                seller_name STRING COMMENT '卖家名称',
                                                                category_one STRING COMMENT '一级类目',
                                                                category_two STRING COMMENT '二级类目',
                                                                category_three STRING COMMENT '三级类目',
                                                                category_four STRING COMMENT '四级类目',
                                                                category_five STRING COMMENT '五级类目',
                                                                category_six STRING COMMENT '六级类目',
                                                                main_img_url STRING COMMENT '主图url',
                                                                sale_label STRING COMMENT '销售标签',
                                                                reorder_label STRING COMMENT '返单情况',
                                                                first_available_days STRING COMMENT '上架天数',
                                                                fba_first_instock_days STRING COMMENT 'FBA首次入库天数',
                                                                fba_instock_cnt STRING COMMENT 'FBA入库次数',
                                                                fba_stock_sale_days STRING COMMENT 'FBA预估可售天数',
                                                                fba_total_num STRING COMMENT 'FBA总库存',
                                                                fba_inbound_num STRING COMMENT '在途库存',
                                                                fba_warehouse_num STRING COMMENT '在库库存',
                                                                top1_asin_fba_stock_sale_days STRING COMMENT '第一热卖ASIN-FBA可售天数',
                                                                top2_asin_fba_stock_sale_days STRING COMMENT '第二热卖ASIN-FBA可售天数',
                                                                top3_asin_fba_stock_sale_days STRING COMMENT '第三热卖ASIN-FBA可售天数',
                                                                top1_asin_fba_total_num STRING COMMENT '第一热卖ASIN-FBA总库存',
                                                                top2_asin_fba_total_num STRING COMMENT '第二热卖ASIN-FBA总库存',
                                                                top3_asin_fba_total_num STRING COMMENT '第三热卖ASIN-FBA总库存',
                                                                top1_asin_fba_inbound_num STRING COMMENT '第一热卖ASIN-在途库存',
                                                                top2_asin_fba_inbound_num STRING COMMENT '第二热卖ASIN-在途库存',
                                                                top3_asin_fba_inbound_num STRING COMMENT '第三热卖ASIN-在途库存',
                                                                top1_asin_fba_warehouse_num STRING COMMENT '第一热卖ASIN-在库库存',
                                                                top2_asin_fba_warehouse_num STRING COMMENT '第二热卖ASIN-在库库存',
                                                                top3_asin_fba_warehouse_num STRING COMMENT '第三热卖ASIN-在库库存',
                                                                clicks STRING COMMENT '点击数',
                                                                order_num STRING COMMENT '广告销售量',
                                                                impressions STRING COMMENT '曝光量',
                                                                cost STRING COMMENT '广告花费',
                                                                sale_amt STRING COMMENT '广告销售额',
                                                                ctr STRING COMMENT 'CTR',
                                                                cvr STRING COMMENT 'CVR',
                                                                cpc STRING COMMENT 'CPC',
                                                                acos STRING COMMENT 'ACOS',
                                                                cpa STRING COMMENT 'CPA',
                                                                ds STRING COMMENT '数据日期（日）'
)
    COMMENT '父asin标签表'
    PARTITIONED BY (ds STRING)
    STORED AS ORC;


INSERT overwrite TABLE dwd.dwd_prd_parent_asin_index_df PARTITION (ds='')
select
    null as id,
    null as top_parent_asin,
    null as tenant_id,
    null as profile_id,
    null as marketplace_id,
    null as marketplace_name,
    null as seller_id,
    null as seller_name,
    null as category_one,
    null as category_two,
    null as category_three,
    null as category_four,
    null as category_five,
    null as category_six,
    null as main_img_url,
    null as sale_label,
    null as reorder_label,
    null as first_available_days,
    null as fba_first_instock_days,
    null as fba_instock_cnt,
    null as fba_stock_sale_days,
    null as fba_total_num,
    null as fba_inbound_num,
    null as fba_warehouse_num,
    null as top1_asin_fba_stock_sale_days,
    null as top2_asin_fba_stock_sale_days,
    null as top3_asin_fba_stock_sale_days,
    null as top1_asin_fba_total_num,
    null as top2_asin_fba_total_num,
    null as top3_asin_fba_total_num,
    null as top1_asin_fba_inbound_num,
    null as top2_asin_fba_inbound_num,
    null as top3_asin_fba_inbound_num,
    null as top1_asin_fba_warehouse_num,
    null as top2_asin_fba_warehouse_num,
    null as top3_asin_fba_warehouse_num,
    null as clicks,
    null as order_num,
    null as impressions,
    null as cost,
    null as sale_amt,
    null as ctr,
    null as cvr,
    null as cpc,
    null as acos,
    null as cpa
from () t1
;


select DISTINCT
    t1.market_place_id
              ,t1.market_place_name
              ,t1.parent_asin
              ,t1.seller_id
              ,COALESCE(t2.seller_name,t1.sold_by) as seller_name
              ,t2.tenant_id
              ,t2.profile_id
              ,t1.breadcrumbs_feature
              ,SPLIT(breadcrumbs_feature, '>')[0] AS category_one
              ,SPLIT(breadcrumbs_feature, '>')[1] AS category_two
              ,SPLIT(breadcrumbs_feature, '>')[2] AS category_three
              ,SPLIT(breadcrumbs_feature, '>')[3] AS category_four
              ,SPLIT(breadcrumbs_feature, '>')[4] AS category_five
              ,SPLIT(breadcrumbs_feature, '>')[5] AS category_six
              ,main_image_url

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
             ,b.main_image_url
        FROM    (
                    SELECT  *
                         ,ROW_NUMBER() OVER (PARTITION BY market_place_id,parent_asin,asin ORDER BY created_at DESC ) AS rn
                    FROM    ods.ods_crawler_amazon_asin_master_slave_df
                    WHERE   ds ='20240809'
                      AND     market_place_id <> ''
                ) a
                    LEFT OUTER JOIN (
            SELECT  *
            FROM    (
                        SELECT  parent_asin
                             ,breadcrumbs_feature
                             ,seller_id
                             ,sold_by
                             ,market_place_id,main_image_url
                             ,ROW_NUMBER() OVER (PARTITION BY market_place_id,parent_asin ORDER BY data_date DESC ) AS rn
                        FROM    ods.ods_crawler_amazon_product_details_df
                        WHERE   ds ='20240809'
                          AND     breadcrumbs_feature IS NOT NULL
                    ) t
            WHERE   rn = 1
        ) b
                                    ON      a.market_place_id = b.market_place_id
                                        AND     a.parent_asin = b.parent_asin
                    left outer join (select * from  dim.dim_base_marketplace_info_df where  ds ='20240809' ) d
                                    on a.market_place_id = d.market_place_id
                    left outer join (select distinct marketplace_id,asin1,seller_id from  ods.ods_report_get_merchant_listings_all_data_df where ds ='20240809')  e
                                    on  a.asin = e.asin1
                                        and a.market_place_id = e.marketplace_id
        WHERE   a.rn = 1
    ) t1
        left outer join (select * from ods.ods_report_authorization_account_df where ds ='20240806') t2
                        on t1.market_place_id = t2.marketplace_id
                            and t1.seller_id = t2.seller_id
;
