--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:20:39
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.adm_amazon_adv_strategy_campaign_d(
    tenant_id STRING COMMENT '租户ID',
    profile_id STRING COMMENT '配置Id',
    marketplace_id STRING COMMENT '市场ID',
    marketplace_name STRING COMMENT '市场名称',
    currency_code STRING COMMENT '币种',
    seller_id STRING COMMENT '卖家ID',
    seller_name STRING COMMENT '卖家名称(亚马逊上的店铺名称)',
    ad_type STRING COMMENT '广告类型',
    campaign_id STRING COMMENT '广告活动ID',
    campaign_name STRING COMMENT '广告活动名称',
    ad_mode STRING COMMENT '广告投放模式',
    campaign_status STRING COMMENT '广告活动状态',
    campaign_biding_strategy STRING COMMENT '广告活动竞价策略',
    campaign_budget_amt DECIMAL(18,6) COMMENT '广告活动预算',
    campaign_budget_type STRING COMMENT '广告活动预算类型',
    ad_group_id_list STRING COMMENT '广告组id列表',
    ad_group_num BIGINT COMMENT '广告组数量',
    parent_asin STRING COMMENT '父aisn',
    selling_price STRING COMMENT '售价',
    main_asin_url STRING COMMENT '商品链接',
    main_image_url STRING COMMENT '商品主图',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '花费',
    sale_amt DECIMAL(18,6) COMMENT '销售额',
    sale_num BIGINT COMMENT '销量',
    ctr DECIMAL(18,6) COMMENT 'CTR',
    cvr DECIMAL(18,6) COMMENT 'CVR',
    cpc DECIMAL(18,6) COMMENT 'CPC',
    acos DECIMAL(18,6) COMMENT 'ACOS',
    adv_days BIGINT COMMENT '广告天数',
    fba_first_instock_date DATETIME COMMENT 'FBA首次入库时间',
    fba_total_num BIGINT COMMENT 'fba总库存',
    fba_warehouse_num BIGINT COMMENT 'FBA在库数量',
    fba_instock_cnt BIGINT COMMENT 'FBA入库次数',
    n15d_avg_sale_num DECIMAL(18,6) COMMENT '近15天日均销量',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊广告策略广告活动粒度数据(统计最近30天)')
    LIFECYCLE 30;


INSERT OVERWRITE TABLE adm_amazon_adv_strategy_campaign_d PARTITION (ds = '${bizdate}')
--最近30天的广告活动的效果
SELECT  a.tenant_id
     ,a.profile_id
     ,a.marketplace_id
     ,a.marketplace_name
     ,a.currency_code
     ,a.seller_id
     ,a.seller_name
     ,'商品推广' ad_type
     ,a.campaign_id
     ,campaign_name
     ,ad_mode
     ,campaign_status
     ,campaign_biding_strategy
     ,campaign_budget_amt
     ,campaign_budget_type
     ,ad_group_id_list
     ,ad_group_num
     ,a.parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,impressions
     ,clicks
     ,CAST(cost AS DECIMAL(18,6)) cost
     ,CAST(sale_amt AS DECIMAL(18,6)) sale_amt
     ,order_num
     ,CAST(CASE   WHEN impressions <> 0 THEN clicks / impressions
    END AS DECIMAL(18,6)) ctr
     ,CAST(CASE   WHEN clicks <> 0 THEN order_num / clicks
    END AS DECIMAL(18,6)) cvr
     ,CAST(CASE   WHEN clicks <> 0 THEN cost / clicks
    END AS DECIMAL(18,6)) cpc
     ,CAST(CASE   WHEN sale_amt <> 0 THEN cost / sale_amt
    END AS DECIMAL(18,6)) acos
     ,DATEDIFF(GETDATE(),min_report_date,'dd')+1 adv_days
     ,fba_first_instock_date
     ,fba_stock_num
     ,fba_warehouse_num
     ,fba_instock_cnt
     ,n15d_avg_sale_num
     ,'${bizdate}' data_dt
     ,GETDATE() etl_data_dt

FROM    (
            SELECT  s1.tenant_id
                 ,s1.profile_id
                 ,s3.marketplace_id
                 ,s3.marketplace_name
                 ,s3.seller_id
                 ,s3.seller_name
                 ,ad_mode
                 ,parent_asin
                 ,s1.campaign_id
                 ,s3.campaign_name
                 ,campaign_budget_currency_code currency_code
                 ,COUNT(DISTINCT s1.ad_group_id) ad_group_num
                 ,WM_CONCAT(DISTINCT '_&_',CONCAT(s1.campaign_id,'_/_',s1.ad_group_id)) ad_group_id_list
                 ,SUM(impressions) impressions
                 ,SUM(clicks) clicks
                 ,SUM(cost) cost
                 ,SUM(w7d_sale_amt) sale_amt
                 ,SUM(w7d_units_sold_clicks) order_num
                 ,MAX(report_date) max_report_date
                 ,MIN(report_date) min_report_date
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,seller_id
                             ,campaign_id
                             ,campaign_budget_currency_code
                             ,ad_group_id
                             ,impressions
                             ,clicks
                             ,cost
                             ,w7d_sale_amt
                             ,w7d_units_sold_clicks
                             ,report_date
                        FROM    whde.dwd_mkt_adv_amazon_sp_product_ds
                        WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
                          AND campaign_status='ENABLED'
                    ) s1
                        LEFT JOIN   (
                SELECT  tenant_id
                     ,profile_id
                     ,marketplace_id
                     ,seller_id
                     ,campaign_id
                     ,ad_group_id
                     ,top_cost_parent_asin parent_asin
                     ,MAX(marketplace_name) marketplace_name
                     ,MAX(seller_name) seller_name
                     ,MAX(campaign_name) campaign_name
                     ,MAX(ad_mode) ad_mode
                FROM    whde.adm_amazon_adv_sku_wide_d
                WHERE   ds = '${bizdate}'
                GROUP BY tenant_id
                       ,profile_id
                       ,marketplace_id
                       ,seller_id
                       ,campaign_id
                       ,ad_group_id
                       ,top_cost_parent_asin
            ) s3
                                    ON      s1.tenant_id = s3.tenant_id
                                        AND     s1.profile_id = s3.profile_id
                                        AND     s1.seller_id = s3.seller_id
                                        AND     s1.campaign_id = s3.campaign_id
                                        AND     s1.ad_group_id = s3.ad_group_id
            GROUP BY s1.tenant_id
                   ,s1.profile_id
                   ,s3.marketplace_id
                   ,s3.marketplace_name
                   ,s3.seller_id
                   ,s3.seller_name
                   ,ad_mode
                   ,parent_asin
                   ,s1.campaign_id
                   ,s3.campaign_name
                   ,campaign_budget_currency_code
        ) a
            LEFT JOIN   (
    SELECT  tenant_id
         ,marketplace_id
         ,seller_id
         ,parent_asin
         ,main_image_url
         ,selling_price
         ,breadcrumbs_feature category_list
         ,afn_total_num fba_stock_num
         ,fba_first_instock_time fba_first_instock_date
         ,afn_warehouse_num fba_warehouse_num
         ,afnstock_n15d_avg_sale_num n15d_avg_sale_num
         ,fba_instock_cnt
    FROM    whde.dws_itm_spu_amazon_parent_asin_index_df
    WHERE   ds = MAX_PT('whde.dws_itm_spu_amazon_parent_asin_index_df')
) b --父aisn、以及类目、库存
                        ON      a.tenant_id = b.tenant_id
                            AND     a.marketplace_id = b.marketplace_id
                            AND     a.seller_id = b.seller_id
                            AND     a.parent_asin = b.parent_asin
            LEFT JOIN   (
    SELECT           tenant_id
         ,profile_id
         ,seller_id
         ,campaign_id
         ,parent_asin
         ,campaign_biding_strategy
         ,campaign_budget_amt
         ,campaign_budget_type
         ,campaign_status
    FROM    (
                SELECT  tenant_id
                     ,profile_id
                     ,seller_id
                     ,campaign_id
                     ,parent_asin
                     ,report_date
                     ,campaign_biding_strategy
                     ,campaign_budget_amt
                     ,campaign_budget_type
                     ,campaign_status
                     ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,seller_id,campaign_id ORDER BY report_date DESC ) rn
                FROM    whde.adm_amazon_adv_sku_wide_d
                WHERE   ds = '${bizdate}'
            )
    WHERE   rn = 1
) c  --取最新的预算
                        ON a.tenant_id=c.tenant_id
                            AND a.profile_id=c.profile_id
                            AND a.seller_id=c.seller_id
                            AND a.campaign_id=c.campaign_id
            LEFT JOIN   (
    SELECT  marketplace_id
         ,parent_asin
         ,MAX(link) main_asin_url
    FROM    whde.dws_itm_spu_amazon_parent_asin_index_df
    WHERE   ds = MAX_PT('whde.dws_itm_spu_amazon_parent_asin_index_df')
      AND     NVL(parent_asin,'') <> ''
    GROUP BY marketplace_id
           ,parent_asin
) f
                        ON      a.marketplace_id = f.marketplace_id
                            AND     a.parent_asin = f.parent_asin
WHERE c.campaign_status='已启动'
;

DROP TABLE IF EXISTS adm_amazon_adv_strategy_campaign_d_tem1
;

DROP TABLE IF EXISTS adm_amazon_adv_strategy_campaign_d_tem2
;