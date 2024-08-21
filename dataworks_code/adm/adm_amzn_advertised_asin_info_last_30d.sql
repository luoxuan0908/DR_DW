--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:19:33
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.adm_amzn_advertised_asin_info_last_30d(
                                                                          tenant_id STRING COMMENT '租户ID',
                                                                          profile_id STRING COMMENT '配置Id',
                                                                          marketplace_id STRING COMMENT '市场ID',
                                                                          marketplace_name STRING COMMENT '市场名称',
                                                                          seller_id STRING COMMENT '卖家ID',
                                                                          seller_name STRING COMMENT '卖家名称(亚马逊上的店铺名称)',
                                                                          report_date DATETIME COMMENT '报告日期',
                                                                          country_code STRING COMMENT '国家编码',
                                                                          portfolio_id STRING COMMENT 'portfolioID',
                                                                          campaign_id STRING COMMENT '广告活动ID',
                                                                          campaign_name STRING COMMENT '广告活动名称',
                                                                          campaign_status STRING COMMENT '广告活动状态',
                                                                          campaign_biding_strategy STRING COMMENT '广告活动竞价策略',
                                                                          ad_mode STRING COMMENT '广告投放模式',
                                                                          campaign_budget_amt DECIMAL(18,6) COMMENT '广告活动预算',
    campaign_budget_type STRING COMMENT '广告活动预算类型',
    campaign_budget_currency_code STRING COMMENT '广告活动预算币种',
    ad_group_id STRING COMMENT '广告组ID',
    ad_group_name STRING COMMENT '广告组名称',
    ad_id STRING COMMENT '广告ID',
    advertised_asin STRING COMMENT '广告asin',
    parent_asin STRING COMMENT '父asin',
    selling_price STRING COMMENT '售价',
    title STRING COMMENT '商品标题',
    link STRING COMMENT '商品链接',
    category_one STRING COMMENT '一级类目（面包屑导航拆分）',
    category_two STRING COMMENT '二级类目（面包屑导航拆分）',
    category_three STRING COMMENT '三级类目（面包屑导航拆分）',
    category_four STRING COMMENT '四级类目（面包屑导航拆分）',
    category_five STRING COMMENT '五级类目（面包屑导航拆分）',
    category_six STRING COMMENT '六级类目（面包屑导航拆分）',
    main_image_url STRING COMMENT '商品主图',
    fba_first_instock_date DATETIME COMMENT 'FBA首次入库时间',
    asin_fba_stock_num BIGINT COMMENT 'asin的fba库存',
    advertised_sku STRING COMMENT '广告sku',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '花费',
    sale_amt DECIMAL(18,6) COMMENT '广告点击后的7天内订购的总销售额',
    same_sku_sale_amt DECIMAL(18,6) COMMENT '广告点击后的7天内同sku订购的总销售额',
    order_num BIGINT COMMENT '广告点击后的7天内订购的总单位数',
    same_sku_order_num BIGINT COMMENT '广告点击后的7天内同sku订购的总单位数',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    top_cost_parent_asin STRING COMMENT '广告组下花费最高的父asin'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊广告数据全量宽表（只保留最近31\n天，小时更新日表）')
    LIFECYCLE 30;


DROP TABLE IF EXISTS adm_amazon_adv_sku_wide_ds_${bizdate}_tem1
;

CREATE TABLE adm_amazon_adv_sku_wide_ds_${bizdate}_tem1 AS
SELECT  a.tenant_id
     ,a.profile_id
     ,a.market_place_id marketplace_id
     ,a.market_place_name marketplace_name
     ,a.seller_id
     ,a.seller_name
     ,report_date
     ,country_code
     ,a.portfolio_id
     ,a.campaign_id
     ,e.campaign_name
     ,e.campaign_status
     ,e.campaign_biding_strategy
     ,e.ad_mode
     ,campaign_budget_amt
     ,campaign_budget_type
     ,campaign_budget_currency_code
     ,a.ad_group_id
     ,e1.ad_group_name
     ,ad_id
     ,advertised_asin
     ,COALESCE(b.parent_asin,g.parent_asin) parent_asin
     ,selling_price
     ,title
     ,link
     ,SPLIT_PART(breadcrumbs_feature,'>',1) category_one
     ,SPLIT_PART(breadcrumbs_feature,'>',2) category_two
     ,SPLIT_PART(breadcrumbs_feature,'>',3) category_three
     ,SPLIT_PART(breadcrumbs_feature,'>',4) category_four
     ,SPLIT_PART(breadcrumbs_feature,'>',5) category_five
     ,SPLIT_PART(breadcrumbs_feature,'>',6) category_six
     ,main_image_url
     ,fba_first_instock_date
     ,fba_stock_num
     ,impressions
     ,clicks
     ,cost
     ,sales_7d  sale_amt
     ,attributed_sales_same_sku_7d  same_sku_sale_amt
     ,units_sold_clicks_7d    order_num
     ,units_sold_same_sku_7d  same_sku_order_num

FROM                (SELECT *
                     FROM   whde.dwd_mkt_adv_amazon_sp_product_ds -- whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds
                     WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
                       AND     ds <= '${bizdate}' --只保存最近30天
                    ) a


                        left outer join (

    SELECT  tenant_id
         ,marketplace_id
         ,seller_id
         ,asin --子asin
         ,MIN(operation_date) AS fba_first_instock_date
         ,COUNT(DISTINCT operation_date) fba_instock_num
    FROM    whde.ods_get_ledger_detail_view_data
    WHERE   ds='${bizdate}'
      AND     event_type = 'Receipts' --是已接收库存
    GROUP BY tenant_id
           ,marketplace_id
           ,seller_id
           ,asin)b
                                        ON      a.tenant_id = b.tenant_id
                                            AND     a.market_place_id = b.marketplace_id
                                            AND     a.seller_id = b.seller_id
                                            AND     a.advertised_asin = b.asin
                        LEFT JOIN   (
    SELECT  tenant_id
         ,profile_id
         ,portfolio_id
         ,seller_id
         ,campaign_id
         ,campaign_name
         ,ad_mode
         ,campaign_status
         ,campaign_biding_strategy
    FROM    (
                SELECT  tenant_id
                     ,profile_id
                     ,portfolio_id
                     ,seller_id
                     ,campaign_id
                     ,campaign_name
                     ,ad_mode
                     ,campaign_status
                     ,campaign_biding_strategy
                     ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,seller_id,campaign_id ORDER BY latest_report_date DESC ) rn
                FROM    whde.dws_mkt_adv_amazon_camp_status_df
                WHERE   ds = MAX_PT('open_dw.dws_mkt_adv_amazon_camp_status_df')
            )
    WHERE   rn = 1
      AND     NVL(seller_id,'') <> ''
) e --取广告活动的最新状态
                                    ON      a.profile_id = e.profile_id
                                        AND     a.tenant_id = e.tenant_id
                                        AND     a.seller_id = e.seller_id
                                        AND     a.campaign_id = e.campaign_id
                        LEFT JOIN   (
    SELECT  tenant_id
         ,profile_id
         ,portfolio_id
         ,seller_id
         ,campaign_id
         ,ad_group_id
         ,ad_group_name
    FROM    whde.dws_mkt_adv_amazon_camp_status_df
    WHERE   ds = MAX_PT('open_dw.dws_mkt_adv_amazon_camp_status_df')
      AND     NVL(seller_id,'') <> ''
) e1 --取广告组的最新状态
                                    ON      a.profile_id = e1.profile_id
                                        AND     a.tenant_id = e1.tenant_id
                                        AND     a.seller_id = e1.seller_id
                                        AND     a.campaign_id = e1.campaign_id
                                        AND     a.ad_group_id = e1.ad_group_id
                        LEFT JOIN   (
    SELECT  market_place_id marketplace_id
         ,asin
         ,MAX(parent_asin) parent_asin
    FROM    whde.dwd_amzn_asin_to_parent_df
    WHERE   ds = MAX_PT('whde.dwd_amzn_asin_to_parent_df')
      AND     NVL(parent_asin,'') <> ''
    GROUP BY market_place_id
           ,asin
) g --补充子asin对应的父aisn
                                    ON      a.market_place_id = g.marketplace_id
                                        AND     a.advertised_asin = g.asin
                        LEFT JOIN   (
    SELECT  seller_id --卖家ID
         ,market_place_id --站点ID
         ,parent_asin --父ASIN
         ,link --商品链接
         ,brand --品牌名或店铺
         ,scribing_price --划线价
         ,selling_price --售价
         ,date_first_available --上架时间
         ,title --标题
         ,main_image_url --主图链接
         ,breadcrumbs_feature
         ,reviews_ratings
         ,reviews_stars
         ,sellers_rank
         ,sellers_rank_category  --面包屑导航
    FROM    whde.amazon_product_details
    WHERE   pt = '${bizdate}'
) h --父aisn、以及类目
                                    on     a.market_place_id = h.market_place_id
                                        AND     a.seller_id = h.seller_id
                                        AND     g.asin = h.parent_asin
;

INSERT OVERWRITE TABLE adm_amzn_advertised_asin_info_last_30d PARTITION (ds = '${bizdate}')
SELECT  a.tenant_id
     ,a.profile_id
     ,a.market_place_id
     ,a.marketplace_name
     ,a.seller_id
     ,a.seller_name
     ,a.erp_store_id
     ,a.erp_store_name
     ,a.report_date
     ,a.country_code
     ,a.portfolio_id
     ,a.campaign_id
     ,a.campaign_name
     ,a.campaign_status
     ,a.campaign_biding_strategy
     ,a.ad_mode
     ,a.campaign_budget_amt
     ,a.campaign_budget_type
     ,a.campaign_budget_currency_code
     ,a.ad_group_id
     ,a.ad_group_name
     ,a.ad_id
     ,a.advertised_asin
     ,a.parent_asin
     ,a.selling_price
     ,a.title
     ,a.link
     ,a.category_one
     ,a.category_two
     ,a.category_three
     ,a.category_four
     ,a.category_five
     ,a.category_six
     ,a.main_image_url
     ,a.fba_first_instock_date
     ,a.fba_stock_num
     ,a.advertised_sku
     ,a.impressions
     ,a.clicks
     ,a.cost
     ,a.sale_amt
     ,a.same_sku_sale_amt
     ,a.order_num
     ,a.same_sku_order_num
     ,'${bizdate}'
     ,GETDATE()
     ,b.parent_asin as top_cost_parent_asin
FROM    adm_amazon_adv_sku_wide_ds_${bizdate}_tem1 a
            LEFT JOIN   (
    SELECT  tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,parent_asin
    FROM    (
                SELECT  tenant_id
                     ,profile_id
                     ,campaign_id
                     ,ad_group_id
                     ,parent_asin
                     ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id ORDER BY sum_cost DESC ) rn
                FROM    (
                            SELECT  tenant_id
                                 ,profile_id
                                 ,campaign_id
                                 ,ad_group_id
                                 ,parent_asin
                                 ,SUM(cost) sum_cost
                            FROM    adm_amazon_adv_sku_wide_ds_${bizdate}_tem1
                            GROUP BY tenant_id
                                   ,profile_id
                                   ,campaign_id
                                   ,ad_group_id
                                   ,parent_asin
                        )
            )
    WHERE   rn = 1
) b
                        ON      a.tenant_id = b.tenant_id
                            AND     a.profile_id = b.profile_id
                            AND     a.campaign_id = b.campaign_id
                            AND     a.ad_group_id = b.ad_group_id
;
DROP TABLE IF EXISTS adm_amazon_adv_sku_wide_ds_${bizdate}_tem1
;