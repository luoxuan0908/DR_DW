--@exclude_input=whde.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-27 23:39:41
--********************************************************************--
--drop table  WHDE.adm_amazon_adv_sku_wide_d;
CREATE TABLE IF NOT EXISTS WHDE.adm_amazon_adv_sku_wide_d(
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
     ,a.marketplace_id
     ,a.marketplace_name
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
     ,advertised_sku
     ,impressions
     ,clicks
     ,cost
     ,w7d_sale_amt sale_amt
     ,w7d_sale_same_sku_amt same_sku_sale_amt
     ,w7d_units_sold_clicks order_num
     ,w7d_units_sold_same_sku same_sku_order_num
FROM    (
            SELECT  a.tenant_id
                 ,a.profile_id
                 ,b.marketplace_id
                 ,b.marketplace_name
                 ,b.seller_id
                 ,b.seller_name
                 ,report_date
                 ,data_last_update_time
                 ,country_code
                 ,portfolio_id
                 ,campaign_id
                 ,campaign_name
                 ,campaign_status
                 ,campaign_budget_amt
                 ,campaign_budget_type
                 ,campaign_budget_currency_code
                 ,ad_group_id
                 ,ad_group_name
                 ,ad_id
                 ,advertised_asin
                 ,advertised_sku
                 ,impressions
                 ,clicks
                 ,cost
                 ,w7d_sale_amt
                 ,w7d_sale_same_sku_amt
                 ,w7d_units_sold_clicks
                 ,w7d_units_sold_same_sku
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,report_id
                             ,report_type
                             ,seller_id
                             ,report_date
                             ,data_last_update_time
                             ,country_code
                             ,portfolio_id
                             ,campaign_id
                             ,campaign_name
                             ,campaign_status
                             ,campaign_budget_amt
                             ,campaign_budget_type
                             ,campaign_budget_currency_code
                             ,ad_group_id
                             ,ad_group_name
                             ,ad_id
                             ,advertised_asin
                             ,advertised_sku
                             ,impressions
                             ,clicks
                             ,cost
                             ,w7d_sale_amt
                             ,w7d_sale_same_sku_amt
                             ,w7d_units_sold_clicks
                             ,w7d_units_sold_same_sku
                        FROM   whde.dwd_mkt_adv_amazon_sp_product_ds
                        WHERE   ds >= TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
                          AND     ds <= '${bizdate}' --只保存最近30天
                    ) a
                        LEFT JOIN   (SELECT  tenant_id
                                          ,profile_id
                                          ,marketplace_id
                                          ,marketplace_name
                                          ,timezone
                                          ,seller_id
                                          ,seller_name
                                     FROM    whde.dwd_sit_shp_amazon_seller_sites_store_df
                                     WHERE   ds = MAX_PT('whde.dwd_sit_shp_amazon_seller_sites_store_df')
            ) b --取站点、店铺
                                    ON      a.profile_id = b.profile_id
                                        AND     a.tenant_id = b.tenant_id
        ) a
            LEFT JOIN   (
    SELECT  tenant_id
         ,marketplace_id
         ,seller_id
         ,asin
         ,MAX(parent_asin) parent_asin
         ,MAX(selling_price) selling_price
         ,MAX(title) title
         ,MAX(link) link
         ,MAX(breadcrumbs_feature) breadcrumbs_feature
         ,MAX(main_image_url) main_image_url
         ,MIN(fba_first_instock_time) fba_first_instock_date
         ,SUM(afn_total_num) fba_stock_num
    FROM    whde.dws_itm_sku_amazon_asin_index_df
    WHERE   ds = MAX_PT('whde.dws_itm_sku_amazon_asin_index_df')
    GROUP BY tenant_id
           ,marketplace_id
           ,seller_id
           ,asin
) b --父aisn、以及类目、库存
                        ON      a.tenant_id = b.tenant_id
                            AND     a.marketplace_id = b.marketplace_id
                            AND     a.seller_id = b.seller_id
                            AND     a.advertised_asin = b.asin
            LEFT JOIN   (
    SELECT  tenant_id
         ,profile_id
         ,campaign_id
         ,campaign_name
         ,CASE   WHEN targeting_type = 'MANUAL' THEN '手动投放'
                 WHEN targeting_type = 'AUTO' THEN '自动投放'
        END ad_mode
         ,CASE   WHEN status = 'PAUSED' THEN '已暂停'
                 WHEN status = 'ARCHIVED' THEN '已归档'
                 WHEN status = 'ENABLED' THEN '已启动'
        END campaign_status
         ,CASE   WHEN GET_JSON_OBJECT(dynamic_bidding,'$.strategy') = 'AUTO_FOR_SALES' THEN '动态竞价-提高和降低'
                 WHEN GET_JSON_OBJECT(dynamic_bidding,'$.strategy') = 'LEGACY_FOR_SALES' THEN '动态竞价-只降低'
                 WHEN GET_JSON_OBJECT(dynamic_bidding,'$.strategy') = 'MANUAL' THEN '固定竞价'
        END campaign_biding_strategy
    FROM    whde.adm_amazon_adv_camp_status_df
    WHERE   ds = MAX_PT('whde.adm_amazon_adv_camp_status_df')
) e --取广告活动的最新状态
                        ON      a.profile_id = e.profile_id
                            AND     a.tenant_id = e.tenant_id
                            AND     a.campaign_id = e.campaign_id
            LEFT JOIN   (
    SELECT  tenant_id
         ,profile_id
         ,campaign_id
         ,ad_group_id
         ,ad_group_name
    FROM    whde.adm_amazon_adv_ad_group_status_df
    WHERE   ds = MAX_PT('whde.adm_amazon_adv_ad_group_status_df')
) e1 --取广告组的最新状态
                        ON      a.profile_id = e1.profile_id
                            AND     a.tenant_id = e1.tenant_id
                            AND     a.campaign_id = e1.campaign_id
                            AND     a.ad_group_id = e1.ad_group_id
            LEFT JOIN (select * from  (select *  ,market_place_id marketplace_id,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                       from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                       where rn =1 and parent_asin is not null
) g --补充子asin对应的父aisn
                      ON      a.marketplace_id = g.marketplace_id
                          AND     a.advertised_asin = g.asin
;

INSERT OVERWRITE TABLE adm_amazon_adv_sku_wide_d PARTITION (ds = '${bizdate}')
SELECT  a.tenant_id
     ,a.profile_id
     ,a.marketplace_id
     ,a.marketplace_name
     ,a.seller_id
     ,a.seller_name
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
     ,b.parent_asin AS top_cost_parent_asin
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
