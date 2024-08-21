
drop table  if exists dwd.dwd_prd_asin_info_df;
CREATE TABLE if not exists dwd.dwd_prd_asin_info_df (
     id STRING COMMENT '由tenant_id、profile_id和asin生成的MD5哈希值',
     seller_asin STRING COMMENT '卖家ASIN',
     tenant_id STRING COMMENT '租户ID',
     profile_id STRING COMMENT 'Profile ID',
     marketplace_id STRING COMMENT '市场ID',
     marketplace_name STRING COMMENT '包含国家中文名称的市场名称',
     seller_id STRING COMMENT '卖家ID',
     seller_name STRING COMMENT '卖家名称',
     fba_total_num INT COMMENT 'FBA总库存量',
     fba_inbound_num INT COMMENT 'FBA入库总数量',
     fba_warehouse_num INT COMMENT 'FBA仓库库存量',
     fba_first_instock_days INT COMMENT '首次入库日期至今的天数',
     fba_instock_cnt INT COMMENT 'FBA入库次数',
     afn_fulfillable_quantity DOUBLE COMMENT 'AFN可配送库存量',
     color STRING COMMENT '商品颜色',
     size STRING COMMENT '商品尺寸',
     order_num_rank INT COMMENT '订单数量排名',
     order_num_rate DOUBLE COMMENT '订单数量占比',
     breadcrumbs_category_one STRING COMMENT '面包屑类别一',
     breadcrumbs_category_two STRING COMMENT '面包屑类别二',
     breadcrumbs_category_three STRING COMMENT '面包屑类别三',
     breadcrumbs_category_four STRING COMMENT '面包屑类别四',
     breadcrumbs_category_five STRING COMMENT '面包屑类别五',
     breadcrumbs_category_six STRING COMMENT '面包屑类别六',
     title STRING COMMENT '商品标题',
     link STRING COMMENT '商品链接',
     brand STRING COMMENT '商品品牌',
     main_image_url STRING COMMENT '主图URL',
     n1d_sale_num BIGINT COMMENT '过去1天的销量',
     n1d_sale_amt DECIMAL(18, 6) COMMENT '过去1天的销售金额',
    n7d_sale_num BIGINT COMMENT '过去7天的销量',
    n7d_sale_amt DECIMAL(18, 6) COMMENT '过去7天的销售金额',
    n15d_sale_num BIGINT COMMENT '过去15天的销量',
    n15d_sale_amt DECIMAL(18, 6) COMMENT '过去15天的销售金额',
    n30d_sale_num BIGINT COMMENT '过去30天的销量',
    n30d_sale_amt DECIMAL(18, 6) COMMENT '过去30天的销售金额',
    n60d_sale_num BIGINT COMMENT '过去60天的销量',
    n60d_sale_amt DECIMAL(18, 6) COMMENT '过去60天的销售金额',
    n90d_sale_num BIGINT COMMENT '过去90天的销量',
    n90d_sale_amt DECIMAL(18, 6) COMMENT '过去90天的销售金额',
    n180d_sale_num BIGINT COMMENT '过去180天的销量',
    n180d_sale_amt DECIMAL(18, 6) COMMENT '过去180天的销售金额',
    n365d_sale_num BIGINT COMMENT '过去365天的销量',
    n365d_sale_amt DECIMAL(18, 6) COMMENT '过去365天的销售金额'
    )  COMMENT '子asin宽表'
    partitioned by (ds string)
    STORED AS orc;






WITH all_asin AS (
    SELECT  tenant_id
         ,seller_id
         ,marketplace_id
         ,seller_sku
         ,asin
    FROM     --//所有商品报告
             (           SELECT  tenant_id
                              ,seller_id
                              ,marketplace_id
                              ,seller_sku
                              ,asin
                         FROM    amz.mid_scm_ivt_amazon_fba_stock_current_num_df
                         WHERE   ds = '20240820'
                         GROUP BY tenant_id
                                ,seller_id
                                ,marketplace_id
                                ,seller_sku
                                ,asin
                         UNION ALL --销售数据
                         SELECT  tenant_id
                              ,seller_id
                              ,marketplace_id
                              ,seller_sku
                              ,asin
                         FROM    amz.mid_amzn_all_orders_df
                         WHERE   ds = '20240820'
                         GROUP BY tenant_id
                                ,seller_id
                                ,marketplace_id
                                ,seller_sku
                                ,asin
             ) T
    GROUP BY tenant_id
           ,seller_id
           ,marketplace_id
           ,seller_sku
           ,asin
),inventory_summary AS (
    SELECT
        tenant_id,
        marketplace_id,
        seller_id,
        asin,
        afn_total_quantity,
        afn_fulfillable_quantity,
        (afn_inbound_working_quantity + afn_inbound_shipped_quantity + afn_inbound_receiving_quantity) AS afn_inbound_total_num,
        afn_warehouse_quantity,
        row_number() over (PARTITION BY tenant_id, marketplace_id, seller_id, asin ORDER BY data_last_update_time DESC) AS rn
    FROM
        ods.ods_report_get_fba_myi_unsuppressed_inventory_data_df
    WHERE
        ds = '20240813'
),
     ledger_data AS (
         SELECT
             tenant_id,
             marketplace_id,
             seller_id,
             asin,
             MIN(operation_date) AS first_instock_date,
             count(distinct operation_date) as fba_instock_cnt
         FROM
             ods.ods_report_get_ledger_detail_view_data_df -- 亚马逊库存分类账详细视图报告
         WHERE
             ds = '20240813' AND     event_type = 'Receipts' --是已接收库存
         GROUP BY
             tenant_id, marketplace_id, seller_id, asin
     ),
     authing AS (
         SELECT tenant_id, seller_id, marketplace_id, profile_id, seller_name
         FROM ods.ods_report_authorization_account_df
         WHERE ds = '20240813'
     ),
     market_info AS (
         SELECT market_place_id as marketplace_id, country_cn_name
         FROM dim.dim_base_marketplace_info_df
         WHERE ds = '20240813'
     ),
     asin_colour AS (
         SELECT
             market_place_id as marketplace_id,
             asin,
             dimensions,
             SUBSTRING_INDEX(dimensions, ' ', 1) AS color,
             TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(dimensions, ' ', -2), ' ', 1)) AS size
        ,row_number() over (PARTITION BY market_place_id, asin ORDER BY created_at DESC) AS rn
FROM
    ods.ods_crawler_amazon_dimensions_detail_df
WHERE ds = '20240813'
    ),
    product_detail AS (
SELECT
    market_place_id AS marketplace_id,
    DIM_ASIN AS `asin`,
    REPLACE(breadcrumbs_feature,'            >               ','>') AS breadcrumbs_feature,
    SPLIT(breadcrumbs_feature,'>')[0] AS breadcrumbs_category_one,
    SPLIT(breadcrumbs_feature,'>')[1] AS breadcrumbs_category_two,
    SPLIT(breadcrumbs_feature,'>')[2] AS breadcrumbs_category_three,
    SPLIT(breadcrumbs_feature,'>')[3] AS breadcrumbs_category_four,
    SPLIT(breadcrumbs_feature,'>')[4] AS breadcrumbs_category_five,
    SPLIT(breadcrumbs_feature,'>')[5] AS breadcrumbs_category_six,
    link,
    brand,
    scribing_price,
    selling_price,
    reviews_ratings AS ratings_num,
    reviews_stars AS ratings_stars,
    sellers_rank_category AS best_sellers_rank_category,
    sellers_rank AS best_sellers_rank,
    sellers_rank_last_detail AS best_sellers_rank_detail,
    date_first_available AS first_available_time,
    created_at,
    title,
    main_image_url,
    ROW_NUMBER() OVER(PARTITION BY market_place_id, DIM_asin ORDER BY created_at DESC) AS rn
FROM
    ods.ods_crawler_amazon_product_details_df
WHERE ds = '20240813'
    ),    product_sale AS (
SELECT
    t.*,

    CAST(NVL(t.quantity,0) AS BIGINT) AS ordered_num,
    CAST(NVL(item_price,0) AS DECIMAL(18,6)) AS item_amt,
    row_number() over (PARTITION BY seller_id,marketplace_id, t.amazon_order_id, t.merchant_order_id, t.purchase_date, t.sku,asin ORDER BY data_last_update_time DESC) AS rn
FROM ods.ods_report_get_flat_file_all_orders_data_by_order_date_general_df t
WHERE ds = '20240813'
    ),order_stat AS (
SELECT
--     tenant_id,
--     seller_id,
--     sku,
--     asin,
         tenant_id
        ,seller_id
        ,marketplace_id
        ,sku
        ,asin,
--         ,market_place_type
--         ,marketplace_website
--         ,country_code
--         ,country_cn_name
--         ,currency
    SUM(CASE WHEN purchase_date = DATE_SUB('2024-08-13', 1) THEN ordered_num ELSE 0 END) AS n1d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 7) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n7d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 15) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n15d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 30) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n30d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 60) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n60d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 90) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n90d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 180) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n180d_sale_num,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 365) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) AS n365d_sale_num,
    SUM(CASE WHEN purchase_date = DATE_SUB('2024-08-13', 1) THEN item_amt ELSE 0 END) AS n1d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 7) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n7d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 15) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n15d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 30) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n30d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 60) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n60d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 90) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n90d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 180) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n180d_sale_amt,
    SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 365) AND purchase_date < '2024-08-13' THEN item_amt ELSE 0 END) AS n365d_sale_amt,
    RANK() OVER (ORDER BY SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 365) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) DESC) AS sales_rank,
    100.0 * SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 365) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END) / SUM(SUM(CASE WHEN purchase_date >= DATE_SUB('2024-08-13', 365) AND purchase_date < '2024-08-13' THEN ordered_num ELSE 0 END)) OVER () AS sales_percentage

FROM product_sale
WHERE rn = 1 AND ordered_num > 0 AND order_status <> 'Canceled'
-- GROUP BY tenant_id, seller_id, sku, asin

group by tenant_id
        ,seller_id
        ,marketplace_id
        ,sku
        ,asin
--         ,market_place_type
--         ,marketplace_website
--         ,country_code
--         ,country_cn_name
--         ,currency

    )
insert overwrite table dwd.dwd_prd_asin_info_df partition (ds = '20240813')
SELECT
    MD5(CONCAT(t1.tenant_id, '_', t4.profile_id, '_', t1.asin)) AS id,
    t1.asin AS seller_asin,
    t1.tenant_id,
    t4.profile_id,
    t1.marketplace_id,
    CONCAT(t5.country_cn_name, '站') AS marketplace_name,
    t1.seller_id,
    t4.seller_name,
    t2.afn_total_quantity AS fba_total_num,
    t2.afn_inbound_total_num AS fba_inbound_num,
    t2.afn_warehouse_quantity AS fba_warehouse_num,
    DATEDIFF(CURRENT_DATE(), t3.first_instock_date) AS fba_first_instock_days,
    t3.first_instock_date AS fba_instock_cnt,
    --  计算 预计可销售天数
    case  when  nvl(t8.n15d_sale_num,0)<>0 then t2.afn_fulfillable_quantity /t8.n15d_sale_num /15 else 0 end  as afn_fulfillable_quantity,
    t6.color,
    t6.size,
    t8.sales_rank AS order_num_rank,
    t8.sales_percentage AS order_num_rate,
    t7.breadcrumbs_category_one,
    t7.breadcrumbs_category_two,
    t7.breadcrumbs_category_three,
    t7.breadcrumbs_category_four,
    t7.breadcrumbs_category_five,
    t7.breadcrumbs_category_six,
    t7.title,
    t7.link,
    t7.brand,
    t7.main_image_url,
    t8.n1d_sale_num,
    t8.n1d_sale_amt,
    t8.n7d_sale_num,
    t8.n7d_sale_amt,
    t8.n15d_sale_num,
    t8.n15d_sale_amt,
    t8.n30d_sale_num,
    t8.n30d_sale_amt,
    t8.n60d_sale_num,
    t8.n60d_sale_amt,
    t8.n90d_sale_num,
    t8.n90d_sale_amt,
    t8.n180d_sale_num,
    t8.n180d_sale_amt,
    t8.n365d_sale_num,
    t8.n365d_sale_amt
FROM
    all_asin t1
        LEFT JOIN inventory_summary t2
                  ON t1.tenant_id = t2.tenant_id
                      AND t1.seller_id = t2.seller_id
                      AND t1.marketplace_id = t2.marketplace_id
                      AND t1.asin = t2.asin
                      and  t2.rn =1
        LEFT JOIN ledger_data t3
                  ON t1.tenant_id = t3.tenant_id
                      AND t1.marketplace_id = t3.marketplace_id
                      AND t1.seller_id = t3.seller_id
                      AND t1.asin = t3.asin
        LEFT JOIN authing t4
                  ON t1.tenant_id = t4.tenant_id
                      AND t1.seller_id = t4.seller_id
                      AND t1.marketplace_id = t4.marketplace_id
        LEFT JOIN market_info t5
                  ON t1.marketplace_id = t5.marketplace_id
        LEFT JOIN asin_colour t6
                  ON t1.asin = t6.asin  and t1.marketplace_id = t6.marketplace_id and t6.rn=1
        LEFT JOIN product_detail t7
                  ON t1.marketplace_id = t7.marketplace_id
                      AND t1.asin = t7.asin and t7.rn=1
        left join order_stat t8 on t1.tenant_id = t8.tenant_id and t1.seller_id = t8.seller_id and t1.asin = t8.asin
where  coalesce(t4.profile_id ,'') <>'';

