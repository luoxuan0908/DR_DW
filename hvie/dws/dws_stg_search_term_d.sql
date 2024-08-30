-- 替换  adm_amazon_adv_strategy_search_term_d

CREATE TABLE IF NOT EXISTS amz.dws_stg_search_term_d(
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
    ad_group_num BIGINT COMMENT '广告组数量',
    ad_group_id_list STRING COMMENT '广告组id列表(通过_/_拼接)',
    parent_asin STRING COMMENT '父aisn',
    selling_price STRING COMMENT '售价',
    main_asin_url STRING COMMENT '商品链接',
    main_image_url STRING COMMENT '商品主图',
    term_type STRING COMMENT '搜索词类型',
    search_term STRING COMMENT '搜索词',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '花费',
    sale_amt DECIMAL(18,6) COMMENT '销售额',
    order_num BIGINT COMMENT '销量',
    ctr DECIMAL(18,6) COMMENT 'CTR',
    cvr DECIMAL(18,6) COMMENT 'CVR',
    cpc DECIMAL(18,6) COMMENT 'CPC',
    acos DECIMAL(18,6) COMMENT 'ACOS',
    category_list STRING COMMENT '类目',
    cate_impressions BIGINT COMMENT '类目曝光量',
    cate_clicks BIGINT COMMENT '类目点击量',
    cate_cost DECIMAL(18,6) COMMENT '类目花费',
    cate_sale_amt DECIMAL(18,6) COMMENT '类目销售额',
    cate_order_num BIGINT COMMENT '类目销量',
    cate_ctr DECIMAL(18,6) COMMENT '类目CTR',
    cate_cvr DECIMAL(18,6) COMMENT '类目CVR',
    cate_cpc DECIMAL(18,6) COMMENT '类目CPC',
    cate_acos DECIMAL(18,6) COMMENT '类目ACOS',
    aba_rank BIGINT COMMENT 'aba排名',
    aba_date date COMMENT 'aba日期',
    adv_days BIGINT COMMENT '广告天数',
    fba_first_instock_date timestamp COMMENT 'FBA首次入库时间',
    fba_stock_num BIGINT COMMENT '父asin的fba库存',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期',
    stock_sale_days BIGINT COMMENT '预计可售天数(当前库存除以最近15天日均销量)',
    fba_warehouse_num BIGINT COMMENT 'FBA在库数量',
    n15d_avg_sale_num DECIMAL(18,6) COMMENT '近15天日均销量',
    fba_instock_cnt BIGINT COMMENT 'FBA入库次数',
    ad_group_name_list STRING COMMENT '广告组名称列表'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊广告策略搜索词数据(统计最近30天，父asin+广告活动+搜索词粒度)')
    ;


--广告活动下的花费最高的父asin
INSERT OVERWRITE TABLE amz.dws_stg_search_term_d PARTITION (ds = '${bizdate}')
--最近30天的搜索词的效果
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
     ,ad_group_num
     ,ad_group_id_list
     ,a.parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,CASE   WHEN LENGTH(REGEXP_REPLACE(a.search_term,'([^0-9])','')) > 0
    AND LENGTH(REGEXP_REPLACE(a.search_term,'([^a-z])','')) > 0
    AND INSTR (a.search_term,' ')<1
    AND LENGTH(a.search_term) = 10 THEN '搜索品'
             ELSE '搜索词'
    END term_type
     ,a.search_term
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
     ,b.category_list
     ,cate_impressions
     ,cate_clicks
     ,cate_cost
     ,cate_sale_amt
     ,cate_order_num
     ,cate_ctr
     ,cate_cvr
     ,cate_cpc
     ,cate_acos
     ,aba_rank
     ,aba_date
     ,DATEDIFF(current_date(),min_report_date) + 1 adv_days
     ,fba_first_instock_date
     ,fba_stock_num
     ,'${bizdate}' data_dt
     ,current_date() etl_data_dt
     ,cast(CASE WHEN nvl(n15d_avg_sale_num,0)<>0 then fba_stock_num/n15d_avg_sale_num end  as BIGINT )stock_sale_days
     ,fba_warehouse_num
     ,n15d_avg_sale_num
     ,fba_instock_cnt
     ,ad_group_name_list
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
                 ,search_term
                 ,COUNT(DISTINCT s1.ad_group_id) ad_group_num
--                  ,WM_CONCAT(DISTINCT '_&_',s1.ad_group_id) ad_group_id_list
--                  ,WM_CONCAT(DISTINCT '_&_',s3.ad_group_name) ad_group_name_list
                 ,CONCAT_WS(',', COLLECT_SET(CONCAT('_&_', s1.ad_group_id))) AS ad_group_id_list
                 ,CONCAT_WS(',', COLLECT_SET(CONCAT('_&_', s3.ad_group_name))) AS ad_group_name_list

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
                             ,search_term
                             ,ad_group_id
                             ,impressions
                             ,clicks
                             ,cost
                             ,w7d_sale_amt
                             ,w7d_units_sold_clicks
                             ,report_date
                        FROM  amz.dwd_adv_sp_search_term_ds --  whde.dwd_mkt_adv_amazon_sp_search_term_ds
                        WHERE   ds >= date_format(date_sub(to_date(from_unixtime(unix_timestamp('20240827', 'yyyyMMdd'))), 30),'yyyyMMdd' ) -- TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
                          AND     campaign_status = 'ENABLED'
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
                     ,MAX(ad_group_name)ad_group_name
                     ,MAX(ad_mode) ad_mode
                     ,MAX(campaign_status)campaign_status
                FROM  amz.mid_amazon_adv_sku_wide_d --  whde.adm_amazon_adv_sku_wide_d
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
            --WHERE s3.campaign_status='已启动'
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
                   ,search_term
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
    FROM  amz.dwd_prd_parent_asin_index_df -- whde.dws_itm_spu_amazon_parent_asin_index_df
    WHERE   ds = '${bizdate}'-- MAX_PT('WHDE.dws_itm_spu_amazon_parent_asin_index_df')
) b --父aisn、以及类目、库存
                        ON      a.tenant_id = b.tenant_id
                            AND     a.marketplace_id = b.marketplace_id
                            AND     a.seller_id = b.seller_id
                            AND     a.parent_asin = b.parent_asin
            LEFT JOIN   (
    SELECT  tenant_id
         ,marketplace_id
         ,category_list
         ,cate_impressions
         ,cate_clicks
         ,cate_cost
         ,cate_sale_amt
         ,cate_order_num
         ,cate_ctr
         ,cate_cvr
         ,cate_cpc
         ,cate_acos
    FROM amz.dwd_adv_category_index_df   -- whde.adm_amazon_adv_strategy_category_d
    WHERE   ds = '${bizdate}'
) d --类目数据
                        ON      b.tenant_id = d.tenant_id
                            AND     b.marketplace_id = d.marketplace_id
                            AND     b.category_list = d.category_list
            LEFT JOIN   (
    SELECT  marketplace_id
         ,search_term
         ,MIN(CAST(search_frequency_rank AS BIGINT)) aba_rank
         ,MAX(rank_date) aba_date
    FROM    (select * from amz.mid_itm_spu_amazon_search_keyword_info_ws  --whde.dwd_itm_spu_amazon_search_keyword_info_ws
             WHERE   ws = '${bizdate}' -- MAX_PT('whde.dwd_itm_spu_amazon_search_keyword_info_ws')
            )a
    GROUP BY  marketplace_id
           ,search_term
) e --aba搜索排名
                        ON      LOWER(a.search_term) = LOWER(e.search_term)
                            AND a.marketplace_id=e.marketplace_id
            LEFT JOIN   (
    SELECT  marketplace_id
         ,parent_asin
         ,MAX(link) main_asin_url --逻辑有待修改20240501
    FROM amz.dwd_prd_parent_asin_index_df  -- whde.dws_itm_spu_amazon_parent_asin_index_df
    WHERE   ds = '${bizdate}' -- MAX_PT('whde.dws_itm_spu_amazon_parent_asin_index_df')
      AND     NVL(parent_asin,'') <> ''
    GROUP BY marketplace_id
           ,parent_asin
) f
                        ON      a.marketplace_id = f.marketplace_id
                            AND     a.parent_asin = f.parent_asin
;

