-- 转换 whde.adm_amazon_adv_strategy_target_d
CREATE TABLE IF NOT EXISTS amz.dws_stg_target_d(
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
    ad_group_id STRING COMMENT '广告组id',
    ad_group_name STRING COMMENT '广告组名称',
    parent_asin STRING COMMENT '父aisn',
    selling_price STRING COMMENT '售价',
    main_asin_url STRING COMMENT '商品链接',
    main_image_url STRING COMMENT '商品主图',
    target_type STRING COMMENT '投放词类型（投放品/投放词）',
    target_text STRING COMMENT '投放词/品',
    target_id_list STRING COMMENT '投放ID列表(通过_/_拼接)',
    impressions BIGINT COMMENT '曝光量',
    clicks BIGINT COMMENT '点击量',
    cost DECIMAL(18,6) COMMENT '花费',
    sale_amt DECIMAL(18,6) COMMENT '销售额',
    sale_num BIGINT COMMENT '销量',
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
    adv_rank BIGINT COMMENT '广告搜索排名',
    norm_rank BIGINT COMMENT '自然搜索排名',
    adv_days BIGINT COMMENT '广告天数',
    fba_first_instock_date timestamp COMMENT 'FBA首次入库时间',
    fba_total_num BIGINT COMMENT 'fba总库存',
    fba_warehouse_num BIGINT COMMENT 'FBA在库数量',
    fba_instock_cnt BIGINT COMMENT 'FBA入库次数',
    n15d_avg_sale_num DECIMAL(18,6) COMMENT '近15天日均销量',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊广告策略投放词/品数据(统计最近30天)')
    ;




--投放词的排名取下出单搜索词的50%分位数
DROP TABLE IF EXISTS adm_amazon_adv_strategy_target_d_tem3
;

CREATE TABLE adm_amazon_adv_strategy_target_d_tem3 AS
SELECT  a.tenant_id
     ,a.profile_id
     ,a.seller_id
     ,a.campaign_id
     ,a.ad_group_id
     ,a.targeting
     ,a.parent_asin
     ,PERCENTILE(NVL(rank_adv_abs,999999),0.5) adv_rank
     ,PERCENTILE(NVL(rank_norm_abs,999999),0.5) norm_rank
FROM    (
            SELECT  a.tenant_id
                 ,a.profile_id
                 ,a.seller_id
                 ,a.campaign_id
                 ,a.ad_group_id
                 ,CASE   WHEN INSTR(targeting,'asin') >= 1 THEN REPLACE(SPLIT(targeting,'=')[1],'"','')
                         WHEN INSTR(targeting,'category') >= 1 THEN SPLIT(REPLACE(SPLIT(targeting,'=')[1],'"',''),'price')[0]
                         ELSE targeting
                END targeting
                 ,a.search_term
                 ,parent_asin
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,seller_id
                             ,campaign_id
                             ,ad_group_id
                             ,targeting
                             ,search_term
                        FROM amz.dwd_adv_sp_search_term_ds   --whde.dwd_mkt_adv_amazon_sp_search_term_ds
                        WHERE   ds >=date_format(date_sub(to_date(from_unixtime(unix_timestamp('20240827', 'yyyyMMdd'))), 30),'yyyyMMdd' ) -- TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
                          AND     w7d_units_sold_clicks > 0
                        GROUP BY tenant_id
                               ,profile_id
                               ,seller_id
                               ,campaign_id
                               ,ad_group_id
                               ,targeting
                               ,search_term
                    ) a
                        LEFT JOIN   (
                SELECT  tenant_id
                     ,profile_id
                     ,seller_id
                     ,campaign_id
                     ,ad_group_id
                     ,top_cost_parent_asin parent_asin
                FROM amz.mid_amazon_adv_sku_wide_d   -- whde.adm_amazon_adv_sku_wide_d
                WHERE   ds = '${bizdate}'
                GROUP BY tenant_id
                       ,profile_id
                       ,seller_id
                       ,campaign_id
                       ,ad_group_id
                       ,top_cost_parent_asin
            ) b
                                    ON      a.tenant_id = b.tenant_id
                                        AND     a.profile_id = b.profile_id
                                        AND     a.seller_id = b.seller_id
                                        AND     a.campaign_id = b.campaign_id
                                        AND     a.ad_group_id = b.ad_group_id
        ) a
            LEFT JOIN   (
    SELECT  parent_asin
         ,search_term
         ,rank_adv_abs
         ,rank_norm_abs
    FROM amz.dwd_adv_search_term_pasin_rank_df   -- whde.adm_amazon_adv_search_term_pasin_rank_df
    WHERE   ds ='${bizdate}' -- MAX_PT('whde.adm_amazon_adv_search_term_pasin_rank_df')
) b
                        ON      a.parent_asin = b.parent_asin
                            AND     LOWER(a.search_term) = LOWER(b.search_term)
GROUP BY a.tenant_id
       ,a.profile_id
       ,a.seller_id
       ,a.campaign_id
       ,a.ad_group_id
       ,a.targeting
       ,a.parent_asin
;

--最近30天投放词的效果
INSERT OVERWRITE TABLE amz.dws_stg_target_d PARTITION (ds = '${bizdate}')
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
     ,a.ad_group_id
     ,ad_group_name
     ,a.parent_asin
     ,selling_price
     ,main_asin_url
     ,main_image_url
     ,target_type
     ,a.targeting target_text
     ,target_id_list
     ,impressions
     ,clicks
     ,CAST(cost AS DECIMAL(18,6)) cost
     ,CAST(sale_amt AS DECIMAL(18,6)) sale_amt
     ,sale_num
     ,CAST(CASE   WHEN impressions <> 0 THEN clicks / impressions
    END AS DECIMAL(18,6)) ctr
     ,CAST(CASE   WHEN clicks <> 0 THEN sale_num / clicks
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
     ,CAST(adv_rank AS BIGINT)
     ,CAST(norm_rank AS BIGINT)
     ,DATEDIFF(current_date(),min_report_date,'dd') + 1 adv_days
     ,fba_first_instock_date
     ,fba_stock_num
     ,fba_warehouse_num
     ,fba_instock_cnt
     ,n15d_avg_sale_num
     ,'${bizdate}' data_dt
     ,current_date() etl_data_dt
FROM    (
            SELECT  s1.tenant_id
                 ,s1.profile_id
                 ,s3.marketplace_id
                 ,s3.marketplace_name
                 ,s3.seller_id
                 ,s3.seller_name
                 ,s1.campaign_id
                 ,s3.campaign_name
                 ,currency_code
                 ,s1.ad_group_id
                 ,s3.ad_group_name
                 ,ad_mode
                 ,parent_asin
                 ,target_type
                 ,targeting
--                  ,WM_CONCAT(DISTINCT '_/_',keyword_id) target_id_list
                 ,CONCAT_WS(',', COLLECT_SET(CONCAT('_/_', keyword_id))) AS target_id_list
                 ,SUM(impressions) impressions
                 ,SUM(clicks) clicks
                 ,SUM(cost) cost
                 ,SUM(w7d_sale_amt) sale_amt
                 ,SUM(w7d_units_sold_clicks) sale_num
                 ,MAX(report_date) max_report_date
                 ,MIN(report_date) min_report_date
            FROM    (
                        SELECT  tenant_id
                             ,profile_id
                             ,seller_id
                             ,campaign_id
                             ,campaign_budget_currency_code currency_code
                             ,ad_group_id
                             ,CASE   WHEN INSTR(targeting,'asin') >= 1 THEN '投放品'
                                     WHEN INSTR(targeting,'category') >= 1 THEN '投放类目'
                                     ELSE '投放词'
                            END target_type
                             ,CASE   WHEN INSTR(targeting,'asin') >= 1 THEN REPLACE(SPLIT(targeting,'=')[1],'"','')
                                     WHEN INSTR(targeting,'category') >= 1 THEN SPLIT(REPLACE(SPLIT(targeting,'=')[1],'"',''),'price',1)[0]
                                     ELSE targeting
                            END targeting
                             ,keyword_id
                             ,impressions
                             ,clicks
                             ,cost
                             ,w7d_sale_amt
                             ,w7d_units_sold_clicks
                             ,report_date
                        FROM amz.dwd_adv_sp_target_ds   --whde.dwd_mkt_adv_amazon_sp_target_ds
                        WHERE   ds >=date_format(date_sub(to_date(from_unixtime(unix_timestamp('20240827', 'yyyyMMdd'))), 30),'yyyyMMdd' ) --TO_CHAR(DATEADD(TO_DATE('${bizdate}','yyyymmdd'),-30,'dd'),'yyyymmdd')
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
                     ,MAX(ad_group_name) ad_group_name
                     ,MAX(ad_mode) ad_mode
                     ,MAX(campaign_status)campaign_status
                FROM  amz.mid_amazon_adv_sku_wide_d  -- whde.adm_amazon_adv_sku_wide_d
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
            WHERE s3.campaign_status='已启动'
            GROUP BY s1.tenant_id
                   ,s1.profile_id
                   ,s3.marketplace_id
                   ,s3.marketplace_name
                   ,s3.seller_id
                   ,s3.seller_name
                   ,s1.campaign_id
                   ,s3.campaign_name
                   ,currency_code
                   ,s1.ad_group_id
                   ,s3.ad_group_name
                   ,ad_mode
                   ,parent_asin
                   ,target_type
                   ,targeting
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
    FROM  amz.dwd_prd_parent_asin_index_df --  whde.dws_itm_spu_amazon_parent_asin_index_df
    WHERE   ds = '${bizdate}'-- MAX_PT('whde.dws_itm_spu_amazon_parent_asin_index_df')
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
    FROM  amz.dwd_adv_category_index_df -- whde.adm_amazon_adv_strategy_category_d
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
    FROM  amz.mid_itm_spu_amazon_search_keyword_info_ws -- whde.dwd_itm_spu_amazon_search_keyword_info_ws
    WHERE   ws = '${bizdate}'-- MAX_PT('whde.dwd_itm_spu_amazon_search_keyword_info_ws')
    GROUP BY marketplace_id
           ,search_term
) e --aba搜索排名
                        ON      LOWER(a.targeting) = LOWER(e.search_term)
                            AND a.marketplace_id=e.marketplace_id
            LEFT JOIN adm_amazon_adv_strategy_target_d_tem3 f --投放词排名
                      ON      a.tenant_id = f.tenant_id
                          AND     a.profile_id = f.profile_id
                          AND     a.seller_id = f.seller_id
                          AND     a.campaign_id = f.campaign_id
                          AND     a.ad_group_id = f.ad_group_id
                          AND     LOWER(a.targeting) = LOWER(f.targeting)
            LEFT JOIN   (


    SELECT  marketplace_id
         ,parent_asin
         ,MAX(link) main_asin_url --逻辑有待修改20240501
    FROM amz.dwd_prd_parent_asin_index_df  -- whde.dws_itm_spu_amazon_parent_asin_index_df
    WHERE   ds ='${bizdate}' -- MAX_PT('whde.dws_itm_spu_amazon_parent_asin_index_df')
      AND     NVL(parent_asin,'') <> ''
    GROUP BY marketplace_id
           ,parent_asin
) g
                        ON      a.marketplace_id = g.marketplace_id
                            AND     a.parent_asin = g.parent_asin
;

DROP TABLE IF EXISTS adm_amazon_adv_strategy_target_d_tem3
;

