
CREATE TABLE IF NOT EXISTS amz.dwd_adv_category_index_df(
    tenant_id STRING COMMENT '租户ID',
    marketplace_id STRING COMMENT '市场ID',
    marketplace_name STRING COMMENT '市场名称',
    ad_type STRING COMMENT '广告类型',
    currency STRING COMMENT '币种',
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
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊广告类目效果数据（统计最近30天）')
    ;

INSERT OVERWRITE TABLE amz.dwd_adv_category_index_df PARTITION (ds = '20240823')
SELECT  tenant_id
     ,marketplace_id
     ,marketplace_name
     ,'商品推广' ad_type
     ,campaign_budget_currency_code currency_code
     ,CASE   WHEN LENGTH(category_one) > 0
    AND LENGTH(category_two) > 0
    AND LENGTH(category_three) > 0
    AND LENGTH(category_four) > 0
    AND LENGTH(category_five) > 0
    AND LENGTH(category_six) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three,'>',category_four,'>',category_five,'>',category_six)
             WHEN LENGTH(category_one) > 0
                 AND LENGTH(category_two) > 0
                 AND LENGTH(category_three) > 0
                 AND LENGTH(category_four) > 0
                 AND LENGTH(category_five) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three,'>',category_four,'>',category_five)
             WHEN LENGTH(category_one) > 0
                 AND LENGTH(category_two) > 0
                 AND LENGTH(category_three) > 0
                 AND LENGTH(category_four) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three,'>',category_four)
             WHEN LENGTH(category_one) > 0
                 AND LENGTH(category_two) > 0
                 AND LENGTH(category_three) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three)
             WHEN LENGTH(category_one) > 0
                 AND LENGTH(category_two) > 0 THEN CONCAT(category_one,'>',category_two)
             WHEN LENGTH(category_one) > 0 THEN category_one
             ELSE ''
    END category_list
     ,SUM(impressions) cate_impressions
     ,SUM(clicks) cate_clicks
     ,CAST(SUM(cost) AS DECIMAL(18,6)) cate_cost
     ,CAST(SUM(sale_amt) AS DECIMAL(18,6)) cate_sale_amt
     ,SUM(order_num) cate_order_num
     ,CAST(CASE   WHEN SUM(impressions) <> 0 THEN SUM(clicks) / SUM(impressions)
    END AS DECIMAL(18,6)) cate_ctr
     ,CAST(CASE   WHEN SUM(clicks) <> 0 THEN SUM(order_num) / SUM(clicks)
    END AS DECIMAL(18,6)) cate_cvr
     ,CAST(CASE   WHEN SUM(clicks) <> 0 THEN SUM(cost) / SUM(clicks)
    END AS DECIMAL(18,6)) cate_cpc
     ,CAST(CASE   WHEN SUM(sale_amt) <> 0 THEN SUM(cost) / SUM(sale_amt)
    END AS DECIMAL(18,6)) cate_acos
     ,'20240823' data_dt
     ,current_date() etl_data_dt
FROM    amz.mid_amazon_adv_sku_wide_d
WHERE   ds = '20240823'
  AND date_format(fba_first_instock_date,'yyyymmdd')<= date_format(date_sub(to_date(from_unixtime(unix_timestamp('20240823', 'yyyyMMdd'))), 90),'yyyyMMdd')--限制老品
GROUP BY tenant_id
       ,marketplace_id
       ,marketplace_name
       ,campaign_budget_currency_code
       ,CASE   WHEN LENGTH(category_one) > 0
    AND LENGTH(category_two) > 0
    AND LENGTH(category_three) > 0
    AND LENGTH(category_four) > 0
    AND LENGTH(category_five) > 0
    AND LENGTH(category_six) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three,'>',category_four,'>',category_five,'>',category_six)
               WHEN LENGTH(category_one) > 0
                   AND LENGTH(category_two) > 0
                   AND LENGTH(category_three) > 0
                   AND LENGTH(category_four) > 0
                   AND LENGTH(category_five) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three,'>',category_four,'>',category_five)
               WHEN LENGTH(category_one) > 0
                   AND LENGTH(category_two) > 0
                   AND LENGTH(category_three) > 0
                   AND LENGTH(category_four) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three,'>',category_four)
               WHEN LENGTH(category_one) > 0
                   AND LENGTH(category_two) > 0
                   AND LENGTH(category_three) > 0 THEN CONCAT(category_one,'>',category_two,'>',category_three)
               WHEN LENGTH(category_one) > 0
                   AND LENGTH(category_two) > 0 THEN CONCAT(category_one,'>',category_two)
               WHEN LENGTH(category_one) > 0 THEN category_one
               ELSE ''
    END


;