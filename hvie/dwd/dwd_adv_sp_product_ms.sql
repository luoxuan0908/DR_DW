--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 23:36:11
--********************************************************************--
-- set odps.sql.python.version = cp37;
drop table if EXISTS amz.dwd_adv_sp_product_ms;
CREATE TABLE IF NOT EXISTS amz.dwd_adv_sp_product_ms(
   profile_id STRING COMMENT 'profileId',
   report_id STRING COMMENT 'reportId',
   report_type STRING COMMENT '广告类型',
   report_date STRING COMMENT 'time_unit=DAILY时有效，数据具体日期',
   data_last_update_time STRING COMMENT '亚马逊数据生产时间',
   seller_id STRING COMMENT '卖家ID',
   tenant_id STRING COMMENT '租户ID',
   country_code STRING COMMENT '国家编码',
   portfolio_id STRING,
   campaign_name STRING,
   campaign_id STRING,
   campaign_status STRING,
   campaign_budget_amount STRING,
   campaign_budget_type STRING,
   campaign_budget_currency_code STRING,
   ad_group_name STRING,
   ad_group_id STRING,
   ad_id STRING,
   advertised_asin STRING,
   advertised_sku STRING,
   impressions STRING,
   clicks STRING,
   cost_per_click STRING,
   click_through_rate STRING,
   cost STRING,
   spend STRING,
   purchases_1d STRING,
   purchases_7d STRING,
   purchases_14d STRING,
   purchases_30d STRING,
   purchases_same_sku_1d STRING,
   purchases_same_sku_7d STRING,
   purchases_same_sku_14d STRING,
   purchases_same_sku_30d STRING,
   units_sold_clicks_1d STRING,
   units_sold_clicks_7d STRING,
   units_sold_clicks_14d STRING,
   units_sold_clicks_30d STRING,
   sales_1d STRING,
   sales_7d STRING,
   sales_14d STRING,
   sales_30d STRING,
   attributed_sales_same_sku_1d STRING,
   attributed_sales_same_sku_7d STRING,
   attributed_sales_same_sku_14d STRING,
   attributed_sales_same_sku_30d STRING,
   units_sold_same_sku_1d STRING,
   units_sold_same_sku_7d STRING,
   units_sold_same_sku_14d STRING,
   units_sold_same_sku_30d STRING,
   kindle_edition_normalized_pages_read_14d STRING,
   kindle_edition_normalized_pages_royalties_14d STRING,
   sales_other_sku_7d STRING,
   units_sold_other_sku_7d STRING,
   acos_clicks_7d STRING,
   acos_clicks_14d STRING,
   roas_clicks_7d STRING,
   roas_clicks_14d STRING,
   update_time_row STRING,
   marketplace_id STRING,
   update_time_zh STRING
)
    PARTITIONED BY (ms STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊报告表按分钟增量存储、每次刷新前一天数据')
   ;

insert overwrite table amz.dwd_adv_sp_product_ms partition (ms)
select
    profile_id
     ,report_id
     ,report_type
     ,report_date
     ,data_last_update_time
     ,seller_id
     ,tenant_id
     ,country_code
     ,portfolio_id
     ,campaign_name
     ,campaign_id
     ,campaign_status
     ,campaign_budget_amt
     ,campaign_budget_type
     ,campaign_budget_currency_code
     ,ad_group_name
     ,ad_group_id
     ,ad_id
     ,advertised_asin
     ,advertised_sku
     ,impressions
     ,clicks
     ,cost_per_click
     ,click_through_rate
     ,cost
     ,spend
     ,purchases_1d
     ,purchases_7d
     ,purchases_14d
     ,purchases_30d
     ,purchases_same_sku_1d
     ,purchases_same_sku_7d
     ,purchases_same_sku_14d
     ,purchases_same_sku_30d
     ,units_sold_clicks_1d
     ,units_sold_clicks_7d
     ,units_sold_clicks_14d
     ,units_sold_clicks_30d
     ,sales_1d
     ,sales_7d
     ,sales_14d
     ,sales_30d
     ,attributed_sales_same_sku_1d
     ,attributed_sales_same_sku_7d
     ,attributed_sales_same_sku_14d
     ,attributed_sales_same_sku_30d
     ,units_sold_same_sku_1d
     ,units_sold_same_sku_7d
     ,units_sold_same_sku_14d
     ,units_sold_same_sku_30d
     ,kindle_edition_normalized_pages_read_14d
     ,kindle_edition_normalized_pages_royalties_14d
     ,sales_other_sku_7d
     ,units_sold_other_sku_7d
     ,acos_clicks_7d
     ,acos_clicks_14d
     ,roas_clicks_7d
     ,roas_clicks_14d
     --,to_char(to_date(suncent_dw.convert_time(update_time_zh,timezone),'yyyy-mm-dd hh:ss:mi'),'yyyymmddhh')
     ,update_time_zh  update_time_raw --数据更新原始时间
     ,marketplace_id
     ,update_time_zh
     ,ms
from (
         select
             t1.profile_id
              ,t1.report_id
              ,t1.report_type
              ,t1.report_date
              ,t1.data_last_update_time
              ,t1.seller_id
              ,t1.tenant_id
              ,t1.country_code
              ,t1.portfolio_id
              ,t1.campaign_name
              ,t1.campaign_id
              ,t1.campaign_status
              ,t1.campaign_budget_amt
              ,t1.campaign_budget_type
              ,t1.campaign_budget_currency_code
              ,t1.ad_group_name
              ,t1.ad_group_id
              ,t1.ad_id
              ,t1.advertised_asin
              ,t1.advertised_sku
              ,t1.impressions
              ,t1.clicks
              ,t1.cost_per_click
              ,t1.click_through_rate
              ,t1.cost
              ,t1.spend
              ,t1.purchases_1d
              ,t1.purchases_7d
              ,t1.purchases_14d
              ,t1.purchases_30d
              ,t1.purchases_same_sku_1d
              ,t1.purchases_same_sku_7d
              ,t1.purchases_same_sku_14d
              ,t1.purchases_same_sku_30d
              ,t1.units_sold_clicks_1d
              ,t1.units_sold_clicks_7d
              ,t1.units_sold_clicks_14d
              ,t1.units_sold_clicks_30d
              ,t1.sales_1d
              ,t1.sales_7d
              ,t1.sales_14d
              ,t1.sales_30d
              ,t1.attributed_sales_same_sku_1d
              ,t1.attributed_sales_same_sku_7d
              ,t1.attributed_sales_same_sku_14d
              ,t1.attributed_sales_same_sku_30d
              ,t1.units_sold_same_sku_1d
              ,t1.units_sold_same_sku_7d
              ,t1.units_sold_same_sku_14d
              ,t1.units_sold_same_sku_30d
              ,t1.kindle_edition_normalized_pages_read_14d
              ,t1.kindle_edition_normalized_pages_royalties_14d
              ,t1.sales_other_sku_7d
              ,t1.units_sold_other_sku_7d
              ,t1.acos_clicks_7d
              ,t1.acos_clicks_14d
              ,t1.roas_clicks_7d
              ,t1.roas_clicks_14d
              --,from_unixtime(cast(cast(data_last_update_time as bigint) / 1000 as bigint)) update_time_zh
              ,data_last_update_time update_time_zh
              ,t2.marketplace_id
              ,t2.timezone
              ,concat(substring(ds,1,10),'00') as ms
         from amz.mid_amzn_sp_advertised_product_by_advertiser_report_ds  t1
                  inner join (
             select  profile_id
                  ,marketplace_id
                  ,timezone
             from    amz.dim_base_seller_sites_store_df
             where   ds ='20240827' -- max_pt('whde.dwd_sit_shp_amazon_seller_sites_store_df')
               and store_auth_status = '已授权'
             group by  profile_id
                    ,marketplace_id
                    ,timezone
         )t2 on t1.profile_id=t2.profile_id
         where substring(t1.ds,1,8)>='20240827'
           and substring(t1.ds,1,8)<='20240827'
           and coalesce(t1.data_last_update_time,'')<>''
     )tt1
;