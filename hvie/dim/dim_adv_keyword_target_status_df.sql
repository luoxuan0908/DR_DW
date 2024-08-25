--********************************************************************--
--author:luoxuan
--create time:2024-08-23
--********************************************************************--
drop table if exists amz.dim_adv_keyword_target_status_df;
CREATE TABLE IF NOT EXISTS amz.dim_adv_keyword_target_status_df
(
    tenant_id                STRING COMMENT '租户ID'
    ,profile_id              STRING COMMENT '配置Id'
    ,campaign_id             STRING COMMENT '广告活动ID'
    ,ad_group_id             STRING COMMENT '广告组ID'
    ,ad_type                 STRING COMMENT '广告类型'
    ,keyword_id              STRING COMMENT '投放词ID'
    ,keyword_text            STRING COMMENT '投放词'
    ,keyword_stem            STRING COMMENT '投放词的词干，去除单复数'
    ,parent_asin             STRING COMMENT '父asin'
    ,match_type              STRING COMMENT '匹配类型'
    ,`status`                STRING COMMENT '关键词状态:ENABLED, PAUSED, ARCHIVED'
    ,bid                     DECIMAL(18,6) COMMENT '竞价'
    ,serving_status          STRING COMMENT '广告活动实时状态'
    ,native_language_keyword STRING COMMENT '目标受众的本地语言关键词'
    ,create_datetime         timestamp COMMENT '创建时间'
    ,update_datetime         timestamp COMMENT '更新时间'
    ,data_dt                 STRING COMMENT '数据日期'
    ,etl_data_dt             date COMMENT '数据加载日期'
)
    PARTITIONED BY
        (
        ds                       STRING
        )
    STORED AS ORC
    TBLPROPERTIES ('comment' = '广告投放词最新状态，全量表日更新')

;
INSERT OVERWRITE TABLE amz.dim_adv_keyword_target_status_df PARTITION (ds = '20240823')
SELECT
      a.tenant_id
     ,a.profile_id
     ,a.campaign_id
     ,a.ad_group_id
     ,a.ad_type
     ,a.keyword_id
     ,a.keyword_text
     ,a.keyword_text
     ,null as parent_asin
     ,a.match_type
     ,a.status
     ,a.bid
     ,a.serving_status
     ,a.native_language_keyword
     ,a.create_datetime
     ,a.update_datetime
     ,'20240823' data_dt
     ,current_date() etl_data_dt
FROM
    (SELECT tenant_id
          ,profile_id
          ,ad_type
          ,keyword_id
          ,keyword_text
          ,match_type
          ,ad_group_id
          ,campaign_id
          ,status
          ,bid
          ,serving_status
          ,native_language_keyword
          ,create_datetime
          ,update_datetime
     FROM    (
                 SELECT  tenant_id
                      ,profile_id
                      ,ad_type
                      ,keyword_id
                      ,keyword_text
                      ,match_type
                      ,ad_group_id
                      ,campaign_id
                      ,status
                      ,bid
                      ,serving_status
                      ,native_language_keyword
                      ,create_datetime
                      ,update_datetime
                      ,ROW_NUMBER() OVER (PARTITION BY tenant_id,profile_id,campaign_id,ad_group_id,ad_type,keyword_id ORDER BY update_datetime DESC ) rn
                 FROM    (
                             SELECT  tenant_id
                                  ,profile_id
                                  ,ad_type
                                  ,keyword_id
                                  ,keyword_text
                                  ,match_type
                                  ,ad_group_id
                                  ,campaign_id
                                  ,status
                                  ,bid
                                  ,serving_status
                                  ,native_language_keyword
                                  ,create_datetime
                                  ,update_datetime
                             FROM   amz.dim_adv_keyword_target_status_df
                             WHERE   ds = '20240821'
                             UNION ALL
                             SELECT  tenant_id
                                  ,profile_id
                                  ,ad_type
                                  ,keyword_id
                                  ,keyword_text
                                  ,match_type
                                  ,ad_group_id
                                  ,campaign_id
                                  ,status
                                  ,cast(bid as DECIMAL(18,6))
                                  ,serving_status
                                  ,native_language_keyword
                                  ,create_datetime
                                  ,update_datetime
                             FROM    ods.ods_report_amzn_ad_keyword_data_df
                             WHERE   ds ='20240823'
                         ) t1
             ) t2
     WHERE   rn = 1
    )a
    LEFT JOIN (
        SELECT  tenant_id
             ,profile_id
             ,campaign_id
             ,ad_group_id
             ,top_cost_parent_asin parent_asin
        FROM    amz.mid_amazon_adv_sku_wide_d
        WHERE   ds = '20240822'
        GROUP BY tenant_id
               ,profile_id
               ,campaign_id
               ,ad_group_id
               ,top_cost_parent_asin

    ) b
    ON a.tenant_id=b.tenant_id
        AND a.profile_id=b.profile_id
        AND a.campaign_id=b.campaign_id
        AND a.ad_group_id=b.ad_group_id

;

select  count(1) from dim_adv_keyword_target_status_df;

select  * from dim_adv_keyword_target_status_df;

select
    tenant_id, profile_id, seller_id,  campaign_id, campaign_name, ad_group_id, ad_group_name,  advertised_asin, purchased_asin, advertised_sku, keyword_id, keyword, keyword_type
from amz.mid_amzn_sp_purchased_product_by_asin_report_ds where ds='20240801';




select
       a.tenant_id
     , a.profile_id
     , a.campaign_id
     , a.ad_group_id
     , g.parent_asin
     , sum(cost) over (partition by a.tenant_id, a.profile_id, a.campaign_id, a.ad_group_id, g.parent_asin) as cost_rank
FROM (
         SELECT tenant_id
              , profile_id
              , seller_id
              , campaign_id
              , ad_group_id
              , ad_group_name
              , advertised_asin
              , advertised_sku
              , cost
         FROM amz.mid_amzn_sp_advertised_product_by_advertiser_report_ds -- 9968
         WHERE ds >= '20240722'
           AND ds <= '20240823'                                            --只保存最近30天
     ) a
         LEFT JOIN (SELECT tenant_id
                         , profile_id
                         , marketplace_id
                         , marketplace_name
                         , timezone
                         , seller_id
                         , seller_name, ds
                    FROM dwd.dwd_base_seller_sites_store_df
                    WHERE ds = '20240809'
) b
     ON a.profile_id = b.profile_id AND a.tenant_id = b.tenant_id
  LEFT JOIN (
     select * from
        (select * ,market_place_id marketplace_id,ROW_NUMBER() OVER (PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
         from amz.mid_amzn_asin_to_parent_df where ds ='20240823') t
     where rn =1 and parent_asin is not null
) g
      ON b.marketplace_id = g.marketplace_id
         AND a.advertised_asin = g.asin;




SELECT  tenant_id
     ,profile_id
     ,campaign_id
     ,ad_group_id
     ,top_cost_parent_asin parent_asin
FROM    amz.mid_amazon_adv_sku_wide_d
WHERE   ds = '20240822'
GROUP BY tenant_id
       ,profile_id
       ,campaign_id
       ,ad_group_id
       ,top_cost_parent_asin