--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-09 00:20:56
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dim_user_permission_info_df
(
    id	string	 comment '主键'
    ,tenant_id	string	 comment '租户ID'
    ,tenant_name	string	 comment '租户名称'
    ,market_place_id	string	 comment '市场ID'
    ,market_place_name	string	 comment '市场名称'
    ,adv_manager_id	string	 comment '广告负责人ID'
    ,adv_manager_name	string	 comment '广告负责人'
    ,adv_department_list_id	string	 comment '广告部门ID'
    ,adv_department_list_name	string	 comment '广告部门列表'
    ,seller_id	string	 comment '卖家ID'
    ,seller_name	string	 comment '卖家名称'
    ,profile_id	string	 comment '配置ID'
    ,parent_asin	string	 comment '父asin'
    ,category_list_id	string	 comment '商品类目ID'
    ,category_list_name	string	 comment '商品类目名称'
    ,is_enabled BIGINT COMMENT '是否启用:0,禁用;1,启用'
    ,remark STRING COMMENT '备注'
)
    STORED AS ALIORC
    LIFECYCLE 1000;




DROP TABLE IF EXISTS dim_user_permission_info_df_tmp1
;


CREATE TABLE dim_user_permission_info_df_tmp1 AS
SELECT  DISTINCT a.marketplace_id
               ,COALESCE(b.parent_asin,c.parent_asin) parent_asin
               ,manager_id
               ,country_code
FROM    asin_related_user a
            LEFT OUTER JOIN (select * from  (select *  ,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                             from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
                             where rn =1 and parent_asin is not null
) b
                            ON      a.asin = b.asin
                                AND     a.marketplace_id = b.market_place_id
            LEFT OUTER JOIN (
    SELECT  DISTINCT parent_asin
                   ,market_place_id
    FROM    dwd_amzn_asin_to_parent_df
    WHERE   ds = '${bizdate}'
) c
                            ON      a.asin = c.parent_asin
                                AND     a.marketplace_id = c.market_place_id
WHERE   a.pt = '${bizdate}'
  AND     a.asin <> ''
;

INSERT OVERWRITE TABLE dim_user_permission_info_df
SELECT  DISTINCT abs(HASH(a.tenant_id,c.profile_id,a.parent_asin,e.manager_id)) id
               ,a.tenant_id
               ,d.company_name
               ,a.marketplace_id
               ,c.marketplace_name
               ,e.manager_id
               ,d.username
               ,abs(HASH(d.org_list))
               ,d.org_list
               ,c.seller_id
               ,c.seller_name
               ,c.profile_id
               ,a.parent_asin
               ,''
               ,breadcrumbs_feature
               ,CASE   WHEN e.manager_id IS NOT NULL THEN 1
                       ELSE 0
    END
               ,''
FROM    (
            SELECT  *
            FROM    whde.dws_itm_spu_amazon_parent_asin_index_df
            WHERE   ds = '${bizdate}'
        ) a
            LEFT OUTER JOIN (
    SELECT  *
    FROM    dwd_sit_shp_amazon_seller_sites_store_df
    WHERE   ds = '${bizdate}'
) C
                            ON      a.tenant_id = c.tenant_id
                                AND     a.marketplace_id = c.marketplace_id
                                AND     a.seller_id = c.seller_id
            LEFT OUTER JOIN (select * from dim_user_permission_info_df_tmp1 where parent_asin is not null )e
                            ON      a.marketplace_id = e.marketplace_id
                                AND     a.parent_asin = e.parent_asin
            LEFT OUTER JOIN (
    SELECT  DISTINCT tenant_id
                   ,company_name
                   ,username
                   ,cast(store_id as string )store_id
                   ,first_org
                   ,second_org
                   ,third_org
                   ,CASE   WHEN third_org <> '' AND third_org IS NOT NULL THEN CONCAT(first_org,'>',second_org,'>',third_org)
                           WHEN second_org <> '' AND second_org IS NOT NULL THEN CONCAT(first_org,'>',second_org)
                           ELSE first_org
        END org_list
    FROM    user_info
    WHERE   pt = '${bizdate}'
      AND     status = 1

) d
                            ON      a.tenant_id = d.tenant_id
                                AND     e.manager_id = d.store_id
;

