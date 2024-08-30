
CREATE TABLE IF NOT EXISTS amz.dws_stg_parent_asin_index_ds
(
    tenant_id                STRING COMMENT '租户ID'
    ,profile_id               STRING COMMENT '配置ID'
    ,marketplace_id           STRING COMMENT '市场ID'
    ,seller_id                STRING COMMENT '卖家ID'
    ,adv_manager_id           STRING COMMENT '广告负责人ID'
    ,adv_manager_name         STRING COMMENT '广告负责人名称'
    ,top_parent_asin          STRING COMMENT '父aisn'
    ,fba_first_instock_time   timestamp COMMENT 'FBA首次入库时间'
    ,fba_first_instock_days   BIGINT COMMENT 'FBA首次入库距今天数'
    ,mature_label             STRING COMMENT '成熟期标志,1:成熟期'
    ,stock_sale_days          BIGINT COMMENT '父ASIN可售库存天数'
    ,stock_label              STRING COMMENT '库存成熟标志,1:库存充足'
    ,category                 STRING COMMENT '类目'
    ,create_time              timestamp COMMENT '创建时间'
    ,marketplace_name STRING comment '站点名称'
    ,self_category STRING comment '客户自定义类目'
    ,season_label STRING comment '季节标签'
    ,adv_start_date STRING comment '首次广告日期'
    ,adv_days bigint comment '首次广告至今天数'
    ,adv_weeks decimal(18,6) comment '首次广告至今周数'
    ,craw_post_code STRING comment '关键词爬虫邮编'
    )
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ORC
    TBLPROPERTIES ('comment' = '父ASIN基础指标(新）')

;


------------------------------------------算库存标签>>开始--------------------------------------------
--关联相关指标
drop table if exists dws_mkt_adv_strategy_parent_asin_index_tmp01_1;
create table dws_mkt_adv_strategy_parent_asin_index_tmp01_1 as
select tenant_id
     ,marketplace_id
     ,seller_id
     ,parent_asin
     ,asin
     ,color
     ,stock_sale_days
     ,all_sale_num
     ,row_number()over(partition by tenant_id,marketplace_id,seller_id,parent_asin,color order by all_sale_num desc ) as asin_rk
from (
         select q1.tenant_id
              ,q1.marketplace_id
              ,q1.seller_id
              ,q1.parent_asin
              ,q1.asin
              ,'' color
              ,q1.stock_sale_days
              ,q1.all_sale_num
         from (
                  select tenant_id
                       ,marketplace_id
                       ,concat(country_cn_name,'站') as marketplace_name
                       ,seller_id
                       ,parent_asin
                       ,asin
                       ,case when nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) = 0 then 0               --库存为0则库存天数为0
                             when nvl(sum(afnstock_n15d_avg_sale_num),0) = 0 then 999  --近15天销量为0则库存天数充足
                             else (nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) ) /nvl(sum(afnstock_n15d_avg_sale_num),0) end as stock_sale_days  --库存天数
                       ,nvl(sum(n30d_sale_num),0) as all_sale_num                               --近30天总销量
                  from  amz.dwd_prd_asin_sku_index_df -- whde.dws_itm_sku_amazon_asin_index_df
                  where ds = '${bizdate}'
                  group by  tenant_id
                         ,marketplace_id
                         ,concat(country_cn_name,'站')
                         ,seller_id
                         ,parent_asin
                         ,asin
              )q1
         --  left join (
         --             select concat(country,'站') as marketplace_name
         --                   ,parent_asin
         --                   ,asin
         --                   ,color
         --             from whde.ods_asq_jdy_asin_list
         --             where ds = max_pt('whde.ods_asq_jdy_asin_list')
         --             group by concat(country,'站')
         --                     ,parent_asin
         --                     ,asin
         --                     ,color
         --            ) q3
         --  on q1.marketplace_name = q3.marketplace_name and q1.parent_asin = q3.parent_asin and q1.asin = q3.asin
     ) s1
where color is not null
;

--改成通过第一热卖的asin库存判断，不再按照颜色判断
--select tenant_id
--       ,seller_id
--       ,marketplace_id
--       ,seller_sku
--       ,asin
--       ,parent_asin
--       ,ROW_NUMBER() OVER(PARTITION BY  tenant_id,seller_id,marketplace_id,parent_asin ORDER BY n30d_sale_num desc) AS rn
--from  whde.dws_itm_sku_amazon_asin_index_df
--where ds = '${bizdate}'
--and data_dt  = '${bizdate}'


--标记主色
drop table if exists dws_mkt_adv_strategy_parent_asin_index_tmp01_2;
create table dws_mkt_adv_strategy_parent_asin_index_tmp01_2 as
select tenant_id
     ,marketplace_id
     ,seller_id
     ,parent_asin
     ,color
     ,row_number()over(partition by tenant_id,marketplace_id,seller_id,parent_asin order by all_sale_num desc ) as color_rk
from (
         select tenant_id
              ,marketplace_id
              ,seller_id
              ,parent_asin
              ,color
              ,sum(all_sale_num) as all_sale_num
         from dws_mkt_adv_strategy_parent_asin_index_tmp01_1
         where color is not null
         group by tenant_id
                ,marketplace_id
                ,seller_id
                ,parent_asin
                ,color
     )q1
;


--标记主色热卖子ASIN的库存天数
drop table if exists dws_mkt_adv_strategy_parent_asin_index_tmp01_3;
create table dws_mkt_adv_strategy_parent_asin_index_tmp01_3 as
select q1.tenant_id
     ,q1.marketplace_id
     ,q1.seller_id
     ,q1.parent_asin
     ,min(case when q1.stock_sale_days >= 15 then 1 else 0 end) asin_stock_label  --三个主色热卖子asin库存都要大于15天
from dws_mkt_adv_strategy_parent_asin_index_tmp01_1  q1
         inner join dws_mkt_adv_strategy_parent_asin_index_tmp01_2 q2
                    on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.seller_id = q2.seller_id and q1.parent_asin = q2.parent_asin and q1.color = q2.color
where q2.color_rk = 1 and q1.asin_rk <= 3  --销量最高的花色，同时下属销量TOP3的子asin
group by q1.tenant_id
       ,q1.marketplace_id
       ,q1.seller_id
       ,q1.parent_asin
;



--父ASIN库存标签
drop table if exists dws_mkt_adv_strategy_parent_asin_index_tmp02_1;
create table dws_mkt_adv_strategy_parent_asin_index_tmp02_1 as
select tenant_id
     ,marketplace_id
     ,concat(cn_country_name,'站') as marketplace_name
     ,seller_id
     ,parent_asin
     ,case when length(breadcrumbs_category_one) > 0
    and length(breadcrumbs_category_two) > 0
    and length(breadcrumbs_category_three) > 0
    and length(breadcrumbs_category_four) > 0
    and length(breadcrumbs_category_five) > 0
    and length(breadcrumbs_category_six) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three,'>',breadcrumbs_category_four,'>',breadcrumbs_category_five,'>',breadcrumbs_category_six)
           when length(breadcrumbs_category_one) > 0
               and length(breadcrumbs_category_two) > 0
               and length(breadcrumbs_category_three) > 0
               and length(breadcrumbs_category_four) > 0
               and length(breadcrumbs_category_five) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three,'>',breadcrumbs_category_four,'>',breadcrumbs_category_five)
           when length(breadcrumbs_category_one) > 0
               and length(breadcrumbs_category_two) > 0
               and length(breadcrumbs_category_three) > 0
               and length(breadcrumbs_category_four) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three,'>',breadcrumbs_category_four)
           when length(breadcrumbs_category_one) > 0
               and length(breadcrumbs_category_two) > 0
               and length(breadcrumbs_category_three) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three)
           when length(breadcrumbs_category_one) > 0
               and length(breadcrumbs_category_two) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two)
           when length(breadcrumbs_category_one) > 0 then breadcrumbs_category_one else breadcrumbs_feature end as category
     ,fba_first_instock_time
--      ,datediff(to_date('${bizdate}','yyyymmdd'),to_date(substr(fba_first_instock_time,1,10),'yyyy-mm-dd'),'dd') as fba_first_instock_days
    ,datediff(from_unixtime(unix_timestamp('${bizdate}', 'yyyymmdd'), 'yyyy-MM-dd'),substr(fba_first_instock_time, 1, 10) ) as fba_first_instock_days
     ,case when nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) = 0 then 0               --库存为0则库存天数为0
           when nvl(sum(afnstock_n15d_avg_sale_num),0) = 0 then 999  --近15天销量为0则库存天数充足
           else (nvl(sum(afn_fulfillable_num),0) + nvl(sum(afn_reserved_fc_transfers_num),0) + nvl(sum(afn_reserved_fc_processing_num),0) ) /nvl(sum(afnstock_n15d_avg_sale_num),0) end as stock_sale_days  --库存天数                              --近30天总销量
from amz.dwd_prd_parent_asin_index_df -- whde.dws_itm_spu_amazon_parent_asin_index_df
where ds = '${bizdate}'
group by  tenant_id
       ,marketplace_id
       ,concat(cn_country_name,'站')
       ,seller_id
       ,parent_asin
       ,case when length(breadcrumbs_category_one) > 0
    and length(breadcrumbs_category_two) > 0
    and length(breadcrumbs_category_three) > 0
    and length(breadcrumbs_category_four) > 0
    and length(breadcrumbs_category_five) > 0
    and length(breadcrumbs_category_six) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three,'>',breadcrumbs_category_four,'>',breadcrumbs_category_five,'>',breadcrumbs_category_six)
             when length(breadcrumbs_category_one) > 0
                 and length(breadcrumbs_category_two) > 0
                 and length(breadcrumbs_category_three) > 0
                 and length(breadcrumbs_category_four) > 0
                 and length(breadcrumbs_category_five) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three,'>',breadcrumbs_category_four,'>',breadcrumbs_category_five)
             when length(breadcrumbs_category_one) > 0
                 and length(breadcrumbs_category_two) > 0
                 and length(breadcrumbs_category_three) > 0
                 and length(breadcrumbs_category_four) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three,'>',breadcrumbs_category_four)
             when length(breadcrumbs_category_one) > 0
                 and length(breadcrumbs_category_two) > 0
                 and length(breadcrumbs_category_three) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two,'>',breadcrumbs_category_three)
             when length(breadcrumbs_category_one) > 0
                 and length(breadcrumbs_category_two) > 0 then concat(breadcrumbs_category_one,'>',breadcrumbs_category_two)
             when length(breadcrumbs_category_one) > 0 then breadcrumbs_category_one else breadcrumbs_feature end
       ,fba_first_instock_time
--        ,datediff(to_date('${bizdate}','yyyymmdd'),to_date(substr(fba_first_instock_time,1,10),'yyyy-mm-dd'),'dd')
    ,datediff(from_unixtime(unix_timestamp('${bizdate}', 'yyyymmdd'), 'yyyy-MM-dd'),substr(fba_first_instock_time, 1, 10) )
;


--综合库存标签、成熟期标签、季节标签
drop table if exists dws_mkt_adv_strategy_parent_asin_index_tmp02_2;
create table dws_mkt_adv_strategy_parent_asin_index_tmp02_2 as
select q1.tenant_id
     ,q3.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.parent_asin
     ,q1.category
     ,q4.series  as self_category
     ,q1.fba_first_instock_time
     ,q1.fba_first_instock_days
     --,case when q1.fba_first_instock_days is null then null
     --      when q1.fba_first_instock_days > 180 then 1
     --     else  0 end as
     ,1 mature_label
     ,q1.stock_sale_days
     ,case when q1.stock_sale_days >= 30 and q2.asin_stock_label = 1 then 1
           else 0 end as stock_label
     ,'四季' season_label
     ,q4.postcode as craw_post_code   --对应商品的关键词爬虫邮编
from dws_mkt_adv_strategy_parent_asin_index_tmp02_1 q1
         left join dws_mkt_adv_strategy_parent_asin_index_tmp01_3 q2  --库存判断条件1
                   on q1.tenant_id = q2.tenant_id and q1.marketplace_id = q2.marketplace_id and q1.seller_id = q2.seller_id and q1.parent_asin = q2.parent_asin
         inner join (
    select tenant_id
         ,profile_id
         ,marketplace_id
         ,seller_id
    from  amz.dim_base_seller_sites_store_df -- dwd_sit_shp_amazon_seller_sites_store_df            --授权店铺清单
    where ds ='20240828' --  max_pt('whde.dwd_sit_shp_amazon_seller_sites_store_df ')
      and profile_id is not null
    group by tenant_id
           ,profile_id
           ,marketplace_id
           ,seller_id
)q3
                    on q1.tenant_id = q3.tenant_id and q1.marketplace_id = q3.marketplace_id and q1.seller_id = q3.seller_id
         left join (
    select concat(case when country = '阿联酋' then '阿拉伯联合酋长国' else country end,'站') as marketplace_name
         ,parent_asin
         ,max(series) as series
         ,case when max(season) = '四季' then '季中'
               when max(season) = '春夏季' and month(date_format('${bizdate}','yyyymmdd')) = 2 then '季前'
                       when max(season) = '春夏季' and month(date_format('${bizdate}','yyyymmdd')) between 3 and 7 then '季中'
                       when max(season) = '春夏季' and month(date_format('${bizdate}','yyyymmdd')) > 7 then '过季'
                       when max(season) = '秋冬季' and month(date_format('${bizdate}','yyyymmdd')) = 8 then '季前'
                       when max(season) = '秋冬季' and month(date_format('${bizdate}','yyyymmdd')) between 9 and 12 then '季中'
                       when max(season) = '秋冬季' and month(date_format('${bizdate}','yyyymmdd')) < 8 then '过季' end as season_label
                 ,case when max(season) = '秋冬季' then '10001' when max(season) = '春夏季' then '94513' when max(season) = '四季' then '33166' end as postcode
           from  amz.dim_base_asq_jdy_asin_list -- ods_asq_jdy_asin_list      --自带品类
           where ds is not null  and nvl(series,'') <> ''
           group by concat(case when country = '阿联酋' then '阿拉伯联合酋长国' else country end,'站')
                   ,parent_asin
          )q4
on q1.marketplace_name = q4.marketplace_name and q1.parent_asin = q4.parent_asin
;

------------------------------------------算打广告时间>>开始--------------------------------------------

drop table if exists dws_mkt_adv_strategy_parent_asin_index_tmp03;
create table dws_mkt_adv_strategy_parent_asin_index_tmp03 as
select s1.tenant_id
     ,s1.marketplace_id
     ,s1.parent_asin
     ,date_format(s1.create_date,'yyyymmdd') as adv_start_date
     ,datediff(date_format('${bizdate}','yyyymmdd'),s1.create_date) as adv_days
     ,round(datediff(date_format('${bizdate}','yyyymmdd'),s1.create_date) / 7,1) as adv_weeks
from (
         select q1.tenant_id
              ,q3.marketplace_id
              ,q3.parent_asin
              ,min(q1.create_date) as create_date
         from (
                  select tenant_id
                       ,profile_id
                       ,sku
                       ,asin
                       ,create_date
                  from amz.dim_adv_pro_product_status_df  --  adm_amazon_adv_pro_product_status_df
                  where ds ='${bizdate}'
              )q1
                  inner join (
             select tenant_id
                  ,profile_id
                  ,marketplace_id
                  ,seller_id
             from amz.dim_base_seller_sites_store_df -- dwd_sit_shp_amazon_seller_sites_store_df            --授权店铺清单
             where ds = '${bizdate}' -- max_pt('whde.dwd_sit_shp_amazon_seller_sites_store_df ')
               and profile_id is not null
             group by tenant_id
                    ,profile_id
                    ,marketplace_id
                    ,seller_id
         )q2
                             on q1.tenant_id = q2.tenant_id and q1.profile_id = q2.profile_id
                  inner join(select * from  (select *,market_place_id marketplace_id,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
                                             from amz.mid_amzn_asin_to_parent_df where ds ='${bizdate}') t
                             where rn =1
         )q3
                            on q2.marketplace_id = q3.marketplace_id and q1.asin = q3.asin
         group by q1.tenant_id
                ,q3.marketplace_id
                ,q3.parent_asin
     )s1
;


--支持重跑
alter table amz.dws_stg_parent_asin_index_ds drop if exists partition (ds = '${bizdate}');

--插入最新数据
insert into table amz.dws_stg_parent_asin_index_ds partition (ds = '${bizdate}')
(
 tenant_id
,profile_id
,marketplace_id
,marketplace_name
,seller_id
,adv_manager_id
,adv_manager_name
,top_parent_asin
,fba_first_instock_time
,fba_first_instock_days
,mature_label
,stock_sale_days
,stock_label
,category
,self_category
,create_time
,season_label
,craw_post_code
,adv_start_date
,adv_days
,adv_weeks
)
select  q1.tenant_id
     ,q1.profile_id
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q2.adv_manager_id
     ,q2.adv_manager_name
     ,q2.parent_asin top_parent_asin
     ,q1.fba_first_instock_time
     ,q1.fba_first_instock_days
     ,q1.mature_label
     ,cast(q1.stock_sale_days as bigint ) as stock_sale_days
     ,q1.stock_label
     ,q1.category
     ,q1.category self_category
     ,current_date() as create_time
     ,q1.season_label
     ,q1.craw_post_code
     ,q3.adv_start_date
     ,q3.adv_days
     ,q3.adv_weeks
from dws_mkt_adv_strategy_parent_asin_index_tmp02_2 q1
         inner join amz.dim_user_permission_info_df q2 -- dim_user_permission_info_df  q2  --dws_mkt_adv_asq_parent_asin_manager_ds q2
                    on q1.marketplace_id = q2.market_place_id and q1.seller_id = q2.seller_id and q1.parent_asin = q2.parent_asin
         left join dws_mkt_adv_strategy_parent_asin_index_tmp03 q3
                   on q1.tenant_id = q3.tenant_id and q1.marketplace_id = q3.marketplace_id and q1.parent_asin = q3.parent_asin
;
