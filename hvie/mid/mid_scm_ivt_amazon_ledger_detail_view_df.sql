--  dwd_scm_ivt_amazon_ledger_detail_view_df
CREATE TABLE IF NOT EXISTS amz.mid_scm_ivt_amazon_ledger_detail_view_df(
    report_type STRING COMMENT '报告类型',
    report_id STRING COMMENT '报告ID',
    start_time timestamp COMMENT '报告开始时间',
    end_time timestamp COMMENT '报告结束时间',
    data_last_update_time timestamp COMMENT '报告数据亚马逊生产时间',
    operation_date timestamp COMMENT '数据日期',
    ori_operation_date STRING COMMENT '原始数据日期',
    store_id BIGINT COMMENT 'ERP店铺ID',
    seller_id STRING COMMENT '卖家ID',
    marketplace_id STRING COMMENT '站点ID',
    ori_marketplace_id STRING COMMENT '原始站点ID',
    country_code STRING COMMENT '国家编码',
    fnsku STRING COMMENT '亚马逊fnsku',
    asin STRING COMMENT '子asin',
    seller_sku STRING COMMENT '亚马逊seller sku',
    title STRING COMMENT '商品的名称',
    event_type STRING COMMENT '导致库存发生变化的动作类型（如配送、接收、供应商退货、库房转运、盘点或买家退货）',
    reference_id STRING COMMENT '交易编号（如货件编号或盘点编号）',
    product_num BIGINT COMMENT '交易的商品数量',
    fulfillment_center STRING COMMENT '储存库存的运营中心',
    disposition STRING COMMENT '商品的状态（如可售或已残损）',
    reason STRING COMMENT '下载的报告显示原因代码，在线报告则显示具体描述。查看下面的【盘点类型和原因代码】表，了解完整的代码和描述',
    reconciled_num BIGINT COMMENT '已通过其他盘点动作进行调整的商品数量',
    unreconciled_num BIGINT COMMENT '未通过其他盘点动作进行调整的商品数量',
    record_id STRING COMMENT 'MySQL 记录表ID',
    tenant_id STRING COMMENT '租户ID',
    data_src STRING COMMENT '数据源名',
    table_src STRING COMMENT '来源表名',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt date COMMENT '数据加载日期'
)
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊库存分类账详细视图报告');

with tmp_dwd_scm_ivt_amazon_ledger_detail_view_df_tb1 as (
select report_type
     , report_id
     , start_time  ----时差转换，统一转成北京时间
     , end_time
     , data_last_update_time
     , nvl(operation_date, data_last_update_time) operation_date
     , ori_operation_date
     , store_id
     , seller_id
     , t2.marketplace_id
     , t1.marketplace_id as                       ori_marketplace_id

     , t1.country_code
     , fnsku
     , asin
     , seller_sku
     , title
     , event_type
     , reference_id
     , product_num
     , fulfillment_center
     , disposition
     , reason
     , reconciled_num
     , unreconciled_num
     , record_id   -- 'MySQL 记录表ID',
     , tenant_id   --'租户ID'
     , data_src    -- 数据源名
     , table_src   -- 来源表名
     , data_dt     -- '数据日期'
     , etl_data_dt -- '数据加载日期'

from (SELECT report_type
           , report_id
           , current_date()                                                                    AS start_time
           , current_date()                                                                    AS end_time
           , cast(data_last_update_time as timestamp)                                          AS data_last_update_time
           -- ,DATEADD(TO_DATE(operation_date,'yyyy-mm-ddThh:mi:ssZ'),8,'hh') AS  operation_date ----时差转换，统一转成北京时间
--            , DATE_FORMAT(
--             FROM_UNIXTIME(
--                     UNIX_TIMESTAMP(CONCAT(SUBSTR(operation_date, 1, 10), ' ', SUBSTR(operation_date, 12, 8)),
--                                    'yyyy-MM-dd HH:mm:ss') -
--                     (CAST(SUBSTR(operation_date, 20, 3) AS BIGINT) * 3600 +
--                      CAST(SUBSTR(operation_date, 23, 2) AS BIGINT) * 60) + 8 * 3600
--             ),
--             'yyyyMMdd HH:mm:ss'
--              )                                                                                 AS operation_date
          ,FROM_UNIXTIME(
              UNIX_TIMESTAMP(
                  CONCAT(
                      SUBSTR(operation_date, 1, 10),      -- 提取日期部分 (yyyy-MM-dd)
                       ' ',
                       SUBSTR(operation_date, 12, 8)       -- 提取时间部分 (HH:mm:ss)
                  ),
                    'yyyy-MM-dd HH:mm:ss'                   -- 转换为 Unix 时间戳
              )
           -
       -- 计算时区偏移量，将其转换为秒数并减去
           (
               CAST(SUBSTR(operation_date, 20, 3) AS BIGINT) * 3600  -- 小时部分的秒数
               +
               CAST(SUBSTR(operation_date, 23, 2) AS BIGINT) * 60    -- 分钟部分的秒数
           )
           + 8 * 3600                                                -- 加上 8 小时，调整为北京时间
           ) AS operation_date
           , operation_date                                                                    as ori_operation_date

           , CAST(CASE WHEN store_id <> '' then store_id else null end as BIGINT)              as store_id
           , seller_id
           , marketplace_id

           , IF(country = 'GB', 'UK', country)                                                 AS country_code -- 英国特殊处理 GB 替换成UK
           , fnsku
           , asin
           , msku                                                                              as seller_sku
           , title
           , event_type
           , reference_id
           , cast( quantity as BIGINT)              AS product_num
           , fulfillmentcenter                                                                 as fulfillment_center
           , disposition
           , reason
           , CASE WHEN reconciled_quantity is not null then quantity else null end    AS reconciled_num
           , CASE WHEN unreconciled_quantity is not null then quantity else null end  AS unreconciled_num
           , ''                                                                                   record_id    -- 'MySQL 记录表ID',
           , tenant_id                                                                                         --'租户ID'
           , 'report'                                                                          AS data_src     -- 数据源名
           , 'get_ledger_detail_view_data'                                                     AS table_src    -- 来源表名
           , '${last_day}'                                                                     AS data_dt      -- '数据日期'
           , current_date                                                                      AS etl_data_dt  -- '数据加载日期'

      FROM (SELECT case when report_type = '' then null else report_type end                            as report_type,
                   case when store_id = '' then null else store_id end                                  as store_id,
                   case when seller_id = '' then null else seller_id end                                as seller_id,
                   case when marketplace_id = '' then null else marketplace_id end                      as marketplace_id,
                   case when report_id = '' then null else report_id end                                as report_id,
                   case when start_date = '' then null else start_date end                              as start_date,
                   case when end_date = '' then null else end_date end                                  as end_date,
                   case
                       when data_last_update_time = '' then null
                       else data_last_update_time end                                                   as data_last_update_time,

                   case
                       when operation_date = '' then null
                       else replace(operation_date, '\\\"', '') end                                     as operation_date,
                   --case when operation_date = '' or operation_date is null then data_last_update_time end as operation_date ,
                   case when fnsku = '' then null else fnsku end                                        as fnsku,
                   case when asin = '' then null else asin end                                          as asin,
                   case when msku = '' then null else msku end                                          as msku,
                   case when title = '' then null else title end                                        as title,
                   case when event_type = '' then null else event_type end                              as event_type,
                   case when reference_id = '' then null else reference_id end                          as reference_id,
                   case when quantity = '' then null else quantity end                                  as quantity,
                   case when fulfillmentcenter = '' then null else fulfillmentcenter end                as fulfillmentcenter,
                   case when disposition = '' then null else disposition end                            as disposition,
                   case when reason = '' then null else reason end                                      as reason,
                   case when reconciled_quantity = '' then null else reconciled_quantity end            as reconciled_quantity,
                   case
                       when unreconciled_quantity = '' then null
                       else unreconciled_quantity end                                                   as unreconciled_quantity,
                   case when left_quantity = '' then null else left_quantity end                        as left_quantity,
                   case when tenant_id = '' then null else tenant_id end                                as tenant_id,
                   case when country = '' then null else country end                                    as country
            FROM ods.ods_report_get_ledger_detail_view_data_df -- whde.ods_get_ledger_detail_view_data
            WHERE ds = '${last_day}') t) t1
         left join
     (SELECT country_code
           , market_place_id marketplace_id
      from dim.dim_base_marketplace_info_df
      where ds = '${last_day}' --  whde.dim_marketplace_info_df
     ) t2
     on t1.country_code = t2.country_code
    ), tmp_dwd_scm_ivt_amazon_ledger_detail_view_df_tb2
AS (
select
    report_type
     ,report_id
     ,start_time
     ,end_time
     ,data_last_update_time
     ,operation_date
     ,ori_operation_date
     ,store_id
     ,seller_id
     ,marketplace_id
     ,ori_marketplace_id

     ,t1.country_code
     ,fnsku
     ,asin
     ,seller_sku
     ,title
     ,event_type
     ,reference_id
     ,product_num
     ,fulfillment_center
     ,disposition
     ,reason
     ,reconciled_num
     ,unreconciled_num
     ,record_id -- 'MySQL 记录表ID',
     ,tenant_id  --'租户ID'
     ,data_src -- 数据源名
     ,table_src -- 来源表名
     ,data_dt -- '数据日期'
     ,etl_data_dt -- '数据加载日期'
     ,ROW_NUMBER() over (partition by seller_id,store_id,fnsku,asin,seller_sku,event_type,operation_date,country_code,product_num,fulfillment_center,disposition,reason,tenant_id order by data_last_update_time desc,start_time desc,end_time desc ) rnk
from (

         select
             report_type
              ,report_id
              ,start_time
              ,end_time
              ,data_last_update_time
              ,cast(operation_date as timestamp) as operation_date
              ,ori_operation_date
              ,store_id
              ,seller_id
              ,marketplace_id
              ,ori_marketplace_id
              ,country_code
              ,fnsku
              ,asin
              ,seller_sku
              ,title
              ,event_type
              ,reference_id
              ,product_num
              ,fulfillment_center
              ,disposition
              ,reason
              ,reconciled_num
              ,unreconciled_num
              ,record_id -- 'MySQL 记录表ID',
              ,tenant_id  --'租户ID'
              ,data_src -- 数据源名
              ,table_src -- 来源表名
              ,data_dt -- '数据日期'
              ,etl_data_dt -- '数据加载日期'
         from tmp_dwd_scm_ivt_amazon_ledger_detail_view_df_tb1
         UNION ALL
         select
             report_type
              ,report_id
              ,start_time
              ,end_time
              ,data_last_update_time
              ,cast(operation_date as timestamp) as operation_date
              ,ori_operation_date
              ,store_id
              ,seller_id
              ,marketplace_id
              ,ori_marketplace_id

              ,country_code
              ,fnsku
              ,asin
              ,seller_sku
              ,title
              ,event_type
              ,reference_id
              ,product_num
              ,fulfillment_center
              ,disposition
              ,reason
              ,reconciled_num
              ,unreconciled_num
              ,record_id -- 'MySQL 记录表ID',
              ,tenant_id  --'租户ID'
              ,data_src -- 数据源名
              ,table_src -- 来源表名
              ,data_dt -- '数据日期'
              ,etl_data_dt -- '数据加载日期'
         from amz.mid_scm_ivt_amazon_ledger_detail_view_df
         where ds = '${last_2_day}' -- t-2

     )t1
)
INSERT OVERWRITE TABLE amz.mid_scm_ivt_amazon_ledger_detail_view_df PARTITION (ds = '${last_day}')
SELECT
    report_type
     ,report_id
     ,start_time
     ,end_time
     ,data_last_update_time
     ,operation_date
     ,ori_operation_date
     ,store_id
     ,seller_id
     ,marketplace_id
     ,ori_marketplace_id
     ,country_code
     ,fnsku
     ,asin
     ,seller_sku
     ,title
     ,event_type
     ,reference_id
     ,product_num
     ,fulfillment_center
     ,disposition
     ,reason
     ,reconciled_num
     ,unreconciled_num
     ,record_id -- 'MySQL 记录表ID',
     ,tenant_id  --'租户ID'
     ,data_src -- 数据源名
     ,table_src -- 来源表名
     ,data_dt -- '数据日期'
     ,etl_data_dt -- '数据加载日期'
from
    tmp_dwd_scm_ivt_amazon_ledger_detail_view_df_tb2
where rnk = 1


