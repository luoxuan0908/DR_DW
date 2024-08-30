
CREATE TABLE IF NOT EXISTS amz.dim_base_tenant_info_df(
     tenant_id STRING COMMENT '租户id',
     tenant_name STRING COMMENT '租户名',
     is_enabled BIGINT COMMENT '是否启用:0,禁用;1,启用',
     remark STRING COMMENT '备注'
) PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='租户维表')
    ;


INSERT OVERWRITE TABLE amz.dim_base_tenant_info_df PARTITION(ds='${last_day}')
select distinct tenant_id,company_name,1,'实验客户'
from ods.ods_report_user_info_df
where ds = '${last_day}'
;

