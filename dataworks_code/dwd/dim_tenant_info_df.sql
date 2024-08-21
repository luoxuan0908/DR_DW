--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:35:45
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dim_tenant_info_df(
  tenant_id STRING COMMENT '租户id',
  tenant_name STRING COMMENT '租户名',
  is_enabled BIGINT COMMENT '是否启用:0,禁用;1,启用',
  remark STRING COMMENT '备注'
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='租户维表')
    LIFECYCLE 1000;

ALTER  table dim_tenant_info_df set LIFECYCLE 1000;

INSERT OVERWRITE TABLE dim_tenant_info_df
select distinct tenant_id,company_name,company_name,1,'实验客户'
from user_info
where pt = '${bizdate}'
;

