--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-07 01:32:25
--********************************************************************--
CREATE TABLE IF NOT EXISTS whde.ods_get_ledger_detail_view_data
(
    report_type            STRING COMMENT '报告类型'
    ,store_id              STRING COMMENT 'ERP店铺ID'
    ,seller_id             STRING COMMENT '卖家ID'
    ,marketplace_id        STRING COMMENT '站点ID'
    ,report_id             STRING COMMENT '报告ID'
    ,start_date            STRING COMMENT '报告开始时间'
    ,end_date              STRING COMMENT '报告结束时间'
    ,data_last_update_time STRING COMMENT '报告数据亚马逊生产时间'
    ,operation_date        STRING COMMENT '数据日期'
    ,fnsku                 STRING COMMENT '亚马逊为储存在亚马逊运营中心并从亚马逊运营中心配送的商品分配的唯一标识。'
    ,asin                  STRING COMMENT '由 10 个字母或数字组成，用于标识商品的唯一序列。ASIN 由亚马逊分配。您可以在商品详情页面找到商品的 ASIN'
    ,msku                  STRING COMMENT 'msku'
    ,title                 STRING COMMENT '商品的名称'
    ,event_type            STRING COMMENT '导致库存发生变化的动作类型（如配送、接收、供应商退货、库房转运、盘点或买家退货）'
    ,reference_id          STRING COMMENT '交易编号（如货件编号或盘点编号）'
    ,quantity              STRING COMMENT '交易的商品数量'
    ,fulfillmentcenter     STRING COMMENT '营运中心'
    ,disposition           STRING COMMENT '商品的状态（如可售或已残损）'
    ,reason                STRING COMMENT '下载的报告显示原因代码，在线报告则显示具体描述。查看下面的【盘点类型和原因代码】表，了解完整的代码和描述'
    ,reconciled_quantity   STRING COMMENT '已通过其他盘点动作进行调整的商品数量'
    ,unreconciled_quantity STRING COMMENT '未通过其他盘点动作进行调整的商品数量'
    ,left_quantity         STRING COMMENT '剩余数量'
    ,tenant_id             STRING COMMENT '租户ID'
    ,country               STRING COMMENT '国家'
)
    PARTITIONED BY
(
    ds                     STRING
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment' = '亚马逊库存分类账详细视图报告')
    LIFECYCLE 365
;