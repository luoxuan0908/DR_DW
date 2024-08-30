CREATE TABLE IF NOT EXISTS amz.dim_base_asq_jdy_asin_list(
   seller_sku STRING COMMENT 'seller-sku',
   sku STRING COMMENT 'sku',
   parent_style STRING COMMENT '父体链接货号',
   style STRING COMMENT '子体货号',
   shop STRING COMMENT '店铺',
   color STRING COMMENT '颜色',
   cn_color STRING COMMENT '中文颜色',
   parent_asin STRING COMMENT '父ASIN',
   asin STRING COMMENT 'ASIN',
   fnsku STRING COMMENT 'FNSKU',
   size STRING COMMENT '尺码',
   country STRING COMMENT '国家',
   series STRING COMMENT '系列',
   brand STRING COMMENT '品牌',
   season STRING COMMENT '季节',
   hot_sale_start STRING COMMENT '热卖期-开始',
   hot_sale_end STRING COMMENT '热卖期-结束',
   hot_sale STRING COMMENT '热卖期-月份',
   creator STRING COMMENT 'creator',
   updater STRING COMMENT 'updater',
   deleter STRING COMMENT 'deleter',
   createtime STRING COMMENT 'createTime',
   updatetime STRING COMMENT 'updateTime',
   deletetime STRING COMMENT 'deleteTime'
)
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='asin表')
  ;