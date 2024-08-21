--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-30 00:35:42
--********************************************************************--
--odps sql
--********************************************************************--
--author:王敏佳
--create time:2024-01-05 14:39:14
--********************************************************************--
create table if not exists dws_mkt_adv_strategy_adjust_bid_result_hf
(
    tenant_id                STRING COMMENT '租户id'
    ,row_id                   STRING COMMENT 'row_id'
    ,profile_id               STRING COMMENT '配置id'
    ,adv_manager_id           STRING COMMENT '操作人员id'
    ,adv_manager_name         STRING COMMENT '操作人员名称'
    ,campaign_id              STRING COMMENT '广告活动id'
    ,campaign_name            STRING COMMENT '广告活动名称'
    ,ad_group_id              STRING COMMENT '广告组id'
    ,ad_group_name            STRING COMMENT '广告组名称'
    ,top_parent_asin          STRING COMMENT '父asin'
    ,term_type                STRING COMMENT '操作对象类型'
    ,target_id                STRING COMMENT '投放对象id'
    ,target_term              STRING COMMENT '投放对象'
    ,match_type               STRING COMMENT '匹配类型'
    ,norm_rank                STRING COMMENT '自然排名'
    ,adv_rank                 STRING COMMENT '广告排名'
    ,bid_adjust_label         STRING COMMENT '竞价调整类型'
    ,before_adjust_bid        STRING COMMENT '调整前竞价'
    ,adjust_bid               STRING COMMENT '目标竞价'
    ,after_adjust_bid         STRING COMMENT '调整后竞价'
    ,if_adjust_success        STRING COMMENT '是否调整成功'
    ,params                   STRING COMMENT '调整参数'
)
    partitioned by
(
    hs                  string
)
    stored as aliorc
    tblproperties ('comment' = '竞价调整结果记录')
    lifecycle 720
;
