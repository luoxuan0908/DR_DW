--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-10 23:51:56
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.dwd_amzn_parent_asin_inf0_df
(

        id	bigint	 comment '自增主键id'
        ,market_place_id	string	 comment '市场id'
        ,data_date	datetime	 comment '排行榜日期'
        ,parent_asin	string	 comment '父asin'
        ,link	string	 comment '商品链接'
        ,brand	string	 comment '店铺或品牌名'
        ,scribing_price	string	 comment '划线价'
        ,selling_price	string	 comment '售价'
        ,reviews_ratings	bigint	 comment '评论数量'
        ,reviews_stars	decimal(18,6)	 comment '评分'
        ,reviews_distribution_detail	string	 comment '各星级评论所占百分比'
        ,answered_questions	bigint	 comment '问答数量'
        ,ships_from	string	 comment '第一个asin的发货位置'
        ,sold_by	string	 comment '第一个子asin的供货商'
        ,fit_info	string	 comment '尺寸信息'
        ,fit_detail	string	 comment '尺寸分布数量'
        ,sellers_rank	bigint	 comment '一级分类排名'
        ,sellers_rank_category	string	 comment '一级分类名称'
        ,sellers_rank_last_detail	string	 comment '小类排名情况'
        ,date_first_available	datetime	 comment '上架时间'
        ,package_dimensions	string	 comment '尺寸详情'
        ,breadcrumbs_feature	string	 comment '面包屑导航'
        ,title	string	 comment '标题'
        ,main_image_url	string	 comment '主图链接'
        ,local_image_url	string	 comment '转存后的图片链接'
        ,description	string	 comment '商品说明'
        ,is_available	bigint	 comment '商品是否正常在售'
        ,created_at	datetime	 comment '创建时间'
        ,updated_at	datetime	 comment '修改时间'
        ,coupon	string	 comment '优惠信息'
        ,seller_id	string	 comment '卖家id'
        ,is_load_full	bigint	 comment '页面是否加载完全'
        ,dim_asin	string	 comment '默认asin'
        ,immutable_params	string	 comment '异步请求参数'
        ,product_facts_detail	string	 comment '商品详细属性信息'
        ,child_asin	string	 comment '子asin'
        ,product_overview_feature	string	 comment '五点描述详情'
        ,product_information	string	 comment '商品标签集'
        ,product_description	string	 comment '描述详情'
        ,ratings_by_feature	string	 comment '功能点评分'
        ,product_description_img	string	 comment '描述详情里的图片链接'
        ,delivery_charges	  string comment	'运费'
)
PARTITIONED BY
(
    ds                     STRING
)
STORED AS ALIORC
TBLPROPERTIES ('comment' = '商品详情表')
LIFECYCLE 365
;

SELECT
  id,
  task_id,
  market_place_id,
  data_date,
  parent_asin,
  link,
  brand,
  scribing_price,
  selling_price,
  reviews_ratings,
  reviews_stars,
  reviews_distribution_detail,
  answered_questions,
  ships_from,
  sold_by,
  fit_info,
  fit_detail,
  sellers_rank,
  sellers_rank_last_detail,
  sellers_rank_href,
  date_first_available,
  package_dimensions,
  breadcrumbs_feature,
  title,
  main_image_url,
  local_image_url,
  description,
  is_available,
  created_at,
  updated_at,
  coupon,
  seller_id,
  is_load_full,
  sellers_rank_category,
  dim_asin,
  immutable_params,
  product_facts_detail,
  child_asin,
  product_overview_feature,
  product_information,
  product_description,
  ratings_by_feature,
  product_description_img,
  delivery_charges,
  pt
 FROM whde.amazon_product_details
 WHERE pt = '20240409'