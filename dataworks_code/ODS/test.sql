--odps sql
--********************************************************************--
--author:Ada
--create time:2024-04-06 21:40:21
--********************************************************************--

select * from ods_amzn_sp_campaigns_by_campaign_placement_report
where ds is not null
;

select * from amazon_product_details
where pt = max_pt("amazon_product_details")
--and to_char(data_date,'yyyymmdd') = '20240417'
order by data_date desc
    limit 100
;

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
    SUM(CASE WHEN task_id IS NULL THEN 1 ELSE 0 END) AS null_task_id,
    SUM(CASE WHEN market_place_id IS NULL THEN 1 ELSE 0 END) AS null_market_place_id,
    SUM(CASE WHEN data_date IS NULL THEN 1 ELSE 0 END) AS null_data_date,
    SUM(CASE WHEN parent_asin IS NULL  or TRIM(parent_asin) = '' THEN 1 ELSE 0 END) AS null_parent_asin,
    SUM(CASE WHEN link IS NULL  or TRIM(link) = ''  THEN 1 ELSE 0 END) AS null_link,
    SUM(CASE WHEN brand IS NULL  or TRIM(brand) = ''  THEN 1 ELSE 0 END) AS null_brand,
    SUM(CASE WHEN scribing_price IS NULL THEN 1 ELSE 0 END) AS null_scribing_price,
    SUM(CASE WHEN selling_price IS NULL THEN 1 ELSE 0 END) AS null_selling_price,
    SUM(CASE WHEN reviews_ratings IS NULL THEN 1 ELSE 0 END) AS null_reviews_ratings,
    SUM(CASE WHEN reviews_stars IS NULL THEN 1 ELSE 0 END) AS null_reviews_stars,
    SUM(CASE WHEN reviews_distribution_detail IS NULL THEN 1 ELSE 0 END) AS null_reviews_distribution_detail,
    SUM(CASE WHEN answered_questions IS NULL THEN 1 ELSE 0 END) AS null_answered_questions,
    SUM(CASE WHEN ships_from IS NULL  or TRIM(ships_from) = '' THEN 1 ELSE 0 END) AS null_ships_from,
    SUM(CASE WHEN sold_by IS NULL or TRIM(sold_by) = '' THEN 1 ELSE 0 END) AS null_sold_by,
    SUM(CASE WHEN fit_info IS NULL THEN 1 ELSE 0 END) AS null_fit_info,
    SUM(CASE WHEN fit_detail IS NULL THEN 1 ELSE 0 END) AS null_fit_detail,
    SUM(CASE WHEN sellers_rank IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank,
    SUM(CASE WHEN sellers_rank_last_detail IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank_last_detail,
    SUM(CASE WHEN sellers_rank_href IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank_href,
    SUM(CASE WHEN date_first_available IS NULL THEN 1 ELSE 0 END) AS null_date_first_available,
    SUM(CASE WHEN package_dimensions IS NULL THEN 1 ELSE 0 END) AS null_package_dimensions,
    SUM(CASE WHEN breadcrumbs_feature IS NULL THEN 1 ELSE 0 END) AS null_breadcrumbs_feature,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_title,
    SUM(CASE WHEN main_image_url IS NULL THEN 1 ELSE 0 END) AS null_main_image_url,
    SUM(CASE WHEN local_image_url IS NULL THEN 1 ELSE 0 END) AS null_local_image_url,
    SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS null_description,
    SUM(CASE WHEN is_available IS NULL THEN 1 ELSE 0 END) AS null_is_available,
    SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS null_created_at,
    SUM(CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END) AS null_updated_at,
    SUM(CASE WHEN coupon IS NULL THEN 1 ELSE 0 END) AS null_coupon,
    SUM(CASE WHEN seller_id IS NULL   or TRIM(seller_id) = ''  THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN is_load_full IS NULL THEN 1 ELSE 0 END) AS null_is_load_full,
    SUM(CASE WHEN sellers_rank_category IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank_category,
    SUM(CASE WHEN dim_asin IS NULL THEN 1 ELSE 0 END) AS null_dim_asin,
    SUM(CASE WHEN immutable_params IS NULL THEN 1 ELSE 0 END) AS null_immutable_params,
    SUM(CASE WHEN product_facts_detail IS NULL THEN 1 ELSE 0 END) AS null_product_facts_detail,
    SUM(CASE WHEN child_asin IS NULL  or TRIM(child_asin) = ''  THEN 1 ELSE 0 END) AS null_child_asin,
    SUM(CASE WHEN product_overview_feature IS NULL THEN 1 ELSE 0 END) AS null_product_overview_feature,
    SUM(CASE WHEN product_information IS NULL THEN 1 ELSE 0 END) AS null_product_information,
    SUM(CASE WHEN product_description IS NULL THEN 1 ELSE 0 END) AS null_product_description,
    SUM(CASE WHEN ratings_by_feature IS NULL THEN 1 ELSE 0 END) AS null_ratings_by_feature,
    SUM(CASE WHEN product_description_img IS NULL THEN 1 ELSE 0 END) AS null_product_description_img,
    SUM(CASE WHEN delivery_charges IS NULL THEN 1 ELSE 0 END) AS null_delivery_charges
FROM whde.amazon_product_details
where pt = '20240416'
;

select  *
FROM whde.amazon_product_details
where pt = '20240417'
    limit 1000
;
SELECT COUNT(*) AS total_duplicates
FROM (
         SELECT *, COUNT(*) AS count
         FROM whde.amazon_product_details
         where  pt = '20240417'
         GROUP BY id, task_id, market_place_id, data_date, parent_asin, link, brand,
             scribing_price, selling_price, reviews_ratings, reviews_stars,
             reviews_distribution_detail, answered_questions, ships_from, sold_by,
             fit_info, fit_detail, sellers_rank, sellers_rank_last_detail,
             sellers_rank_href, date_first_available, package_dimensions,
             breadcrumbs_feature, title, main_image_url, local_image_url,
             description, is_available, created_at, updated_at, coupon, seller_id,
             is_load_full, sellers_rank_category, dim_asin, immutable_params,
             product_facts_detail, child_asin, product_overview_feature,
             product_information, product_description, ratings_by_feature,
             product_description_img, delivery_charges, pt
         HAVING count > 1
     ) t;

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
    SUM(CASE WHEN task_id IS NULL THEN 1 ELSE 0 END) AS null_task_id,
    SUM(CASE WHEN market_place_id IS NULL THEN 1 ELSE 0 END) AS null_market_place_id,
    SUM(CASE WHEN data_date IS NULL THEN 1 ELSE 0 END) AS null_data_date,
    SUM(CASE WHEN parent_asin IS NULL THEN 1 ELSE 0 END) AS null_parent_asin,
    SUM(CASE WHEN link IS NULL THEN 1 ELSE 0 END) AS null_link,
    SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) AS null_brand,
    SUM(CASE WHEN scribing_price IS NULL THEN 1 ELSE 0 END) AS null_scribing_price,
    SUM(CASE WHEN selling_price IS NULL THEN 1 ELSE 0 END) AS null_selling_price,
    SUM(CASE WHEN reviews_ratings IS NULL THEN 1 ELSE 0 END) AS null_reviews_ratings,
    SUM(CASE WHEN reviews_stars IS NULL THEN 1 ELSE 0 END) AS null_reviews_stars,
    SUM(CASE WHEN reviews_distribution_detail IS NULL THEN 1 ELSE 0 END) AS null_reviews_distribution_detail,
    SUM(CASE WHEN answered_questions IS NULL THEN 1 ELSE 0 END) AS null_answered_questions,
    SUM(CASE WHEN ships_from IS NULL THEN 1 ELSE 0 END) AS null_ships_from,
    SUM(CASE WHEN sold_by IS NULL THEN 1 ELSE 0 END) AS null_sold_by,
    SUM(CASE WHEN fit_info IS NULL THEN 1 ELSE 0 END) AS null_fit_info,
    SUM(CASE WHEN fit_detail IS NULL THEN 1 ELSE 0 END) AS null_fit_detail,
    SUM(CASE WHEN sellers_rank IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank,
    SUM(CASE WHEN sellers_rank_last_detail IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank_last_detail,
    SUM(CASE WHEN sellers_rank_href IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank_href,
    SUM(CASE WHEN date_first_available IS NULL THEN 1 ELSE 0 END) AS null_date_first_available,
    SUM(CASE WHEN package_dimensions IS NULL THEN 1 ELSE 0 END) AS null_package_dimensions,
    SUM(CASE WHEN breadcrumbs_feature IS NULL THEN 1 ELSE 0 END) AS null_breadcrumbs_feature,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_title,
    SUM(CASE WHEN main_image_url IS NULL THEN 1 ELSE 0 END) AS null_main_image_url,
    SUM(CASE WHEN local_image_url IS NULL THEN 1 ELSE 0 END) AS null_local_image_url,
    SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS null_description,
    SUM(CASE WHEN is_available IS NULL THEN 1 ELSE 0 END) AS null_is_available,
    SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS null_created_at,
    SUM(CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END) AS null_updated_at,
    SUM(CASE WHEN coupon IS NULL THEN 1 ELSE 0 END) AS null_coupon,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN is_load_full IS NULL THEN 1 ELSE 0 END) AS null_is_load_full,
    SUM(CASE WHEN sellers_rank_category IS NULL THEN 1 ELSE 0 END) AS null_sellers_rank_category,
    SUM(CASE WHEN dim_asin IS NULL THEN 1 ELSE 0 END) AS null_dim_asin,
    SUM(CASE WHEN immutable_params IS NULL THEN 1 ELSE 0 END) AS null_immutable_params,
    SUM(CASE WHEN product_facts_detail IS NULL THEN 1 ELSE 0 END) AS null_product_facts_detail,
    SUM(CASE WHEN child_asin IS NULL THEN 1 ELSE 0 END) AS null_child_asin,
    SUM(CASE WHEN product_overview_feature IS NULL THEN 1 ELSE 0 END) AS null_product_overview_feature,
    SUM(CASE WHEN product_information IS NULL THEN 1 ELSE 0 END) AS null_product_information,
    SUM(CASE WHEN product_description IS NULL THEN 1 ELSE 0 END) AS null_product_description,
    SUM(CASE WHEN ratings_by_feature IS NULL THEN 1 ELSE 0 END) AS null_ratings_by_feature,
    SUM(CASE WHEN product_description_img IS NULL THEN 1 ELSE 0 END) AS null_product_description_img,
    SUM(CASE WHEN delivery_charges IS NULL THEN 1 ELSE 0 END) AS null_delivery_charges
FROM whde.amazon_product_details;