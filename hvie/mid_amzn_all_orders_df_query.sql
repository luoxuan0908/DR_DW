
SELECT
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT marketplace_id) AS dis_marketplace_id,
    COUNT(DISTINCT marketplace_type) AS dis_marketplace_type,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT amazon_order_id) AS dis_amazon_order_id,
    COUNT(DISTINCT merchant_order_id) AS dis_merchant_order_id,
    COUNT(DISTINCT ori_purchase_time) AS dis_ori_purchase_time,
    COUNT(DISTINCT purchase_time) AS dis_purchase_time,
    COUNT(DISTINCT ori_last_update_time) AS dis_ori_last_update_time,
    COUNT(DISTINCT last_update_time) AS dis_last_update_time,
    COUNT(DISTINCT order_status) AS dis_order_status,
    COUNT(DISTINCT fulfillment_channel) AS dis_fulfillment_channel,
    COUNT(DISTINCT sales_channel) AS dis_sales_channel,
    COUNT(DISTINCT order_channel) AS dis_order_channel,
    COUNT(DISTINCT ship_service_level) AS dis_ship_service_level,
    COUNT(DISTINCT product_name) AS dis_product_name,
    COUNT(DISTINCT seller_sku) AS dis_seller_sku,
    COUNT(DISTINCT asin) AS dis_asin,
    COUNT(DISTINCT item_status) AS dis_item_status,
    COUNT(DISTINCT ordered_num) AS dis_ordered_num,
    COUNT(DISTINCT currency) AS dis_currency,
    COUNT(DISTINCT item_amt) AS dis_item_amt,
    COUNT(DISTINCT item_tax) AS dis_item_tax,
    COUNT(DISTINCT shipping_fee) AS dis_shipping_fee,
    COUNT(DISTINCT shipping_tax) AS dis_shipping_tax,
    COUNT(DISTINCT gift_wrap_fee) AS dis_gift_wrap_fee,
    COUNT(DISTINCT gift_wrap_tax) AS dis_gift_wrap_tax,
    COUNT(DISTINCT item_promotion_discount) AS dis_item_promotion_discount,
    COUNT(DISTINCT ship_promotion_discount) AS dis_ship_promotion_discount,
    COUNT(DISTINCT ship_city) AS dis_ship_city,
    COUNT(DISTINCT ship_state) AS dis_ship_state,
    COUNT(DISTINCT ship_postal_code) AS dis_ship_postal_code,
    COUNT(DISTINCT ship_country) AS dis_ship_country,
    COUNT(DISTINCT promotion_ids) AS dis_promotion_ids,
    COUNT(DISTINCT cpf) AS dis_cpf,
    COUNT(DISTINCT is_business_order) AS dis_is_business_order,
    COUNT(DISTINCT purchase_order_number) AS dis_purchase_order_number,
    COUNT(DISTINCT price_designation) AS dis_price_designation,
    COUNT(DISTINCT fulfilled_by) AS dis_fulfilled_by,
    COUNT(DISTINCT buyer_company_name) AS dis_buyer_company_name,
    COUNT(DISTINCT buyer_tax_registration_country) AS dis_buyer_tax_registration_country,
    COUNT(DISTINCT buyer_tax_registration_type) AS dis_buyer_tax_registration_type,
    COUNT(DISTINCT is_iba) AS dis_is_iba,
    COUNT(DISTINCT order_invoice_type) AS dis_order_invoice_type,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT data_src) AS dis_data_src,
    COUNT(DISTINCT table_src) AS dis_table_src,
    COUNT(DISTINCT data_dt) AS dis_data_dt,
    COUNT(DISTINCT etl_data_dt) AS dis_etl_data_dt
FROM
    dwd_amzn_all_orders_df
WHERE
    ds = '20240819';


select data_last_update_time,ori_last_update_time,last_update_time
FROM
    dwd_amzn_all_orders_df
WHERE
    ds = '20240819';




SELECT
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT marketplace_id) AS dis_marketplace_id,
    COUNT(DISTINCT marketplace_type) AS dis_marketplace_type,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT amazon_order_id) AS dis_amazon_order_id,
    COUNT(DISTINCT merchant_order_id) AS dis_merchant_order_id,
    COUNT(DISTINCT ori_purchase_time) AS dis_ori_purchase_time,
    COUNT(DISTINCT purchase_time) AS dis_purchase_time,
    COUNT(DISTINCT ori_last_update_time) AS dis_ori_last_update_time,
    COUNT(DISTINCT last_update_time) AS dis_last_update_time,
    COUNT(DISTINCT order_status) AS dis_order_status,
    COUNT(DISTINCT fulfillment_channel) AS dis_fulfillment_channel,
    COUNT(DISTINCT sales_channel) AS dis_sales_channel,
    COUNT(DISTINCT order_channel) AS dis_order_channel,
    COUNT(DISTINCT ship_service_level) AS dis_ship_service_level,
    COUNT(DISTINCT product_name) AS dis_product_name,
    COUNT(DISTINCT seller_sku) AS dis_seller_sku,
    COUNT(DISTINCT asin) AS dis_asin,
    COUNT(DISTINCT item_status) AS dis_item_status,
    COUNT(DISTINCT ordered_num) AS dis_ordered_num,
    COUNT(DISTINCT currency) AS dis_currency,
    COUNT(DISTINCT item_amt) AS dis_item_amt,
    COUNT(DISTINCT item_tax) AS dis_item_tax,
    COUNT(DISTINCT shipping_fee) AS dis_shipping_fee,
    COUNT(DISTINCT shipping_tax) AS dis_shipping_tax,
    COUNT(DISTINCT gift_wrap_fee) AS dis_gift_wrap_fee,
    COUNT(DISTINCT gift_wrap_tax) AS dis_gift_wrap_tax,
    COUNT(DISTINCT item_promotion_discount) AS dis_item_promotion_discount,
    COUNT(DISTINCT ship_promotion_discount) AS dis_ship_promotion_discount,
    COUNT(DISTINCT ship_city) AS dis_ship_city,
    COUNT(DISTINCT ship_state) AS dis_ship_state,
    COUNT(DISTINCT ship_postal_code) AS dis_ship_postal_code,
    COUNT(DISTINCT ship_country) AS dis_ship_country,
    COUNT(DISTINCT promotion_ids) AS dis_promotion_ids,
    COUNT(DISTINCT cpf) AS dis_cpf,
    COUNT(DISTINCT is_business_order) AS dis_is_business_order,
    COUNT(DISTINCT purchase_order_number) AS dis_purchase_order_number,
    COUNT(DISTINCT price_designation) AS dis_price_designation,
    COUNT(DISTINCT fulfilled_by) AS dis_fulfilled_by,
    COUNT(DISTINCT buyer_company_name) AS dis_buyer_company_name,
    COUNT(DISTINCT buyer_tax_registration_country) AS dis_buyer_tax_registration_country,
    COUNT(DISTINCT buyer_tax_registration_type) AS dis_buyer_tax_registration_type,
    COUNT(DISTINCT is_iba) AS dis_is_iba,
    COUNT(DISTINCT order_invoice_type) AS dis_order_invoice_type,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT data_src) AS dis_data_src,
    COUNT(DISTINCT table_src) AS dis_table_src,
    COUNT(DISTINCT data_dt) AS dis_data_dt,
    COUNT(DISTINCT etl_data_dt) AS dis_etl_data_dt
FROM
    amz.mid_amzn_all_orders_df
WHERE
    ds = '20240819';


select count(1)
FROM
    amz.mid_amzn_all_orders_df
WHERE
    ds = '20240819';




SELECT
    count(1) AS count_1,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT profile_id) AS dis_profile_id,
    COUNT(DISTINCT market_place_id) AS dis_market_place_id,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT report_date) AS dis_report_date,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT country_code) AS dis_country_code,
    COUNT(DISTINCT portfolio_id) AS dis_portfolio_id,
    COUNT(DISTINCT campaign_id) AS dis_campaign_id,
    COUNT(DISTINCT campaign_name) AS dis_campaign_name,
    COUNT(DISTINCT campaign_status) AS dis_campaign_status,
    COUNT(DISTINCT campaign_budget_amt) AS dis_campaign_budget_amt,
    COUNT(DISTINCT campaign_budget_type) AS dis_campaign_budget_type,
    COUNT(DISTINCT campaign_budget_currency_code) AS dis_campaign_budget_currency_code,
    COUNT(DISTINCT ad_group_id) AS dis_ad_group_id,
    COUNT(DISTINCT ad_group_name) AS dis_ad_group_name,
    COUNT(DISTINCT ad_id) AS dis_ad_id,
    COUNT(DISTINCT advertised_asin) AS dis_advertised_asin,
    COUNT(DISTINCT advertised_sku) AS dis_advertised_sku,
    COUNT(DISTINCT impressions) AS dis_impressions,
    COUNT(DISTINCT clicks) AS dis_clicks,
    COUNT(DISTINCT cost_per_click) AS dis_cost_per_click,
    COUNT(DISTINCT click_through_rate) AS dis_click_through_rate,
    COUNT(DISTINCT cost) AS dis_cost,
    COUNT(DISTINCT spend) AS dis_spend,
    COUNT(DISTINCT purchases_1d) AS dis_purchases_1d,
    COUNT(DISTINCT purchases_7d) AS dis_purchases_7d,
    COUNT(DISTINCT purchases_14d) AS dis_purchases_14d,
    COUNT(DISTINCT purchases_30d) AS dis_purchases_30d,
    COUNT(DISTINCT purchases_same_sku_1d) AS dis_purchases_same_sku_1d,
    COUNT(DISTINCT purchases_same_sku_7d) AS dis_purchases_same_sku_7d,
    COUNT(DISTINCT purchases_same_sku_14d) AS dis_purchases_same_sku_14d,
    COUNT(DISTINCT purchases_same_sku_30d) AS dis_purchases_same_sku_30d,
    COUNT(DISTINCT units_sold_clicks_1d) AS dis_units_sold_clicks_1d,
    COUNT(DISTINCT units_sold_clicks_7d) AS dis_units_sold_clicks_7d,
    COUNT(DISTINCT units_sold_clicks_14d) AS dis_units_sold_clicks_14d,
    COUNT(DISTINCT units_sold_clicks_30d) AS dis_units_sold_clicks_30d,
    COUNT(DISTINCT sales_1d) AS dis_sales_1d,
    COUNT(DISTINCT sales_7d) AS dis_sales_7d,
    COUNT(DISTINCT sales_14d) AS dis_sales_14d,
    COUNT(DISTINCT sales_30d) AS dis_sales_30d,
    COUNT(DISTINCT attributed_sales_same_sku_1d) AS dis_attributed_sales_same_sku_1d,
    COUNT(DISTINCT attributed_sales_same_sku_7d) AS dis_attributed_sales_same_sku_7d,
    COUNT(DISTINCT attributed_sales_same_sku_14d) AS dis_attributed_sales_same_sku_14d,
    COUNT(DISTINCT attributed_sales_same_sku_30d) AS dis_attributed_sales_same_sku_30d,
    COUNT(DISTINCT units_sold_same_sku_1d) AS dis_units_sold_same_sku_1d,
    COUNT(DISTINCT units_sold_same_sku_7d) AS dis_units_sold_same_sku_7d,
    COUNT(DISTINCT units_sold_same_sku_14d) AS dis_units_sold_same_sku_14d,
    COUNT(DISTINCT units_sold_same_sku_30d) AS dis_units_sold_same_sku_30d,
    COUNT(DISTINCT kindle_edition_normalized_pages_read_14d) AS dis_kindle_edition_normalized_pages_read_14d,
    COUNT(DISTINCT kindle_edition_normalized_pages_royalties_14d) AS dis_kindle_edition_normalized_pages_royalties_14d,
    COUNT(DISTINCT sales_other_sku_7d) AS dis_sales_other_sku_7d,
    COUNT(DISTINCT units_sold_other_sku_7d) AS dis_units_sold_other_sku_7d,
    COUNT(DISTINCT acos_clicks_7d) AS dis_acos_clicks_7d,
    COUNT(DISTINCT acos_clicks_14d) AS dis_acos_clicks_14d,
    COUNT(DISTINCT roas_clicks_7d) AS dis_roas_clicks_7d,
    COUNT(DISTINCT roas_clicks_14d) AS dis_roas_clicks_14d,
    COUNT(DISTINCT table_src) AS dis_table_src,
    COUNT(DISTINCT data_dt) AS dis_data_dt,
    COUNT(DISTINCT etl_data_dt) AS dis_etl_data_dt
FROM
    dwd_amzn_sp_advertised_product_by_advertiser_report_ds
WHERE
    ds = '20240819';




SELECT
    count(1) AS count_1
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT profile_id) AS dis_profile_id,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT report_date) AS dis_report_date,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT country_code) AS dis_country_code,
    COUNT(DISTINCT campaign_id) AS dis_campaign_id,
    COUNT(DISTINCT campaign_name) AS dis_campaign_name,
    COUNT(DISTINCT campaign_status) AS dis_campaign_status,
    COUNT(DISTINCT campaign_budget_amt) AS dis_campaign_budget_amt,
    COUNT(DISTINCT campaign_budget_type) AS dis_campaign_budget_type,
    COUNT(DISTINCT campaign_budget_currency_code) AS dis_campaign_budget_currency_code,
    COUNT(DISTINCT campaign_bidding_strategy) AS dis_campaign_bidding_strategy,
    COUNT(DISTINCT placement_classification) AS dis_placement_classification,
    COUNT(DISTINCT impressions) AS dis_impressions,
    COUNT(DISTINCT clicks) AS dis_clicks,
    COUNT(DISTINCT cost_per_click) AS dis_cost_per_click,
    COUNT(DISTINCT click_through_rate) AS dis_click_through_rate,
    COUNT(DISTINCT cost) AS dis_cost,
    COUNT(DISTINCT purchases_1d) AS dis_purchases_1d,
    COUNT(DISTINCT purchases_7d) AS dis_purchases_7d,
    COUNT(DISTINCT purchases_14d) AS dis_purchases_14d,
    COUNT(DISTINCT purchases_30d) AS dis_purchases_30d,
    COUNT(DISTINCT purchases_same_sku_1d) AS dis_purchases_same_sku_1d,
    COUNT(DISTINCT purchases_same_sku_7d) AS dis_purchases_same_sku_7d,
    COUNT(DISTINCT purchases_same_sku_14d) AS dis_purchases_same_sku_14d,
    COUNT(DISTINCT purchases_same_sku_30d) AS dis_purchases_same_sku_30d,
    COUNT(DISTINCT units_sold_clicks_1d) AS dis_units_sold_clicks_1d,
    COUNT(DISTINCT units_sold_clicks_7d) AS dis_units_sold_clicks_7d,
    COUNT(DISTINCT units_sold_clicks_14d) AS dis_units_sold_clicks_14d,
    COUNT(DISTINCT units_sold_clicks_30d) AS dis_units_sold_clicks_30d,
    COUNT(DISTINCT sales_1d) AS dis_sales_1d,
    COUNT(DISTINCT sales_7d) AS dis_sales_7d,
    COUNT(DISTINCT sales_14d) AS dis_sales_14d,
    COUNT(DISTINCT sales_30d) AS dis_sales_30d,
    COUNT(DISTINCT attributed_sales_same_sku_1d) AS dis_attributed_sales_same_sku_1d,
    COUNT(DISTINCT attributed_sales_same_sku_7d) AS dis_attributed_sales_same_sku_7d,
    COUNT(DISTINCT attributed_sales_same_sku_14d) AS dis_attributed_sales_same_sku_14d,
    COUNT(DISTINCT attributed_sales_same_sku_30d) AS dis_attributed_sales_same_sku_30d,
    COUNT(DISTINCT units_sold_same_sku_1d) AS dis_units_sold_same_sku_1d,
    COUNT(DISTINCT units_sold_same_sku_7d) AS dis_units_sold_same_sku_7d,
    COUNT(DISTINCT units_sold_same_sku_14d) AS dis_units_sold_same_sku_14d,
    COUNT(DISTINCT units_sold_same_sku_30d) AS dis_units_sold_same_sku_30d,
    COUNT(DISTINCT kindle_edition_normalized_pages_read_14d) AS dis_kindle_edition_normalized_pages_read_14d,
    COUNT(DISTINCT kindle_edition_normalized_pages_royalties_14d) AS dis_kindle_edition_normalized_pages_royalties_14d,
    COUNT(DISTINCT table_src) AS dis_table_src,
    COUNT(DISTINCT data_dt) AS dis_data_dt,
    COUNT(DISTINCT etl_data_dt) AS dis_etl_data_dt
FROM
    amz.mid_amzn_sp_campaigns_by_campaign_placement_report_ds
WHERE
    ds = '20240819';





SELECT
    count(1) AS count_1,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT profile_id) AS dis_profile_id,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT report_date) AS dis_report_date,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT country_code) AS dis_country_code,
    COUNT(DISTINCT portfolio_id) AS dis_portfolio_id,
    COUNT(DISTINCT campaign_id) AS dis_campaign_id,
    COUNT(DISTINCT campaign_name) AS dis_campaign_name,
    COUNT(DISTINCT ad_group_id) AS dis_ad_group_id,
    COUNT(DISTINCT ad_group_name) AS dis_ad_group_name,
    COUNT(DISTINCT campaign_budget_currency_code) AS dis_campaign_budget_currency_code,
    COUNT(DISTINCT advertised_asin) AS dis_advertised_asin,
    COUNT(DISTINCT purchased_asin) AS dis_purchased_asin,
    COUNT(DISTINCT advertised_sku) AS dis_advertised_sku,
    COUNT(DISTINCT keyword_id) AS dis_keyword_id,
    COUNT(DISTINCT keyword) AS dis_keyword,
    COUNT(DISTINCT keyword_type) AS dis_keyword_type,
    COUNT(DISTINCT match_type) AS dis_match_type,
    COUNT(DISTINCT purchases_1d) AS dis_purchases_1d,
    COUNT(DISTINCT purchases_7d) AS dis_purchases_7d,
    COUNT(DISTINCT purchases_14d) AS dis_purchases_14d,
    COUNT(DISTINCT purchases_30d) AS dis_purchases_30d,
    COUNT(DISTINCT purchases_other_sku_1d) AS dis_purchases_other_sku_1d,
    COUNT(DISTINCT purchases_other_sku_7d) AS dis_purchases_other_sku_7d,
    COUNT(DISTINCT purchases_other_sku_14d) AS dis_purchases_other_sku_14d,
    COUNT(DISTINCT purchases_other_sku_30d) AS dis_purchases_other_sku_30d,
    COUNT(DISTINCT units_sold_clicks_1d) AS dis_units_sold_clicks_1d,
    COUNT(DISTINCT units_sold_clicks_7d) AS dis_units_sold_clicks_7d,
    COUNT(DISTINCT units_sold_clicks_14d) AS dis_units_sold_clicks_14d,
    COUNT(DISTINCT units_sold_clicks_30d) AS dis_units_sold_clicks_30d,
    COUNT(DISTINCT units_sold_other_sku_1d) AS dis_units_sold_other_sku_1d,
    COUNT(DISTINCT units_sold_other_sku_7d) AS dis_units_sold_other_sku_7d,
    COUNT(DISTINCT units_sold_other_sku_14d) AS dis_units_sold_other_sku_14d,
    COUNT(DISTINCT units_sold_other_sku_30d) AS dis_units_sold_other_sku_30d,
    COUNT(DISTINCT sales_1d) AS dis_sales_1d,
    COUNT(DISTINCT sales_7d) AS dis_sales_7d,
    COUNT(DISTINCT sales_14d) AS dis_sales_14d,
    COUNT(DISTINCT sales_30d) AS dis_sales_30d,
    COUNT(DISTINCT sales_other_sku_1d) AS dis_sales_other_sku_1d,
    COUNT(DISTINCT sales_other_sku_7d) AS dis_sales_other_sku_7d,
    COUNT(DISTINCT sales_other_sku_14d) AS dis_sales_other_sku_14d,
    COUNT(DISTINCT sales_other_sku_30d) AS dis_sales_other_sku_30d,
    COUNT(DISTINCT kindle_edition_normalized_pages_read_14d) AS dis_kindle_edition_normalized_pages_read_14d,
    COUNT(DISTINCT kindle_edition_normalized_pages_royalties_14d) AS dis_kindle_edition_normalized_pages_royalties_14d
FROM
    amz.mid_amzn_sp_purchased_product_by_asin_report_ds
WHERE
    ds = '20240819';



SELECT
    count(1) AS count_1,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT profile_id) AS dis_profile_id,
    COUNT(DISTINCT market_place_id) AS dis_market_place_id,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT report_date) AS dis_report_date,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT country_code) AS dis_country_code,
    COUNT(DISTINCT portfolio_id) AS dis_portfolio_id,
    COUNT(DISTINCT campaign_id) AS dis_campaign_id,
    COUNT(DISTINCT campaign_name) AS dis_campaign_name,
    COUNT(DISTINCT campaign_status) AS dis_campaign_status,
    COUNT(DISTINCT campaign_budget_amt) AS dis_campaign_budget_amt,
    COUNT(DISTINCT campaign_budget_type) AS dis_campaign_budget_type,
    COUNT(DISTINCT campaign_budget_currency_code) AS dis_campaign_budget_currency_code,
    COUNT(DISTINCT ad_group_id) AS dis_ad_group_id,
    COUNT(DISTINCT ad_group_name) AS dis_ad_group_name,
    COUNT(DISTINCT keyword_id) AS dis_keyword_id,
    COUNT(DISTINCT keyword) AS dis_keyword,
    COUNT(DISTINCT keyword_type) AS dis_keyword_type,
    COUNT(DISTINCT keyword_bid) AS dis_keyword_bid,
    COUNT(DISTINCT match_type) AS dis_match_type,
    COUNT(DISTINCT targeting) AS dis_targeting,
    COUNT(DISTINCT ad_keyword_status) AS dis_ad_keyword_status,
    COUNT(DISTINCT search_term) AS dis_search_term,
    COUNT(DISTINCT impressions) AS dis_impressions,
    COUNT(DISTINCT clicks) AS dis_clicks,
    COUNT(DISTINCT cost_per_click) AS dis_cost_per_click,
    COUNT(DISTINCT click_through_rate) AS dis_click_through_rate,
    COUNT(DISTINCT cost) AS dis_cost,
    COUNT(DISTINCT purchases_1d) AS dis_purchases_1d,
    COUNT(DISTINCT purchases_7d) AS dis_purchases_7d,
    COUNT(DISTINCT purchases_14d) AS dis_purchases_14d,
    COUNT(DISTINCT purchases_30d) AS dis_purchases_30d,
    COUNT(DISTINCT purchases_same_sku_1d) AS dis_purchases_same_sku_1d,
    COUNT(DISTINCT purchases_same_sku_7d) AS dis_purchases_same_sku_7d,
    COUNT(DISTINCT purchases_same_sku_14d) AS dis_purchases_same_sku_14d,
    COUNT(DISTINCT purchases_same_sku_30d) AS dis_purchases_same_sku_30d,
    COUNT(DISTINCT units_sold_clicks_1d) AS dis_units_sold_clicks_1d,
    COUNT(DISTINCT units_sold_clicks_7d) AS dis_units_sold_clicks_7d,
    COUNT(DISTINCT units_sold_clicks_14d) AS dis_units_sold_clicks_14d,
    COUNT(DISTINCT units_sold_clicks_30d) AS dis_units_sold_clicks_30d,
    COUNT(DISTINCT sales_1d) AS dis_sales_1d,
    COUNT(DISTINCT sales_7d) AS dis_sales_7d,
    COUNT(DISTINCT sales_14d) AS dis_sales_14d,
    COUNT(DISTINCT sales_30d) AS dis_sales_30d,
    COUNT(DISTINCT attributed_sales_same_sku_1d) AS dis_attributed_sales_same_sku_1d,
    COUNT(DISTINCT attributed_sales_same_sku_7d) AS dis_attributed_sales_same_sku_7d,
    COUNT(DISTINCT attributed_sales_same_sku_14d) AS dis_attributed_sales_same_sku_14d,
    COUNT(DISTINCT attributed_sales_same_sku_30d) AS dis_attributed_sales_same_sku_30d,
    COUNT(DISTINCT units_sold_same_sku_1d) AS dis_units_sold_same_sku_1d,
    COUNT(DISTINCT units_sold_same_sku_7d) AS dis_units_sold_same_sku_7d,
    COUNT(DISTINCT units_sold_same_sku_14d) AS dis_units_sold_same_sku_14d,
    COUNT(DISTINCT units_sold_same_sku_30d) AS dis_units_sold_same_sku_30d,
    COUNT(DISTINCT kindle_edition_normalized_pages_read_14d) AS dis_kindle_edition_normalized_pages_read_14d,
    COUNT(DISTINCT kindle_edition_normalized_pages_royalties_14d) AS dis_kindle_edition_normalized_pages_royalties_14d,
    COUNT(DISTINCT sales_other_sku_7d) AS dis_sales_other_sku_7d,
    COUNT(DISTINCT units_sold_other_sku_7d) AS dis_units_sold_other_sku_7d,
    COUNT(DISTINCT acos_clicks_7d) AS dis_acos_clicks_7d,
    COUNT(DISTINCT acos_clicks_14d) AS dis_acos_clicks_14d,
    COUNT(DISTINCT roas_clicks_7d) AS dis_roas_clicks_7d,
    COUNT(DISTINCT roas_clicks_14d) AS dis_roas_clicks_14d
FROM
    amz.mid_amzn_sp_search_term_by_search_term_report_ds
WHERE
    ds = '20240819';





SELECT
    count(1) AS count_1,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT profile_id) AS dis_profile_id,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT report_date) AS dis_report_date,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT country_code) AS dis_country_code,
    COUNT(DISTINCT portfolio_id) AS dis_portfolio_id,
    COUNT(DISTINCT campaign_id) AS dis_campaign_id,
    COUNT(DISTINCT campaign_name) AS dis_campaign_name,
    COUNT(DISTINCT campaign_status) AS dis_campaign_status,
    COUNT(DISTINCT campaign_budget_amt) AS dis_campaign_budget_amt,
    COUNT(DISTINCT campaign_budget_type) AS dis_campaign_budget_type,
    COUNT(DISTINCT campaign_budget_currency_code) AS dis_campaign_budget_currency_code,
    COUNT(DISTINCT ad_group_id) AS dis_ad_group_id,
    COUNT(DISTINCT ad_group_name) AS dis_ad_group_name,
    COUNT(DISTINCT keyword_id) AS dis_keyword_id,
    COUNT(DISTINCT keyword) AS dis_keyword,
    COUNT(DISTINCT keyword_type) AS dis_keyword_type,
    COUNT(DISTINCT keyword_bid) AS dis_keyword_bid,
    COUNT(DISTINCT match_type) AS dis_match_type,
    COUNT(DISTINCT targeting) AS dis_targeting,
    COUNT(DISTINCT ad_keyword_status) AS dis_ad_keyword_status,
    COUNT(DISTINCT impressions) AS dis_impressions,
    COUNT(DISTINCT clicks) AS dis_clicks,
    COUNT(DISTINCT cost_per_click) AS dis_cost_per_click,
    COUNT(DISTINCT click_through_rate) AS dis_click_through_rate,
    COUNT(DISTINCT cost) AS dis_cost,
    COUNT(DISTINCT purchases_1d) AS dis_purchases_1d,
    COUNT(DISTINCT purchases_7d) AS dis_purchases_7d,
    COUNT(DISTINCT purchases_14d) AS dis_purchases_14d,
    COUNT(DISTINCT purchases_30d) AS dis_purchases_30d,
    COUNT(DISTINCT purchases_same_sku_1d) AS dis_purchases_same_sku_1d,
    COUNT(DISTINCT purchases_same_sku_7d) AS dis_purchases_same_sku_7d,
    COUNT(DISTINCT purchases_same_sku_14d) AS dis_purchases_same_sku_14d,
    COUNT(DISTINCT purchases_same_sku_30d) AS dis_purchases_same_sku_30d,
    COUNT(DISTINCT units_sold_clicks_1d) AS dis_units_sold_clicks_1d,
    COUNT(DISTINCT units_sold_clicks_7d) AS dis_units_sold_clicks_7d,
    COUNT(DISTINCT units_sold_clicks_14d) AS dis_units_sold_clicks_14d,
    COUNT(DISTINCT units_sold_clicks_30d) AS dis_units_sold_clicks_30d,
    COUNT(DISTINCT sales_1d) AS dis_sales_1d,
    COUNT(DISTINCT sales_7d) AS dis_sales_7d,
    COUNT(DISTINCT sales_14d) AS dis_sales_14d,
    COUNT(DISTINCT sales_30d) AS dis_sales_30d,
    COUNT(DISTINCT attributed_sales_same_sku_1d) AS dis_attributed_sales_same_sku_1d,
    COUNT(DISTINCT attributed_sales_same_sku_7d) AS dis_attributed_sales_same_sku_7d,
    COUNT(DISTINCT attributed_sales_same_sku_14d) AS dis_attributed_sales_same_sku_14d,
    COUNT(DISTINCT attributed_sales_same_sku_30d) AS dis_attributed_sales_same_sku_30d,
    COUNT(DISTINCT units_sold_same_sku_1d) AS dis_units_sold_same_sku_1d,
    COUNT(DISTINCT units_sold_same_sku_7d) AS dis_units_sold_same_sku_7d,
    COUNT(DISTINCT units_sold_same_sku_14d) AS dis_units_sold_same_sku_14d,
    COUNT(DISTINCT units_sold_same_sku_30d) AS dis_units_sold_same_sku_30d,
    COUNT(DISTINCT kindle_edition_normalized_pages_read_14d) AS dis_kindle_edition_normalized_pages_read_14d,
    COUNT(DISTINCT kindle_edition_normalized_pages_royalties_14d) AS dis_kindle_edition_normalized_pages_royalties_14d,
    COUNT(DISTINCT sales_other_sku_7d) AS dis_sales_other_sku_7d,
    COUNT(DISTINCT units_sold_other_sku_7d) AS dis_units_sold_other_sku_7d,
    COUNT(DISTINCT acos_clicks_7d) AS dis_acos_clicks_7d,
    COUNT(DISTINCT acos_clicks_14d) AS dis_acos_clicks_14d,
    COUNT(DISTINCT roas_clicks_7d) AS dis_roas_clicks_7d,
    COUNT(DISTINCT roas_clicks_14d) AS dis_roas_clicks_14d

FROM
    amz.mid_amzn_sp_targeting_by_targeting_report_ds
WHERE
    ds = '20240819';


SELECT
    count(1) AS count_1,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT record_id) AS dis_record_id,
    COUNT(DISTINCT marketplace_id) AS dis_marketplace_id,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT erp_store_id) AS dis_erp_store_id,
    COUNT(DISTINCT seller_sku) AS dis_seller_sku,
    COUNT(DISTINCT fnsku) AS dis_fnsku,
    COUNT(DISTINCT asin) AS dis_asin,
    COUNT(DISTINCT product_name) AS dis_product_name,
    COUNT(DISTINCT status) AS dis_status,
    COUNT(DISTINCT product_price) AS dis_product_price,
    COUNT(DISTINCT mfn_listing_exists) AS dis_mfn_listing_exists,
    COUNT(DISTINCT mfn_fulfillable_quantity) AS dis_mfn_fulfillable_quantity,
    COUNT(DISTINCT afn_listing_exists) AS dis_afn_listing_exists,
    COUNT(DISTINCT afn_warehouse_quantity) AS dis_afn_warehouse_quantity,
    COUNT(DISTINCT afn_fulfillable_quantity) AS dis_afn_fulfillable_quantity,
    COUNT(DISTINCT afn_unsellable_quantity) AS dis_afn_unsellable_quantity,
    COUNT(DISTINCT afn_reserved_quantity) AS dis_afn_reserved_quantity,
    COUNT(DISTINCT afn_total_quantity) AS dis_afn_total_quantity,
    COUNT(DISTINCT per_unit_volume) AS dis_per_unit_volume,
    COUNT(DISTINCT afn_inbound_working_quantity) AS dis_afn_inbound_working_quantity,
    COUNT(DISTINCT afn_inbound_shipped_quantity) AS dis_afn_inbound_shipped_quantity,
    COUNT(DISTINCT afn_inbound_receiving_quantity) AS dis_afn_inbound_receiving_quantity,
    COUNT(DISTINCT data_src) AS dis_data_src,
    COUNT(DISTINCT table_src) AS dis_table_src,
    COUNT(DISTINCT data_dt) AS dis_data_dt,
    COUNT(DISTINCT etl_data_dt) AS dis_etl_data_dt,
    COUNT(DISTINCT afn_researching_quantity) AS dis_afn_researching_quantity,
    COUNT(DISTINCT afn_reserved_future_supply) AS dis_afn_reserved_future_supply,
    COUNT(DISTINCT afn_future_supply_buyable) AS dis_afn_future_supply_buyable,
    COUNT(DISTINCT afn_fulfillable_quantity_local) AS dis_afn_fulfillable_quantity_local,
    COUNT(DISTINCT afn_fulfillable_quantity_remote) AS dis_afn_fulfillable_quantity_remote,
    COUNT(DISTINCT marketplace_type) AS dis_marketplace_type,
    COUNT(DISTINCT en_country_name) AS dis_en_country_name,
    COUNT(DISTINCT cn_country_name) AS dis_cn_country_name,
    COUNT(DISTINCT country_code) AS dis_country_code
FROM
    amz.mid_scm_ivt_amazon_fba_stock_current_num_df
WHERE
    ds = '20240819';


SELECT
    count(1) AS count_1,
    COUNT(DISTINCT report_type) AS dis_report_type,
    COUNT(DISTINCT report_id) AS dis_report_id,
    COUNT(DISTINCT start_time) AS dis_start_time,
    COUNT(DISTINCT end_time) AS dis_end_time,
    COUNT(DISTINCT data_last_update_time) AS dis_data_last_update_time,
    COUNT(DISTINCT operation_date) AS dis_operation_date,
    COUNT(DISTINCT ori_operation_date) AS dis_ori_operation_date,
    COUNT(DISTINCT store_id) AS dis_store_id,
    COUNT(DISTINCT seller_id) AS dis_seller_id,
    COUNT(DISTINCT marketplace_id) AS dis_marketplace_id,
    COUNT(DISTINCT ori_marketplace_id) AS dis_ori_marketplace_id,
    COUNT(DISTINCT country_code) AS dis_country_code,
    COUNT(DISTINCT fnsku) AS dis_fnsku,
    COUNT(DISTINCT asin) AS dis_asin,
    COUNT(DISTINCT seller_sku) AS dis_seller_sku,
    COUNT(DISTINCT title) AS dis_title,
    COUNT(DISTINCT event_type) AS dis_event_type,
    COUNT(DISTINCT reference_id) AS dis_reference_id,
    COUNT(DISTINCT product_num) AS dis_product_num,
    COUNT(DISTINCT fulfillment_center) AS dis_fulfillment_center,
    COUNT(DISTINCT disposition) AS dis_disposition,
    COUNT(DISTINCT reason) AS dis_reason,
    COUNT(DISTINCT reconciled_num) AS dis_reconciled_num,
    COUNT(DISTINCT unreconciled_num) AS dis_unreconciled_num,
    COUNT(DISTINCT record_id) AS dis_record_id,
    COUNT(DISTINCT tenant_id) AS dis_tenant_id


FROM
    amz.mid_scm_ivt_amazon_ledger_detail_view_df
WHERE
    ds = '20240819';

-- temp_scm_ivt_amazon_asin_df 表进行 select distinct





WITH tmp_inventory AS (
    SELECT
        tenant_id,
        marketplace_id,
        marketplace_type,
        en_country_name,
        cn_country_name,
        country_code,
        seller_id,
        seller_sku,
        fnsku,
        asin,
        -- 计算FBA总库存 = FBA在库 + FBA在途
        afn_warehouse_quantity + (afn_inbound_working_quantity + afn_inbound_shipped_quantity + afn_inbound_receiving_quantity) AS afn_total_num,
        -- FBA在库库存
        afn_warehouse_quantity AS afn_warehouse_num,
        -- FBA在途库存
        (afn_inbound_working_quantity + afn_inbound_shipped_quantity + afn_inbound_receiving_quantity) AS afn_inbound_num
    FROM
        amz.mid_scm_ivt_amazon_fba_stock_current_num_df
    WHERE
        ds = '20240820'
),
tmp_sales AS (
         SELECT
             tenant_id,
             marketplace_id,
             seller_id,
             seller_sku,
             asin,
             -- 计算库存可售天数
             SUM(CASE WHEN purchase_time BETWEEN date_add(date_format('20240820', 'yyyymmdd'), -7)  AND date_format('20240820', 'yyyymmdd') THEN ordered_num ELSE 0 END) / 7 AS afnstock_n7d_avg_sale_num,
             SUM(CASE WHEN purchase_time BETWEEN date_add(date_format('20240820', 'yyyymmdd'), -15) AND date_format('20240820', 'yyyymmdd') THEN ordered_num ELSE 0 END) / 15 AS afnstock_n15d_avg_sale_num,
             SUM(CASE WHEN purchase_time BETWEEN date_add(date_format('20240820', 'yyyymmdd'), -30) AND date_format('20240820', 'yyyymmdd') THEN ordered_num ELSE 0 END) / 30 AS afnstock_n30d_avg_sale_num,
             SUM(CASE WHEN purchase_time BETWEEN date_add(date_format('20240820', 'yyyymmdd'), -60) AND date_format('20240820', 'yyyymmdd') THEN ordered_num ELSE 0 END) / 60 AS afnstock_n60d_avg_sale_num
         FROM
             amz.mid_amzn_all_orders_df
         WHERE
             ds = '20240820'
           AND purchase_time >= date_add(date_format('20240820', 'yyyymmdd'), -60)
         GROUP BY
             tenant_id,
             marketplace_id,
             seller_id,
             seller_sku,
             asin
     )
SELECT
    I.tenant_id,
    I.marketplace_id,
    I.marketplace_type,
    I.en_country_name,
    I.cn_country_name,
    I.country_code,
    I.seller_id,
    I.seller_sku,
    I.fnsku,
    I.asin,
    -- FBA总库存
    I.afn_total_num,
    -- 在库库存
    I.afn_warehouse_num,
    -- 在途库存
    I.afn_inbound_num,
    -- 近7天日均销量（用于计算库存可售天数）
    S.afnstock_n7d_avg_sale_num,
    -- 近15天日均销量
    S.afnstock_n15d_avg_sale_num,
    -- 近30天日均销量
    S.afnstock_n30d_avg_sale_num,
    -- 近60天日均销量
    S.afnstock_n60d_avg_sale_num,
    -- 库存可售天数的计算（可以选择用不同的时间窗口）
    CASE
        WHEN S.afnstock_n7d_avg_sale_num > 0 THEN I.afn_total_num / S.afnstock_n7d_avg_sale_num
        ELSE NULL
        END AS stock_days_7d,
    CASE
        WHEN S.afnstock_n15d_avg_sale_num > 0 THEN I.afn_total_num / S.afnstock_n15d_avg_sale_num
        ELSE NULL
        END AS stock_days_15d,
    CASE
        WHEN S.afnstock_n30d_avg_sale_num > 0 THEN I.afn_total_num / S.afnstock_n30d_avg_sale_num
        ELSE NULL
        END AS stock_days_30d,
    CASE
        WHEN S.afnstock_n60d_avg_sale_num > 0 THEN I.afn_total_num / S.afnstock_n60d_avg_sale_num
        ELSE NULL
        END AS stock_days_60d,
    '20240820' AS data_dt,
    current_date() AS etl_data_dt
FROM
    tmp_inventory I
        LEFT JOIN
    tmp_sales S
    ON
        I.tenant_id = S.tenant_id
            AND I.marketplace_id = S.marketplace_id
            AND I.seller_id = S.seller_id
            AND I.seller_sku = S.seller_sku
            AND I.asin = S.asin;
