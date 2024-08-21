'''PyODPS 3
请确保不要使用从 MaxCompute下载数据来处理。下载数据操作常包括Table/Instance的open_reader以及 DataFrame的to_pandas方法。
推荐使用 PyODPS DataFrame（从 MaxCompute 表创建）和MaxCompute SQL来处理数据。
更详细的内容可以参考：https://help.aliyun.com/document_detail/90481.html
'''


##1、计算报告日期范围：首先，它通过执行SQL查询来计算输入表中报告日期的跨度，判断是否存在补充数据的情况。
##2、数据迁移与转换：根据计算出的日期范围和给定的条件（如报告天数），代码决定如何从一个临时表迁移数据到目标表，同时进行数据转换。
##3、处理数据重复和时间窗口：使用ROW_NUMBER()窗口函数来处理重复数据，确保每个唯一组合（如tenant_id, profile_id, store_id, campaign_id, ad_group_id, keyword_id, search_term, report_date）只选择最新的记录。
##4、数据插入：根据n_lag1和n_lag2的值，分别处理数据，这可能涉及到不同的时间窗口和数据筛选逻辑。最后，将处理后的数据插入到分区表中。
def calculate_n_lag(input_table_name, ds, n_lag1, n_lag2, report_days):
    """
    根据报告日期的范围计算n_lag值。
    """
    try:
        query = f"SELECT datediff(max(report_date), min(report_date), 'dd') FROM {input_table_name} WHERE ds = '{ds}'"
        instance = o.execute_sql(query)
        # Using open_reader with tunnel=True and limit=False to ensure all data can be fetched
        with instance.open_reader(tunnel=True, limit=False) as reader:
            for record in reader:
                n = record[0]
                # Ensure that n is not None and is a number before comparing
                if n is not None:
                    return n_lag1 if n <= report_days else n_lag2
            # If no records are found or n is None, use a default fallback
            print(f"No valid data found for query: {query}, using default n_lag1")
            return n_lag1
    except Exception as e:
        print(f"Error calculating n_lag for {input_table_name}: {e}")
        # In case of any exception, fallback to n_lag1
        return n_lag1

def execute_insert_sql(ds, n_lag, input_table_name, out_table_name, fields, partition_by_fields):
    """
    使用给定的参数执行SQL插入语句。
    """
    # 构建并执行SQL模板
    insert_sql = f"""
    INSERT OVERWRITE TABLE {out_table_name} PARTITION (ds)
    SELECT
        {", ".join(fields)},
        GETDATE() AS etl_data_dt,
        TO_CHAR(report_date, 'yyyymmdd') AS ds
    FROM (
        SELECT
            {", ".join(fields)},
            ROW_NUMBER() OVER (PARTITION BY {", ".join(partition_by_fields)} ORDER BY etl_data_dt DESC) AS rn
        FROM {input_table_name}
        WHERE ds >= TO_CHAR(DATEADD(TO_DATE({ds}, 'yyyymmdd'), -{n_lag}, 'dd'), 'yyyymmdd')
    ) tmp
    WHERE rn = 1
    """
    try:
        o.execute_sql(insert_sql)
    except Exception as e:
        print(f'Error executing insert SQL for {out_table_name}:', e)

# 通用参数
ds = args['bizdate']
n_lag1 = args['n_lag1']
n_lag2 = args['n_lag2']
report_days = 10


fields_search_term = [
    "tenant_id","profile_id","market_place_id"
        ,"report_id"
        ,"report_type"
        ,"seller_id"
        ,"report_date"
        ,"data_last_update_time"
        ,"country_code"
        ,"portfolio_id"
        ,"campaign_id"
        ,"campaign_name"
        ,"campaign_status"
        ,"campaign_budget_amt"
        ,"campaign_budget_type"
        ,"campaign_budget_currency_code"
        ,"ad_group_id"
        ,"ad_group_name"
        ,"keyword_id"
        ,"keyword"
        ,"keyword_type"
        ,"keyword_bid"
        ,"match_type"
        ,"targeting"
        ,"ad_keyword_status"
        ,"search_term"
        ,"impressions"
        ,"clicks"
        ,"cost_per_click"
        ,"click_through_rate"
        ,"cost"
        ,"purchases_1d"
        ,"purchases_7d"
        ,"purchases_14d"
        ,"purchases_30d"
        ,"purchases_same_sku_1d"
        ,"purchases_same_sku_7d"
        ,"purchases_same_sku_14d"
        ,"purchases_same_sku_30d"
        ,"units_sold_clicks_1d"
        ,"units_sold_clicks_7d"
        ,"units_sold_clicks_14d"
        ,"units_sold_clicks_30d"
        ,"sales_1d"
        ,"sales_7d"
        ,"sales_14d"
        ,"sales_30d"
        ,"attributed_sales_same_sku_1d"
        ,"attributed_sales_same_sku_7d"
        ,"attributed_sales_same_sku_14d"
        ,"attributed_sales_same_sku_30d"
        ,"units_sold_same_sku_1d"
        ,"units_sold_same_sku_7d"
        ,"units_sold_same_sku_14d"
        ,"units_sold_same_sku_30d"
        ,"kindle_edition_normalized_pages_read_14d"
        ,"kindle_edition_normalized_pages_royalties_14d"
        ,"sales_other_sku_7d"
        ,"units_sold_other_sku_7d"
        ,"acos_clicks_7d"
        ,"acos_clicks_14d"
        ,"roas_clicks_7d"
        ,"roas_clicks_14d"
]

partition_by_fields_search_term = ["tenant_id", "profile_id", "campaign_id", "ad_group_id", "keyword_id", "search_term", "report_date"]



fields_targeting = [
    "tenant_id","profile_id",
                        "report_id",
                        "report_type",
                        "seller_id",
                        "report_date",
                        "data_last_update_time",
                        "country_code",
                        "portfolio_id",
                        "campaign_id",
                        "campaign_name",
                        "campaign_status",
                        "campaign_budget_amt",
                        "campaign_budget_type",
                        "campaign_budget_currency_code",
                        "ad_group_id",
                        "ad_group_name",
                        "keyword_id",
                        "keyword",
                        "keyword_type",
                        "keyword_bid",
                        "match_type",
                        "targeting",
                        "ad_keyword_status",
                        "impressions",
                        "clicks",
                        "cost_per_click",
                        "click_through_rate",
                        "cost",
                        "purchases_1d",
                        "purchases_7d",
                        "purchases_14d",
                        "purchases_30d",
                        "purchases_same_sku_1d",
                        "purchases_same_sku_7d",
                        "purchases_same_sku_14d",
                        "purchases_same_sku_30d",
                        "units_sold_clicks_1d",
                        "units_sold_clicks_7d",
                        "units_sold_clicks_14d",
                        "units_sold_clicks_30d",
                        "sales_1d",
                        "sales_7d",
                        "sales_14d",
                        "sales_30d",
                        "attributed_sales_same_sku_1d",
                        "attributed_sales_same_sku_7d",
                        "attributed_sales_same_sku_14d",
                        "attributed_sales_same_sku_30d",
                        "units_sold_same_sku_1d",
                        "units_sold_same_sku_7d",
                        "units_sold_same_sku_14d",
                        "units_sold_same_sku_30d",
                        "kindle_edition_normalized_pages_read_14d",
                        "kindle_edition_normalized_pages_royalties_14d",
                        "sales_other_sku_7d",
                        "units_sold_other_sku_7d",
                        "acos_clicks_7d",
                        "acos_clicks_14d",
                        "roas_clicks_7d",
                        "roas_clicks_14d"
]

partition_by_fields_targeting = ["tenant_id", "profile_id", "campaign_id", "ad_group_id", "keyword_id", "report_date"]



fields_placement = [
               "tenant_id"
               ,"profile_id"
               ,"report_id"
               ,"report_type"
               ,"seller_id"
               ,"report_date"
               ,"data_last_update_time"
               ,"country_code"
               ,"campaign_id"
               ,"campaign_name"
               ,"campaign_status"
               ,"campaign_budget_amt"
               ,"campaign_budget_type"
               ,"campaign_budget_currency_code"
               ,"campaign_bidding_strategy"
               ,"placement_classification"
               ,"impressions"
               ,"clicks"
               ,"cost_per_click"
               ,"click_through_rate"
               ,"cost"
               ,"purchases_1d"
               ,"purchases_7d"
               ,"purchases_14d"
               ,"purchases_30d"
               ,"purchases_same_sku_1d"
               ,"purchases_same_sku_7d"
               ,"purchases_same_sku_14d"
               ,"purchases_same_sku_30d"
               ,"units_sold_clicks_1d"
               ,"units_sold_clicks_7d"
               ,"units_sold_clicks_14d"
               ,"units_sold_clicks_30d"
               ,"sales_1d"
               ,"sales_7d"
               ,"sales_14d"
               ,"sales_30d"
               ,"attributed_sales_same_sku_1d"
               ,"attributed_sales_same_sku_7d"
               ,"attributed_sales_same_sku_14d"
               ,"attributed_sales_same_sku_30d"
               ,"units_sold_same_sku_1d"
               ,"units_sold_same_sku_7d"
               ,"units_sold_same_sku_14d"
               ,"units_sold_same_sku_30d"
               ,"kindle_edition_normalized_pages_read_14d"
               ,"kindle_edition_normalized_pages_royalties_14d"
]

partition_by_fields_placement = ["tenant_id", "profile_id", "campaign_id", "placement_classification", "report_date"]




fields_product_by_adv = [
             "tenant_id"
            ,"profile_id"
            ,"market_place_id"
            ,"report_id"
            ,"report_type"
            ,"seller_id"
            ,"report_date"
            ,"data_last_update_time"
            ,"country_code"
            ,"portfolio_id"
            ,"campaign_id"
            ,"campaign_name"
            ,"campaign_status"
            ,"campaign_budget_amt"
            ,"campaign_budget_type"
            ,"campaign_budget_currency_code"
            ,"ad_group_id"
            ,"ad_group_name"
            ,"ad_id"
            ,"advertised_asin"
            ,"advertised_sku"
            ,"impressions"
            ,"clicks"
            ,"cost_per_click"
            ,"click_through_rate"
            ,"cost"
            ,"spend"
            ,"purchases_1d"
            ,"purchases_7d"
            ,"purchases_14d"
            ,"purchases_30d"
            ,"purchases_same_sku_1d"
            ,"purchases_same_sku_7d"
            ,"purchases_same_sku_14d"
            ,"purchases_same_sku_30d"
            ,"units_sold_clicks_1d"
            ,"units_sold_clicks_7d"
            ,"units_sold_clicks_14d"
            ,"units_sold_clicks_30d"
            ,"sales_1d"
            ,"sales_7d"
            ,"sales_14d"
            ,"sales_30d"
            ,"attributed_sales_same_sku_1d"
            ,"attributed_sales_same_sku_7d"
            ,"attributed_sales_same_sku_14d"
            ,"attributed_sales_same_sku_30d"
            ,"units_sold_same_sku_1d"
            ,"units_sold_same_sku_7d"
            ,"units_sold_same_sku_14d"
            ,"units_sold_same_sku_30d"
            ,"kindle_edition_normalized_pages_read_14d"
            ,"kindle_edition_normalized_pages_royalties_14d"
            ,"sales_other_sku_7d"
            ,"units_sold_other_sku_7d"
            ,"acos_clicks_7d"
            ,"acos_clicks_14d"
            ,"roas_clicks_7d"
            ,"roas_clicks_14d"
]

partition_by_fields_product_by_adv = ["tenant_id", "profile_id", "campaign_id", "ad_group_id", "ad_id", "advertised_sku", "report_date"]


fields_product_by_asin = [
             "tenant_id"
            ,"profile_id"
            ,"report_id"
            ,"report_type"
            ,"seller_id"
            ,"report_date"
            ,"data_last_update_time"
            ,"country_code"
            ,"portfolio_id"
            ,"campaign_id"
            ,"campaign_name"
            ,"ad_group_id"
            ,"ad_group_name"
            ,"campaign_budget_currency_code"
            ,"advertised_asin"
            ,"purchased_asin"
            ,"advertised_sku"
            ,"keyword_id"
            ,"keyword"
            ,"keyword_type"
            ,"match_type"
            ,"purchases_1d"
            ,"purchases_7d"
            ,"purchases_14d"
            ,"purchases_30d"
            ,"purchases_other_sku_1d"
            ,"purchases_other_sku_7d"
            ,"purchases_other_sku_14d"
            ,"purchases_other_sku_30d"
            ,"units_sold_clicks_1d"
            ,"units_sold_clicks_7d"
            ,"units_sold_clicks_14d"
            ,"units_sold_clicks_30d"
            ,"units_sold_other_sku_1d"
            ,"units_sold_other_sku_7d"
            ,"units_sold_other_sku_14d"
            ,"units_sold_other_sku_30d"
            ,"sales_1d"
            ,"sales_7d"
            ,"sales_14d"
            ,"sales_30d"
            ,"sales_other_sku_1d"
            ,"sales_other_sku_7d"
            ,"sales_other_sku_14d"
            ,"sales_other_sku_30d"
            ,"kindle_edition_normalized_pages_read_14d"
            ,"kindle_edition_normalized_pages_royalties_14d"
]

partition_by_fields_product_by_asin = ["tenant_id"
, "profile_id"
, "campaign_id"
, "ad_group_id"
, "advertised_asin"
, "purchased_asin"
, "advertised_sku"
, "keyword_id"
, "match_type"
, "report_date"
]

# 场景参数
scenario_params = [
    {
        "input_table_name": "whde.ods_amzn_sp_search_term_by_search_term_report",
        "out_table_name": "whde.dwd_amzn_sp_search_term_by_search_term_report_ds",
        "fields": fields_search_term,
        "partition_by_fields": partition_by_fields_search_term
    },
    {
        "input_table_name": "whde.ods_amzn_sp_targeting_by_targeting_report",
        "out_table_name": "whde.dwd_amzn_sp_targeting_by_targeting_report_ds",
        "fields": fields_targeting,
        "partition_by_fields": partition_by_fields_targeting
    },
    {
        "input_table_name": "whde.ods_amzn_sp_campaigns_by_campaign_placement_report",
        "out_table_name": "whde.dwd_amzn_sp_campaigns_by_campaign_placement_report_ds",
        "fields": fields_placement,
        "partition_by_fields": partition_by_fields_placement
    },
    {
        "input_table_name": "whde.ods_amzn_sp_advertised_product_by_advertiser_report",
        "out_table_name": "whde.dwd_amzn_sp_advertised_product_by_advertiser_report_ds",
        "fields": fields_product_by_adv,
        "partition_by_fields": partition_by_fields_product_by_adv
    },
    {
        "input_table_name": "whde.ods_amzn_sp_purchased_product_by_asin_report",
        "out_table_name": "whde.dwd_amzn_sp_purchased_product_by_asin_report_ds",
        "fields": fields_product_by_asin,
        "partition_by_fields": partition_by_fields_product_by_asin
    }

]

# 对每个场景计算n_lag并执行插入SQL
for scenario in scenario_params:
    n_lag = calculate_n_lag(scenario["input_table_name"], ds, n_lag1, n_lag2, report_days)
    execute_insert_sql(ds, n_lag, **scenario)