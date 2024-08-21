--@exclude_input=whde.dim_marketplace_info_df
--odps sql
--********************************************************************--
--author:Ada
--create time:2024-03-03 19:19:06
--********************************************************************--



CREATE TABLE IF NOT EXISTS whde.adm_amazon_adv_search_term_pasin_rank_df(
                                                                            parent_asin STRING COMMENT '父asin',
                                                                            search_term STRING COMMENT '搜素词',
                                                                            rank_norm_abs BIGINT COMMENT '最近4个小时的自然搜索排名',
                                                                            rank_adv_abs BIGINT COMMENT '最近4个小时的广告搜索排名',
                                                                            cnt_norm_ns BIGINT COMMENT '前16自然坑位的小时数',
                                                                            cnt_adv_ns BIGINT COMMENT '前16广告坑位的小时数',
                                                                            cnt_norm_p1 BIGINT COMMENT '前1页自然坑位的小时数',
                                                                            cnt_adv_p1 BIGINT COMMENT '前1页广告坑位的小时数',
                                                                            cnt_total BIGINT COMMENT '总小时数',
                                                                            ns_norm_rate DECIMAL(18,6) COMMENT '前16坑位的自然排名时间占比',
    ns_adv_rate DECIMAL(18,6) COMMENT '前16坑位的广告排名时间占比',
    p1_norm_rate DECIMAL(18,6) COMMENT '前1页的自然排名时间占比',
    p1_adv_rate DECIMAL(18,6) COMMENT '前1页的广告排名占比',
    p1_norm_label BIGINT COMMENT '首页的自然排名标签',
    p1_adv_label BIGINT COMMENT '首页的广告排名标签',
    ns_norm_label BIGINT COMMENT '前16坑位的广告标签',
    ns_adv_label BIGINT COMMENT '前16坑位的自然标签',
    data_time DATETIME COMMENT '数据时间',
    create_time DATETIME COMMENT '创建时间',
    data_dt STRING COMMENT '数据日期',
    etl_data_dt DATETIME COMMENT '数据加载日期',
    ns_norm_fast_rate DECIMAL(18,6) COMMENT '前16自然坑位的时间占比，24小时窗口',
    ns_adv_fast_rate DECIMAL(18,6) COMMENT '前16广告坑位时间占比，24小时窗口',
    p1_norm_fast_rate DECIMAL(18,6) COMMENT '自然坑位首页时间占比，24小时窗口',
    p1_adv_fast_rate DECIMAL(18,6) COMMENT '广告坑位首页时间占比，24小时窗口',
    marketplace_id STRING COMMENT '站点编码',
    marketplace_name STRING COMMENT '站点名称'
    )
    PARTITIONED BY (ds STRING)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='亚马逊 搜索词-父asin 搜索排名表')
    LIFECYCLE 366;



DROP TABLE IF EXISTS tmp_adv_search_sub_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_sub_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  t1.marketplace_id
     ,t2.parent_asin
     ,t1.search_term
     ,t1.is_sponsored
     ,CONCAT(TO_CHAR(data_date,'yyyymmdd'),
             CASE    WHEN data_hour < 10 THEN CONCAT('0',data_hour)
                     ELSE data_hour
                 END
      ) ymdh
     ,NVL(t1.page,999999) page
     ,NVL(t1.search_rank,999999) search_rank
     ,NVL((t1.page - 1) * 68 + t1.search_rank,999999) rank_abs
     ,nvl(period,'day') period
FROM    whde.dwd_itm_sku_amazon_skw_asin_rank_info_hs t1
            LEFT JOIN   (
    SELECT  asin
         ,parent_asin
         ,market_place_id marketplace_id
    from (select *,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
          from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
    where rn =1
) t2
                        ON      t1.asin = t2.asin
                            and t1.marketplace_id=t2.marketplace_id
WHERE   SUBSTR(t1.hs,1,8) >= TO_CHAR(DATEADD(CURRENT_DATE(),-2,'dd'),'yyyymmdd')
  and nvl(period,'day') ='day'
;

DROP TABLE IF EXISTS tmp_adv_search_sub_fast_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_sub_fast_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  t1.marketplace_id
     ,t2.parent_asin
     ,t1.search_term
     ,t1.is_sponsored
     ,CONCAT(TO_CHAR(data_date,'yyyymmdd'),
             CASE    WHEN data_hour < 10 THEN CONCAT('0',data_hour)
                     ELSE data_hour
                 END
      ) ymdh
     ,NVL(t1.page,999999) page
     ,NVL(t1.search_rank,999999) search_rank
     ,NVL((t1.page - 1) * 68 + t1.search_rank,999999) rank_abs
     ,nvl(period,'day') period
FROM    whde.dwd_itm_sku_amazon_skw_asin_rank_info_hs t1
            LEFT JOIN     (
    SELECT  asin
         ,parent_asin
         ,market_place_id marketplace_id
    from (select *,ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) rn
          from whde.dwd_amzn_asin_to_parent_df where ds ='${bizdate}') t
    where rn =1
)t2
                          ON      t1.asin = t2.asin
                              and t1.marketplace_id=t2.marketplace_id
WHERE    SUBSTR(t1.hs,1,8) >= TO_CHAR(DATEADD(CURRENT_DATE(),-4,'dd'),'yyyymmdd')
  and nvl(period,'day') ='day'
;

---最新的自然排名
DROP TABLE IF EXISTS tmp_adv_search_norm_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_norm_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  marketplace_id
     ,parent_asin
     ,search_term
     ,page
     ,search_rank
     ,rank_abs
FROM    (
            SELECT  marketplace_id
                 ,parent_asin
                 ,search_term
                 ,page
                 ,search_rank
                 ,rank_abs
                 ,ROW_NUMBER() OVER (PARTITION BY marketplace_id,parent_asin,search_term ORDER BY rank_abs ASC ) AS rn
            FROM    tmp_adv_search_sub_${bizdate}${bizhour}
            WHERE
                --  TO_DATE(ymdh,'yyyymmddhh') >= DATEADD(GETDATE() ,-4,'hh')
                --  AND
                is_sponsored = 0
        )
WHERE   rn = 1
;

---广告排名
DROP TABLE IF EXISTS tmp_adv_search_adv_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_adv_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  marketplace_id
     ,parent_asin
     ,search_term
     ,page
     ,search_rank
     ,rank_abs
FROM    (
            SELECT  marketplace_id
                 ,parent_asin
                 ,search_term
                 ,page
                 ,search_rank
                 ,rank_abs
                 ,ROW_NUMBER() OVER (PARTITION BY marketplace_id, parent_asin,search_term ORDER BY rank_abs ASC ) AS rn
            FROM    tmp_adv_search_sub_${bizdate}${bizhour}
            WHERE
                -- TO_DATE(ymdh,'yyyymmddhh') >= DATEADD(GETDATE() ,-4,'hh')
                -- AND
                is_sponsored = 1
        )
WHERE   rn = 1
;


DROP TABLE IF EXISTS tmp_adv_term_rank_pasin_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_term_rank_pasin_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  t1.marketplace_id
     ,t1.parent_asin
     ,t1.search_term
     ,t2.rank_abs rank_norm_abs
     ,t3.rank_abs rank_adv_abs
FROM    (
            SELECT  marketplace_id
                 ,parent_asin
                 ,search_term
            FROM    tmp_adv_search_norm_${bizdate}${bizhour}
            UNION
            SELECT  marketplace_id
                 ,parent_asin
                 ,search_term
            FROM    tmp_adv_search_adv_${bizdate}${bizhour}
        ) t1
            LEFT JOIN tmp_adv_search_norm_${bizdate}${bizhour} t2
ON      t1.parent_asin = t2.parent_asin
    AND     t1.search_term = t2.search_term
    AND     t1.marketplace_id = t2.marketplace_id
    LEFT JOIN tmp_adv_search_adv_${bizdate}${bizhour} t3
    ON      t1.parent_asin = t3.parent_asin
    AND     t1.search_term = t3.search_term
    AND     t1.marketplace_id = t3.marketplace_id
;


DROP TABLE IF EXISTS tmp_adv_search_sub_gp_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_sub_gp_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  marketplace_id
     ,parent_asin
     ,search_term
     ,cnt_norm_ns
     ,cnt_adv_ns
     ,cnt_norm_p1
     ,cnt_adv_p1
     ,cnt_total
     ,CASE   WHEN cnt_total >= 5 THEN cnt_norm_ns / cnt_total
    END ns_norm_rate
     ,CASE   WHEN cnt_total >= 5 THEN cnt_adv_ns / cnt_total
    END ns_adv_rate
     ,CASE   WHEN cnt_total >= 5 THEN cnt_norm_p1 / cnt_total
    END p1_norm_rate
     ,CASE   WHEN cnt_total >= 5 THEN cnt_adv_p1 / cnt_total
    END p1_adv_rate
FROM    (
            SELECT  marketplace_id
                 ,parent_asin
                 ,search_term
                 ,COUNT(DISTINCT
                        CASE    WHEN rank_abs <= 16 AND is_sponsored = 0 THEN ymdh
                            END
                  ) cnt_norm_ns
                 ,COUNT(DISTINCT
                        CASE    WHEN rank_abs <= 16 AND is_sponsored = 1 THEN ymdh
                            END
                  ) cnt_adv_ns
                 ,COUNT(DISTINCT CASE    WHEN page = 1 AND is_sponsored = 0 THEN ymdh
                END) cnt_norm_p1
                 ,COUNT(DISTINCT CASE    WHEN page = 1 AND is_sponsored = 1 THEN ymdh
                END) cnt_adv_p1
                 ,COUNT(DISTINCT ymdh) cnt_total
            FROM    tmp_adv_search_sub_${bizdate}${bizhour}
            where period='day'
              and parent_asin is not null
            GROUP BY marketplace_id
                   ,parent_asin
                   ,search_term
        )
WHERE   cnt_total >= 5
;



DROP TABLE IF EXISTS tmp_adv_search_sub_gp_fast_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_sub_gp_fast_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  marketplace_id
     ,parent_asin
     ,search_term
     ,CASE   WHEN cnt_total >= 3 THEN cnt_norm_ns / cnt_total
    END ns_norm_fast_rate
     ,CASE   WHEN cnt_total >= 3 THEN cnt_adv_ns / cnt_total
    END ns_adv_fast_rate
     ,CASE   WHEN cnt_total >= 3 THEN cnt_norm_p1 / cnt_total
    END p1_norm_fast_rate
     ,CASE   WHEN cnt_total >= 3 THEN cnt_adv_p1 / cnt_total
    END p1_adv_fast_rate
FROM    (
            SELECT  marketplace_id
                 ,parent_asin
                 ,search_term
                 ,COUNT(DISTINCT
                        CASE    WHEN rank_abs <= 16 AND is_sponsored = 0 THEN ymdh
                            END
                  ) cnt_norm_ns
                 ,COUNT(DISTINCT
                        CASE    WHEN rank_abs <= 16 AND is_sponsored = 1 THEN ymdh
                            END
                  ) cnt_adv_ns
                 ,COUNT(DISTINCT CASE    WHEN page = 1 AND is_sponsored = 0 THEN ymdh
                END) cnt_norm_p1
                 ,COUNT(DISTINCT CASE    WHEN page = 1 AND is_sponsored = 1 THEN ymdh
                END) cnt_adv_p1
                 ,COUNT(DISTINCT ymdh) cnt_total
            FROM    tmp_adv_search_sub_fast_${bizdate}${bizhour}
            where period='day'
              and  parent_asin IS NOT NULL
            GROUP BY marketplace_id
                   ,parent_asin
                   ,search_term
        )
WHERE   cnt_total >= 3
;


DROP TABLE IF EXISTS tmp_adv_search_rank_label_${bizdate}${bizhour}
;

CREATE TABLE tmp_adv_search_rank_label_${bizdate}${bizhour}
    LIFECYCLE 1 AS
SELECT  t1.marketplace_id
     ,t4.marketplace_name
     ,t1.parent_asin
     ,t1.search_term
     ,t1.cnt_norm_ns
     ,t1.cnt_adv_ns
     ,t1.cnt_norm_p1
     ,t1.cnt_adv_p1
     ,t1.cnt_total
     ,t1.ns_norm_rate
     ,t1.ns_adv_rate
     ,t1.p1_norm_rate
     ,t1.p1_adv_rate
     ,CASE   WHEN p1_norm_rate >= 0.5 THEN 1
             ELSE 0
    END p1_norm_label
     ,CASE   WHEN p1_adv_rate >= 0.5 THEN 1
             ELSE 0
    END p1_adv_label
     ,CASE   WHEN ns_norm_rate >= 0.5 THEN 1
             ELSE 0
    END ns_norm_label
     ,CASE   WHEN ns_adv_rate >= 0.5 THEN 1
             ELSE 0
    END ns_adv_label
     ,t2.rank_norm_abs
     ,t2.rank_adv_abs
     ,t3.ns_norm_fast_rate
     ,t3.ns_adv_fast_rate
     ,t3.p1_norm_fast_rate
     ,t3.p1_adv_fast_rate
FROM    tmp_adv_search_sub_gp_${bizdate}${bizhour} t1
LEFT JOIN tmp_adv_term_rank_pasin_${bizdate}${bizhour} t2
ON      t1.marketplace_id = t2.marketplace_id
    AND     t1.parent_asin = t2.parent_asin
    AND     t1.search_term = t2.search_term
    LEFT JOIN tmp_adv_search_sub_gp_fast_${bizdate}${bizhour} t3
    ON      t1.marketplace_id = t3.marketplace_id
    AND     t1.parent_asin = t3.parent_asin
    AND     t1.search_term = t3.search_term
    left join (
    SELECT  market_place_id marketplace_id
    ,concat(country_cn_name,'站') marketplace_name
    FROM    whde.dim_marketplace_info_df
    GROUP BY market_place_id
    ,country_cn_name
    )t4 on t1.marketplace_id=t4.marketplace_id
WHERE   t1.parent_asin IS NOT NULL
;




INSERT OVERWRITE TABLE adm_amazon_adv_search_term_pasin_rank_df PARTITION (ds = '${bizdate}')
SELECT
    parent_asin
     ,search_term
     ,rank_norm_abs
     ,rank_adv_abs
     ,cnt_norm_ns
     ,cnt_adv_ns
     ,cnt_norm_p1
     ,cnt_adv_p1
     ,cnt_total
     ,ns_norm_rate
     ,ns_adv_rate
     ,p1_norm_rate
     ,p1_adv_rate

     ,p1_norm_label
     ,p1_adv_label
     ,ns_norm_label
     ,ns_adv_label
     ,(
    SELECT  TO_DATE(MAX(ymdh),'yyyymmddHH')
    FROM    tmp_adv_search_sub_${bizdate}${bizhour}
)  data_time
     ,GETDATE() create_time
     ,'${bizdate}' data_dt
     ,getdate()  etl_data_dt

     ,ns_norm_fast_rate
     ,ns_adv_fast_rate
     ,p1_norm_fast_rate
     ,p1_adv_fast_rate
     ,marketplace_id
     ,marketplace_name
FROM    tmp_adv_search_rank_label_${bizdate}${bizhour}
WHERE   parent_asin IS NOT NULL
;
