--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-01 17:18:40
--********************************************************************--

CREATE TABLE IF NOT EXISTS whde.ods_manual_dim_amazon_promotion_calendar(
                                                                            promotion_day STRING COMMENT '',
                                                                            promotion_name STRING COMMENT ''
)
    STORED AS ALIORC
    TBLPROPERTIES ('comment'='大促日期')
    LIFECYCLE 366;




INSERT OVERWRITE TABLE  ods_manual_dim_amazon_promotion_calendar VALUES
('20231120','黑五网一'),
('20231121','黑五网一'),
('20231122','黑五网一'),
('20231123','黑五网一'),
('20231124','黑五网一'),
('20231125','黑五网一'),
('20231126','黑五网一'),
('20231127','黑五网一')
;
