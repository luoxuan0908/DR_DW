
CREATE TABLE IF NOT EXISTS amz.dim_base_amazon_promotion_calendar(
     promotion_day STRING COMMENT '',
     promotion_name STRING COMMENT ''
)
    STORED AS orc
    TBLPROPERTIES ('comment'='大促日期')
;




INSERT OVERWRITE TABLE  amz.dim_base_amazon_promotion_calendar VALUES
('20231120','黑五网一'),
('20231121','黑五网一'),
('20231122','黑五网一'),
('20231123','黑五网一'),
('20231124','黑五网一'),
('20231125','黑五网一'),
('20231126','黑五网一'),
('20231127','黑五网一')
;
