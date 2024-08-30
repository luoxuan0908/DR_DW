
-- drop TABLE IF  EXISTS dim.dim_base_marketplace_info_df;
CREATE TABLE IF NOT EXISTS amz.dim_base_marketplace_info_df(
    market_place_type STRING COMMENT '站点类型',
    country_en_name STRING COMMENT '国家英文名',
    country_cn_name STRING COMMENT '国家中文名',
    country_code STRING COMMENT '国家编码',
    market_place_id STRING COMMENT '站点ID',
    currency_en STRING COMMENT '货币简称',
    currency_cn STRING COMMENT '货币中文名',
    marketplace_website STRING COMMENT '站点网页链接',
    timezone STRING COMMENT '时区',
    endpoint_code STRING COMMENT '端点缩写(na-北美|eu-欧洲|fe-远东)'
) PARTITIONED BY (ds STRING)
    STORED AS ORC
    TBLPROPERTIES ('comment'='亚马逊站点维表');



insert overwrite table amz.dim_base_marketplace_info_df PARTITION (ds='20240826')  VALUES
('North America','Canada','加拿大','CA','A2EUQ1WTGCTBG2','CAD','加元','https://www.amazon.ca','America/Los_Angeles','NA')
,('North America','United States of America','美国','US','ATVPDKIKX0DER','USD','美元','https://www.amazon.com','America/Los_Angeles','NA')
,('North America','Mexico','墨西哥','MX','A1AM78C64UM0Y8','MXN','比索','https://www.amazon.com.mx','America/Los_Angeles','NA')
,('North America','Brazil','巴西','BR','A2Q3Y263D00KWC','BRL','雷亚尔','https://www.amazon.com.br','America/Sao_Paulo','NA')
,('Europe','Spain','西班牙','ES','A1RKKUPIHCS9HS','EUR','欧元','https://www.amazon.es','Europe/Paris','EU')
,('Europe','United Kingdom','英国','UK','A1F83G8C2ARO7P','GBP','英镑','https://www.amazon.co.uk','Europe/London','EU')
,('Europe','France','法国','FR','A13V1IB3VIYZZH','EUR','欧元','https://www.amazon.fr','Europe/Paris','EU')
,('Europe','Belgium','比利时','BE','AMEN7PMS3EDWL','EUR','欧元','https://www.amazon.com.be','Europe/Brussels','EU')
,('Europe','Netherlands','荷兰','NL','A1805IZSGTT6HS','EUR','欧元','https://www.amazon.nl','Europe/Amsterdam','EU')
,('Europe','Germany','德国','DE','A1PA6795UKMFR9','EUR','欧元','https://www.amazon.de','Europe/Paris','EU')
,('Europe','Italy','意大利','IT','APJ6JRA9NG5V4','EUR','欧元','https://www.amazon.it','Europe/Paris','EU')
,('Europe','Sweden','瑞典','SE','A2NODRKZP88ZB9','SEK','瑞典克朗','https://www.amazon.se','Europe/Stockholm','EU')
,('Europe','South Africa','南非','ZA','AE08WJ6YKNBMC','ZAR','南非兰特','N',null,null)
,('Europe','Poland','波兰','PL','A1C3SOZRARQ6R3','PLN','波兰兹罗提','https://www.amazon.pl','Europe/Warsaw','EU')
,('Europe','Egypt','埃及','EG','ARBP9OOSHTCHU','EGP','埃及镑','https://www.amazon.eg',null,null)
,('Europe','Turkey','土耳其','TR','A33AVAJ2PDY3EV','TRY','土耳其里拉','https://www.amazon.com.tr','Europe/Istanbul','EU')
,('Europe','Saudi Arabia','沙特阿拉伯','SA','A17E79C6D8DWNP','SAR','沙特里亚尔','https://www.amazon.sa','Asia/Riyadh','EU')
,('Europe','United Arab Emirates','阿拉伯联合酋长国','AE','A2VIGQ35RCS4UG','AED','迪拉姆','https://www.amazon.ae','Asia/Dubai','EU')
,('Europe','India','印度','IN','A21TJRUUN4KGV','INR','印度卢比','https://www.amazon.in',null,null)
,('Far East','Singapore','新加坡','SG','A19VAU5U5O7RUS','SGD','新加坡元','https://www.amazon.sg',null,null)
,('Far East','Australia','澳大利亚','AU','A39IBJ37TRP1C6','AUD','澳元','https://www.amazon.com.au','Australia/Sydney','FE')
,('Far East','Japan','日本','JP','A1VC38T7YXB528','JPY','日元','https://www.amazon.co.jp','Asia/Tokyo','FE');
