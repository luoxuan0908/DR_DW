--odps sql
--********************************************************************--
--author:Ada
--create time:2024-07-23 23:54:09
--********************************************************************--


select* from  authorization_account
where pt = '${bizdate}';

create table user_related_seller as
select store_id,tenant_name from authorization_account
where pt = '${bizdate}';

insert OVERWRITE table user_related_seller
VALUES
('10000188','tianz'),
('10000188','JLLNLZQ'),
('10000188','LXHSTOREFA'),
('10000188','FIGHTWZW'),
('10000188','QFQJYFA'),
('10000191','LXLBEAUTY'),
('10000191','LYWUTT'),
('10000191','ZZHZBD'),
('10000191','ZLBYLGO'),
('10000191','GOXGKK'),
('10000192','HYSJYZQ'),
('10000192','LXBJYJY'),
('10000192','LKHRICH'),
('10000192','LIUYHYD'),
('10000192','XSKJYFA')
;

select * from user_related_seller
;


select * from asin_related_user
where asin = 'B0BZBNH2MN'
  and  pt = '${bizdate}'
;



select* from  asin_related_user
where pt = '${bizdate}';

select* from  user_info--asin_related_user
where pt = '${bizdate}';

select* from  dim_user_permission_info_df--user_info--asin_related_user

select* from seller_related_user where pt =  '${bizdate}';

insert OVERWRITE table seller_related_user PARTITION  (pt =  '${bizdate}')
select 1
     ,c.seller_id,a.store_id ,a.tenant_name,c.marketplace_id,c.countrycode,a.store_id,'',GETDATE()
from user_related_seller a
         left outer join (
    select* from  user_info--asin_related_user
    where pt = '${bizdate}' and status =1) b
                         on a.store_id = b.store_id
         left outer join (
    select*,SPLIT_PART(seller_name,'-',1) seller_name1 from  authorization_account
    where pt = '${bizdate}') c
                         on a.tenant_name = c.seller_name1
;

insert OVERWRITE table asin_related_user PARTITION  (pt =  '${bizdate}')
select  id, asin, shop_id, belong_id, manager_id, marketplace_id, country_code from asin_related_user
where pt = '20240722'
;


insert OVERWRITE table asin_related_user PARTITION  (pt =  '${bizdate}')
--create TABLE asin_related_user_new as
select distinct 1 , c.parent_asin
              ,a.seller_id
              ,a.manager_id
              ,a.manager_id
              ,a.market_place_id
              ,c.market_place_name
from (select * from seller_related_user where pt =  '${bizdate}')a
         left outer join (select * from get_merchant_listings_all_data where pt =  '${bizdate}' ) b
                         on a.seller_id= b.seller_id
                             and a.market_place_id  =b.marketplace_id
         left outer join (select *，ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) AS rn
                          from dwd_amzn_asin_to_parent_df where ds='${bizdate}' ) c
                         on b.asin1 = c.asin
                             and b.marketplace_id =c.market_place_id
where c.rn=1
;

select * from  seller_related_user where pt =  '${bizdate}' and seller_id = 'AJG2NA4M1JFN7'
;

select* from asin_related_user
where asin = 'B0BZBNH2MN'
  and pt =  '${bizdate}'
;
select * from get_merchant_listings_all_data where pt =  '${bizdate}'
                                               and asin1 = 'B0BZBNH2MN'

select *，ROW_NUMBER() OVER(PARTITION BY market_place_id,asin ORDER BY data_dt desc) AS rn
from dwd_amzn_asin_to_parent_df where ds='${bizdate}' and parent_asin  = 'B0BZBNH2MN'
;

select tenant_id
     ,sum(impressions) impressions
     ,sum(clicks) clicks
     ,sum(cost) cost
     ,sum(sale_amt_adv) sale_amt_adv
     ,sum(sale_num_adv) sale_num_adv
     ,sum(order_num_adv) order_num_adv
     ,sum(sale_amt_total) sale_amt_total
from adm_parent_asin_adv_info_ds_tmp
where ds>='20240622'
group by tenant_id
;


select tenant_id,SELLER_ID, count(*),count(asin1) ,count(asin2) ,count(asin3) ,count(product_id)
from get_merchant_listings_all_data where pt =  '${bizdate}'
group by  tenant_id,SELLER_ID
;
