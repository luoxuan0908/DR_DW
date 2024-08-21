--odps sql
--********************************************************************--
--author:Ada
--create time:2024-05-03 17:08:34
--********************************************************************--

create table if not exists dws_mkt_adv_asq_parent_asin_manager_ds
(
    adv_manager_id    STRING COMMENT '运营负责人ID'
    ,adv_manager_name  STRING COMMENT '运营负责人名称'
    ,marketplace_id    STRING COMMENT '站点ID'
    ,marketplace_name  STRING COMMENT '站点名称'
    ,seller_id         STRING COMMENT '卖家ID'
    ,seller_name       STRING COMMENT '卖家名称'
    ,top_parent_asin   STRING COMMENT '父ASIN'
)
    partitioned by
(
    ds                 string
)
    stored as aliorc
    tblproperties ('comment' = '爱思奇运营负责关系表')
    lifecycle 7
;

alter table dws_mkt_adv_asq_parent_asin_manager_ds drop if exists partition (ds = '${bizdate}');

insert into table dws_mkt_adv_asq_parent_asin_manager_ds partition (ds = '${bizdate}')
(
     adv_manager_id
    ,adv_manager_name
    ,marketplace_id
    ,marketplace_name
    ,seller_id
    ,seller_name
    ,top_parent_asin

)
select  q2.employee_no as manager_id
     ,q1.manager_name
     ,q1.marketplace_id
     ,q1.marketplace_name
     ,q1.seller_id
     ,q1.seller_name
     ,q1.top_parent_asin
from (
         select  case when top_parent_asin = 'B08XYXFN9Q' then '12816' else manager_id end  as manager_id
              ,case when top_parent_asin = 'B08XYXFN9Q' then '陈辉哲' else manager_name end as manager_name
              ,marketplace_id
              ,marketplace_name
              ,seller_id
              ,seller_name
              ,top_parent_asin
              ,row_number()over(partition by marketplace_id,seller_id,top_parent_asin order by manager_id ) as rk
         from asq_dw.dwd_asq_parent_asin_manager_ds
         where ds = '${bizdate}' and manager_id is not null
     )q1
         inner join (
    select name,max(employee_no) as employee_no
    from open_dw.dwd_pub_user_usc_employee_info_df
    where ds = max_pt('open_dw.dwd_pub_user_usc_employee_info_df') and tenant_id = '1555073968741003270' and employee_status = 4 and employee_no is not null
    group by name
)q2
                    on q1.manager_name = q2.name
where q1.rk = 1
;