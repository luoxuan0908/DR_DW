##@resource_reference{"myutils.py"}
'''PyODPS 3
请确保不要使用从 MaxCompute下载数据来处理。下载数据操作常包括Table/Instance的open_reader以及 DataFrame的to_pandas方法。
推荐使用 PyODPS DataFrame（从 MaxCompute 表创建）和MaxCompute SQL来处理数据。
更详细的内容可以参考：https://help.aliyun.com/document_detail/90481.html
'''


import os
import sys
sys.path.append(os.path.dirname(os.path.abspath('myutils.py'))) #引入资源至工作空间。

from myutils import *
import time
import datetime
import pytz
import requests
import json
from tqdm import tqdm
import random
from odps.df import DataFrame
import pandas as pd



# 调取亚马逊接口获得当前竞价
def get_now_bid(row):
    url='http://120.55.240.153:8535/get_now_bid'

    headers={
        'Content-Type': 'application/json',
        # 'Cookie' : 'LOGIN_KEY=idaas_test-trunk_f5fcccc9-2171-4a0d-942a-a451495530aa'
        }
    param={'profileId':row['profile_id'],
            "includeExtendedDataFields": "false",
            'keywordIdFilter':[row['target_id']]
            }
    res=requests.post(url,data=json.dumps(param),headers=headers)
    bid=res.json()['keywords'][0]['bid']
    time.sleep(random.randint(1,3))
    return bid


# 根据排名设置调整竞价
def get_bid_rank(row):

    if row['before_adjust_bid'] is None :
        now_bid = 0.75
    else:
        now_bid = row['before_adjust_bid']

    # norm_rank = int(row['norm_rank'])
    # adv_rank = int(row['adv_rank'])
    adjust_type = str(row['bid_adjust_label'])

    if adjust_type == '竞价不变':
        adjust_bid = now_bid
    elif adjust_type == '上调竞价':
        adjust_bid = min(now_bid * 1.1,1.5)  ##上调比例和上限
    else:
        adjust_bid = max(now_bid * 0.9,0.2)  ##下调比例和下限

    adjust_bid = round(adjust_bid,2)

    return adjust_bid

# 根据竞价生成所需参数
def generate_param(row):
    param={
    "bid": row['adjust_bid'],
    "campaignId": row['campaign_id'],
    "employeeName":row['adv_manager_name'],
    "employeeNo":row['adv_manager_id'],
    "keywordId": row['target_id'],
    "profileId": row['profile_id'],
    "rowId": row['row_id'],
    "tenantId":row['tenant_id']
    }
    return param


# 将dataframe生成指定个df
def generate_batch_df(df, chunk_size=50):
    start = 0
    end = chunk_size
    total_rows = len(df)

    while start < total_rows:
        yield df[start:end]
        start = end
        end += chunk_size


# 调用接口修改竞价
def adjust_bid(param):
    # url='http://fenghuo-pre.zbycorp.com:8080/rpc/intelligentStrategy/location/keyword'
    url='http://120.55.240.153:8535/adjust_bid'
    headers={
    'Content-Type': 'application/json',
    }
    res=requests.post(url,data=json.dumps(param),headers=headers)
    res_json=res.json()
    return res_json



# 分区和表名
hs = args['hs']
pool_table='asq_dw.dws_mkt_adv_strategy_adjust_bid_param_hf'
output_table_name='asq_dw.dws_mkt_adv_strategy_adjust_bid_result_hf'

# 获取数据
sql="""
select tenant_id
      ,row_id
      ,profile_id
      ,adv_manager_id
      ,adv_manager_name
      ,campaign_id
      ,campaign_name
      ,ad_group_id
      ,ad_group_name
      ,top_parent_asin
      ,term_type
      ,target_id
      ,target_term
      ,match_type
      ,clicks
      ,cost
      ,sale_amt
      ,order_num
      ,cpa
      ,cvr
      ,acos
      ,cate_acos
      ,cate_cpa
      ,cate_cvr
      ,norm_rank
      ,adv_rank
      ,bid_adjust_label
from {} where hs='{}'
""".format(pool_table,hs)
adjust_bid_df = read_sql(o,sql)

if adjust_bid_df.shape[0] != 0:
    #获取当前竞价，调整前
    adjust_bid_df['before_adjust_bid'] = adjust_bid_df.apply (get_now_bid,axis=1)

    #根据当前竞价和排名判断需要调整的竞价
    adjust_bid_df['adjust_bid'] = adjust_bid_df.apply (get_bid_rank,axis=1)

    #根据所给的竞价生成调整竞价所需的参数
    adjust_bid_df['params'] = adjust_bid_df.apply (generate_param,axis=1)

    #小批量传递参数
    for small_df in generate_batch_df(adjust_bid_df, chunk_size=50):
        params = small_df[['params']].to_dict(orient='list')
        params = {'result':params['params']}
        adjust_bid(params)

    #查看调整后的竞价
    adjust_bid_df['after_adjust_bid'] = adjust_bid_df.apply (get_now_bid,axis=1)

    #判断是否调整成功
    adjust_bid_df['if_adjust_success'] = adjust_bid_df.apply(lambda row: 1 if row['adjust_bid'] == row['after_adjust_bid'] else 0,axis=1)

    adjust_bid_df = adjust_bid_df[[
            'tenant_id',
            'row_id',
            'profile_id',
            'adv_manager_id',
            'adv_manager_name',
            'campaign_id',
            'campaign_name',
            'ad_group_id',
            'ad_group_name',
            'top_parent_asin',
            'term_type',
            'target_id',
            'target_term',
            'match_type',
            'norm_rank',
            'adv_rank',
            'bid_adjust_label',
            'before_adjust_bid',
            'adjust_bid',
            'after_adjust_bid',
            'if_adjust_success',
            'params'
    ]]
    # # 写入结果数据

    adjust_bid_df = adjust_bid_df.astype(str)
    adjust_bid_df = DataFrame(adjust_bid_df)
    adjust_bid_df.persist(output_table_name,partition='hs={}'.format(hs),odps=o,overwrite = False,create_partition = True )
else :
    print ('数据为空')

