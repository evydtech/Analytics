# Databricks notebook source
# MAGIC %md
# MAGIC ## RDD

# COMMAND ----------

import sys
import hashlib
from pyspark.sql.functions import *

my_data = spark.table('bru_stg.stg_patients')
for colz in my_data.columns:
    my_data = my_data.withColumn(colz, col(colz).cast('string'))
    
def add_required_filed(data):
    yidu_pkid = hashlib.md5(str(data).encode('utf-8')).hexdigest()
    data['yidu_pkid'] = yidu_pkid

    data['visit_sn'] = data.get('visit_id') or data.get('visit_sn') or data.get('inpatient_visit_id') or data.get('outpatient_visit_id')
    data['file_name'] = 'file_name'
    return data

my_rdd = my_data.rdd\
    .map(lambda x: x.asDict(recursive=True))\
    .map(lambda x: add_required_filed(x)) 

mp_rdd2 = my_rdd.map(lambda x: (x['id'], x)).groupByKey().map(lambda x: (x[0], {'inner_name': list(x[1])}))
mp_rdd3 = my_rdd.map(lambda x: (x['id'], x))

#.map(lambda x: utils.merge_data(x[1][0], x[1][1]))
# .map(lambda x: (x[0], {inner_dict['full_name']: list(x[1])}))

def merge_data(tb_info1, tb_info2):
    if tb_info1 is None:
        return tb_info2
    if tb_info2 is None:
        return tb_info1

    for k in tb_info2.keys():
        if k in tb_info1:
            print('hiasdiasid')
            if not isinstance(tb_info1[k], list):
                tb_info1[k] = [tb_info1[k]]
            if not isinstance(tb_info2[k], list):
                tb_info2[k] = [tb_info2[k]]
            tb_info1[k].extend(tb_info2[k])
        else:
            print('hiasdiasid')
#             print(tb_info1[k])
            tb_info1[k] = tb_info2[k]
    return tb_info1

def add_basic_strategy(data):
    # 加患者人口学信息(MCE代码有限制，pp必须有patient_basic_information_strategy表)
    # patient_basic_information_strategy 长度为1，添加pix_domain.patient_local_sn信息
    if 'patient_basic_information_strategy' not in data[1]:
        return data
    pix_domain = []
    basic_item = data[1]['patient_basic_information_strategy'][0]
    for tmp_basic in data[1]['patient_basic_information_strategy']:
        # 目前只添加了patient_local_sn信息，后续有需要可以添加机构信息
        pix_domain.append({'patient_local_sn': tmp_basic['patient_id']})
        if tmp_basic['patient_id'] == data[0]:
            basic_item = tmp_basic
    basic_item['pix_domain'] = pix_domain
    basic_item['patient_sn'] = data[0].lower()
    data[1]['patient_basic_information_strategy'] = [basic_item]

    return data

mp_rdd4 = mp_rdd3.leftOuterJoin(mp_rdd2).map(lambda x: merge_data(x[1][0], x[1][1]))
mp_rdd4.take(1)

test = my_data.select('id','birthdate','gender')
my_test = test.rdd\
    .map(lambda x: x.asDict(recursive=True))\
    .map(lambda x: add_required_filed(x)) 

mp_rdd3 = my_rdd.map(lambda x: (x['id'], x))
my_test = my_test.map(lambda x: (x['id'], x)).groupByKey().map(lambda x: (x[0], {'inner_name': list(x[1])}))
mp_rdd_test = mp_rdd3.leftOuterJoin(my_test).map(lambda x: merge_data(x[1][0], x[1][1]))


new_rdd = mp_rdd_test.map(lambda x: (x['id'], {x['file_name']: [x]})) 
# new_rdd.collect()

result_rdd = new_rdd.reduceByKey(lambda x, y: merge_data(x, y)).map(add_basic_strategy)

result_rdd= result_rdd.map(lambda x: json.dumps(x[1], ensure_ascii=False))


rdd1 = result_rdd.map(json.loads)
rdd1.collectAsMap()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dataframe

# COMMAND ----------

from pyspark.sql.functions import *

# replace add_required_filed function in utils (for exec_sql) -> don't need to use this function

my_df = my_data

col_list = [i for i in my_data.columns]

my_df = my_df.withColumn(
    'yidu_pk_id', md5(concat_ws('',*col_list))
)
cols_list = ['visit_id','visit_sn','inpatient_visit_id','ssn','outpatient_visit_id']
for colz in cols_list:
    if colz in my_data.columns:
        my_df = my_df.withColumn(
            'visit_sn', col(colz)
        )
        break
    else:
        my_df = my_df.withColumn(
            'visit_sn', lit('')
        )
        
my_df.show()

# COMMAND ----------


