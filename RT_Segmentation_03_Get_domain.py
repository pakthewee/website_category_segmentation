# PySpark Format Conversion

# Import libraries
import pandas as pd
import numpy as np
from math import pi
from datetime import datetime, tmedelta, time
from dateutil.relativedelta import *
from calendar import monthrange
from functools import reduce
from pyspark.sql.functions import *
from pyspark.sql import *


# Function 1: Get domain from table 1
def get_domain_backtest(month):
    path = f"gs://aaa/bbb/ccc"
    df = spark.read.format("delta").load(path).filter(col('par_month').isin(month))
    print("get domain back test data from ABC 02 get performance")
    print("use month: " + str(month))
    df.agg(count("domainname"), countDistinct("domainname")).display()


def get_domain_hitcnt(month, list_domain_i):
    df = spark.table('schema.table1')\
                .filter(col('month').isin(month))\
                .filter(col('domainname').isin(list_domain_i))\
                .select('domainname', 'session_hit_count', 'month').disinct()
    print("get domain hit count")
    df.agg(count('domainname'), countDistinct('domainname')).show()
    return df

client = "ABC"
user_nm = "pakt"
current_month = datetime.now().strftime("%Y%m")
month_l1 = (datetime.now() - tiedelta(days=30)).strftime("%Y%m")
month_l2 = (datetime.now() - tiedelta(days=60)).strftime("%Y%m")
path_main = f"gs://aaa/bbb/ccc"
print(path_main)

print("current month: ", current_month)
print("last 1 month: ", month_l1)
print("last 2 month: ", month_l2)


# Get domain sizing
df_backtest = get_domain_backtest(month_l1)
df_backtest.createOrReplaceTempView("vw_backtest")
list_domain = df_backtest.select('domainname').toPandas()['domainname'].tolist()
print(list_domain)

df_domain_hitcnt = get_domain_hitcnt(month_l2, list_domain)
df_domain_hitcnt.creaeOrReplaceTempView("vw_domain_hitcnt")

# Get back test performance
df_backtest_hitcnt = spark.sql(f"""
                                with df as
                                (
                                select a.*
                                , b.session_hit_cnt
                                , b.session_hit_cnt/30/12/60/60 as tps
                                from vw_backtest a
                                left jion vw_domain_hitcnt b
                                on a.domainname = b.domainname
                                where b.session_hit_cnt > 0
                                )
                                select *
                                from df
                                where tps between 0.04 and 55
                                and lead10k_all_prod <= 10
                                and cvr_all_prod <= 0.2
                                """)

cols = ['domainname',
        'segment',
        'sent_all_prod',
        'clicked_all_prod',
        'filledform_all_prod',
        'ctr_all_prod',
        'cvr_all_prod',
        'lead10k_all_prod',
        'session_hit_cnt',
        'tps']

df_backtest_hitcnt = df_backtest_hitcnt.select(cols)
path_seq = f"{path}/website_backtest_hitcnt"
print(path_seq)
df_backtest_hitcnt = save_delta_parmonth(path_seq, df_backtest_hitcnt, month_l1)
