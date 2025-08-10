# PySpark Format Conversion

# Import libraries

# csv function
def save_csv(path, df):
    df.coalesce(1).write.mode("overwrite").format("csv").option("header", True).save(path)

def change_csv_name(path):
    files = dbutils.fs.ls(path)
    csv.file = [x.path for x in files if x.path.endwite(".csv")][0]
    print(csv_file)

dbutils.fs.cp(csv_file, path.rstrip('/') + ".csv")
dbutils.fs.rm(path, True)
print(path.rstrip('/') + ".csv")

client = "abc"
user_nm = "pakt"
current_month = datetime.now().strftime("%Y%m")
month_l1 = (datetime.now() - timedelta(days = 30)).strftime("%Y%m")
month_l2 = (datetime.now() - timedelta(days = 60)).strftime("%Y%m")
path_main = f"gs://aaa/bbb/ccc"
print(path_main)
print(month_l1)
print(month_l2)

path_seq = f"{main_path}/aaa/bbb/ccc"
print(path_seq)
df = spark.read.format("delta").load(path_seq).filter(col('month').isin(month_l1))
df.display()
df.createOrReplaceTempView("df")


# Group NTILE
df_group_ntile = spark.sql(f"""
with group_ntile as
(
    select *
    , ntile(10) over (order by filledform_all_prod) as filledform_all_prod_ntile
    , ntile(10) over (order by ctr_all_prod) as ctr_all_prod_ntile
    , ntile(10) over (order by cvr_all_prod) as cvr_all_prod_ntile
    , ntile(10) over (order by lead10k_all_prod) as lead10k_all_prod_ntile
    , ntile(10) over (order by lead10k_paid_prod) as lead10k_paid_prod_ntile
    , ntile(10) over (order by tps) as tps_ntile
    from df
)
, group_segment as
(
    select case when filledform_all_prod_ntile >= 8
                    and (ctr_all_prod_ntile >= 7
                    or cvr_all_prod_ntile >= 6
                    or tps_ntile >= 8)
                    and lead10k_all_prod_ntile >= 6 then "1.high"

                when filledform_all_prod_ntile >= 6
                    and (ctr_all_prod_ntile >= 5
                    or cvr_all_prod_ntile >= 4
                    or tps_ntile >= 4)
                    and lead10k_all_prod_ntile >= 6 then "2.mid"

                else "3.low" end as risk_group
            , *
    from group_ntile
)
select risk_group
        , domainname
        , segment
        , sent_all_prod
        , clicked_all_prod
        , filledform_all_prod
        , ctr_all_prod
        , cvr_all_prod
        , lead10k_all_prod
        , tps
        , filledform_all_prod_ntile
        , ctr_all_prod_ntile
        , cvr_all_prod_ntile
        , lead10k_all_prod_ntile
        , tps_ntile
        , month
from group_segment
""")

cols = ['risk_group'
        , 'domainname'
        , 'segment'
        , 'sent_all_prod'
        , 'clicked_all_prod'
        , 'filledform_all_prod'
        , 'ctr_all_prod'
        , 'cvr_all_prod'
        , 'lead10k_all_prod'
        , 'tps'
        , 'filledform_all_prod_ntile'
        , 'ctr_all_prod_ntile'
        , 'cvr_all_prod_ntile'
        , 'lead10k_all_prod_ntile'
        , 'tps_ntile'
        , 'month']

df_group_ntile = df_group_ntile.select(cols)
path_df = f"{path_main}/aaa/bbb/ccc"
df_group_ntile = save_delta_month(path_df, df_group_ntile, month_l1)

path_group_ntile = f"{paht_main}/aaa/bbb/ccc"
print(path_group_ntile)
df_group_ntile = save_csv(path_group_ntile, df_group_ntile)
change_csv_name(path_group_ntile)
