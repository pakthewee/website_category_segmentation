# PySpark Format Conversion

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, broadcast, approx_count_distinct
from pyspark.sql import Row

# Start Spark session (if running as script; comment if running in cluster)
# spark = SparkSession.builder.getOrCreate()

## Function 1: Get domain from table 1
def get_domain(label_use, segment_name, month):
    df = spark.sql(f"""
        WITH df1 AS (
            SELECT DISTINCT id,
                            STRING(domainname) AS domainname_temp,
                            month
            FROM table_1
            WHERE label LIKE '%{label_use}%'
              AND month IN ({month})
        )
        SELECT id,
               '{segment_name}' AS segment,
               CASE 
                   WHEN domainname_temp LIKE '%,%' THEN SUBSTRING(domainname_temp, 2, LOCATE(',',domainname_temp,1)-2)
                   ELSE SUBSTRING(domainname_temp, 2, LOCATE(']', domainname_temp,1)-2)
               END AS domainname,
               month
        FROM df1
    """)
    print(f"segment name: {segment_name}")
    return df

## Function 2: Get domain name from table_2 using keyword
def get_keyword_domain(keyword, segment_name, month):
    keyword_lkup = spark.table("schema.table_2") \
        .filter(col('month') == month) \
        .filter(col('domainname').contains(keyword)) \
        .select('domainname').distinct()

    df = spark.table("schema.table_3") \
        .filter(col("month") == month) \
        .join(broadcast(keyword_lkup), 'domainname', 'inner') \
        .select('id', 'domainname', 'month').distinct()

    print(
        f"domain by {keyword} = {keyword_lkup.agg(approx_count_distinct('domainname')).collect()}",
        f"mobiles volume access = {df.agg(approx_count_distinct('id')).collect()}"
    )

    df = df.withColumn('segment', lit(segment_name))
    cols = ['id', 'segment', 'domainname', 'month']
    df = df.select(cols)
    return df

## Function 3: Get domain name from list of websites
def get_website_domain(list_website, segment_name, month):
    df = spark.table("schema.table_3") \
        .filter(col('month') == month) \
        .filter(col('domainname').isin(list_website)) \
        .select('id', 'domainname', 'month').distinct()

    df = df.withColumn('segment', lit(segment_name))
    cols = ['id', 'segment', 'domainname', 'month']
    df = df.select(cols)
    return df

## Function 4: Get server_hostname from list of websites
def get_website_hostname(list_website, segment_name, month):
    df = spark.table("schema.table_4") \
        .filter(col('month') == month) \
        .filter(col('server_hostname').isin(list_website)) \
        .select('id', col('server_hostname').alias('domainname'), 'month').distinct()

    df = df.withColumn('segment', lit(segment_name))
    cols = ['id', 'segment', 'domainname', 'month']
    df = df.select(cols)
    return df

## Function 5: Convert a list of website info to DataFrame
def get_dataframe(list_ws):
    report_frame = [Row(id=x[0], segment=x[1], month=x[2]) for x in list_ws]
    df = spark.createDataFrame(report_frame)
    return df

## Function 6: Save result from each function to the paths
def get_domain_dataframe(df,method,month):
    row_list = df.collect()
    df_ws = []
    for i in range(len(row_list)):
        i_segment = row_list[i][1]
        print("segment: " + str(i_segment))

        if method == "gws":
            df_ws = get_domain(row_list[i][0], row_list[i][1], row_list[i][2])
            path = f"{path}/aaa/bbb/ccc/gws"
            print(path)
            df_ws.write.format("delta").mode("overwrite").save(path)

        elif method == "kw":
            df_ws = get_keyword_domain(row_list[i][0], row_list[i][1], row_list[i][2])
            path = f"{path}/aaa/bbb/ccc/kw"
            print(path)
            df_ws.write.format("delta").mode("overwrite").save(path)
            
        elif method == "sh":
            df_ws = get_website_hostname(row_list[i][0], row_list[i][1], row_list[i][2])
            path = f"{path}/aaa/bbb/ccc/sh"
            print(path)
            df_ws.write.format("delta").mode("overwrite").save(path)

        else:
            df_ws = get_website_domain(row_list[i][0], row_list[i][1], row_list[i][2])
            path = f"{path}/aaa/bbb/ccc/ws"
            print(path)
            df_ws.write.format("delta").mode("overwrite").save(path)

## Function 7: Function write and read
def write_read_from_path(df, path):
    print(path)
    df.write.mode("overwrite").format("delta").save(path)
    data = spark.read.format("delta").load(path)
    return data


## Function 8: Function save data partition by month
def save_delta_parmonth(path, df, month):
    if df.count() == 0:
        print("skip: without data in this month")
        pass

    else:
        # save data post campaign
        df.write.mode("overwrite").partitionBy("month").option("rerplaceWhere", f"month= {month}").format("delta").save(path)
        dataframe = spark.read.format("delta").load(path)
        dataframe = datafrane.filter(col("month").isin(month))
        return dataframe

## Funtion 9: create empty dataframe
def get_empty_dataframe():
    schema = StructType([
        StructFiemd("text", StringType(), True),
        StructFiemd("segment", StringType(), True),
        StructFiemd("month", StringType(), True),
    ])
    prep_segment = spark.createDataframe([], schema)
    prep_segment = prep_segment if prep_segment is not None else spark.createDataFrame([], schema)
    return prep_segment


# [EDIT] Path Info
client = "ABC"
user_nm = "pakt"

use_month = (datetime.now() - timedelta(days = 30)).strftime("%Y%m")
path_main = f"{path}/aaa/bbb/ccc/ddd"
print(path_master)
print("use month: ", use_month)


# Create an empty dataframe for rour types of website lists.
df_list1 = get_empty_dataframe()
df_list2 = get_empty_dataframe()
df_list3 = get_empty_dataframe()
df_list4 = get_empty_dataframe()


# [EDIT] get domainnam from three functioins
# if there are no datasource in some method, please comment that part

## type 1: get website from table_1
list_gws = [
            ("insurance", "insurance", use_month)
            , ("entertainment", "entertainment", use_month)
            , ("finance", "finance", use_month)
            ]
df_list1 = get_dataframe(list_gws)
df_list1.display()

## type 2: get keyword from table_2
list_kw = [
    ("insurance", "insurance", use_month)
]
df_list2 = get_datafrmae(list_kw)
df_list2.display()

## type 3: get domainname from table_3
list_ws = [
    ("iloveu.com", "entertainment", use_month)
]

df_list3 = get_dataframe(list_ws)
df_list3.display()

## type 4: get server hostname from table_4
list_hs = [
    ("www.iloveu.com", "entertainment", use_month)
]

df_list4 = get_dataframe(list_sh)
df_list4.display()


# Get domain dataframe and save on paht
get_domain_dataframe(df_list1, "gws", use_month)
get_domain_dataframe(df_list2, "kw", use_month)
get_domain_dataframe(df_list3, "ws", use_month)
get_domain_dataframe(df_list4, "sh", use_month)


# Read data from path
# get the lists of path
path_gws = f"{path_main}/aaa/gws/{use_month}"
path_kw = f"{path_main}/aaa/kw/{use_month}"
path_ws = f"{path_main}/aaa/ws/{use_month}"
path_hs = f"{path_main}/aaa/hs/{use_month}"

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False

df_path_gws = spark.createDataFrame(dbutils.fs.ls(path_dpi)) if path_exists(path_gws) else None
df_path_kw = spark.createDataFrame(dbutils.fs.ls(path_dpi)) if path_exists(path_kw) else None
df_path_ws = spark.createDataFrame(dbutils.fs.ls(path_dpi)) if path_exists(path_ws) else None
df_path_hs = spark.createDataFrame(dbutils.fs.ls(path_dpi)) if path_exists(path_hs) else None

dfs = [df for df in [df_path_dpi, df_path_kw, df_path_ws,df_path_hs] if df is not None]
df_path = reduce(lamda df1, df2: df1.union(df2), dfs) if dfs else None

if df_path:
    display(df_path)
else:
    print("No valid paths found.")


## Assuming df_path is a DataFrame containing paths to read from
# and that you have at least one path to read from to infer schema
first_path = df_path.collect()[0][0] # get the first path to infer schema
df_all = spark.read.format("delta").load(first_path, header=True).limit(0)
# Create an empty dataframe with the same schema

for row in df_path.collect():
    i_path_nm = row[0]
    print("Read from path: ", i_pat_nm)
    df = spark.read.format("delta").load(i_path_nm, header=True)
    df_all = df_all.union(df)

# if you want to see the count after the loop
df_all_uniq = df_all.select('*').distinct()
df_all_uniq_count = df_all_uniq.count()
print("total count: ", df_all_uniq_count)

# save to data
path = f"gs://aaa/bbb/ccc"
print(path)
print("current count: ", use_month)
df = save_delta_parmonth(path, df_all_uniq,use_month)

### remove df_gws, df_kw, df_ws, df_hs if exists
# dbutils.fs.rm(path_gws, True)
# dbutils.fs.rm(path_kw, True)
# dbutils.fs.rm(path_ws, True)
# dbutils.fs.rm(path_hs, True)


# check path
path = f"gs://aaa/bbb/ccc"
df = spark.read.format("delta").load(path)
df.groupBy("month").agg(count("id"), countDistinct("id")).display()
df.limit(3).display()
