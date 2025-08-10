# PySpark Format Conversion

def get_abc_model(month):
    path = f"gs://aaa/bbb/ccc"
    df = spark.read.format("delta").load(path).withColumn("month", lit(month))
    print("get ABC model data")
    print("use month: " + str(month))
    df.groupby("month").agg(countDistinct("id")).show()

def get_abc_group_product(df):
    list_life_ins = ["abc_life1", "abc_life2"]
    list_health_ins = ["abc_heath1", "abc_health2", "abc_health3"]

    df = sd.withColumn("product_group",
                       when(col('product').isin(list_life_ins), lit('abc_life'))
                      .when(col('product').isin(list_health_ins), lit('abc_health')).otherwise('other'))
    return df


def get_abc_univ_analysis(month):
    df = spark.table("schema.table_1").filter(col('month').isin(month))
    print("get abc data universe analysis")
    print("use month: ", str(month))
    df.groupBy('month')agg(countDistinct('id'), sum('sent')).show()
    return df


def get_domainlist(month):
    path= f"gs://aaa/bbb/ccc"
    df = spark.read.format("delta").load(path).filter(col('par_month').isin(month))
    print("get domain list from abc 01 get domain")
    print("use month: " + str(month))
    df.agg(count('id'), countDistinct('id')).display()
    return df


# [EDIT] path info
client = "abc"
user_nm = "pakt"
current_month = datetime.now().strftime("%Y%m")
month_l1 = (datetime.now() - timedelta(days=30)).strftime("%Y%m")
month_l2 = (datetime.now() - timedelta(days=60)).strftime("%Y%m")
path_main = f"gs://aaa/bbb/ccc"
print(path_main)

print("current month: ", current_month)
print("last 1 month: ", month_l1)
print("last 2 month: ", month_l2)

# Get website list
# run pipeline
df_domain = get_domainlist(month_l1)
df_domain.groupBy('month').agg(countDistinct('domainname')).display()

df_univ_analysis = get_abc_univ_analysis(month_l1)
df_univ_analysis = get_abc_group_product(df_univ_analysis)

df_domain.createOrReplaceTempView("df_domain")
df_univ_analysis.createOrReplaceTempView("df_aniv_analysis")

# Get back test performance by website list
df_back_test = spark.sql(f"""
                        with abc_univ_ana_all_prod as
                        (
                        select *
                        from df_univ_analysis
                        where campaign_type not in ("retarget")
                        )
                        , abc_univ_ana_paid_prod as
                        (
                        select *
                        from df_univ_analysis
                        where campaign_type not in ("retarget")
                        and product_group not in ("abc_life1", "abc_life2", "abc_life3")
                        )
                        , abc_univ_data_free_prod as
                        (
                        select *
                        from df_univ_analysis
                        where campaign_type not in ("retarget")
                        and product_group in ("abc_life1_free", "abc_life2_free", "abc_life3_free")
                        )
                        , abc_website_univ_ana_all_prod as
                        (
                        select a.domainname
                        , a.segment
                        , a.sent as sent_all_prod
                        , a.clicked as clicked_all_prod
                        , a.filledform as filledform_all_prod
                        from df_domain a
                        left join abc_univ_ana_all_prod b
                        on a.id = b.id
                        )
                        , abc_website_univ_ana_paid_prod as
                        (
                        select a.domainname
                        , a.segment
                        , a.sent as sent_paid_prod
                        , a.clicked as clicked_paid_prod
                        , a.filledform as filledform_paid_prod
                        from df_domain a
                        left join abc_univ_ana_paid_prod b
                        on a.id = b.id
                        )
                        , abc_website_univ_ana_free_prod as
                        (
                        select a.domainname
                        , a.segment
                        , a.sent as sent_free_prod
                        , a.clicked as clicked_free_prod
                        , a.filledform as filledform_free_prod
                        from df_domain a
                        left join abc_univ_ana_free_prod b
                        on a.id = b.id
                        )
                        , abc_website_univ_ana_all_prod_agg as
                        (
                        select domainname
                        , segment
                        , sum(sent_all_prod) as sent_all_prod
                        , sum(clicked_all_prod) as clicked_all_prod
                        , sum(filledform_all_prod) as filledform_all_prod
                        from abc_website_univ_ana_all_prod
                        group by domainname, segment
                        )
                        , abc_website_univ_ana_paid_prod_agg as
                        (
                        select domainname
                        , segment
                        , sum(sent_paid_prod) as sent_paid_prod
                        , sum(clicked_paid_prod) as clicked_paid_prod
                        , sum(filledform_paid_prod) as filledform_paid_prod
                        from abc_website_univ_ana_paid_prod
                        group by domainname, segment
                        )
                        , abc_website_univ_ana_free_prod_agg as
                        (
                        select domainname
                        , segment
                        , sum(sent_free_prod) as sent_free_prod
                        , sum(clicked_free_prod) as clicked_free_prod
                        , sum(filledform_free_prod) as filledform_free_prod
                        from abc_website_univ_ana_free_prod
                        group by domainname, segment
                        )
                        , domain_segment as
                        (
                        select distinct domain, segment
                        from df_domain
                        )
                        , combined as
                        (
                        select a.domainname
                         , a.segment
                         
                         , b.sent_all_prod
                         , b.clicked_all_prod
                         , b.filledform_all_prod
                         , case when b.clicked_all_prod > 0 then b.clicked_all_prod/b.sent_all_prod else 0 end as ctr_all_prod
                         , case when b.filledform_all_prod > 0 then b.filledform_all_prod/b.clicked_all_prod else 0 end as cvr_all_prod
                         , case when b.filledform_all_prod > 0 then b.filledform_all_prod/b.sent_all_prod * 10000 else 0 end as lead10k_all_prod

                         , c.sent_paid_prod
                         , c.clicked_paid_prod
                         , c.filledform_paid_prod
                         , case when c.clicked_paid_prod > 0 then c.clicked_paid_prod/c.sent_paid_prod else 0 end as ctr_paid_prod
                         , case when c.filledform_paid_prod > 0 then c.filledform_paid_prod/c.clicked_paid_prod else 0 end as cvr_paid_prod
                         , case when c.filledform_paid_prod > 0 then c.filledform_paid_prod/c.sent_paid_prod * 10000 else 0 end as lead10k_paid_prod

                         , d.sent_free_prod
                         , d.clicked_free_prod
                         , d.filledform_free_prod
                         , case when d.clicked_free_prod > 0 then d.clicked_free_prod/d.sent_free_prod else 0 end as ctr_free_prod
                         , case when d.filledform_free_prod > 0 then d.filledform_free_prod/d.clicked_fre_prod else 0 end as cvr_free_prod
                         , case when d.filledform_free_prod > 0 then d.filledform_free_prod/d.sent_free_prod * 10000 else 0 end as lead10k_free_prod
                         
                        from domain_segment a
                        left join abc_website_univ_ana_all_prod_agg b on a.domainname = b.domainname and a.segment = b.segment
                        left join abc_website_univ_ana_paid_prod_agg c on a.domainname = c.domainname and a.segment = c.segment
                        left join abc_website_univ_ana_free_prod_agg d on a.domainname = d.domainname and a.segment = d.segment
                        )
                        , use_df as
                        (
                        select *
                        , {month_l1} as month
                        , row_number() over(partition by domainname order by segment) as row_num
                        from combined
                        where filledform_all_prod > 0
                        )
                        select *
                        from use_df
                        where row_num = 1
                        """)


# list columns
back_test_cols = [
    'domainname',
    'segment',
    'sent_all_prod',
    'clicked_all_prod',
    'filledform_all_prod',
    'ctr_all_prod',
    'cvr_all_prod',
    'lead10k_all_prod',
    'sent_paid_prod',
    'clicked_paid_prod',
    'filledform_paid_prod',
    'ctr_paid_prod',
    'cvr_paid_prod',
    'lead10k_paid_prod',
    'sent_free_prod',
    'clicked_free_prod',
    'filledform_free_prod',
    'ctr_free_prod',
    'cvr_free_prod',
    'lead10k_free_prod',
    'month'
]

df_back_test = df_back_test.select(back_test_cols)

# save to delta
path_back_test = f"{main_path}/website_backtest"
print(path_back_test)
df_back_test = save_delta_month(path_back_test, df_back_test, month_l1)
print("number of website clustering back test")
df_back_test.agg(count('domainname'), countDistinct('domainname')).display()


df_back_test.display()
