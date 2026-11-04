# Databricks notebook source
# MAGIC %run "../commons/commons"

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, regexp_replace  # commons uses these without importing them
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import datetime
import time
import json

# COMMAND ----------

schema = "pei_assesment"
end_date = datetime.date(9900, 1, 1)
start_date = datetime.date(1900, 1, 1)

# take today and yesterday from spark only,  so session timezone doesn't cause a mismatch
_d = spark.sql("select current_date() as today, date_sub(current_date(), 1) as yday").first()
today, yesterday = _d["today"], _d["yday"]

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("segment", StringType()),
    StructField("country", StringType()),
])

def reset(name):
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{name}")

def rows_for(df, key, value):
    return df.filter(col(key) == value).orderBy("start_date").collect()

# COMMAND ----------

test_results = []
def run_test(fn):
    try:
        fn()
        status, msg = "PASSED", ""
    except AssertionError as e:
        status, msg = "FAILED", str(e)
    except Exception as e:
        status, msg = "ERROR", f"{type(e).__name__}: {e}"
    test_results.append((fn.__name__, status, msg))
    print(f"{fn.__name__}: {status} {msg}")

# COMMAND ----------

# headers like the raw files ("Customer ID", "Sub-Category" ...) should be cleaned
def test_clean_column_names():
    src = spark.createDataFrame([("PW-19240", "Chairs", 81.88, "21/8/2016")],
                                ["Customer ID", " Sub-Category ", "Price per product", "Order-Date"])

    out = clean_column_names(src)

    assert out.columns == ["customer_id", "sub_category", "price_per_product", "order_date"], f"got {out.columns}"
    # only names should change, not the data
    assert out.collect()[0] == ("PW-19240", "Chairs", 81.88, "21/8/2016")

run_test(test_clean_column_names)

# COMMAND ----------

# spaces trimmed, empty/blank strings become NULL, non-string columns left alone
def test_trim_strings():
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("customer_name", StringType()),
        StructField("segment", StringType()),
    ])
    src = spark.createDataFrame([
        (1, "  Pierre Wener  ", "Consumer "),
        (2, "   ", ""),
        (3, None, "Corporate"),
    ], schema)

    out = {r["id"]: r for r in trim_strings(src).collect()}

    assert out[1]["customer_name"] == "Pierre Wener"
    assert out[1]["segment"] == "Consumer"
    assert out[2]["customer_name"] is None, "blank string should become NULL"
    assert out[2]["segment"] is None, "empty string should become NULL"
    assert out[3]["customer_name"] is None
    assert out[3]["segment"] == "Corporate"
    assert dict(trim_strings(src).dtypes)["id"] == "int"

run_test(test_trim_strings)

# COMMAND ----------

# customer name cleanup, cases taken from the comments in commons
def test_clean_column_values():
    cases = [
        ("Gary567 Hansen", "Gary Hansen"),       # digits at end
        ("Pete@#$ Takahito", "Pete Takahito"),   # symbols at end
        ("Joh12n Smith", "John Smith"),          # junk in the middle of a word
        ("O'Rourke", "O'Rourke"),                # apostrophe between letters is kept
        ("Corey-Lock", "Corey-Lock"),            # hyphen between letters is kept
        ("Bell-", "Bell"),                       # trailing hyphen removed
        ("Anna   Smith", "Anna Smith"),          # extra spaces collapsed
        ("1234", "Unknown"),                     # nothing left after cleaning
        (None, "Unknown"),
    ]
    src = spark.createDataFrame([(i, c[0]) for i, c in enumerate(cases)], "id int, customer_name string")

    out = {r["id"]: r["cleaned"] for r in
           src.withColumn("cleaned", clean_column_values(col("customer_name"))).collect()}

    wrong = [(raw, exp, out[i]) for i, (raw, exp) in enumerate(cases) if out[i] != exp]
    assert not wrong, f"input / expected / got: {wrong}"

run_test(test_clean_column_values)

# COMMAND ----------

# MAGIC %md
# MAGIC SCD Type 2 Merge test cases

# COMMAND ----------

# same data loaded twice, nothing should change
def test_rerun_same_data_no_duplicates():
    tbl = "test_scd2_rerun"
    reset(tbl)

    data = [("PW-19240", "Pierre Wener", "Consumer", "United States"),
            ("GH-14410", "Gary Hansen", "Home Office", "United States")]
    save_table_scd_2(spark.createDataFrame(data, customer_schema), tbl, ["customer_id"])
    out = save_table_scd_2(spark.createDataFrame(data, customer_schema), tbl, ["customer_id"])

    # display(out) # Print output
    
    assert out.count() == 2, f"duplicates created on rerun, got {out.count()} rows"

run_test(test_rerun_same_data_no_duplicates)

# COMMAND ----------

# change records should create new version with changed start time and end time of both records
def test_changed_record_creates_new_version():

    tbl = "test_scd2_change"
    reset(tbl)

    # First load
    save_table_scd_2(spark.createDataFrame([
        ("PW-19240", "Pierre Wener", "Consumer", "United States"),
        ("GH-14410", "Gary Hansen", "Home Office", "United States"),
    ], customer_schema), tbl, ["customer_id"])

    # Second load - Pierre's segment changes
    out = save_table_scd_2(spark.createDataFrame([
        ("PW-19240", "Pierre Wener", "Corporate", "United States"),
        ("GH-14410", "Gary Hansen", "Home Office", "United States"),
    ], customer_schema), tbl, ["customer_id"])

    # display(out) # Print output

    # Existing assertions
    pw = rows_for(out, "customer_id", "PW-19240")

    assert len(pw) == 2
    assert pw[0]["segment"] == "Consumer"
    assert pw[1]["segment"] == "Corporate"

run_test(test_changed_record_creates_new_version)

# COMMAND ----------

# new customer comes in, existing customer not in the batch
def test_new_key_inserted_and_missing_key_kept():
    tbl = "test_scd2_new_key"
    reset(tbl)

    save_table_scd_2(spark.createDataFrame([
        ("PW-19240", "Pierre Wener", "Consumer", "United States"),
        ("GH-14410", "Gary Hansen", "Home Office", "United States"),
    ], customer_schema), tbl, ["customer_id"])

    # GH-14410 not sent this time, KL-16555 is new
    out = save_table_scd_2(spark.createDataFrame([
        ("PW-19240", "Pierre Wener", "Consumer", "United States"),
        ("KL-16555", "Kelly Lampkin", "Corporate", "United States"),
    ], customer_schema), tbl, ["customer_id"])

    # display(out) # Print output

    assert out.count() == 3

    kl = rows_for(out, "customer_id", "KL-16555")
    assert len(kl) == 1
    assert kl[0]["start_date"] == today and kl[0]["end_date"] == end_date

run_test(test_new_key_inserted_and_missing_key_kept)

# COMMAND ----------

# NULLs in compare columns,it should not create another rows, as = doesn't work on NULL's
def test_null_safe_change_detection():
    tbl = "test_scd2_nulls"
    reset(tbl)

    save_table_scd_2(spark.createDataFrame([
        ("PW-19240", "Pierre Wener", None, "United States"), # segment -> null
        ("GH-14410", "Gary Hansen", None, "United States"), # segment -> null
    ], customer_schema), tbl, ["customer_id"])

    out = save_table_scd_2(spark.createDataFrame([
        ("PW-19240", "Pierre Wener", "Consumer", "United States"),  # segment: Consumer
        ("GH-14410", "Gary Hansen", None, "United States"),         # segment -> null
    ], customer_schema), tbl, ["customer_id"])

    # display(out) # Print output

    pw = rows_for(out, "customer_id", "PW-19240")
    assert len(pw) == 2, "NULL : value was not picked up as a change"
    assert pw[0]["segment"] is None and pw[0]["end_date"] == yesterday
    assert pw[1]["segment"] == "Consumer" and pw[1]["end_date"] == end_date

run_test(test_null_safe_change_detection)

# COMMAND ----------

total = len(test_results)
passed = sum(1 for r in test_results if r[1] == "PASSED")
failed = sum(1 for r in test_results if r[1] == "FAILED")
# fail the notebook if anything didn't pass, so a job run shows red
if passed != total:
    raise Exception(f"{total - passed} of {total} tests did not pass - check the report above")

dbutils.notebook.exit(json.dumps({"total": total, "passed": passed}))