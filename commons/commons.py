# Databricks notebook source
base_path = "/Workspace/Users/imazfaiyazi1717@gmail.com/PIE_Group_Assessment/data"

# COMMAND ----------

# MAGIC %md
# MAGIC Common Function

# COMMAND ----------

#consitent column name across all tables
def clean_column_names(df):
    new_cols = []
    for c in df.columns:
        new_c = c.strip().lower().replace(" ", "_").replace("-", "_")
        new_cols.append(new_c)
    return df.toDF(*new_cols)

# COMMAND ----------

#white space trimming
def trim_strings(df):
    for c, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(c, when(trim(col(c)) == "", None).otherwise(trim(col(c))))
    return df

# COMMAND ----------

def clean_column_values(col):
    #removing random junks from customer_name "Gary567 Hansen","Pete@#$ Takahito" etc....
    name = regexp_replace(col, r"(?<=\p{L})[^\p{L}]*[^\p{L}\s][^\p{L}]*[^\p{L}\s][^\p{L}]*(?=\p{Ll})", "") #removing junk in the middle of a word (next letter is lowercase), remove it along with the spaces
    name = regexp_replace(name, r"[^\p{L} '\-]", "") #remove anything that's not a letter/space/ ' /-
    name = regexp_replace(name, r"(?<!\p{L})['\-]|['\-](?!\p{L})", "") #only keeping ' and - if they're between two letters (O'Rourke, Corey-Lock but not "Bell-")
    name = trim(regexp_replace(name, r"\s+", " ")) #fix double spaces
    return when((name.isNull()) | (name == ""), "Unknown").otherwise(name)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS pei_assesment")

def save_table(df, name, partition_by=None):
    writer = df.write.mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(f"pei_assesment.{name}")
    return spark.table(f"pei_assesment.{name}")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, expr

OPEN_END = "DATE '9900-01-01'"

def save_table_scd_2(df, name, merge_keys, partition_by=None):
    table_name = f"pei_assesment.{name}"

    # First load, every row is active, valid from 1900-01-01
    if not spark.catalog.tableExists(table_name):
        writer = (df.withColumn("start_date", lit("1900-01-01").cast("date"))
           .withColumn("end_date", expr(OPEN_END))
           .write.mode("overwrite"))
        if partition_by:
            writer.partitionBy(*partition_by)
        writer.saveAsTable(table_name)
        return spark.table(table_name)

    compare_cols = [c for c in df.columns if c not in merge_keys]
    is_changed = " OR ".join(f"NOT (old.`{c}` <=> new.`{c}`)" for c in compare_cols) or "false"
    keys_match = " AND ".join(f"old.`{k}` = new.`{k}`" for k in merge_keys)

    # Incoming rows whose values differ from the active version
    active = spark.table(table_name).filter(f"end_date = {OPEN_END}")
    changed = (df.alias("new").join(active.alias("old"), expr(keys_match))
                 .filter(is_changed).select("new.*"))

    # Staging, all rows keyed normally (matched : expire), plus changed rows
    # with a NULL key (did not matched : inserted as the new version)
    mk = lambda k: f"_mk_{k}"
    staged = (df.select("*", *[col(k).alias(mk(k)) for k in merge_keys])
                .unionByName(changed.select("*", *[lit(None).cast(df.schema[k].dataType).alias(mk(k))
                                                   for k in merge_keys])))

    merge_cond = " AND ".join(f"old.`{k}` = new.`{mk(k)}`" for k in merge_keys) \
                 + f" AND old.end_date = {OPEN_END}"

    (DeltaTable.forName(spark, table_name).alias("old")
        .merge(staged.alias("new"), merge_cond)
        .whenMatchedUpdate(condition=is_changed, set={"end_date": "date_sub(current_date(), 1)"})
        .whenNotMatchedInsert(values={**{c: f"new.`{c}`" for c in df.columns},
                                      "start_date": "current_date()",
                                      "end_date": OPEN_END})
        .execute())

    return spark.table(table_name)