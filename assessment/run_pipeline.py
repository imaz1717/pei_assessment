# Databricks notebook source
import json
import time

# paths are relative to this notebook - change if your folder names are different
test_notebbok = "../tests/test_case"
tranformation_notebook = "./transformation"
timeout = 3600

# COMMAND ----------

def run_notebook(path, step):
    print(f"[{step}] starting {path}")
    start = time.time()
    try:
        result = dbutils.notebook.run(path, timeout)
    except Exception as e:
        secs = round(time.time() - start, 1)
        print(f"[{step}] FAILED after {secs}s")
        raise Exception(f"{step} failed - pipeline stopped. Details: {e}")
    secs = round(time.time() - start, 1)
    print(f"[{step}] finished in {secs}s")
    return result

# COMMAND ----------

# DBTITLE 1,Unit tests
# test_case raises an exception at the end if any test did not pass the it will stops the pipeline
test_output = run_notebook(test_notebbok, "unit tests")

if test_output:
    summary = json.loads(test_output)
    print(f"tests passed: {summary['passed']} / {summary['total']}")

# COMMAND ----------

# DBTITLE 1,Transformation
transformation_output = run_notebook(tranformation_notebook, "transformation")

if transformation_output:
    for table, rows in json.loads(transformation_output).items():
        print(f"pei_assesment.{table}: {rows} rows")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")