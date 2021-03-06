# Databricks notebook source
# MAGIC %md # Using the DDS package on Databricks
# MAGIC 
# MAGIC This notebook shows you a few features of the [dds package](https://github.com/tjhunter/dds_py). Install `dds-py` to your cluster via PyPI to run this notebook.

# COMMAND ----------

dbutils.fs.rm("/data_managed", recurse=True)
dbutils.fs.rm("/data_cache", recurse=True)

# COMMAND ----------

# MAGIC %md ## Set up DDS for Databricks

# COMMAND ----------

import dds
dds.set_store("dbfs", data_dir="/data_managed", internal_dir="/data_cache")

# COMMAND ----------

# MAGIC %md ## Hello world example

# COMMAND ----------

@dds.data_function("/hello_data")
def data():
  print("Executing data()")
  return "Hello, world"

data()

# COMMAND ----------

# MAGIC %md The data is now on DBFS

# COMMAND ----------

# MAGIC %fs ls /data_managed/

# COMMAND ----------

# MAGIC %fs head /data_managed/hello_data

# COMMAND ----------

# MAGIC %md The data is also cached in the `/data_cache` directory:

# COMMAND ----------

# MAGIC %fs ls /data_cache/blobs

# COMMAND ----------

# Calling this function again does not trigger calculations, the data is stored on DBFS
data()

# COMMAND ----------

# MAGIC %md ## Plotting dependencies
# MAGIC 
# MAGIC The following code introduces dependencies between multiple data functions. We can plot the dependencies with the `dds.eval` function:

# COMMAND ----------

# Try modifying this variable to see what happens
outside_var = 3

@dds.data_function("/f1")
def f1():
  print("eval f1")
  return 1

@dds.data_function("/f2")
def f2():
  print("eval f2")
  return outside_var + f1()

@dds.data_function("/f3")
def f3(): 
  print("eval f3")
  return f1() + f2()

# This is the first time we evaluate it, so everything will be evaluated.
dds.codecs.databricks.displayGraph(f3)

# COMMAND ----------

f3()

# COMMAND ----------

# MAGIC %md Now see what happens when we modify the `outside_var` variable. DDS will detect that the `f2` function (and hence the `f3` function) will be modified and need to be rerun.

# COMMAND ----------

outside_var = 5
dds.codecs.databricks.displayGraph(f3)

# COMMAND ----------

f3()
