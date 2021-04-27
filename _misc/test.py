# Databricks notebook source
from IPython import get_ipython  # type: ignore

ipython = get_ipython()
dict(ipython.user_ns)

# COMMAND ----------

dir(dbutils.entry_point)

# COMMAND ----------

dir(dbutils.entry_point.getSQLContext().conf())

# COMMAND ----------

[z for z in (spark.conf._jconf.getAll().toString()).split(",") if "extraJavaOptions" not in z and "extraClassPath" not in z]

# COMMAND ----------

dir(dbutils.entry_point)

# COMMAND ----------

dbutils.entry_point.getCurrentBindings().values()

# COMMAND ----------

dir(dbutils.entry_point.getDriverConf())

# COMMAND ----------

(dbutils.entry_point.getDriverConf().currentTenant().toString())

# COMMAND ----------

dir(dbutils.entry_point.getNotebookArguments())

# COMMAND ----------

notebook_utils = dbutils.entry_point.getDbutils().notebook()
ctx = notebook_utils.getContext()
(ctx.toJson())

# COMMAND ----------

(notebook_utils.getContext().tags().get("user").get())

# COMMAND ----------


