# Databricks notebook source
def get_current_branch():
  # Introspecting a private API to get the current branch.
  # TODO: use an official API
  notebook_utils = dbutils.entry_point.getDbutils().notebook()
  ctx = notebook_utils.getContext()
  user = ctx.tags().get("user").get()
  # Hardcoding some branches for now for each user
  d = {"timothee.hunter@gmail.com": "main", 
       "brooke.wenig@databricks.com":"brooke"}
  return d.get(user) or "not_found"

# COMMAND ----------

branch = get_current_branch()
branch

# COMMAND ----------

import dds
dds.set_store("dbfs",
              data_dir=f"/data_managed/{branch}",
              internal_dir="/data_cache",
              commit_type="link_only")

# COMMAND ----------

import sklearn
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
import requests
import io
import json

# COMMAND ----------

# The data paths in the store
path_raw = "/wine-quality/raw"
data_url = "https://raw.githubusercontent.com/zygmuntz/wine-quality/master/winequality/winequality-red.csv"

@dds.data_function(path_raw)
def data():
    print("*** in _load_data ***")
    x = requests.get(url=data_url, verify=False).content 
    # hi everyone
    return pd.read_csv(io.StringIO(x.decode('utf8')), sep=";")

# This will cause the data to load
data().head(3)

# COMMAND ----------

def build_model(X_train, y_train):
    print("*** in build_model ***")
    pipeline = make_pipeline(preprocessing.StandardScaler(), 
                             RandomForestRegressor(n_estimators=50))
    hyperparameters = { 'randomforestregressor__max_features' : ['auto', 'sqrt'],
                      'randomforestregressor__max_depth': [None, 5, 3]}
    clf = GridSearchCV(pipeline, hyperparameters, cv=10)
    clf.fit(X_train, y_train)
    return clf
    
def model_stats(clf, X_test, y_test) -> str:
    print("*** in model_stats ***")
    pred = clf.predict(X_test)
    return json.dumps({
        "r2_score": r2_score(y_test, pred), # uncomment me
        "rmse": mean_squared_error(y_test, pred, squared=False)
    })

# Comment
def pipeline():
    # Comment 2
    wine_data = data()
    y = wine_data.quality
    X = wine_data.drop('quality', axis=1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, 
                                                        test_size=0.4, 
                                                        random_state=None, 
                                                        stratify=y)
    clf = dds.keep("/wine-quality/my_model", build_model, X_train, y_train)
    
    dds.keep("/wine-quality/my_model_stats.json", model_stats, clf, X_test, y_test)
    
    print("*** done3ee3 ***")

# Evaluate the full pipeline
# Note that it does not recompute the data, the data is already in cache.
dds.codecs.databricks.displayGraph(pipeline)

# COMMAND ----------

dds.eval(pipeline)

# COMMAND ----------

dds.load("/wine-quality/my_model")

# COMMAND ----------


