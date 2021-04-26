# Databricks notebook source
# MAGIC %sh
# MAGIC pip install dds-py

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -kL https://github.com/restruct/dot-static/blob/master/x64/dot_static?raw=true > /usr/bin/dot
# MAGIC chmod +x /usr/bin/dot
# MAGIC pip install pydotplus

# COMMAND ----------

import dds

# COMMAND ----------

import sklearn
import pandas as pd

import numpy as np
import pandas as pd
 
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
        "mse": mean_squared_error(y_test, pred)
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
# dds.eval(pipeline)
dds.codecs.databricks.displayGraph(pipeline)

# COMMAND ----------

dds.load("/wine-quality/my_model")
