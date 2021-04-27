# Databricks notebook source
# MAGIC %md 
# MAGIC # Machine Learning Pipeline with DDS

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %md ## Set Store

# COMMAND ----------

import dds

branch = get_current_branch()
dds.set_store("dbfs",
              data_dir=f"/data_managed/{branch}",
              internal_dir="/data_cache",
              commit_type="link_only")

# COMMAND ----------

# MAGIC %md ## Get Data

# COMMAND ----------

path_raw = "/wine-quality/raw"
data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"

@dds.data_function(path_raw)
def load_data():
    print("*** in load_data ***")
    x = requests.get(url=data_url, verify=False).content 
    return pd.read_csv(io.StringIO(x.decode("utf8")), sep=";")

# This will cause the data to load
load_data().head(3)

# COMMAND ----------

# MAGIC %md ## Build Model

# COMMAND ----------

def build_model(X_train, y_train):
    print("*** in build_model ***")
    rf = RandomForestRegressor(n_estimators=50, random_state=42)
    hyperparameters = {"max_features": ["auto", "sqrt"],
                       "max_depth": [None, 3, 5]}
    clf = GridSearchCV(rf, hyperparameters, cv=10)
    clf.fit(X_train, y_train)
    return clf
    
def model_stats(clf, X_test, y_test):
    print("*** in model_stats ***")
    pred = clf.predict(X_test)
    return json.dumps({
        "r2_score": r2_score(y_test, pred),
        "mse": mean_squared_error(y_test, pred)
    })

def pipeline():
    wine_data = load_data()
    y = wine_data.quality
    X = wine_data.drop("quality", axis=1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    clf = dds.keep("/wine-quality/my_model", build_model, X_train, y_train)
    
    stats = dds.keep("/wine-quality/my_model_stats.json", model_stats, clf, X_test, y_test)
    
    print("*** done ***")
    return stats

dds.codecs.databricks.displayGraph(pipeline)

# COMMAND ----------

dds.eval(pipeline)

# COMMAND ----------

# MAGIC %md ## Load Artifacts from DDS

# COMMAND ----------

dds.load("/wine-quality/my_model")

# COMMAND ----------

dds.load("/wine-quality/my_model_stats.json")
