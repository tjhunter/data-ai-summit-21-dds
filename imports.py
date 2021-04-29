# Databricks notebook source
import sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
import requests
import io
import json

def get_current_branch():
  # Introspecting a private API to get the current branch.
  # TODO: use an official API
  notebook_utils = dbutils.entry_point.getDbutils().notebook()
  ctx = notebook_utils.getContext()
  user = ctx.tags().get("user").get()
  # Hardcoding some branches for now for each user
  d = {"timothee.hunter@gmail.com": "main", 
       "brooke.wenig@databricks.com":"brooke_dev"}
  return d.get(user) or "main"
