# Databricks notebook source
from machine_learning_dir import ml_class as ML

airbnb_df =  spark.table("default.gold_tbl")
airbnb_df = airbnb_df.sample(fraction=0.1, seed=3)

train_df, val_df, test_df = airbnb_df.randomSplit([.6, .2, .2], seed=42)

data = {"train_df": train_df, "val_df": val_df}
run_name = "Hyperopt - Random Forest Regressor @ AirBnB dataset"
max_evals = 1

search_space_dct = {}
search_space_dct["numTrees"] = {"start": 2, "end": 10, "q": 1}
search_space_dct["maxDepth"] = {"start": 10, "end": 30, "q": 1}

ML.train_model(data=data,run_name=run_name,num_evals=max_evals,search_space_dct=search_space_dct)

# COMMAND ----------


