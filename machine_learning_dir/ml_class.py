import pandas as pd
import matplotlib.pyplot as plt
from functools import partial

from hyperopt import fmin, tpe, Trials, hp
import numpy as np
import mlflow
import mlflow.spark
import hyperopt as MH

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import pyspark.sql.types as T


mlflow.autolog(silent=False, log_input_examples=True)

def _objective_function(params, pipeline, regression_evaluator, string_indexer, rf, train_df, val_df):    
    # set the hyperparameters that we want to tune
    max_depth = params["maxDepth"]
    num_trees = params["numTrees"]

    with mlflow.start_run(nested=True):

        rmse, trained_pipeline, pred_df = _objective_train_and_compute_validation_metrics_on_testing_data_fn(pipeline, rf, regression_evaluator, train_df, val_df, max_depth, num_trees)
    
        # Used for checking for overfitting
        _objective_compute_validation_metrics_on_training_data_fn(trained_pipeline, regression_evaluator, train_df)
        
        # Extract feature importance 
        rf_pipeline = trained_pipeline.stages[2]
        feature_val_pdf = _extract_feature_importance_fn(rf_pipeline.featureImportances, pred_df, "features")

        # # Generate a plot for feature importance and save it 
        _plot_feature_importance_fn(feature_val_pdf)

        # Generate a plot over prices and bedrooms and save it
        _plot_bedrooms_vs_price_fn(pred_df)
        
        # Log pipeline for each run
        mlflow.spark.log_model(trained_pipeline, "trained_pipeline")
        mlflow.log_param("maxDepth", max_depth)
        mlflow.log_param("numTrees", num_trees)

        # Hyperopt minimizes score, here we minimize rmse
        return rmse
    
        
def _objective_train_and_compute_validation_metrics_on_testing_data_fn(pipeline, rf, regression_evaluator, train_df, val_df, max_depth, num_trees):
        
    pipeline = pipeline.copy({rf.maxDepth: max_depth, rf.numTrees: num_trees})

    pipeline_model = pipeline.fit(train_df)
    pred_df = pipeline_model.transform(val_df)
    rmse = regression_evaluator.evaluate(pred_df)

    mlflow.log_metric("RMSE", rmse)

    return rmse, pipeline_model, pred_df


def _objective_compute_validation_metrics_on_training_data_fn(trained_pipeline, regression_evaluator, train_df):

    train_pred_df = trained_pipeline.transform(train_df)
    rmse_train = regression_evaluator.evaluate(train_pred_df)

    mlflow.log_metric("RMSE_train", rmse_train)


def _plot_bedrooms_vs_price_fn(pred_df):

    pred_pdf = pred_df.toPandas()

    fig1, ax = plt.subplots(figsize=(11,9))
    plt.scatter(pred_pdf["bedrooms"], pred_pdf["price"])
    ax.set_title("Scatter Plot of Bedrooms vs Price", size=17)
    ax.set_xlabel("Bedrooms", size=15)
    ax.set_ylabel("Price", size=15)

    # Log figure under the run's root artifact directory
    mlflow.log_figure(fig1, "scatterplot_bedrooms_vs_price.png")

def _extract_feature_importance_fn(featureImp, dataset, featuresCol):
    # Borrowed from here https://www.timlrx.com/blog/feature-selection-using-feature-importance-score-creating-a-pyspark-estimator
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])

    feature_val_pdf = varlist.sort_values('score', ascending = False)

    feature_val_pdf.drop("vals", axis=1, inplace=True)

    return feature_val_pdf

def _plot_feature_importance_fn(feature_val_pdf):

    fig2, ax = plt.subplots(figsize=(9,9))
    plt.barh(feature_val_pdf["name"], feature_val_pdf["score"])
    ax.set_title("Feature Importance Plot", size=17)
    ax.set_ylabel("Feature", size=15)
    ax.set_xlabel("Importance", size=15)

    # Log figure under the run's root artifact directory
    mlflow.log_figure(fig2, "feature_importance.png")

def _build_pipeline_fn(train_df, val_df):
    categorical_cols = [field for (field, dataType) in train_df.dtypes if dataType == "string"]
    index_output_cols = [x + "_Index" for x in categorical_cols]

    string_indexer = StringIndexer(inputCols=categorical_cols, outputCols=index_output_cols, handleInvalid="skip")

    numeric_cols = [field for (field, dataType) in train_df.dtypes if ((dataType == "double") & (field != "price"))]
    assembler_inputs = index_output_cols + numeric_cols
    vec_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    rf = RandomForestRegressor(labelCol="price", maxBins=40, seed=42)
    pipeline = Pipeline(stages=[string_indexer, vec_assembler, rf])
    regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price")

    return pipeline, rf, regression_evaluator, string_indexer


def train_model(data, run_name, num_evals, search_space_dct):

    train_df = data["train_df"]
    val_df = data["val_df"]

    search_space = {
        "numTrees": MH.hp.quniform("numTrees", search_space_dct["numTrees"]["start"], search_space_dct["numTrees"]["end"], search_space_dct["numTrees"]["q"]),
        "maxDepth": MH.hp.quniform("maxDepth", search_space_dct["maxDepth"]["start"], search_space_dct["maxDepth"]["end"], search_space_dct["maxDepth"]["q"])
            }
    
    pipeline, rf, regression_evaluator, string_indexer = _build_pipeline_fn(train_df, val_df)

    # Partial facilitiates that the objective function can be called outside the class 
    fmin_objective = partial(_objective_function, pipeline=pipeline, regression_evaluator=regression_evaluator, string_indexer=string_indexer, rf=rf, train_df=train_df, val_df=val_df)

    mlflow.pyspark.ml.autolog(log_models=False)
    with mlflow.start_run(run_name=run_name):
        best_hyperparam = fmin(fn=fmin_objective, 
                            space=search_space,
                            algo=tpe.suggest, 
                            max_evals=num_evals,
                            trials=Trials(),
                            rstate=np.random.default_rng(42))
    
        best_max_depth = best_hyperparam["maxDepth"]
        best_num_trees = best_hyperparam["numTrees"]

        # Log param and parameters for the final model
        mlflow.log_param("maxDepth", best_max_depth)
        mlflow.log_param("numTrees", best_num_trees)