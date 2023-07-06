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


def _objective_function(params, pipeline, regression_evaluator, rf, train_df, val_df) -> float:
    """
    The objective function is what the model tries to minimize when using hyperopt. 

    :param params: A set of hyperparameters for the model
    :type params: dict

    :param pipeline: The model pipeline including the string indexer, vector assembler and the ml model 
    :type pipeline: pyspark.ml.pipeline.Pipeline

    :param regression_evaluator: The ml model evaluator
    :type regression_evaluator: pyspark.ml.evaluation.RegressionEvaluator

    :param rf: The ml model
    :type rf: pyspark.ml.regression.RandomForestRegressor

    :param train_df: The training data
    :type train_df: pyspark.sql.dataframe.DataFrame

    :param val_df: The validation data
    :type val_df: pyspark.sql.dataframe.DataFrame

    :returns: The rmse of the trained model
    :rtype: float
    """
    # set the hyperparameters that we want to tune
    max_depth = params["maxDepth"]
    num_trees = params["numTrees"]

    with mlflow.start_run(nested=True):

        rmse, trained_pipeline, pred_df = _objective_train_and_compute_validation_metrics_on_testing_data_fn(
            pipeline, rf, regression_evaluator, train_df, val_df, max_depth, num_trees)

        # Used for checking for overfitting
        _objective_compute_validation_metrics_on_training_data_fn(
            trained_pipeline, regression_evaluator, train_df)

        # Extract feature importance
        rf_pipeline = trained_pipeline.stages[2]
        feature_val_pdf = _extract_feature_importance_fn(
            rf_pipeline.featureImportances, pred_df, "features")

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


def _objective_train_and_compute_validation_metrics_on_testing_data_fn(pipeline, rf, regression_evaluator, train_df, val_df, max_depth, num_trees) -> tuple:
    """
    This function train the model and logs the rmse of the validation data.

    :param pipeline: The model pipeline including the string indexer, vector assembler and the ml model 
    :type pipeline: pyspark.ml.pipeline.Pipeline

    :param rf: The ml model
    :type rf: pyspark.ml.regression.RandomForestRegressor

    :param regression_evaluator: The ml model evaluator
    :type regression_evaluator: pyspark.ml.evaluation.RegressionEvaluator

    :param train_df: The training data
    :type train_df: pyspark.sql.dataframe.DataFrame

    :param val_df: The validation data
    :type val_df: pyspark.sql.dataframe.DataFrame

    :param max_depth: The max depth hyperparameter for the RandomForestRegression model. The type is a hyperopt.pyll.base.Apply but each iteration converts it to an int
    :type max_depth: int

    :param num_trees: The number of trees hyperparameter for the RandomForestRegression model. The type is a hyperopt.pyll.base.Apply but each iteration converts it to an int
    :type num_trees: int

    :return: a tuple of rmse, pipeline_model and pred_df
    :rtype: tuple
    """

    # This row allows the model to have different paramters each hyperopt iteration
    pipeline = pipeline.copy({rf.maxDepth: max_depth, rf.numTrees: num_trees})

    pipeline_model = pipeline.fit(train_df)
    pred_df = pipeline_model.transform(val_df)
    rmse = regression_evaluator.evaluate(pred_df)

    mlflow.log_metric("RMSE", rmse)

    return rmse, pipeline_model, pred_df


def _objective_compute_validation_metrics_on_training_data_fn(trained_pipeline, regression_evaluator, train_df) -> None:
    """
    This function validates if the trained model is overfitting. If the rmse_train is a lot lower than the rmse based on validation data. The model might overfit to the training data.
    The function is logging the rmse of the training data.

    :param trained_pipeline: The trained ml pipeline
    :type trained_pipeline: pyspark.ml.pipeline.Pipeline

    :param regression_evaluator: The ml model evaluator
    :type regression_evaluator: pyspark.ml.evaluation.RegressionEvaluator 

    :param train_df: The training data
    :type train_df: pyspark.sql.dataframe.DataFrame 
    """

    train_pred_df = trained_pipeline.transform(train_df)
    rmse_train = regression_evaluator.evaluate(train_pred_df)

    mlflow.log_metric("RMSE_train", rmse_train)


def _plot_bedrooms_vs_price_fn(pred_df) -> None:
    """
    This function produces and saves a scatter plot with bedrooms on the x-axis and price on the y-axis. 

    :param pred_df: A dataframe that contains the extra column with target predicitons. 
    :type pred_df: pyspark.sql.dataframe.DataFrame 
    """
    pred_pdf = pred_df.toPandas()

    fig1, ax = plt.subplots(figsize=(11, 9))
    plt.scatter(pred_pdf["bedrooms"], pred_pdf["price"])
    ax.set_title("Scatter Plot of Bedrooms vs Price", size=17)
    ax.set_xlabel("Bedrooms", size=15)
    ax.set_ylabel("Price", size=15)

    # Log figure under the run's root artifact directory
    mlflow.log_figure(fig1, "scatterplot_bedrooms_vs_price.png")


def _extract_feature_importance_fn(featureImp, dataset, featuresCol) -> pd.core.frame.DataFrame:
    """
    This function will extract and sort the feature importance of the input data. 

    :param featureImp: The feature importance extraced from rf_pipeline.featureImportances   
    :type featureImp: ?

    :param dataset: The dataset containing the predicitons 
    :type dataset: pyspark.sql.dataframe.DataFrame 

    :param featuresCol: What column to investigate 
    :type featuresCol: str

    :return: A pandas dataframe that contains the most important input features in order to predict target feature. 
    :rtype: pd.core.frame.DataFrame
    """
    # Borrowed from here https://www.timlrx.com/blog/feature-selection-using-feature-importance-score-creating-a-pyspark-estimator
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + \
            dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])

    feature_val_pdf = varlist.sort_values('score', ascending=False)

    feature_val_pdf.drop("vals", axis=1, inplace=True)

    return feature_val_pdf


def _plot_feature_importance_fn(feature_val_pdf) -> None:
    """
    This function logs the the feature importance plot.

    :param feature_val_pdf: The feature importance in sorted order
    :type feature_val_pdf: ppd.core.frame.DataFrame
    """

    fig2, ax = plt.subplots(figsize=(9, 9))
    plt.barh(feature_val_pdf["name"], feature_val_pdf["score"])
    ax.set_title("Feature Importance Plot", size=17)
    ax.set_ylabel("Feature", size=15)
    ax.set_xlabel("Importance", size=15)

    # Log figure under the run's root artifact directory
    mlflow.log_figure(fig2, "feature_importance.png")


def _build_pipeline_fn(train_df) -> tuple:
    """
    This function builds the model pipeline which consits of string indexer, vector assembler and the Random Forest Regression model. 
    It also instanciate the regression evaluator which is used to evaluate the model.

    :param train_df: The training data
    :type train_df: pyspark.sql.dataframe.DataFrame

    :return: A tuple of pipeline, rf and regression_evaluator
    :rtype: tuple
    """

    categorical_cols = [field for (
        field, dataType) in train_df.dtypes if dataType == "string"]
    index_output_cols = [x + "_Index" for x in categorical_cols]

    string_indexer = StringIndexer(
        inputCols=categorical_cols, outputCols=index_output_cols, handleInvalid="skip")

    numeric_cols = [field for (field, dataType) in train_df.dtypes if (
        (dataType == "double") & (field != "price"))]
    assembler_inputs = index_output_cols + numeric_cols
    vec_assembler = VectorAssembler(
        inputCols=assembler_inputs, outputCol="features")

    rf = RandomForestRegressor(labelCol="price", maxBins=40, seed=42)
    pipeline = Pipeline(stages=[string_indexer, vec_assembler, rf])
    regression_evaluator = RegressionEvaluator(
        predictionCol="prediction", labelCol="price")

    return pipeline, rf, regression_evaluator


def train_model(data_dct, run_name, num_evals, search_space_dct) -> None:
    """
    This function is the puplic function used by the end user. The purpose is to train N models. The models, their parameters, and scores are saved with MLflow. 

    :param data_dct: Contains both the training data and the validation data
    :type data_dct: dict

    :param run_name: The name of the experiment
    :type run_name: str

    :param num_evals: The number of models that will be trained
    :type num_evals: int

    :param search_space_dct: A dictionary containing the search space used to control the hyperopt parameters in order to build N models
    :type search_space_dct: dict
    """

    train_df = data_dct["train_df"]
    val_df = data_dct["val_df"]

    search_space = {
        "numTrees": MH.hp.quniform("numTrees", search_space_dct["numTrees"]["start"], search_space_dct["numTrees"]["end"], search_space_dct["numTrees"]["q"]),
        "maxDepth": MH.hp.quniform("maxDepth", search_space_dct["maxDepth"]["start"], search_space_dct["maxDepth"]["end"], search_space_dct["maxDepth"]["q"])
    }

    pipeline, rf, regression_evaluator = _build_pipeline_fn(train_df)

    # Partial facilitiates that the objective function can be called in a more flexible way
    fmin_objective = partial(_objective_function, pipeline=pipeline,
                             regression_evaluator=regression_evaluator, rf=rf, train_df=train_df, val_df=val_df)

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
