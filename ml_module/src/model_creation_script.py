import pandas as pd
import pyspark
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.metrics import roc_curve
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum, max, date_sub, lit, current_date, datediff
import mlflow
import mlflow.spark

def create_spark_session():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("my_pipeline") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()
    return spark

def load_data(spark):
    df = spark.read.csv("../../ecommerce_data_with_trends.csv", header=True, inferSchema=True)
    return df

def prepare_data(df):
    df_grouped = df.groupBy("customer_id").agg(
        {"total_amount": "sum", "quantity": "avg"}
    ).withColumnRenamed("sum(total_amount)", "total_spent") \
     .withColumnRenamed("avg(quantity)", "avg_quantity")

    vector_assembler = VectorAssembler(inputCols=["total_spent", "avg_quantity"], outputCol="features")
    df_kmeans = vector_assembler.transform(df_grouped)
    return df_kmeans

def train_kmeans(df_kmeans):
    kmeans = KMeans(featuresCol="features", k=2, seed=1)
    model = kmeans.fit(df_kmeans)
    predictions = model.transform(df_kmeans)
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print(f"Silhouette Score: {silhouette}")
    return predictions

def visualize_clusters(predictions):
    pandas_df = predictions.select("customer_id", "total_spent", "avg_quantity", "prediction").toPandas()
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(8, 6))
    sns.scatterplot(
        data=pandas_df, 
        x="total_spent", 
        y="avg_quantity", 
        hue="prediction", 
        palette="viridis", 
        s=100, 
        alpha=0.6,
        edgecolor="k"
    )
    plt.title("Répartition des clients selon les clusters")
    plt.xlabel("Dépenses totales (total_spent)")
    plt.ylabel("Quantité moyenne (avg_quantity)")
    plt.legend(title="Cluster", loc="upper right", bbox_to_anchor=(1.15, 1))
    plt.savefig("../output/kmeans_clusters.png")
    # plt.show() enable this line to get a view of the plot

def feature_engineering(df):
    customer_features = df.groupBy("customer_id").agg(
        sum("total_amount").alias("total_spent"),
        count("transaction_id").alias("number_of_transactions"),
        avg("total_amount").alias("avg_transaction_value"),
        max("timestamp").alias("last_purchase_date")
    )
    final_df = customer_features.withColumn(
        "recency_days", 
        datediff(current_date(), col("last_purchase_date"))
    ).withColumn(
        "label", 
        when(col("recency_days") > 90, 1.0).otherwise(0.0)
    )
    return final_df

def prepare_model_data(final_df):
    feature_columns = ["total_spent", "number_of_transactions", "avg_transaction_value", "recency_days"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    model_data = assembler.transform(final_df)
    train_data, test_data = model_data.randomSplit([0.7, 0.3], seed=42)
    return train_data, test_data

def train_logistic_regression(train_data, test_data):
    with mlflow.start_run():
        mlflow.log_param("maxIter", 10)
        feature_columns = ["total_spent", "number_of_transactions", "avg_transaction_value", "recency_days"]
        mlflow.log_param("features", feature_columns)

        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=10
        )
        model = lr.fit(train_data)
        mlflow.spark.log_model(model, "logistic_regression_model")

        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        print(f"Model Accuracy: {accuracy}")
        mlflow.log_metric("accuracy", accuracy)

        predictions.select("customer_id", "label", "prediction").show(5)
        return predictions

def visualize_model_results(predictions):
    plt.figure(figsize=(15, 10))
    plt.subplot(2, 2, 1)
    class_counts = predictions.groupBy("label").count().toPandas()
    sns.barplot(x="label", y="count", data=class_counts)
    plt.title("Distribution des classes")
    plt.xlabel("Classe")
    plt.ylabel("Nombre de clients")

    plt.subplot(2, 2, 2)
    conf_matrix = predictions.groupBy("label", "prediction").count().toPandas()
    conf_matrix_pivot = conf_matrix.pivot(index="label", columns="prediction", values="count").fillna(0)
    sns.heatmap(conf_matrix_pivot, annot=True, fmt="g", cmap="Blues")
    plt.title("Matrice de confusion")
    plt.xlabel("Prédiction")
    plt.ylabel("Valeur réelle")

    plt.tight_layout()
    plt.savefig("../output/model_analysis.png")
    # plt.show() enable this line to get a view of the plot

    plt.figure(figsize=(10, 6))
    predictions_pd = predictions.select("recency_days", "prediction", "total_spent").toPandas()
    plt.scatter(predictions_pd["recency_days"], predictions_pd["total_spent"], 
               c=predictions_pd["prediction"], cmap="coolwarm", alpha=0.6)
    plt.colorbar(label="Prédiction")
    plt.xlabel("Récence (jours)")
    plt.ylabel("Dépenses totales")
    plt.title("Relation entre récence, dépenses et prédiction de churn")
    plt.savefig("../output/recency_spending_analysis.png")
    # plt.show() enable this line to get a view of the plot

def main():
    spark = create_spark_session()
    df = load_data(spark)
    df_kmeans = prepare_data(df)
    predictions_kmeans = train_kmeans(df_kmeans)
    visualize_clusters(predictions_kmeans)

    final_df = feature_engineering(df)
    train_data, test_data = prepare_model_data(final_df)
    predictions_lr = train_logistic_regression(train_data, test_data)
    visualize_model_results(predictions_lr)

if __name__ == "__main__":
    main()