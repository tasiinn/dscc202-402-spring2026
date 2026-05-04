# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: ML Inference for Sentiment Prediction
# MAGIC
# MAGIC ## Purpose
# MAGIC Apply pre-trained sentiment model to predict tweet sentiment.
# MAGIC Enrich data with ML predictions for comparison with ground truth labels.
# MAGIC
# MAGIC ## Requirements
# MAGIC - Load model from Unity Catalog: workspace.default.tweet_sentiment_model
# MAGIC - Create Spark UDF for distributed ML inference
# MAGIC - Map model labels (LABEL_0/1/2) to sentiment strings (negative/neutral/positive)
# MAGIC - Scale confidence scores to 0-100 range
# MAGIC - Create binary sentiment indicators for classification metrics
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta table: `tweets_gold`
# MAGIC - Row count matches silver
# MAGIC - predicted_score in range 0-100
# MAGIC - predicted_sentiment: negative/neutral/positive
# MAGIC - Binary IDs (0 or 1) for ground truth and predictions
# MAGIC
# MAGIC ## Model Information
# MAGIC - Model: twitter-roberta-base-sentiment
# MAGIC - Output: Struct with label (string) and score (double)
# MAGIC - Labels: LABEL_0=negative, LABEL_1=neutral, LABEL_2=positive
# MAGIC
# MAGIC ## Reference
# MAGIC See Lab 0.5 (MLops) for MLflow model loading and Spark UDF patterns

# COMMAND ----------

# TODO: Import necessary libraries
# You will need:
# - pyspark.pipelines (as dp)
# - pyspark.sql.types and pyspark.sql.functions
# - mlflow for model loading
import subprocess
subprocess.run(["pip", "install", "transformers==4.35.2", "torch", "--quiet"], check=True)
import pyspark.pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, when, lower
import mlflow
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")
 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create Gold Streaming Table
# MAGIC
# MAGIC TODO: Define streaming table "tweets_gold" with descriptive comment

# COMMAND ----------

# TODO: Create streaming table definition
dp.create_streaming_table(
    name="tweets_gold",
    comment="Tweet data enriched with ML sentiment predictions from Unity Catalog model"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Configure MLflow Registry
# MAGIC
# MAGIC TODO: Set MLflow registry to Unity Catalog
# MAGIC Use: mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# TODO: Configure MLflow registry

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Define Model Output Schema
# MAGIC
# MAGIC TODO: Define StructType for model output with fields:
# MAGIC - label (StringType): LABEL_0, LABEL_1, or LABEL_2
# MAGIC - score (DoubleType): Confidence score 0.0-1.0

# COMMAND ----------

# TODO: Define model output schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
 
model_output_schema = StructType([
    StructField("label", StringType(), True),
    StructField("score", DoubleType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Load Model and Create Spark UDF
# MAGIC
# MAGIC TODO: Load sentiment model from Unity Catalog and create Spark UDF
# MAGIC - Model URI: "models:/workspace.default.tweet_sentiment_model/1"
# MAGIC - Use: mlflow.pyfunc.spark_udf(spark, model_uri, result_type)
# MAGIC
# MAGIC This enables distributed ML inference across all Spark executors.

# COMMAND ----------

# TODO: Load model and create Spark UDF
MODEL_URI = "models:/workspace.default.small_sentiment_model/1"
 
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=MODEL_URI,
    result_type=model_output_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Define Gold Transformation Flow
# MAGIC
# MAGIC TODO: Create @dp.append_flow function that:
# MAGIC 1. Reads from tweets_silver streaming table
# MAGIC 2. Applies model UDF to cleaned_text column
# MAGIC 3. Extracts label from model output struct
# MAGIC 4. Extracts score and scales to 0-100 (multiply by 100)
# MAGIC 5. Maps labels to sentiment strings:
# MAGIC    - LABEL_0 → "negative"
# MAGIC    - LABEL_1 → "neutral"
# MAGIC    - LABEL_2 → "positive"
# MAGIC 6. Creates binary sentiment_id (0=negative, 1=positive/neutral)
# MAGIC 7. Creates binary predicted_sentiment_id (0=negative, 1=positive/neutral)
# MAGIC 8. Selects final columns (9 total)
# MAGIC
# MAGIC Reference: Lab 0.5 for model UDF application and struct parsing

# COMMAND ----------

# TODO: Define append_flow function for gold transformation
gold_schema = StructType([
    StructField("timestamp",              StringType(),  True),
    StructField("mention",                StringType(),  True),
    StructField("cleaned_text",           StringType(),  True),
    StructField("text",                   StringType(),  True),
    StructField("sentiment",              StringType(),  True),
    StructField("predicted_sentiment",    StringType(),  True),
    StructField("predicted_score",        DoubleType(),  True),
    StructField("sentiment_id",           IntegerType(), True),
    StructField("predicted_sentiment_id", IntegerType(), True),
])
 
@dp.append_flow(target="tweets_gold")
def transform_gold():
    return (
        spark.readStream
             # Process small batches to stay under 1GB Serverless memory limit
             .option("maxBytesPerTrigger", "5m")
             .table("tweets_silver")
             .withColumn("prediction",          predict_udf(col("cleaned_text")))
             .withColumn("predicted_sentiment", lower(col("prediction.label")))
             .withColumn("predicted_score",     (col("prediction.score") * 100).cast("double"))
             .withColumn("sentiment_id",
                         when(col("sentiment") == "0", 0)
                         .otherwise(1).cast("int"))
             .withColumn("predicted_sentiment_id",
                         when(col("predicted_sentiment") == "negative", 0)
                         .otherwise(1).cast("int"))
             .select(
                 "timestamp", "mention", "cleaned_text", "text", "sentiment",
                 "predicted_sentiment", "predicted_score",
                 "sentiment_id", "predicted_sentiment_id"
             )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After pipeline execution, verify:
# MAGIC - Row count matches silver
# MAGIC - predicted_score: 0-100 range
# MAGIC - predicted_sentiment: "negative", "neutral", or "positive"
# MAGIC - sentiment_id and predicted_sentiment_id: 0 or 1
# MAGIC - All rows have predictions (no nulls)
