# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Basic Streaming Operations
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Welcome to the Bakehouse franchise analytics team! As transactions flow through the system, we need to monitor email marketing campaign effectiveness in real-time. This notebook demonstrates the fundamentals of Apache Spark Structured Streaming using real bakehouse sales data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Build streaming DataFrames with `readStream`
# MAGIC 2. Apply transformations to streaming data
# MAGIC 3. Write streaming query results with `writeStream`
# MAGIC 4. Monitor streaming queries
# MAGIC 5. Understand Databricks Free Edition streaming patterns
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC We'll use `samples.bakehouse.sales_transactions` (3,333 transactions) enhanced with synthetic `traffic_source` and `device` columns to simulate real marketing data.
# MAGIC
# MAGIC **Note**: This lab uses `trigger(availableNow=True)` for compatibility with Databricks Free Edition serverless compute. The Structured Streaming APIs are identical to production environments - only the trigger mode differs!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Unity Catalog resources for this notebook
# MAGIC CREATE CATALOG IF NOT EXISTS streaming_examples_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_examples_catalog.basic_streaming;
# MAGIC CREATE VOLUME IF NOT EXISTS streaming_examples_catalog.basic_streaming.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
working_dir = "/Volumes/streaming_examples_catalog/basic_streaming/workspace"
checkpoint_dir = f"{working_dir}/checkpoints"

print(f"Working directory: {working_dir}")
print(f"Checkpoint directory: {checkpoint_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup Previous Runs
# MAGIC
# MAGIC Before starting, we need to stop any active streaming queries and clean up previous output directories. This ensures a clean slate for the lab.

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

# Clean up directories from previous runs
try:
    dbutils.fs.rm(working_dir, recurse=True)
    print(f"✅ Cleaned up working directory: {working_dir}")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")

print("Ready to start!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Important: Databricks Free Edition Compatibility
# MAGIC
# MAGIC This lab is designed for **Databricks Free Edition** with serverless compute, which has specific streaming limitations:
# MAGIC
# MAGIC ### What Doesn't Work in Free Edition:
# MAGIC - ❌ **Cannot use `display()` on streaming DataFrames** - Interactive streaming display not supported
# MAGIC - ❌ **No continuous streaming** - Cannot use `trigger(processingTime=...)` or default continuous triggers
# MAGIC - ❌ **No memory sinks** - Cannot write to `.format("memory")`
# MAGIC
# MAGIC ### What Works in Free Edition:
# MAGIC - ✅ **Use `trigger(availableNow=True)`** - Processes all available data in batch mode
# MAGIC - ✅ **Write to Delta Lake** - Use `.format("delta")` for all outputs
# MAGIC - ✅ **Verify with batch reads** - Read streaming outputs as batch DataFrames and use `display()`
# MAGIC
# MAGIC ## Pattern Used Throughout This Lab:
# MAGIC
# MAGIC ```python
# MAGIC # 1. Create streaming DataFrame
# MAGIC streaming_df = spark.readStream.format("delta").load(source_path)
# MAGIC
# MAGIC # 2. Apply transformations
# MAGIC result_df = streaming_df.filter(...).select(...)
# MAGIC
# MAGIC # 3. Write with writeStream
# MAGIC query = (result_df.writeStream
# MAGIC     .format("delta")
# MAGIC     .outputMode("append")
# MAGIC     .option("checkpointLocation", checkpoint_path)
# MAGIC     .trigger(availableNow=True)
# MAGIC     .start(output_path))
# MAGIC
# MAGIC # 4. Wait and stop
# MAGIC query.awaitTermination()
# MAGIC query.stop()
# MAGIC
# MAGIC # 5. Verify by reading as batch
# MAGIC result = spark.read.format("delta").load(output_path)
# MAGIC display(result)
# MAGIC ```
# MAGIC
# MAGIC This approach works within Free Edition constraints while teaching the exact same Structured Streaming APIs used in production!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Prepare Streaming Source Data
# MAGIC
# MAGIC Load the Bakehouse transactions and add synthetic columns to simulate:
# MAGIC - **traffic_source**: How customers found us (email, social, direct, search)
# MAGIC - **device**: What device they used (iOS, Android, Desktop)
# MAGIC
# MAGIC This creates a realistic streaming source that we'll query throughout the lab.

# COMMAND ----------

from pyspark.sql.functions import col, rand, when, lit

# Load base transactions from samples
transactions_df = spark.table("samples.bakehouse.sales_transactions")

# Add synthetic traffic_source column
# Distribution: email (30%), social (25%), direct (25%), search (20%)
streaming_source_df = (transactions_df
    .withColumn("traffic_source",
        when(rand() < 0.30, lit("email"))
        .when(rand() < 0.55, lit("social"))
        .when(rand() < 0.80, lit("direct"))
        .otherwise(lit("search"))
    )
    .withColumn("device",
        when(rand() < 0.35, lit("iOS"))
        .when(rand() < 0.65, lit("Android"))
        .otherwise(lit("Desktop"))
    )
)

# Write as Delta table (our streaming source)
(streaming_source_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(f"{working_dir}/streaming_source")
)

print(f"✅ Created streaming source with {streaming_source_df.count():,} transactions")
display(streaming_source_df.limit(10))

# COMMAND ----------

# Verify the streaming source was created successfully
source_check_df = spark.read.format("delta").load(f"{working_dir}/streaming_source")
assert source_check_df.count() == 3333, "Should have 3,333 transactions"
assert "traffic_source" in source_check_df.columns, "Should have traffic_source column"
assert "device" in source_check_df.columns, "Should have device column"
print("✅ Streaming source data prepared successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Build Streaming DataFrames
# MAGIC
# MAGIC Create a streaming DataFrame from our Delta source using `readStream`. The key difference from batch DataFrames is that streaming DataFrames continuously monitor the source for new data.
# MAGIC
# MAGIC **Key Parameter**: `maxFilesPerTrigger=1` controls how many files to process per trigger, allowing us to simulate streaming behavior even with a static dataset.

# COMMAND ----------

# Create a streaming DataFrame
df = (spark.readStream
    .option("maxFilesPerTrigger", 1)  # Process 1 file per trigger to simulate streaming
    .format("delta")
    .load(f"{working_dir}/streaming_source")
)

# Check if this is a streaming DataFrame
print(f"Is this a streaming DataFrame? {df.isStreaming}")
print(f"Schema preview: {df.columns[:5]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC **Important**: Notice we cannot use `display(df)` directly on the streaming DataFrame in Free Edition. We'll write it to a Delta table and verify the output in the next section.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Apply Transformations
# MAGIC
# MAGIC Now let's apply transformations to our streaming DataFrame. We'll:
# MAGIC 1. **Filter** for transactions from email marketing campaigns
# MAGIC 2. **Add a column** to identify mobile devices (iOS or Android)
# MAGIC 3. **Select** relevant columns for our email campaign analysis
# MAGIC
# MAGIC These transformations work identically to batch DataFrames!

# COMMAND ----------

from pyspark.sql.functions import col

# Process the data frame:
# 1) Filter for "email" traffic source
# 2) Add column "is_mobile" indicating whether device is iOS or Android
# 3) Select columns: user_id, event_timestamp, mobile
traffic_df = (df
    .filter(col("traffic_source") == "email")
    .withColumn("is_mobile", col("device").isin(["iOS", "Android"]))
    .select("transactionID", "dateTime", "device", "is_mobile", "totalPrice")
)

print("✅ Transformations applied to streaming DataFrame")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Write Streaming Query Results
# MAGIC
# MAGIC Take the final streaming DataFrame and write to a Delta table using `writeStream`. This is where the streaming execution happens.
# MAGIC
# MAGIC **Key Components**:
# MAGIC - **format("delta")**: Write to Delta Lake format
# MAGIC - **outputMode("append")**: Only write new rows (no aggregations here)
# MAGIC - **option("checkpointLocation", ...)**: Enable fault tolerance by saving query progress
# MAGIC - **trigger(availableNow=True)**: Process all available data in batch mode (Free Edition compatible)

# COMMAND ----------

checkpoint_path = f"{checkpoint_dir}/email_traffic"
output_path = f"{working_dir}/email_traffic_output"

# Write streaming query
email_query = (traffic_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # Free Edition compatible trigger
    .start(output_path)
)

print(f"✅ Streaming query started: {email_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for Query Completion
# MAGIC
# MAGIC The `awaitTermination()` method waits for the streaming query to process all available data. With `trigger(availableNow=True)`, the query will process all data and stop automatically.

# COMMAND ----------

# Wait for the query to process all data
email_query.awaitTermination()
print("✅ Query completed processing")

# Stop the query
email_query.stop()
print("✅ Query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 5: Verify Output
# MAGIC
# MAGIC Now that we've written the streaming data to Delta, let's verify the results by reading it back as a **batch DataFrame**. This is the Free Edition pattern for viewing streaming results.

# COMMAND ----------

# Read the output as a batch DataFrame
email_traffic_df = spark.read.format("delta").load(output_path)

# Display the results
print(f"Total rows in email traffic: {email_traffic_df.count():,}")
display(email_traffic_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Email Campaign Results
# MAGIC
# MAGIC Let's analyze the email campaign effectiveness by comparing mobile vs desktop conversions.

# COMMAND ----------

# Analyze mobile vs desktop
from pyspark.sql.functions import count, sum as _sum, avg

email_analysis = (email_traffic_df
    .groupBy("is_mobile")
    .agg(
        count("*").alias("transaction_count"),
        _sum("totalPrice").alias("total_revenue"),
        avg("totalPrice").alias("avg_transaction_value")
    )
    .orderBy("is_mobile", ascending=False)
)

display(email_analysis)

# COMMAND ----------

# Analyze by device type
device_analysis = (email_traffic_df
    .groupBy("device")
    .agg(
        count("*").alias("transaction_count"),
        _sum("totalPrice").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

display(device_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 6: Monitor Streaming Query
# MAGIC
# MAGIC Learn how to monitor and control streaming queries using the query "handle" returned by `writeStream.start()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Monitoring Properties
# MAGIC
# MAGIC Even though our query has completed, we can still inspect its properties:

# COMMAND ----------

# Query ID (unique identifier)
print(f"Query ID: {email_query.id}")

# COMMAND ----------

# Query status
print(f"Query Status:")
print(f"  - Is Active: {email_query.isActive}")
print(f"  - Query Name: {email_query.name if email_query.name else 'Unnamed'}")

# COMMAND ----------

# Last progress information
if email_query.lastProgress:
    print("Last Progress:")
    print(f"  - Batch ID: {email_query.lastProgress.get('batchId', 'N/A')}")
    print(f"  - Input Rows: {email_query.lastProgress.get('numInputRows', 0):,}")
else:
    print("No progress information available")

# COMMAND ----------

# MAGIC %md
# MAGIC ### List All Active Queries
# MAGIC
# MAGIC Check which streaming queries are currently running in this notebook.

# COMMAND ----------

active_queries = spark.streams.active
print(f"Active streaming queries: {len(active_queries)}")
for query in active_queries:
    print(f"  - Query ID: {query.id}, Active: {query.isActive}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 7: Cleanup
# MAGIC
# MAGIC Always stop streaming queries and clean up resources when done.

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

print("✅ All queries stopped")

# COMMAND ----------

# Optional: Remove output directories to clean up
try:
    dbutils.fs.rm(f"{working_dir}/email_traffic_output", recurse=True)
    dbutils.fs.rm(checkpoint_path, recurse=True)
    print("✅ Cleaned up output directories")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC Congratulations! You've learned the fundamentals of Spark Structured Streaming on Databricks Free Edition:
# MAGIC
# MAGIC ✅ **Built streaming DataFrames** with `readStream` from Delta sources
# MAGIC
# MAGIC ✅ **Applied transformations** (filter, withColumn, select) to streaming data
# MAGIC
# MAGIC ✅ **Wrote streaming results** with `writeStream` using Delta format and checkpoints
# MAGIC
# MAGIC ✅ **Used Free Edition compatible patterns** with `trigger(availableNow=True)` and batch verification
# MAGIC
# MAGIC ✅ **Monitored queries** using the query handle properties
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Streaming DataFrames** use the same transformations as batch DataFrames
# MAGIC 2. **Checkpoints** enable fault tolerance by saving query progress
# MAGIC 3. **trigger(availableNow=True)** processes all available data in batch mode (Free Edition compatible)
# MAGIC 4. **Write-Verify Pattern**: Write streams to Delta → Read as batch → Display
# MAGIC 5. **Always stop queries** and clean up resources when done
# MAGIC
# MAGIC ## Next Steps:
# MAGIC
# MAGIC Continue to the next notebook to learn about **Output Modes** (Append vs Complete) and understand how different aggregation patterns work in streaming!
