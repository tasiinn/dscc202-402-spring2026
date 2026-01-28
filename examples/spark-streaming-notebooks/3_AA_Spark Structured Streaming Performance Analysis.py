# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Concurrent Streaming Operations
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Real-world streaming applications often need to run multiple queries simultaneously. For example, the Bakehouse analytics team might need to:
# MAGIC - Monitor hourly sales trends
# MAGIC - Track product performance in real-time
# MAGIC - Generate alerts for high-value transactions
# MAGIC
# MAGIC All these queries should run concurrently to provide comprehensive real-time insights. This notebook teaches you how to manage multiple streaming queries and compare concurrent vs sequential execution.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Start and manage **multiple streaming queries** simultaneously
# MAGIC 2. Compare **concurrent vs sequential** execution
# MAGIC 3. Monitor query progress and performance
# MAGIC 4. Handle query lifecycle (start, monitor, stop)
# MAGIC 5. Understand Free Edition streaming patterns for production use
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC We'll use `samples.bakehouse.sales_transactions` (3,333 transactions) with synthetic category labels to demonstrate concurrent streaming operations.
# MAGIC
# MAGIC **Note**: This simplified notebook focuses on query management rather than advanced performance optimization. It's designed for Databricks Free Edition serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Unity Catalog resources for this notebook
# MAGIC CREATE CATALOG IF NOT EXISTS streaming_examples_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_examples_catalog.concurrent_streaming;
# MAGIC CREATE VOLUME IF NOT EXISTS streaming_examples_catalog.concurrent_streaming.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
import time
from datetime import datetime

working_dir = "/Volumes/streaming_examples_catalog/concurrent_streaming/workspace"
checkpoint_dir = f"{working_dir}/checkpoints"

# Control variable: Set to False to run streams sequentially
RUN_CONCURRENT = False

print(f"Working directory: {working_dir}")
print(f"Checkpoint directory: {checkpoint_dir}")
print(f"Execution mode: {'CONCURRENT' if RUN_CONCURRENT else 'SEQUENTIAL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup Previous Runs

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
# MAGIC # Section 1: Prepare Streaming Source Data
# MAGIC
# MAGIC Load the Bakehouse transactions and add a synthetic `category` field to group transactions by size:
# MAGIC - **Small**: totalPrice < $50
# MAGIC - **Medium**: $50 ≤ totalPrice < $150
# MAGIC - **Large**: totalPrice ≥ $150
# MAGIC
# MAGIC This creates realistic data for our concurrent streaming queries.

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# Load base transactions from samples
transactions_df = spark.table("samples.bakehouse.sales_transactions")

# Add synthetic category field based on transaction value
streaming_source_df = (transactions_df
    .withColumn("category",
        when(col("totalPrice") < 50, lit("Small"))
        .when(col("totalPrice") < 150, lit("Medium"))
        .otherwise(lit("Large"))
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
display(streaming_source_df.groupBy("category").count().orderBy("category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Define Streaming Queries
# MAGIC
# MAGIC We'll define **two concurrent streaming queries** that demonstrate different streaming patterns:
# MAGIC
# MAGIC 1. **Query 1: Hourly Sales Trends** (Windowed aggregation with append mode)
# MAGIC 2. **Query 2: Category Analysis** (Non-windowed aggregation with complete mode)
# MAGIC
# MAGIC Each query reads from the same source but performs different analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Hourly Sales Trends
# MAGIC
# MAGIC Calculate sales metrics for each hour using windowed aggregations.

# COMMAND ----------

from pyspark.sql.functions import window, sum as _sum, count, avg

def create_hourly_trends_query():
    """
    Create a streaming query for hourly sales trends.
    Uses append mode with watermarks.
    """
    # Read stream with watermark
    streaming_df = (spark.readStream
        .format("delta")
        .load(f"{working_dir}/streaming_source")
        .withWatermark("dateTime", "10 minutes")
    )

    # Aggregate by hourly windows
    hourly_trends = (streaming_df
        .groupBy(window(col("dateTime"), "1 hour"))
        .agg(
            count("*").alias("transaction_count"),
            _sum("totalPrice").alias("total_revenue"),
            avg("totalPrice").alias("avg_transaction_value")
        )
        .select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            "transaction_count",
            "total_revenue",
            "avg_transaction_value"
        )
    )

    return hourly_trends

print("✅ Hourly trends query defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Category Analysis
# MAGIC
# MAGIC Calculate running totals for each transaction category (Small/Medium/Large).

# COMMAND ----------

def create_category_analysis_query():
    """
    Create a streaming query for category analysis.
    Uses complete mode for non-windowed aggregations.
    """
    # Read stream (no watermark needed for complete mode)
    streaming_df = (spark.readStream
        .format("delta")
        .load(f"{working_dir}/streaming_source")
    )

    # Aggregate by category
    category_analysis = (streaming_df
        .groupBy("category")
        .agg(
            count("*").alias("transaction_count"),
            _sum("totalPrice").alias("total_revenue"),
            avg("totalPrice").alias("avg_transaction_value")
        )
        .orderBy(col("total_revenue").desc())
    )

    return category_analysis

print("✅ Category analysis query defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Execute Streaming Queries
# MAGIC
# MAGIC Now we'll execute our queries in either **concurrent** or **sequential** mode, depending on the `RUN_CONCURRENT` control variable.
# MAGIC
# MAGIC This comparison helps understand:
# MAGIC - How concurrent queries share cluster resources
# MAGIC - The performance difference between concurrent and sequential execution
# MAGIC - How to manage multiple query lifecycles

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Paths and Start Queries

# COMMAND ----------

# Define output paths
hourly_output = f"{working_dir}/hourly_trends_output"
category_output = f"{working_dir}/category_analysis_output"

hourly_checkpoint = f"{checkpoint_dir}/hourly_trends"
category_checkpoint = f"{checkpoint_dir}/category_analysis"

# Create queries
hourly_query_df = create_hourly_trends_query()
category_query_df = create_category_analysis_query()

print("✅ Queries created, ready to start execution")

# COMMAND ----------

if RUN_CONCURRENT:
    # CONCURRENT EXECUTION
    print("=" * 60)
    print("CONCURRENT EXECUTION MODE")
    print("=" * 60)
    print("Starting both queries simultaneously...\n")

    start_time = time.time()

    # Start both queries at the same time
    query1 = (hourly_query_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", hourly_checkpoint)
        .trigger(availableNow=True)
        .start(hourly_output)
    )
    print(f"✅ Query 1 started: {query1.id}")

    query2 = (category_query_df
        .writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", category_checkpoint)
        .trigger(availableNow=True)
        .start(category_output)
    )
    print(f"✅ Query 2 started: {query2.id}")

    # Wait for both queries to complete
    print("\nWaiting for queries to complete...")
    query1.awaitTermination()
    query2.awaitTermination()

    end_time = time.time()
    elapsed_concurrent = end_time - start_time

    # Stop queries
    query1.stop()
    query2.stop()

    print(f"\n✅ Both queries completed")
    print(f"⏱️  Total execution time (CONCURRENT): {elapsed_concurrent:.2f} seconds")
    print("=" * 60)

else:
    # SEQUENTIAL EXECUTION
    print("=" * 60)
    print("SEQUENTIAL EXECUTION MODE")
    print("=" * 60)
    print("Running queries one at a time...\n")

    start_time = time.time()

    # Start and complete Query 1
    print("Starting Query 1...")
    query1 = (hourly_query_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", hourly_checkpoint)
        .trigger(availableNow=True)
        .start(hourly_output)
    )
    print(f"✅ Query 1 started: {query1.id}")

    query1.awaitTermination()
    query1_time = time.time()
    query1.stop()
    print(f"✅ Query 1 completed in {query1_time - start_time:.2f} seconds\n")

    # Start and complete Query 2
    print("Starting Query 2...")
    query2 = (category_query_df
        .writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", category_checkpoint)
        .trigger(availableNow=True)
        .start(category_output)
    )
    print(f"✅ Query 2 started: {query2.id}")

    query2.awaitTermination()
    end_time = time.time()
    query2.stop()
    print(f"✅ Query 2 completed in {end_time - query1_time:.2f} seconds\n")

    elapsed_sequential = end_time - start_time

    print(f"✅ All queries completed")
    print(f"⏱️  Total execution time (SEQUENTIAL): {elapsed_sequential:.2f} seconds")
    print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Verify Results
# MAGIC
# MAGIC Let's read the output from both queries and verify they completed successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1 Results: Hourly Sales Trends

# COMMAND ----------

# Read hourly trends output
hourly_results_df = spark.read.format("delta").load(hourly_output)

print(f"Hourly Trends Results:")
print(f"  - Total hourly windows: {hourly_results_df.count():,}")
print(f"  - Unique hours: {hourly_results_df.select('hour_start').distinct().count()}")

display(hourly_results_df.orderBy("hour_start"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2 Results: Category Analysis

# COMMAND ----------

# Read category analysis output
category_results_df = spark.read.format("delta").load(category_output)

print(f"Category Analysis Results:")
print(f"  - Total categories: {category_results_df.count()}")

display(category_results_df.orderBy(col("total_revenue").desc()))

# COMMAND ----------

# Detailed category breakdown
print("\nCategory Breakdown:")
for row in category_results_df.orderBy(col("total_revenue").desc()).collect():
    print(f"  {row['category']:8s}: {row['transaction_count']:,} transactions, "
          f"${row['total_revenue']:,.2f} revenue, "
          f"${row['avg_transaction_value']:.2f} avg")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 5: Query Monitoring
# MAGIC
# MAGIC Learn how to monitor streaming queries and check their progress.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Active Queries

# COMMAND ----------

# List all active streaming queries
active_queries = spark.streams.active
print(f"Currently active streaming queries: {len(active_queries)}")

if len(active_queries) > 0:
    for query in active_queries:
        print(f"  - Query ID: {query.id}")
        print(f"    Name: {query.name if query.name else 'Unnamed'}")
        print(f"    Status: {'Active' if query.isActive else 'Inactive'}")
else:
    print("  No active queries (all completed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Progress Information
# MAGIC
# MAGIC For queries that have completed, we can still inspect their final progress.

# COMMAND ----------

# Note: Since our queries have completed with availableNow trigger,
# we can't show real-time progress. In production with continuous triggers,
# you would use query.recentProgress to monitor ongoing streams.

print("Query Monitoring Notes:")
print("  - Use query.id to uniquely identify queries")
print("  - Use query.isActive to check if still running")
print("  - Use query.lastProgress for final stats")
print("  - Use query.recentProgress for historical metrics (continuous mode)")
print("  - Use spark.streams.active to list all running queries")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 6: Cleanup

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

print("✅ All queries stopped")

# COMMAND ----------

# Optional: Remove output directories to clean up
try:
    dbutils.fs.rm(hourly_output, recurse=True)
    dbutils.fs.rm(category_output, recurse=True)
    dbutils.fs.rm(checkpoint_dir, recurse=True)
    print("✅ Cleaned up output directories")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC Congratulations! You've learned how to manage concurrent streaming queries in Spark Structured Streaming on Databricks Free Edition:
# MAGIC
# MAGIC ✅ **Defined multiple streaming queries** with different aggregation patterns
# MAGIC
# MAGIC ✅ **Executed queries concurrently** to maximize resource utilization
# MAGIC
# MAGIC ✅ **Compared concurrent vs sequential** execution to understand performance
# MAGIC
# MAGIC ✅ **Monitored query lifecycle** (start, wait, stop)
# MAGIC
# MAGIC ✅ **Verified results** from multiple simultaneous queries
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Multiple queries can run simultaneously** on the same cluster
# MAGIC 2. **Each query needs its own checkpoint directory** for fault tolerance
# MAGIC 3. **Concurrent execution** can be more efficient than sequential
# MAGIC 4. **Query management** is critical:
# MAGIC    - Track queries with unique IDs
# MAGIC    - Use `awaitTermination()` to wait for completion
# MAGIC    - Always `stop()` queries when done
# MAGIC 5. **Monitor with** `spark.streams.active` and query properties
# MAGIC
# MAGIC ## When to Use Concurrent Streaming:
# MAGIC
# MAGIC - **Multiple analytics needs**: Hourly trends + category analysis + alerts
# MAGIC - **Different output modes**: Append for events + complete for dashboards
# MAGIC - **Resource efficiency**: Process same source data for multiple purposes
# MAGIC - **Real-time dashboards**: Multiple metrics updated simultaneously
# MAGIC
# MAGIC ## Production Considerations:
# MAGIC
# MAGIC - Monitor cluster resources with multiple concurrent streams
# MAGIC - Use appropriate trigger intervals for each query
# MAGIC - Configure checkpoint locations properly
# MAGIC - Handle query failures gracefully
# MAGIC - Consider query priorities for resource allocation
# MAGIC
# MAGIC ## Next Steps:
# MAGIC
# MAGIC You've completed the Spark Structured Streaming series! You now know:
# MAGIC - Basic streaming operations (Notebook 1)
# MAGIC - Output modes (Notebook 2)
# MAGIC - Concurrent query management (Notebook 3)
# MAGIC
# MAGIC Apply these patterns to build production streaming pipelines on Databricks Free Edition!
