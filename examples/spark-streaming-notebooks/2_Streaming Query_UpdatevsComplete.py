# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Streaming Output Modes: Append vs Complete
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC The Bakehouse analytics team needs to understand different streaming output modes to build the right dashboards. Should we show:
# MAGIC - **All transactions** as they arrive (append)?
# MAGIC - **Hourly summaries** for only completed time windows (append with watermarks)?
# MAGIC - **Running totals** that constantly update (complete)?
# MAGIC
# MAGIC This notebook demonstrates the differences between **Append** and **Complete** output modes in Spark Structured Streaming, helping you choose the right mode for your use case.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand **Append Mode** - for windowed aggregations with watermarks
# MAGIC 2. Understand **Complete Mode** - for non-windowed aggregations
# MAGIC 3. Compare output modes and when to use each
# MAGIC 4. Learn about watermarks for handling late data
# MAGIC 5. Master Free Edition streaming patterns
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC We'll use `samples.bakehouse.sales_transactions` (3,333 transactions) split into batches to simulate streaming arrivals.
# MAGIC
# MAGIC **Note**: "Update" mode is not supported in Databricks Free Edition, so this notebook focuses on the two supported modes: **Append** and **Complete**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Unity Catalog resources for this notebook
# MAGIC CREATE CATALOG IF NOT EXISTS streaming_examples_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_examples_catalog.output_modes;
# MAGIC CREATE VOLUME IF NOT EXISTS streaming_examples_catalog.output_modes.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
working_dir = "/Volumes/streaming_examples_catalog/output_modes/workspace"
checkpoint_dir = f"{working_dir}/checkpoints"

print(f"Working directory: {working_dir}")
print(f"Checkpoint directory: {checkpoint_dir}")

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
# MAGIC # Important: Output Modes in Databricks Free Edition
# MAGIC
# MAGIC Databricks Free Edition supports **two output modes** for streaming queries:
# MAGIC
# MAGIC ## Append Mode ✅
# MAGIC - **Use case**: Windowed aggregations with watermarks
# MAGIC - **Behavior**: Only writes **finalized windows** to output (windows that won't receive late data)
# MAGIC - **Requires**: Watermark configuration to determine when windows are complete
# MAGIC - **Example**: Hourly sales summaries (write only after each hour ends + watermark delay)
# MAGIC
# MAGIC ## Complete Mode ✅
# MAGIC - **Use case**: Non-windowed aggregations (global totals, running sums)
# MAGIC - **Behavior**: Rewrites **entire result table** every trigger
# MAGIC - **Requires**: No watermark needed
# MAGIC - **Example**: Total sales by product (always shows complete snapshot)
# MAGIC
# MAGIC ## Update Mode ❌ (NOT SUPPORTED in Free Edition)
# MAGIC - Cannot use `outputMode("update")` on serverless compute
# MAGIC - Use append or complete modes instead
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Generate Streaming Source Data
# MAGIC
# MAGIC To simulate streaming arrivals, we'll:
# MAGIC 1. Load the Bakehouse transactions
# MAGIC 2. Split into 5 time-based batches
# MAGIC 3. Write each batch sequentially to a Delta table
# MAGIC
# MAGIC This creates a streaming source where data "arrives" gradually, mimicking real-world streaming behavior.

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Load base transactions from samples
transactions_df = spark.table("samples.bakehouse.sales_transactions")

# Sort by dateTime to simulate chronological streaming
transactions_df = transactions_df.orderBy("dateTime")

# Add a batch number (0-4) to split data into 5 batches
window_spec = Window.orderBy("dateTime")
batched_df = transactions_df.withColumn("row_num", row_number().over(window_spec))

# Calculate batch size (3333 / 5 ≈ 667 per batch)
total_rows = transactions_df.count()
batch_size = total_rows // 5

print(f"Total transactions: {total_rows:,}")
print(f"Batch size: ~{batch_size} transactions per batch")

# COMMAND ----------

# Write batches sequentially to simulate streaming arrivals
from pyspark.sql.functions import lit

for batch_id in range(5):
    start_row = batch_id * batch_size + 1
    end_row = (batch_id + 1) * batch_size if batch_id < 4 else total_rows + 1

    batch_df = (batched_df
        .filter((col("row_num") >= start_row) & (col("row_num") < end_row))
        .drop("row_num")
        .withColumn("batch_id", lit(batch_id))
    )

    # Append each batch to the streaming source
    (batch_df
     .write
     .format("delta")
     .mode("append")  # Append each batch
     .save(f"{working_dir}/streaming_source")
    )

    print(f"✅ Wrote batch {batch_id}: {batch_df.count():,} transactions")

print(f"\n✅ Streaming source ready with {total_rows:,} transactions in 5 batches")

# COMMAND ----------

# Verify the streaming source
source_check_df = spark.read.format("delta").load(f"{working_dir}/streaming_source")
print(f"Total records in streaming source: {source_check_df.count():,}")
display(source_check_df.groupBy("batch_id").count().orderBy("batch_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Append Mode - Windowed Aggregations
# MAGIC
# MAGIC **Append mode** is designed for windowed aggregations where you want to output only **finalized windows** (windows that won't receive more late data).
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Windowing**: Group data into time buckets (e.g., every 1 hour)
# MAGIC - **Watermark**: Define how long to wait for late data (e.g., "10 minutes")
# MAGIC - **Finalization**: Only windows older than current watermark are written to output
# MAGIC
# MAGIC ## Use Case:
# MAGIC Calculate **hourly sales by franchise**, writing results only after each hour is complete.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Hourly Sales with Append Mode

# COMMAND ----------

from pyspark.sql.functions import window, sum as _sum, count, col

# Create streaming DataFrame with watermark
streaming_df = (spark.readStream
    .format("delta")
    .load(f"{working_dir}/streaming_source")
    .withWatermark("dateTime", "10 minutes")  # Wait up to 10 minutes for late data
)

# Aggregate by hourly windows and franchiseID
hourly_sales = (streaming_df
    .groupBy(
        window(col("dateTime"), "1 hour"),  # 1-hour windows
        col("franchiseID")
    )
    .agg(
        count("*").alias("transaction_count"),
        _sum("totalPrice").alias("total_sales")
    )
    .select(
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        "franchiseID",
        "transaction_count",
        "total_sales"
    )
)

print("✅ Hourly sales aggregation defined with watermark")

# COMMAND ----------

# Write with APPEND mode
append_checkpoint = f"{checkpoint_dir}/append_mode"
append_output = f"{working_dir}/append_output"

append_query = (hourly_sales
    .writeStream
    .format("delta")
    .outputMode("append")  # Only write finalized windows
    .option("checkpointLocation", append_checkpoint)
    .trigger(availableNow=True)
    .start(append_output)
)

print(f"✅ Append mode query started: {append_query.id}")

# COMMAND ----------

# Wait for query to complete
append_query.awaitTermination()
print("✅ Query completed processing")

append_query.stop()
print("✅ Query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Append Mode Results
# MAGIC
# MAGIC Let's read the output as a batch DataFrame and analyze the results.

# COMMAND ----------

# Read append mode output
append_results_df = spark.read.format("delta").load(append_output)

print(f"Total finalized hourly windows: {append_results_df.count():,}")
display(append_results_df.orderBy("hour_start", "franchiseID"))

# COMMAND ----------

# Analyze append mode output
print("Append Mode Summary:")
print(f"  - Total finalized windows: {append_results_df.count():,}")
print(f"  - Unique hours: {append_results_df.select('hour_start').distinct().count()}")
print(f"  - Unique franchises: {append_results_df.select('franchiseID').distinct().count()}")

# Show hourly totals across all franchises
hourly_totals = (append_results_df
    .groupBy("hour_start", "hour_end")
    .agg(
        _sum("transaction_count").alias("total_transactions"),
        _sum("total_sales").alias("total_revenue")
    )
    .orderBy("hour_start")
)

display(hourly_totals)

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Observation**: Append mode only wrote windows that passed the watermark threshold. Windows are **immutable** once written - they won't be updated even if late data arrives.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Complete Mode - Non-Windowed Aggregations
# MAGIC
# MAGIC **Complete mode** rewrites the **entire result table** every trigger. It's designed for:
# MAGIC - Global aggregations (no time windows)
# MAGIC - Running totals that need to show the full current state
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - No watermark needed
# MAGIC - Entire result table is recomputed and rewritten each trigger
# MAGIC - Use for dashboards showing current totals
# MAGIC
# MAGIC ## Use Case:
# MAGIC Calculate **total sales by product** (running totals across all time).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Product Sales with Complete Mode

# COMMAND ----------

# DBTITLE 1,Task 3.1: Product Sales with Complete Mode
from pyspark.sql.functions import avg

# Create streaming DataFrame (no watermark needed for complete mode)
streaming_df_complete = (spark.readStream
    .format("delta")
    .load(f"{working_dir}/streaming_source")
)

# Aggregate by product (no time window)
product_sales = (streaming_df_complete
    .groupBy("product")
    .agg(
        count("*").alias("transaction_count"),
        _sum("totalPrice").alias("total_revenue"),
        avg("totalPrice").alias("avg_transaction_value")
    )
    .orderBy(col("total_revenue").desc())
)

print("✅ Product sales aggregation defined (no watermark)")

# COMMAND ----------

from pyspark.sql.functions import avg

# Recreate product_sales with correct avg function
product_sales = (streaming_df_complete
    .groupBy("product")
    .agg(
        count("*").alias("transaction_count"),
        _sum("totalPrice").alias("total_revenue"),
        avg("totalPrice").alias("avg_transaction_value")
    )
)

print("✅ Product sales aggregation defined (no watermark)")

# COMMAND ----------

# Write with COMPLETE mode
complete_checkpoint = f"{checkpoint_dir}/complete_mode"
complete_output = f"{working_dir}/complete_output"

complete_query = (product_sales
    .writeStream
    .format("delta")
    .outputMode("complete")  # Rewrite entire result table every trigger
    .option("checkpointLocation", complete_checkpoint)
    .trigger(availableNow=True)
    .start(complete_output)
)

print(f"✅ Complete mode query started: {complete_query.id}")

# COMMAND ----------

# Wait for query to complete
complete_query.awaitTermination()
print("✅ Query completed processing")

complete_query.stop()
print("✅ Query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Complete Mode Results

# COMMAND ----------

# Read complete mode output
complete_results_df = spark.read.format("delta").load(complete_output)

print(f"Total products in results: {complete_results_df.count():,}")
display(complete_results_df.orderBy(col("total_revenue").desc()))

# COMMAND ----------

# Analyze complete mode output
print("Complete Mode Summary:")
print(f"  - Total products: {complete_results_df.count():,}")
print(f"  - Total revenue across all products: ${complete_results_df.agg(_sum('total_revenue')).collect()[0][0]:,.2f}")
print(f"  - Total transactions: {complete_results_df.agg(_sum('transaction_count')).collect()[0][0]:,}")

# Show top 10 products by revenue
print("\nTop 10 Products by Revenue:")
display(complete_results_df.orderBy(col("total_revenue").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Observation**: Complete mode wrote the **full result table** showing totals for all products. If we triggered again with more data, the entire table would be rewritten with updated totals.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Comparison - When to Use Each Mode
# MAGIC
# MAGIC Let's compare the two output modes side by side.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append Mode vs Complete Mode
# MAGIC
# MAGIC | Feature | Append Mode | Complete Mode |
# MAGIC |---------|-------------|---------------|
# MAGIC | **Use Case** | Windowed aggregations | Non-windowed aggregations |
# MAGIC | **Watermark** | Required | Not needed |
# MAGIC | **Output Behavior** | Only finalized windows | Entire result table rewritten |
# MAGIC | **Data Written** | Incremental (new windows only) | Complete snapshot every trigger |
# MAGIC | **Update Support** | Windows are immutable once written | Totals update with each trigger |
# MAGIC | **Best For** | Time-series analytics, event logs | Dashboards showing current totals |
# MAGIC | **Example** | Hourly sales by franchise | Total sales by product |
# MAGIC
# MAGIC ## When to Use Append Mode:
# MAGIC - ✅ Windowed aggregations (hourly, daily, etc.)
# MAGIC - ✅ Event-time based analytics
# MAGIC - ✅ You need immutable finalized results
# MAGIC - ✅ Late data should be handled with watermarks
# MAGIC
# MAGIC ## When to Use Complete Mode:
# MAGIC - ✅ Non-windowed aggregations (global totals)
# MAGIC - ✅ Running totals across all time
# MAGIC - ✅ Dashboards showing current state
# MAGIC - ✅ Small result tables that fit in memory
# MAGIC
# MAGIC ## Update Mode (NOT in Free Edition):
# MAGIC - ❌ **Not supported** in Databricks Free Edition
# MAGIC - Would allow incremental updates to rows without rewriting entire table
# MAGIC - Use append or complete modes as alternatives

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visual Comparison

# COMMAND ----------

# Compare record counts
print("Output Mode Comparison:")
print(f"  Append Mode Records: {append_results_df.count():,} (finalized hourly windows)")
print(f"  Complete Mode Records: {complete_results_df.count():,} (all products)")
print("")
print("Key Differences:")
print("  - Append wrote only finalized time windows (with watermark)")
print("  - Complete wrote full product totals (no time dimension)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 5: Cleanup

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

print("✅ All queries stopped")

# COMMAND ----------

# Optional: Remove output directories to clean up
try:
    dbutils.fs.rm(append_output, recurse=True)
    dbutils.fs.rm(complete_output, recurse=True)
    dbutils.fs.rm(checkpoint_dir, recurse=True)
    print("✅ Cleaned up output directories")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC Congratulations! You've learned how to use different output modes in Spark Structured Streaming on Databricks Free Edition:
# MAGIC
# MAGIC ✅ **Append Mode** - For windowed aggregations with watermarks (only finalized windows written)
# MAGIC
# MAGIC ✅ **Complete Mode** - For non-windowed aggregations (entire result table rewritten)
# MAGIC
# MAGIC ✅ **Watermarks** - Define how long to wait for late data before finalizing windows
# MAGIC
# MAGIC ✅ **Comparison** - Understand when to use each mode for your use case
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Append mode requires watermarks** for windowed aggregations
# MAGIC 2. **Complete mode rewrites entire output** every trigger (best for small result tables)
# MAGIC 3. **Update mode not available** in Free Edition - use append or complete instead
# MAGIC 4. **Choose based on use case**:
# MAGIC    - Time-series analytics → Append with windows
# MAGIC    - Running totals → Complete
# MAGIC 5. **Write-Verify Pattern** works for both modes
# MAGIC
# MAGIC ## Next Steps:
# MAGIC
# MAGIC Continue to the next notebook to learn about **Concurrent Streaming** and how to manage multiple streaming queries simultaneously!
