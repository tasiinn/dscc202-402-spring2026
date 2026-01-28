# Spark Structured Streaming Notebooks for Databricks Free Edition

This directory contains three Databricks notebooks that teach Apache Spark Structured Streaming concepts specifically designed to work with **Databricks Free Edition** serverless compute.

## 🎯 Overview

These notebooks demonstrate production Structured Streaming patterns adapted for Free Edition constraints. Students learn the same APIs used in enterprise environments - the only difference is the trigger mode and verification approach.

### Notebooks

1. **Bakehouse Basic Streaming Operations** - `1_Streaming Query_Basic_class_template.py`
   - Introduction to readStream, writeStream, transformations, and monitoring
   - Dataset: `samples.bakehouse.sales_transactions` (3,333 rows)

2. **Bakehouse Streaming Output Modes: Append vs Complete** - `2_Streaming Query_UpdatevsComplete.py`
   - Understanding append mode (windowed aggregations with watermarks)
   - Understanding complete mode (non-windowed aggregations)
   - Dataset: `samples.bakehouse.sales_transactions` (3,333 rows)

3. **Bakehouse Concurrent Streaming Operations** - `3_AA_Spark Structured Streaming Performance Analysis.py`
   - Managing multiple streaming queries simultaneously
   - Concurrent vs sequential execution patterns
   - Dataset: `samples.bakehouse.sales_transactions` (3,333 rows)

## 🚨 Databricks Free Edition Limitations

Databricks Free Edition has specific constraints that affect streaming operations:

### What Does NOT Work in Free Edition

| Feature | Limitation | Impact |
|---------|-----------|--------|
| **Continuous Streaming** | Cannot use `trigger(processingTime=...)` or default continuous triggers | Must use batch-style processing instead |
| **Memory Sinks** | Cannot use `.format("memory")` | Cannot write to memory tables for SQL queries |
| **Interactive Display** | Cannot use `display()` on streaming DataFrames | Must write to Delta and read as batch to view results |
| **Update Mode** | Cannot use `.outputMode("update")` | Must use "append" or "complete" modes only |
| **Custom Compute** | Restricted to serverless compute | No custom cluster configurations or resource pools |

### Workarounds Implemented in These Notebooks

These notebooks demonstrate production-ready patterns that work within Free Edition constraints:

#### 1. Batch-Style Trigger (Instead of Continuous)

**Enterprise Pattern** (NOT supported):
```python
query = df.writeStream.trigger(processingTime="10 seconds").start()
```

**Free Edition Pattern** (used in these notebooks):
```python
query = df.writeStream.trigger(availableNow=True).start()
# Processes all available data once, then stops
```

**Note**: The `availableNow` trigger processes all available data in batch mode. The Structured Streaming APIs are identical - only the trigger changes!

#### 2. Delta Sinks with Batch Verification (Instead of Memory Sinks)

**Enterprise Pattern** (NOT supported):
```python
# Write to memory
query = df.writeStream.format("memory").queryName("my_table").start()
# Display directly
display(spark.sql("SELECT * FROM my_table"))
```

**Free Edition Pattern** (used in these notebooks):
```python
# Write to Delta
query = df.writeStream.format("delta").start(output_path)
query.awaitTermination()
query.stop()

# Read as batch to verify
result_df = spark.read.format("delta").load(output_path)
display(result_df)  # display() works on batch DataFrames!
```

#### 3. Append or Complete Modes Only (No Update Mode)

**Enterprise Pattern** (NOT supported):
```python
query = df.writeStream.outputMode("update").start()  # ❌ Not supported
```

**Free Edition Patterns** (used in these notebooks):
```python
# For windowed aggregations with watermarks
query = df.withWatermark("timestamp", "10 minutes") \
  .groupBy(window("timestamp", "1 hour")) \
  .agg(...) \
  .writeStream \
  .outputMode("append") \  # ✅ Only finalized windows written
  .start()

# For non-windowed aggregations
query = df.groupBy("category") \
  .agg(...) \
  .writeStream \
  .outputMode("complete") \  # ✅ Entire result table rewritten
  .start()
```

#### 4. Unity Catalog Volumes (Instead of DBFS Mounts)

**Free Edition Pattern** (used in these notebooks):
```sql
CREATE CATALOG IF NOT EXISTS streaming_examples_catalog;
CREATE SCHEMA IF NOT EXISTS streaming_examples_catalog.basic_streaming;
CREATE VOLUME IF NOT EXISTS streaming_examples_catalog.basic_streaming.workspace;
```

```python
working_dir = "/Volumes/streaming_examples_catalog/basic_streaming/workspace"
checkpoint_dir = f"{working_dir}/checkpoints"
```

## 📊 What IS Demonstrable in Free Edition

Despite the limitations, these notebooks successfully demonstrate all core Structured Streaming concepts:

### ✅ Fully Demonstrable Concepts

- **Streaming DataFrames**: `readStream` from Delta tables
- **Transformations**: All DataFrame operations (filter, select, join, etc.)
- **Stateful Aggregations**: groupBy with count, sum, avg
- **Windowing**: Time-based windows with `window()` function
- **Watermarks**: Handling late data with `withWatermark()`
- **Output Modes**: Append and complete modes
- **Checkpointing**: Fault-tolerant state management
- **Stream-Static Joins**: Enriching streaming data with reference tables
- **Multiple Queries**: Running concurrent streaming queries
- **Query Monitoring**: Tracking query status and progress
- **Lifecycle Management**: Starting, stopping, and cleaning up queries

### 🔄 Core Streaming Pattern Used Throughout

All three notebooks follow this consistent pattern:

```python
# 1. Create streaming DataFrame
streaming_df = spark.readStream.format("delta").load(source_path)

# 2. Apply transformations
result_df = streaming_df.filter(...).groupBy(...).agg(...)

# 3. Write with checkpoint and trigger
query = (result_df.writeStream
    .format("delta")
    .outputMode("append")  # or "complete"
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # Free Edition compatible
    .start(output_path))

# 4. Wait and stop
query.awaitTermination()
query.stop()

# 5. Verify by reading as batch
result = spark.read.format("delta").load(output_path)
display(result)
```

## 🚀 Quick Start

### Prerequisites

- Databricks Free Edition account
- Access to sample datasets: `samples.bakehouse.sales_transactions`

### Running the Notebooks

1. **Import notebooks** into your Databricks workspace
2. **Run Notebook 1** first to learn basic patterns
3. **Run Notebook 2** to understand output modes
4. **Run Notebook 3** to learn concurrent query management

Each notebook is self-contained with:
- Setup and cleanup sections
- Its own Unity Catalog schema
- Clear documentation and comments
- Verification steps to check results

### Expected Runtime

- Notebook 1: ~5-10 minutes
- Notebook 2: ~10-15 minutes
- Notebook 3: ~10-15 minutes

## 📚 Learning Outcomes

After completing these notebooks, students will be able to:

1. **Build streaming pipelines** using readStream and writeStream
2. **Apply transformations** to streaming data (same as batch)
3. **Choose appropriate output modes** (append vs complete)
4. **Implement watermarks** for handling late data
5. **Manage multiple concurrent queries** effectively
6. **Monitor and troubleshoot** streaming applications
7. **Work within Free Edition constraints** while learning production patterns

## 🔑 Key Differences from Enterprise Notebooks

If you're familiar with Structured Streaming on enterprise Databricks clusters, here are the key differences:

| Aspect | Enterprise | Free Edition (These Notebooks) |
|--------|-----------|-------------------------------|
| **Trigger** | Continuous processing with time intervals | Batch processing with `availableNow=True` |
| **Output Verification** | Interactive `display()` on streams | Write to Delta → Read as batch → Display |
| **Sinks** | Memory, console, Delta, Kafka | Delta only (with batch verification) |
| **Output Modes** | Append, Update, Complete | Append, Complete (no update) |
| **Compute** | Custom clusters, autoscaling | Serverless only |
| **Resource Pools** | Fair scheduler pools configurable | Not applicable to serverless |

### What Remains the Same

- ✅ All Structured Streaming APIs (`readStream`, `writeStream`, transformations)
- ✅ DataFrame operations and SQL functions
- ✅ Watermarking and windowing logic
- ✅ Checkpoint mechanism and fault tolerance
- ✅ State management for aggregations
- ✅ Stream-static joins

**The skills you learn here transfer directly to production Spark Streaming environments!**

## 🎓 Pedagogical Notes for Instructors

These notebooks prioritize:

1. **Clear explanations** of Free Edition constraints and workarounds
2. **Business context** (Bakehouse franchise analytics) for relatability
3. **Progressive complexity** (basic → modes → concurrent)
4. **Verification patterns** to build confidence in results
5. **Production readiness** - patterns used are production-quality

The write-verify pattern (write stream to Delta, read as batch) is actually a **best practice** even in enterprise environments for testing and validation!

## 🛠️ Troubleshooting

### Common Issues

**Issue**: "Cannot use `display()` on streaming DataFrame"
- **Solution**: This is expected! Write to Delta with `writeStream`, then read as batch and display.

**Issue**: "`outputMode("update")` not supported"
- **Solution**: Use `outputMode("append")` with watermarks for windowed aggs, or `outputMode("complete")` for non-windowed aggs.

**Issue**: Checkpoint conflicts
- **Solution**: Each query needs its own checkpoint location. Clear old checkpoints with `dbutils.fs.rm(checkpoint_path, recurse=True)`.

**Issue**: Query never completes
- **Solution**: With `trigger(availableNow=True)`, queries should complete automatically. Check for errors in query progress.

## 📖 Additional Resources

- **Databricks Free Edition Docs**: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- **Structured Streaming Guide**: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- **Solution Notebooks**: See `/solutions/0.3 - Spark Streaming.py` for additional examples

## 💡 Tips for Success

1. **Always run cleanup cells** before re-running notebooks
2. **Read Free Edition notes** at the beginning of each notebook
3. **Verify results** using the batch read pattern after each query
4. **Experiment with parameters** like window sizes and watermarks
5. **Check active queries** with `spark.streams.active` if issues arise

---

**Note**: These notebooks use `samples.bakehouse.sales_transactions`, which is pre-loaded in Databricks Free Edition. No external data upload required!

Happy Streaming! 🚀
