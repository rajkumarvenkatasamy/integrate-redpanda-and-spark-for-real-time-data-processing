from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    expr,
    avg,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# Create a SparkSession
spark = SparkSession.builder.appName("StreamingApp").getOrCreate()

# Define the schema for the input data
schema = StructType(
    [
        StructField("Date", StringType(), True),
        StructField("Ticker", StringType(), True),
        StructField("ClosingPriceUSD", DoubleType(), True),
    ]
)

# Read from the Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-0:9092")
    .option("subscribe", "stock_price_topic")
    .load()
)

# Parse the JSON data
parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Convert the Date column to a timestamp
parsed_df = parsed_df.withColumn("Date", expr("to_timestamp(Date, 'yyyy-MM-dd')"))

# Add a watermark
# Watermark range is based on the Date column and is set to 10 days to handle late data and out-of-order data arrival
# The last accepted date for the watermark is based on the
# max(Date) across all partitions in the topic minus 10 days, data older than that is no longer accepted.
parsed_df = parsed_df.withWatermark("Date", "10 days")


# Group by Ticker and a sliding window of 10 days
grouped_df = parsed_df.groupBy("Ticker", window("Date", "10 days"))

# Compute 10 days moving average
moving_avg_df = grouped_df.agg(avg("ClosingPriceUSD").alias("MovingAverage"))

# Prepare the output data for writing to a new kafka topic and that should include,
# ticker, window start, window end, and moving average
output_df = moving_avg_df.select(
    col("Ticker"),
    col("window.start").alias("WindowStart"),
    col("window.end").alias("WindowEnd"),
    col("MovingAverage"),
).selectExpr(
    "CAST(Ticker AS STRING) AS key",
    "to_json(struct(*)) AS value",
)

# Write the output data to a new kafka topic
query = (
    output_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-0:9092")
    .option("topic", "stock_price_moving_avg_topic")
    .option("checkpointLocation", "/tmp/checkpoint")
    .outputMode("update")
    .start()
)

# Wait for the query to terminate
query.awaitTermination()
