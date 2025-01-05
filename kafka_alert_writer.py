from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)
from configs import kafka_config, topic_prefix
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

output_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f"org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username=\"{kafka_config['username']}\" "
        f"password=\"{kafka_config['password']}\";",
    )
    .option("subscribe", f"{topic_prefix}_building_alerts")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "300")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
)

json_schema = StructType(
    [
        StructField(
            "window",
            StructType(
                [
                    StructField("start", TimestampType(), True),
                    StructField("end", TimestampType(), True),
                ]
            ),
            True,
        ),
        StructField("avg_t", DoubleType(), True),
        StructField("avg_h", DoubleType(), True),
        StructField("code", StringType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

output_df = (
    output_df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value", from_json(col("value_deserialized"), json_schema))
    .select("key", "value.*")
)

output_df.writeStream.trigger(processingTime="10 seconds").outputMode("update").format(
    "console"
).option("truncate", "false").start().awaitTermination()
