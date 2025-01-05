from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql import SparkSession
from configs import kafka_config, topic_prefix
import os
import datetime

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)


def initialize_spark():
    return (
        SparkSession.builder.appName("KafkaStreaming")
        .master("local[*]")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.streaming.kafka.maxRatePerPartition", "200")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
        )
        .option("subscribe", f"{topic_prefix}_building_sensors")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5")
        .option("failOnDataLoss", "false")
        .load()
        .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
        .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    )


def process_input_df(input_df):
    json_schema = StructType(
        [
            StructField("sensor_id", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
        ]
    )

    return (
        input_df.selectExpr("CAST(value AS STRING) AS value_deserialized")
        .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
        .withColumn("sensor_id", col("value_json.sensor_id"))
        .withColumn(
            "timestamp",
            from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast(
                "timestamp"
            ),
        )
        .withColumn("temperature", col("value_json.temperature"))
        .withColumn("humidity", col("value_json.humidity"))
        .withWatermark("timestamp", "10 seconds")
        .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), "sensor_id")
        .agg(avg("temperature").alias("avg_t"), avg("humidity").alias("avg_h"))
        .drop("value_deserialized", "value_json")
    )


def read_alert_conditions(spark):
    return spark.read.csv("alerts_conditions.csv", header=True)


def generate_alerts(input_df, alerts_conditions):
    crossed_df = input_df.crossJoin(alerts_conditions)

    return (
        crossed_df.where(
            "(avg_t > temperature_min AND avg_t < temperature_max) OR (avg_h> humidity_min AND avg_h < humidity_max)"
        )
        .withColumn("timestamp", lit(str(datetime.datetime.now())))
        .drop("id")
    )


def main():
    spark = initialize_spark()
    input_df = read_kafka_stream(spark)
    processed_df = process_input_df(input_df)
    alerts_conditions = read_alert_conditions(spark)
    alerts_df = generate_alerts(processed_df, alerts_conditions)

    alerts_query = (
        alerts_df.selectExpr(
            "CAST(sensor_id AS STRING) AS key",
            "to_json(struct(window, avg_t, avg_h, code, message, timestamp)) AS value",
        )
        .writeStream.outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("topic", f"{topic_prefix}_building_alerts")
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username=\"{kafka_config['username']}\" "
            f"password=\"{kafka_config['password']}\";",
        )
        .option("checkpointLocation", "/tmp/alerts_checkpoint")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
