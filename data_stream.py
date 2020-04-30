import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from kafka_server import TOPIC_NAME

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("call_date_time", "original_crime_type_name", "disposition")

    # count the number of original crime type
    agg_df = distinct_table.groupBy(
        psf.window(distinct_table.call_date_time, "24 hours", "60 minutes"),
        distinct_table.original_crime_type_name
    ).count().orderBy("window", "count")

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .queryName("original_crim_type_counts") \
        .outputMode("complete") \
        .format("console") \
        .start()
#        .trigger(processingTime="5 seconds") \

    # TODO attach a ProgressReporter
    #query.awaitTermination()

    #get_min_max_call_times(distinct_table)

    #get_total_stats(distinct_table)

    #get_join(spark, distinct_table)


    spark.streams.awaitAnyTermination()

def get_min_max_call_times(df):
    agg_df = df.groupBy(
        psf.window(df.call_date_time, "24 hours", "60 minutes"),
        df.original_crime_type_name
    ).agg(psf.min(psf.col("call_date_time")), psf.max(psf.col("call_date_time")))

    query = agg_df \
        .writeStream \
        .queryName("min_max_times") \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .start()

    return query


def get_total_stats(df):
    agg_df = df.agg(psf.count(df.call_date_time),
                    psf.min(df.call_date_time),
                    psf.max(df.call_date_time))

    query = agg_df \
        .writeStream \
        .queryName("total_count") \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .start()

    return query

def get_join(spark, df):

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    radio_code_df.printSchema()

    agg_df = df.groupBy(
        psf.window(df.call_date_time, "24 hours", "60 minutes"),
        df.disposition
    ).count()

    # TODO join on disposition column
    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .queryName("join_query") \
        .outputMode("complete") \
        .format("console").start()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.streaming.blockInterval", "200ms") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()

    logger.info("Spark started")


    #spark.sparkContext.setLogLevel("WARN")

    run_spark_job(spark)

    spark.stop()
