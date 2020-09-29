#pyspark
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T


def transform_log_data(input_loc, output_loc):
    default_params = {"infer_schema": "true",
                      "file_type": 'csv',
                      "include_header": 'true',
                      "delimiter": ','}

    df = spark.read.format(default_params["file_type"]) \
        .option("InferSchema", default_params["infer_schema"]) \
        .option("header", default_params["include_header"]) \
        .option("delimiter", default_params["delimiter"]) \
        .load(input_loc)

    df = df.withColumn("row_num", F.row_number().over(Window.partitionBy("Vehicle_id").orderBy("timestamp")))
    # identify the start and end time of each function call
    df = df.withColumn("group", F.sum(F.when(df.event == 'start', 1).otherwise(-1)).over(
        Window.partitionBy("Vehicle_id", "function").orderBy("row_num")) + F.when(df.event == 'end', 1).otherwise(0))

    df = df.withColumn("next_time", F.lead("timestamp").over(
        Window.partitionBy("Vehicle_id", "function", "group").orderBy("row_num")))

    df = df.withColumn("time_taken", df["next_time"] - df["timestamp"])

    df = df.filter(df["event"] == "start")

    df = df.withColumn("start_time", df.timestamp.cast(dataType=T.TimestampType()))

    df = df.select(["Vehicle_id", "function", "start_time", "time_taken"])

    df.write.mode("overwrite").parquet(output_loc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        help='HDFS input', default='/logs')
    parser.add_argument('--output', type=str,
                        help='HDFS output', default='/output')
    args = parser.parse_args()
    spark = SparkSession.builder.appName(
        'Self Driving Log Analytics Transformation').getOrCreate()
    transform_log_data(input_loc=args.input, output_loc=args.output)