from __future__ import print_function

import sys
from random import random
from operator import add
import pyspark.sql.functions as f

from pyspark.sql import SparkSession, DataFrame, GroupedData


def calculate_pi():
    """
        Usage: pi [partitions]
    """
    with create_spark_session("Calc Pi") as spark:
        partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
        n = 100000 * partitions

        def f(_):
            x = random() * 2 - 1
            y = random() * 2 - 1
            return 1 if x ** 2 + y ** 2 <= 1 else 0

        count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
        print("Pi is roughly %f" % (4.0 * count / n))


def create_spark_session(app_name: str):
    return SparkSession \
        .builder \
        .master("local[*]") \
        .appName(app_name) \
        .getOrCreate()


def identify_frauded_ips():
    spark: SparkSession = create_spark_session("Identify Fraud IP's")
    df: DataFrame = spark.read.json("/Users/esoboliev/Downloads/test_data.json").cache()
    # df: DataFrame = spark.read.json("s3n:///aws-capstone-esoboliev/2020/04/28/15/AwsViewsEsoboliev-1-2020-04-28-15-31-26-5e7b2c25-0eda-449c-baf8-d340ec15c44b")
    df.printSchema()
    df.count()

    new_df: GroupedData = df.select("*").where(f.countDistinct("timestamp") > 10).groupBy('user_ip')
    new_df.count()


if __name__ == "__main__":
    identify_frauded_ips()
