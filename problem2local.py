#!/usr/bin/env python3

import os
import subprocess
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, hour, dayofmonth,
    avg, count, max as spark_max, min as spark_min,
    expr, ceil, percentile_approx,
    desc, from_unixtime, col,
    regexp_extract, input_file_name,
    try_to_timestamp, lit
)
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def create_spark_session(app_name="LogParserAppProblem2", master="local[*]"):
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master)
             .getOrCreate())
    return spark

def question1():

    # Initialize Spark session
    print("Creating Spark session...")
    spark = create_spark_session()
    print("Spark session created.")

    # Read log files
    file_path = "./data/sample/application_*/*.log"
    print(f"Reading log files from {file_path}...")
    logs_df = spark.read.text(file_path)

    # Add a column with the file path
    logs_df = logs_df.withColumn('file_path', input_file_name())

    # Extract application_id from the path
    # Pattern: application_<cluster_id>_<app_number>
    logs_df = logs_df.withColumn('application_id',
        regexp_extract('file_path', r'(application_\d+_\d+)', 1))
    
    # parse timestamp from log entry
    logs_df = logs_df.withColumn('timestamp_str',
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1))
    logs_df = logs_df.withColumn('timestamp', try_to_timestamp('timestamp_str', lit('yy/MM/dd HH:mm:ss')))

    # filter out rows with empty timestamps
    logs_with_timestamps = logs_df.filter(col('timestamp').isNotNull())

    # find min and max timestamps per application
    timeline_summary = (logs_with_timestamps.groupBy('application_id')
                        .agg(
                            spark_min('timestamp').alias('start_time'),
                            spark_max('timestamp').alias('end_time')
                        ))
    
    # extract cluster_id from application_id
    timeline_df = timeline_summary.withColumn('cluster_id',
        regexp_extract('application_id', r'(\d+)_\d+', 1))

    # extract app_number from application_id
    timeline_df = timeline_df.withColumn('app_number',
        regexp_extract('application_id', r'\d+_(\d+)', 1))

    # reorder columns
    timeline_df = timeline_df.select('cluster_id', 'application_id', 'app_number', 'start_time', 'end_time')

    # save to CSV
    timeline_df.toPandas().to_csv('./data/output/problem2_timeline.csv', index=False)

