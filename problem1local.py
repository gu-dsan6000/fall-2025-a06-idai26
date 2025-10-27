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
    regexp_extract
)
import pandas as pd

def create_spark_session(app_name="LogParserApp", master="local[*]"):
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master)
             .getOrCreate())
    return spark

def main():

    # Initialize Spark session
    print("Creating Spark session...")
    spark = create_spark_session()
    print("Spark session created.")

    # Read log files
    file_path = "./data/sample/application_*/*.log"
    print(f"Reading log files from {file_path}...")
    logs_df = spark.read.text(file_path)
    print("Log files read into DataFrame.")

    # Parse log entries
    print("Parsing log entries...")
    logs_parsed = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
    col('value').alias('message'))

    # filter out rows with empty log levels
    logs_parsed_filtered = logs_parsed.filter(col('log_level') != '')

    print(f"Total rows parsed: {logs_parsed.count()}")
    logs_parsed.show(5)  # Show first 5 rows
    logs_parsed.printSchema()  # Verify column types


    # Summary 1:  Log level counts
    log_level_counts = (logs_parsed_filtered
        .groupBy('log_level')
        .agg(count('*').alias('count'))
        .orderBy(desc('count'))
        )
    
    # Display and save log level counts
    print("Log Level Counts:")
    log_level_counts.show()
    log_level_counts.toPandas().to_csv('./data/output/problem1_log_level_counts.csv', index=False)

    # Summary 2: Sample of logs
    log_samples = (logs_parsed_filtered
                   .select('message','log_level')
                   .orderBy(expr('rand()'))
                   .limit(10)
                   )

    # Display and save log samples
    print("Log Samples:")
    log_samples.show()
    log_samples.toPandas().to_csv('./data/output/problem1_samples.csv', index=False)


    # Summary 3: Summary Statistics

    # total log entries processed
    total_logs = logs_parsed.count()
    print(f"Total log entries processed: {total_logs}")

    # total lines with log levels
    total_with_levels = logs_parsed_filtered.count()
    print(f"Total log entries with log levels: {total_with_levels}")

    # create summary table of log levels and distribution
    log_level_summary = (logs_parsed_filtered
        .groupBy('log_level')
        .agg(
            count('*').alias('count'),
            (count('*') / total_with_levels * 100).alias('percentage')
        )
        .orderBy(desc('count'))
    )
    print("Log Level Summary:")
    log_level_summary.show()

    # save log level summary to txt
    with open('./data/output/problem1_summary.txt', 'w') as f:
        f.write(f"Total log entries processed: {total_logs}\n")
        f.write(f"Total log entries with log levels: {total_with_levels}\n\n")
        f.write("Log Level Summary:\n")
        log_level_summary_pd = log_level_summary.toPandas()
        f.write(log_level_summary_pd.to_string(index=False))

    # stop spark session
    print("Stopping Spark session...")
    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    main()