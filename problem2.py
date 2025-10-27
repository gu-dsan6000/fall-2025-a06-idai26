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
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Problem 2: Log Analysis and Visualization')
    parser.add_argument('master_url', help='Spark master URL')
    parser.add_argument('--net-id', required=True, help='Your net ID')
    return parser.parse_args()

def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem2_Log_Analysis_and_Visualization")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.780")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    print("Spark session created successfully for cluster execution")
    return spark

def summary_statistics(spark, net_id):
    """ Generate summary statistics for applications and clusters from log files.
    Outputs:
    1. CSV file with start and end times for each application
    2. CSV file with aggregated cluster statistics
    3. Text file with overall summary statistics
    """

    # Read log files
    file_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
    print(f"Reading log files from {file_path}...")
    logs_df = spark.read.text(file_path)
    print("Log files read into DataFrame.")

    # Add a column with the file path
    logs_df = logs_df.withColumn('file_path', input_file_name())

    # Extract application_id from the path
    # Pattern: application_<cluster_id>_<app_number>
    logs_df = logs_df.withColumn('application_id',
        regexp_extract('file_path', r'(application_\d+_\d+)', 1))
    

    ## Part 2.1 -- Time-series data for each application
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

    print("Application timeline data:")
    timeline_df.show(5)

    # save to CSV
    timeline_df.toPandas().to_csv('problem2_timeline.csv', index=False)


    ## Part 2.2 -- Aggregated cluster statistics

    cluster_summary = timeline_df.groupBy('cluster_id').agg(
        count('*').alias('num_applications'),
        spark_min('start_time').alias('cluster_first_app'),
        spark_max('end_time').alias('cluster_last_app')
        ).orderBy(desc('num_applications'))

    print("Cluster summary data:")
    cluster_summary.show(5)

    # save to CSV
    cluster_summary.toPandas().to_csv('problem2_cluster_summary.csv', index=False)

    ## Part 2.3 -- Overall Summary Statistics  
    total_unique_clusters = cluster_summary.count()
    print(f"Total unique clusters: {total_unique_clusters}")

    total_applications = timeline_df.count()
    print(f"Total applications processed: {total_applications}")

    avg_apps_per_cluster = (total_applications / total_unique_clusters) if total_unique_clusters > 0 else 0
    print(f"Average applications per cluster: {avg_apps_per_cluster:.2f}")

    most_used_clusters = (cluster_summary
        .orderBy(desc('num_applications'))
        .limit(5)
        )
    print("Most used clusters (top 5):")
    most_used_clusters.show()

    # save to text file
    with open('problem2_stats.txt', 'w') as f:
        f.write(f"Total unique clusters: {total_unique_clusters}\n")
        f.write(f"Total applications processed: {total_applications}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n\n")
        f.write("Most used clusters (top 5):\n")
        most_used_clusters_pd = most_used_clusters.toPandas()
        f.write(most_used_clusters_pd.to_string(index=False))

    # output summaries for visualization
    # applications per cluster
    apps_per_cluster_pd = cluster_summary.select('cluster_id', 'num_applications').toPandas()

    # job duration distribution for largest cluster
    largest_cluster_id = (cluster_summary
        .orderBy(desc('num_applications'))
        .select('cluster_id')
        .first()['cluster_id'])
    
    largest_cluster_apps = timeline_df.filter(col('cluster_id') == largest_cluster_id)
    largest_cluster_apps = largest_cluster_apps.withColumn('job_duration_seconds',
        (col('end_time').cast('long') - col('start_time').cast('long'))).toPandas()
    return apps_per_cluster_pd, largest_cluster_apps

def visualize_data(apps_per_cluster_pd, largest_cluster_apps):
    """ Generate visualizations for the cluster summary data.

    Outputs:
    1. Bar chart showing the number of applications per cluster
    2. Density plot showing job duration distribution for the largest cluster
    """

    # Visualization 1: Bar chart of applications per cluster
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=apps_per_cluster_pd, x='cluster_id', y='num_applications', 
        hue='cluster_id', palette='viridis', legend=False)
    plt.title('Number of Applications per Cluster')
    plt.xlabel('Cluster ID')
    plt.ylabel('Number of Applications')
    for container in ax.containers:
        ax.bar_label(container)
    plt.savefig('problem2_bar_chart.png')
    plt.close()

    # Visualization 2: Density plot of job durations for largest cluster
    plt.figure(figsize=(10, 6))
    sns.histplot(largest_cluster_apps['job_duration_seconds'], bins=30, kde=True, stat='density', color='skyblue')
    plt.xscale('log')
    plt.title(f'Job Duration Distribution for Largest Cluster (n={len(largest_cluster_apps)})')
    plt.xlabel('Job Duration (seconds, log scale)')
    plt.ylabel('Density')
    plt.savefig('problem2_density_plot.png')
    plt.close()


def main():
    # Parse command-line arguments
    args = parse_args()

    # Initialize Spark session
    spark = create_spark_session(args.master_url)

    # Generate summary statistics
    apps_per_cluster_pd, largest_cluster_apps = summary_statistics(spark, args.net_id)

    # Generate visualizations
    visualize_data(apps_per_cluster_pd, largest_cluster_apps)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main() 