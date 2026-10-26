"""
Spark Session Utility
Initializes a SparkSession with Delta Lake support and optimized local settings.
Designed for both local development and Azure Databricks clusters.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "Healthcare-ETL-Pipeline", is_databricks: bool = False) -> SparkSession:
    """
    Returns an active SparkSession with Delta Lake extensions enabled.
    In Databricks, uses the pre-configured session.
    In local environments, configures local parallelism, memory, and warehouse directory.
    """
    if is_databricks:
        # Azure Databricks natively configures Delta Lake catalog and extensions
        return SparkSession.builder.getOrCreate()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.warehouse.dir", os.path.abspath("data/warehouse"))
        .config("spark.driver.memory", "2g")
    )

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        # Fallback if local Delta JARs are not yet cached
        print(f"[WARN] Delta Lake configuration fallback triggered: {e}")
        fallback_builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "4")
        )
        spark = fallback_builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
