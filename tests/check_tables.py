from src.utilities import get_spark

spark = get_spark()

try:
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS
        spark_catalog.default
    """)

    print("Databases:")
    spark.sql("SHOW DATABASES").show(
        truncate=False
    )

    print("Bronze tables:")
    spark.sql("""
        SHOW TABLES IN spark_catalog.default
    """).show(truncate=False)

finally:
    spark.stop()