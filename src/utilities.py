from pathlib import Path
import os

from pyspark.sql import SparkSession, DataFrame

def get_spark() -> SparkSession:

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    project_root = Path(__file__).resolve().parents[1]

    warehouse_dir = project_root / "spark-warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("databricks-learning-local")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.sql.warehouse.dir",
            warehouse_dir.as_uri()
        )
        .enableHiveSupport()
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()

# def get_spark() -> SparkSession:

#     if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
#         return SparkSession.builder.getOrCreate()

#     from delta import configure_spark_with_delta_pip

#     builder = (
#         SparkSession.builder
#         .master("local[2]")
#         .appName("databricks-learning-local")
#         .config(
#             "spark.sql.extensions",
#             "io.delta.sql.DeltaSparkSessionExtension"
#         )
#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog"
#         )
#         .config(
#             "spark.sql.warehouse.dir",
#             "file:///tmp/databricks-learning-warehouse"
#         )
#     )

#     return configure_spark_with_delta_pip(builder).getOrCreate()


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.table(table_name)


# def write_delta(
#     df: DataFrame,
#     table_name: str,
#     mode: str = "overwrite"
# ) -> None:

#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .option("path", f"data/{table_name}")
#         .saveAsTable(table_name)
#     )

# def write_delta(df, table_name):
#     path = f"data/{table_name}"

#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .save(path)
#     )

#     spark = df.sparkSession

#     (
#         spark.sql(f"DROP TABLE IF EXISTS {table_name}")
#     )

#     (
#         spark.sql(f"""
#             CREATE TABLE {table_name}
#             USING DELTA
#             LOCATION '{path}'
#         """)
#     )


# def write_delta(df: DataFrame, table_name: str) -> None:

#     parts = table_name.split(".")

#     if len(parts) != 3:
#         raise ValueError(
#             "Expected table name: catalog.schema.table"
#         )

#     quoted_table = ".".join(
#         f"`{part}`" for part in parts
#     )

#     path = (
#         Path(__file__).resolve().parents[1]
#         / "data"
#         / table_name
#     ).resolve()

#     path_uri = path.as_uri()

#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .save(path_uri)
#     )

#     spark = df.sparkSession

#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS {quoted_table}
#         USING DELTA
#         LOCATION '{path_uri}'
#     """)

#     print(f"Successfully saved table: {table_name}")


def write_delta(df: DataFrame, table_name: str) -> None:

    parts = table_name.split(".")

    if len(parts) != 3:
        raise ValueError(
            "Expected catalog.schema.table format"
        )

    catalog, schema, table = parts

    quoted_table = ".".join(
        f"`{part}`" for part in parts
    )

    spark = df.sparkSession

    path = (
        Path(__file__).resolve().parents[1]
        / "data"
        / table_name
    ).resolve()

    path.mkdir(parents=True, exist_ok=True)

    path_uri = path.as_uri()

    # Ensure database exists
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS `{catalog}`.`{schema}`"
    )

    # Write Delta data
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path_uri)
    )

    # Remove old catalog registration
    spark.sql(f"DROP TABLE IF EXISTS {quoted_table}")

    # Register Delta table
    spark.sql(f"""
        CREATE TABLE {quoted_table}
        USING DELTA
        LOCATION '{path_uri}'
    """)

    print(f"Successfully created: {table_name}")