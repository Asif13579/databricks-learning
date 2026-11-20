
from src.utilities import get_spark


def main():
    spark = get_spark()

    try:
        df = spark.table(
            "spark_catalog.default.gold_customer_sales"
        )

        df.orderBy(
            "total_revenue",
            ascending=False
        ).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
