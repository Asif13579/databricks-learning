
from src.utilities import get_spark

spark = get_spark()

try:
    data = [
        (1, "Amit", 5000),
        (2, "Rahul", 7000),
        (3, "Neha", 9000)
    ]

    columns = ["customer_id", "customer_name", "sales"]

    df = spark.createDataFrame(data, columns)

    # Write data as a Delta table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("gold_customer_sales")
    )

    print("Gold table created successfully")

    # Verify table
    result = spark.table("gold_customer_sales")
    result.show()

finally:
    spark.stop()
