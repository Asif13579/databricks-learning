import sys
from pathlib import Path 

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

import argparse
from src.utilities import get_spark,read_table,write_delta
from src.transformations import create_customer_gold

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="spark_catalog")
    parser.add_argument("--schema", default="default")
    args = parser.parse_args()

    spark = get_spark()

    try:
        prefix = f"{args.catalog}.{args.schema}"

        print("Reading Silver tables...")

        customers = read_table(spark, f"{prefix}.silver_customers")
        sales = read_table(spark, f"{prefix}.silver_sales")

        print("Customers count:", customers.count())
        print("Sales count:", sales.count())

        gold = create_customer_gold(customers, sales)

        print("Gold schema:")
        gold.printSchema()

        print("Gold count:", gold.count())

        table_name = f"{prefix}.gold_customer_sales"

        print(f"Writing Gold table: {table_name}")

        write_delta(gold, table_name)

        print("Gold aggregation completed!")

        spark.sql(f"SHOW TABLES IN {prefix}").show(truncate=False)

    finally:
        spark.stop()

# def main():
#     parser=argparse.ArgumentParser()
#     parser.add_argument("--catalog",default="spark_catalog")
#     parser.add_argument("--schema",default="default")
#     args=parser.parse_args()

#     spark=get_spark()

#     try:
#         prefix=f"{args.catalog}.{args.schema}"
#         customers=read_table(spark,f"{prefix}.silver_customers")
#         sales=read_table(spark,f"{prefix}.silver_sales")
#         gold=create_customer_gold(customers,sales)

#         write_delta(gold,f"{prefix}.gold_customer_sales")
#         print("Gold aggregation completed!")
#     finally:
#         spark.stop()


if __file__=="__main__":
    main()