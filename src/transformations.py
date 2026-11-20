from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_customers(customers:DataFrame) -> DataFrame:
    return (
        customers.filter(F.col("customer_id").isNotNull()).withColumn("customer_id",F.col('customer_id').cast("int")).withColumn("customer_name",F.trim(F.col("customer_name"))).withColumn("city",F.trim(F.col("city"))).drop_duplicates(['customer_id'])
    )

def clean_sales(sales:DataFrame) -> DataFrame:
    cleaned=sales.withColumn("order_id",F.col("order_id").cast('int')).withColumn("customer_id",F.col('customer_id').cast("int")).withColumn("quantity", F.col("quantity").cast("int")).withColumn("price", F.col("price").cast("double")).withColumn("order_date", F.to_date("order_date")).filter(F.col("order_id").isNotNull()).filter(F.col("customer_id").isNotNull()).filter(F.col("quantity") > 0).filter(F.col("price") >= 0).filter(F.col("order_date").isNotNull()).dropDuplicates(["order_id"])
    return cleaned.withColumn('revenue',F.col("quantity")*F.col("price"))


# def create_customer_gold(customers:DataFrame,sales:DataFrame) -> DataFrame:
#     return sales.groupBy("customer_id").agg(F.countDistinct('order_id').alias("total_orders"),F.sum('revenue').alias("total_revenue")).join(customers,'customer_id','inner').select('customer_id','customer_name','city','total_orders','total_revenue')


def create_customer_gold(
    customers: DataFrame,
    sales: DataFrame
) -> DataFrame:

    return (
        sales
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("revenue").alias("total_revenue")
        )
        .join(customers, "customer_id", "inner")
        .select(
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue"
        )
    )