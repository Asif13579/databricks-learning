from src.utilities import get_spark

spark = get_spark()

print("Spark version:", spark.version)

spark.range(5).show()

spark.stop()