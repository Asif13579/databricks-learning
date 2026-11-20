def test_spark_connection(spark):
    assert spark.version.startswith("3.5.")

    data = [(1, "Rahul"), (2, "Amit")]

    df = spark.createDataFrame(data, ["id", "name"])

    assert df.count() == 2