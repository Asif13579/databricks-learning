
from src.transformations import (
    clean_customers,
    clean_sales,
    create_customer_gold
)


def test_clean_customers(spark):
    data = [
        (1, " Rahul ", "Delhi"),
        (2, "Amit", "Mumbai"),
        (1, "Rahul", "Delhi"),
        (None, "Invalid", "Pune")
    ]

    df = spark.createDataFrame(
        data,
        ["customer_id", "customer_name", "city"]
    )

    result = clean_customers(df)

    assert result.count() == 2

    rahul = result.filter(
        "customer_name = 'Rahul'"
    ).first()

    assert rahul["customer_name"] == "Rahul"


def test_clean_sales(spark):
    data = [
        (101, 1, "Laptop", 2, 50000.0, "2026-01-01"),
        (102, 2, "Mobile", 3, 20000.0, "2026-01-02"),
        (103, 3, "Tablet", -1, 30000.0, "2026-01-03")
    ]

    df = spark.createDataFrame(
        data,
        [
            "order_id", "customer_id", "product",
            "quantity", "price", "order_date"
        ]
    )

    result = clean_sales(df)

    assert result.count() == 2

    laptop = result.filter("order_id = 101").first()

    assert laptop["revenue"] == 100000.0


def test_customer_gold(spark):
    customers = spark.createDataFrame(
        [
            (1, "Rahul", "Delhi"),
            (2, "Amit", "Mumbai")
        ],
        ["customer_id", "customer_name", "city"]
    )

    sales = spark.createDataFrame(
        [
            (101, 1, 100000.0),
            (102, 1, 40000.0),
            (103, 2, 60000.0)
        ],
        ["order_id", "customer_id", "revenue"]
    )

    result = create_customer_gold(customers, sales)

    rahul = result.filter(
        "customer_id = 1"
    ).first()

    assert rahul["total_orders"] == 2
    assert rahul["total_revenue"] == 140000.0
