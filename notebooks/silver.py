import sys 
from pathlib import Path 

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

import argparse
from src.utilities import get_spark,read_table,write_delta
from src.transformations import clean_customers, clean_sales


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--catalog",default="spark_catalog")
    parser.add_argument("--schema",default="default")
    args=parser.parse_args()
    spark=get_spark()

    try:
        prefix=f"{args.catalog}.{args.schema}"
        customers=read_table(spark,f"{prefix}.bronze_customers")
        sales=read_table(spark,f"{prefix}.bronze_sales")
        print("Silver transformation completed!")
    finally:
        spark.stop()


if __name__=="__main__":
    main()

