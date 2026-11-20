import sys  
from pathlib import Path 
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import argparse
from src.utilities import get_spark,read_csv,write_delta

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-path",required=True)
    parser.add_argument("--catalog",default="spark_catalog")
    parser.add_argument("--schema",default="default")

    args=parser.parse_args()
    spark=get_spark()

    try:
        prefix=f"{args.catalog}.{args.schema}"
        customers=read_csv(spark,f"{args.source_path}/customers.csv")
        sales=read_csv(spark,f"{args.source_path}/sales.csv")
        write_delta(customers,f"{prefix}.bronze_customers")
        write_delta(sales,f"{prefix}.bronze_sales")
        print("Bronze ingestion completed!")
    finally:
        spark.stop()


if __name__=="__main__":
    main()

