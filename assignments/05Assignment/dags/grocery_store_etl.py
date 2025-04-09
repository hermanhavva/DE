from airflow import DAG
from airflow.operators.python import task
from datetime import datetime
import polars as pl
import os
import duckdb

import logging
from dotenv import load_dotenv
from dataclasses import dataclass


load_dotenv()
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH")

MYSQL_CONFIG = {
    "host": DB_HOST,  # Connect to MySQL from inside Docker
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": DB_PORT,
}

mysql_uri = f"mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# @dataclass
# class ExtractedFrames:
#     def __init__(
#         self,
#         products: pl.dataframe,
#         customers: pl.dataframe,
#         sales: pl.dataframe,
#         category: pl.dataframe,
#         cities: pl.dataframe,
#         countries: pl.dataframe,
#         employees: pl.dataframe,
#     ):
#
#         self.products_df = products
#         self.customers_df = customers
#         self.sales_df = sales
#         self.category_df = category
#         self.cities_df = cities
#         self.countries_df = countries
#         self.employees_df = employees
#         self.total_rows = (
#             products.height
#             + customers.height
#             + sales.height
#             + category.height
#             + cities.height
#             + countries.height
#             + employees.height
#         )
#
#     products_df: pl.DataFrame
#     customers_df: pl.DataFrame
#     sales_df: pl.DataFrame
#     category_df: pl.DataFrame
#     cities_df: pl.DataFrame
#     countries_df: pl.DataFrame
#     employees_df: pl.DataFrame
#     total_rows: int


@dataclass
class Path:
    csv_data_path = "/usr/local/airflow/csv/"
    csv_data_extracted_path = "/usr/local/airflow/csv_extracted/"
    product_extracted_path = csv_data_extracted_path + "products.csv"
    customers_extracted_path = csv_data_extracted_path + "customers.csv"
    sales_extracted_path = csv_data_extracted_path + "sales.csv"

    categories_path = csv_data_path + "categories.csv"
    processed_path = csv_data_path + "processed.csv"


with DAG(
    "grocery_store_etl",
    start_date=datetime(2024, 1, 1),
    description="a dag representing extracting the data from",
    tags=["grocery_store_dag"],
    schedule="@once",
    catchup=False,
):

    @task
    def extract_data() -> int:
        path = Path()
        total_extracted_rows = 0

        try:
            logger.info("Connecting to MySQL database via URI...")

            # Query 1: products
            query = """
            SELECT 
                productid, productname, price, categoryid, class, isallergic 
            FROM products;
            """
            products_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += products_df.height

            # Query 2: customers
            query = """
            SELECT 
                customerid, firstname, lastname, cityid, address 
            FROM customers;
            """
            customers_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += customers_df.height

            # Query 3: sales
            query = """
            SELECT 
                salesid, salespersonid, customerid, productid, quantity, discount 
            FROM sales;
            """
            sales_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += sales_df.height

        except Exception as e:
            logger.error(f"An error occurred during extraction: {e}")
            raise e

        logger.info("Successfully extracted the data")

        products_df.write_csv(path.product_extracted_path)
        customers_df.write_csv(path.customers_extracted_path)
        sales_df.write_csv(path.sales_extracted_path)

        logger.info("Successfully saved data from MySQL")

        return total_extracted_rows

    @task
    def transform_data(total_extracted_rows: int):
        # for each entry in sales join it with a product_df on productid to have a price
        # then add a new column for each entry in the table called sum (quantity*price*discount)
        # group sales for each different productid, find the total income per category
        path = Path()

        logger.info(f"On the previous step extracted {total_extracted_rows}")

        try:

            product_df_transformed = pl.read_csv(path.product_extracted_path).select(
                ["productid", "categoryid", "price"]
            )

            sales_to_product_price_df = pl.read_csv(path.sales_extracted_path).join(
                product_df_transformed, on="productid", how="inner"
            )

            sales_to_total_income_df = sales_to_product_price_df.with_columns(
                (
                    sales_to_product_price_df["quantity"]
                    * sales_to_product_price_df["price"]
                    * (1 - sales_to_product_price_df["discount"])
                ).alias("totalprice")
            )
            categories_df = pl.read_csv(path.categories_path)

            sales_income_to_category_df = sales_to_total_income_df.join(
                categories_df, on="categoryid", how="inner"
            )

            processed_frame = (
                sales_income_to_category_df.select(
                    ["categoryname", "categoryid", "totalprice"]
                )
                .group_by(["categoryid", "categoryname"])
                .agg(pl.sum("totalprice"))
            )
            processed_frame.write_csv(path.processed_path)

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e
        logger.info("Successfully transformed the data")

    @task
    def save_data_to_duckDB():
        path = Path()

        processed_frame = pl.read_csv(path.processed_path)

        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute(
            """
                CREATE TABLE IF NOT EXISTS category_to_income (
                    categoryid INTEGER,
                    categoryname STRING,
                    totalprice DOUBLE
                );
            """
        )

        conn.execute("INSERT INTO category_to_income SELECT * FROM processed_frame")
        conn.close()

    transform_data(extract_data()) >> save_data_to_duckDB()
