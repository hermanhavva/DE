from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import polars as pl
import os
import duckdb
import pymysql  # MySQL connector
import logging
from dotenv import load_dotenv
from collections import namedtuple
from dataclasses import dataclass
from typing import Optional

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DUCKDB_PATH = os.getenv("DUCKDB_PATH")

MYSQL_CONFIG = {
    "host": DB_HOST,  # Connect to MySQL from inside Docker
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": DB_PORT,
}


@dataclass
class ExtractedFrames:
    def __init__(
        self,
        products: pl.dataframe,
        customers: pl.dataframe,
        sales: pl.dataframe,
        category: pl.dataframe,
        cities: pl.dataframe,
        countries: pl.dataframe,
        employees: pl.dataframe,
    ):

        self.products_df = products
        self.customers_df = customers
        self.sales_df = sales
        self.category_df = category
        self.cities_df = cities
        self.countries_df = countries
        self.employees_df = employees
        self.total_rows = (
            products.count()
            + customers.count()
            + sales.count()
            + category.count()
            + cities.count()
            + countries.count()
            + employees.count()
        )

    products_df: pl.DataFrame
    customers_df: pl.DataFrame
    sales_df: pl.DataFrame
    category_df: pl.DataFrame
    cities_df: pl.DataFrame
    countries_df: pl.DataFrame
    employees_df: pl.DataFrame
    total_rows: int


@dataclass
class ProcessedFrames:
    product_per_generated_income_df: pl.DataFrame


extracted_frames = Optional[None, ExtractedFrames]
processed_frame = Optional[None, pl.dataframe]

with DAG(
    "grocery_store_etl",
    start_date=datetime(2024, 1, 1),
    description="a dag representing extracting the data from",
    tags=["grocery_store_dag"],
    schedule="@once",
    catchup=False,
):

    logger = logging.getLogger(__name__)

    @task
    def extract_data() -> ExtractedFrames:
        global extracted_frames

        try:
            logger.info("Connecting to MySQL database...")

            # Use a context manager to automatically close the connection
            with pymysql.connect(**MYSQL_CONFIG) as conn:

                # First query: products
                query = """
                SELECT 
                    productid, productname, price, categoryid, class, isallergic 
                FROM products;
                """
                products_df = pl.read_database(query, conn)

                # Second query: customers
                query = """
                SELECT 
                    customerid, firstname, lastname, cityid, address 
                FROM customers;
                """
                customers_df = pl.read_database(query, conn)

                # Third query: sales
                query = """
                SELECT 
                    salesid, salespersonid, customerid, productid, quantity, discount 
                FROM sales;
                """
                sales_df = pl.read_database(query, conn)

            # Reading CSV files
            category_df = pl.read_csv("../csv/categories.csv")
            cities_df = pl.read_csv("../csv/cities.csv")
            countries_df = pl.read_csv("../csv/countries.csv")
            employees_df = pl.read_csv("../csv/employees.csv")

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e

        logger.info("Successfully extracted the data")

        extracted_frames(
            products_df=products_df,
            customers_df=customers_df,
            sales_df=sales_df,
            category_df=category_df,
            cities_df=cities_df,
            countries_df=countries_df,
            employees_df=employees_df,
        )
        return extracted_frames.total_rows

    @task
    def transform_data(rows_extracted: int):
        # for each entry in sales join it with a product_df on productid to have a price
        # then add a new column for each entry in the table called sum (quantity*price*discount)
        # group sales for each different productid, find the total income per category
        global processed_frame
        global extracted_frames

        try:
            logger.info(f"On the previous step {rows_extracted} were extracted")

            product_df_transformed = extracted_frames.products_df.select(
                ["ProductID", "CategoryID", "Price"]
            )

            sales_to_product_price_df = extracted_frames.sales_df.join(
                product_df_transformed, on="ProductID", how="inner"
            )

            sales_to_total_income_df = sales_to_product_price_df.with_columns(
                (
                    sales_to_product_price_df["Quantity"]
                    * sales_to_product_price_df["Price"]
                    * (1 - sales_to_product_price_df["Discount"])
                ).alias("TotalPrice")
            )

            sales_income_to_category_df = sales_to_total_income_df.join(
                extracted_frames.category_df, on="CategoryID", how="inner"
            )

            processed_frame = (
                sales_income_to_category_df.select(
                    ["CategoryName", "CategoryID", "TotalPrice"]
                )
                .group_by(["CategoryID", "CategoryName"])
                .agg(pl.sum("TotalPrice"))
            )

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e
        logger.info("Successfully transformed the data")

        return processed_frame.count()

    @task
    def save_data_to_duckDB(rows_to_save: int):
        global DUCKDB_PATH

        logger.info(f"Requested to save {rows_to_save} rows")

        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute(
            """
                CREATE TABLE IF NOT EXISTS category_to_income (
                    CategoryID INTEGER,
                    CategoryName STRING,
                    TotalPrice DOUBLE
                );
            """
        )

        conn.execute("INSERT INTO products SELECT * FROM processed_frame")
        conn.close()

    extract_data() >> transform_data() >> save_data_to_duckDB()
