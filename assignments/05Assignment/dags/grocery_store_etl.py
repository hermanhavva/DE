from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import polars as pl
import duckdb
import pymysql  # MySQL connector
import logging
from dotenv import load_dotenv
from collections import namedtuple

DB_HOST = load_dotenv('DB_HOST')
DB_NAME = load_dotenv('DB_NAME')
DB_USER = load_dotenv('DB_USER')
DB_PASSWORD = load_dotenv('DB_PASSWORD')
DB_PORT = load_dotenv('DB_PORT')

DUCKDB_PATH = load_dotenv('DUCKDB_PATH')

MYSQL_CONFIG = {
    "host": DB_HOST,  # Connect to MySQL from inside Docker
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": DB_PORT
}

extracted_frames = namedtuple('extracted_frames',
['products_df',
            'customers_df',
            'sales_df',
            'category_df',
            'cities_df',
            'countries_df',
            'employees_df'])

with DAG(
    "grocery_store_etl",
    start_date=datetime(2024, 1, 1),
    description="a dag representing extracting the data from",
    tags=['grocery_store_dag'],
    schedule='@once',
    catchup=False):
    logger = logging.getLogger(__name__)


    @task
    def extract_data() -> extracted_frames:
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
                    salesid, salespersonid, customerid, productid, quantity, discount, salesdate, transactionnumber 
                FROM sales;
                """
                sales_df = pl.read_database(query, conn)

            # Reading CSV files
            category_df = pl.read_csv('../csv/categories.csv')
            cities_df = pl.read_csv('../csv/cities.csv')
            countries_df = pl.read_csv('../csv/countries.csv')
            employees_df = pl.read_csv('../csv/employees.csv')

            logger.info("Successfully extracted the data")

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e

        return extracted_frames(
            products_df=products_df,
            customers_df=customers_df,
            sales_df=sales_df,
            category_df=category_df,
            cities_df=cities_df,
            countries_df=countries_df,
            employees_df=employees_df
        )
    @task
    def transform_data(frames: extracted_frames):



    @task
    def save_data_to_duckDB():

    extract_data() >> transform_data() >> save_data_to_duckDB()



