from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import polars as pl
import os
import duckdb

import logging
from dotenv import load_dotenv
from dataclasses import dataclass


logger = logging.getLogger(__name__)

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

mysql_uri = f"mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

with DAG(
    "dating_app_elt",
    start_date=datetime(2024, 1, 1),
    description="a dag representing extracting the data from mysql, loading to duckdb, building models with dbt",
    tags=["dating_app_elt"],
    schedule="@once",
    catchup=False,
):

    @task
    def extract_load_data_to_duckdb() -> int:
        total_extracted_rows = 0
        activities_df = pl.dataframe
        sessions_df = pl.dataframe
        payments_df = pl.dataframe
        registrations_df = pl.dataframe

        try:
            logger.info("Connecting to MySQL database via URI...")

            # Query 1: activities
            query = """
            select
                user_id,
                contact_id,
                activity_type,
                dt as act_dt
            from activities a
            where
                user_id is not null and
                contact_id is not null and
                activity_type is not null and
                dt is not null;
            """
            activities_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += activities_df.height

            # Query 2: sessions
            query = """
            select
                user_id,
                session_start_dt,
                duration,
                session_number
            from sessions
            where
                user_id is not null and
                session_start_dt is not null and
                duration is not null and
                session_number is not null;
            """
            sessions_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += sessions_df.height

            # Query 3: payments
            query = """
            select
                user_id,
                pay_dt,
                amount_usd,
                feature_type_id
            from payments
            where
                user_id is not null and
                pay_dt is not null and
                amount_usd is not null and
                feature_type_id is not null;
            """
            payments_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += payments_df.height

            # Query 4: registrations
            query = """
            SELECT
                user_id,
                reg_dt,
                gender,
                age,
                app,
                country_code
            FROM registrations
            WHERE
                user_id IS NOT NULL AND
                reg_dt IS NOT NULL AND
                gender IS NOT NULL AND
                age IS NOT NULL AND
                app IS NOT NULL AND
                country_code IS NOT NULL AND
                app != "unknown" AND
                age > 0
            """
            registrations_df = pl.read_database_uri(query=query, uri=mysql_uri)
            total_extracted_rows += registrations_df.height

        except Exception as e:
            logger.error(f"An error occurred during extraction: {e}")
            raise e

        logger.info("Successfully extracted the data")

        # Write data to duckdb

        try:
            con = duckdb.connect(DUCKDB_PATH)

            # Register Polars DataFrames
            con.register("activities_df", activities_df)
            con.register("sessions_df", sessions_df)
            con.register("payments_df", payments_df)
            con.register("registrations_df", registrations_df)

            # Create schema if not exists
            con.execute("CREATE SCHEMA IF NOT EXISTS company_x")

            # Drop and recreate each table
            con.execute("DROP TABLE IF EXISTS company_x.activities")
            con.execute("CREATE TABLE company_x.activities AS SELECT * FROM activities_df")

            con.execute("DROP TABLE IF EXISTS company_x.sessions")
            con.execute("CREATE TABLE company_x.sessions AS SELECT * FROM sessions_df")

            con.execute("DROP TABLE IF EXISTS company_x.payments")
            con.execute("CREATE TABLE company_x.payments AS SELECT * FROM payments_df")

            con.execute("DROP TABLE IF EXISTS company_x.registrations")
            con.execute("CREATE TABLE company_x.registrations AS SELECT * FROM registrations_df")

        except Exception as e:
            logger.error(f"An error occurred during extraction: {e}")
            raise e


        logger.info("Successfully extracted and loaded data")

        return total_extracted_rows

    @task.bash
    def invoke_dbt():
        return "cd /usr/local/airflow/dbt_project && dbt deps && dbt build 2>&1 | tee /proc/1/fd/1"

    extract_load_data_to_duckdb() >> invoke_dbt()