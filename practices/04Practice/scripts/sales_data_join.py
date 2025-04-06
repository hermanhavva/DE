from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Data Join").getOrCreate()

# Load CSV files
sales_df = spark.read.csv("../datasets/sales/sales.csv", header=True, inferSchema=True)
products_df = spark.read.csv("../datasets/sales/products.csv", header=True, inferSchema=True)
employees_df = spark.read.csv("../datasets/sales/employees.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("../datasets/sales/customers.csv", header=True, inferSchema=True)
countries_df = spark.read.csv("../datasets/sales/countries.csv", header=True, inferSchema=True)
cities_df = spark.read.csv("../datasets/sales/cities.csv", header=True, inferSchema=True)
categories_df = spark.read.csv("../datasets/sales/categories.csv", header=True, inferSchema=True)

# Perform Joins
joined_df = sales_df.join(products_df, ["ProductId"]) \
    .join(employees_df, sales_df["SalesPersonID"] == employees_df["EmployeeID"]) \
    .join(customers_df, sales_df["CustomerId"] == customers_df["CustomerId"]) \
    .join(cities_df, customers_df["CityId"] == cities_df["CityId"]) \
    .join(countries_df, cities_df["CountryId"] == countries_df["CountryId"]) \
    .join(categories_df, products_df["CategoryId"] == categories_df["CategoryId"]) \
    .select("ProductID", "SalesID", "SalesPersonID", sales_df["CustomerID"], "Quantity", "Discount", "TotalPrice",
            "SalesDate", "ProductName", "Price", products_df["CategoryID"], "Class", "ModifyDate", "Resistant",
            "IsAllergic", "VitalityDays", col("CityName").alias("CustomerCityName"), cities_df["CountryID"],
            col("CountryName").alias("CustomerCountryName")
            )

# Perform aggregations

# 1 approach
sales_by_country_df = (joined_df.groupBy("CustomerCityName").count().alias("TotalSalesNum").
                       orderBy("count", ascending=False))

# Show the result
sales_by_country_df.show(10, truncate=False)

# 2 approach
# Create Temporary table in PySpark
joined_df.createOrReplaceTempView("sales_by_country_df_prep_temp")

sql_str = "select CustomerCityName, count(*) as TotalSalesNum from  sales_by_country_df_prep_temp" + \
          " group by CustomerCityName order by TotalSalesNum desc"
spark.sql(sql_str).show(10)

# Stop Spark session
spark.stop()
