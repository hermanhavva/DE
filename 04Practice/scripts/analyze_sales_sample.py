from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("../datasets/sales/sales.csv", header=True, inferSchema=True)

# Show the first few rows
df.show()

# Filter sales where a discount was applied
discounted_sales = df.filter(col("Discount") > 0)
discounted_sales.show()

# Aggregate total sales per SalesPersonID
sales_per_person = df.groupBy("SalesPersonID").agg(sum("TotalPrice").alias("TotalSales"))
sales_per_person.show()

# Find the top-selling product based on quantity sold
top_selling_product = df.groupBy("ProductID").agg(sum("Quantity").alias("TotalQuantity")) \
    .orderBy(desc("TotalQuantity"))
top_selling_product.show()

# Stop the Spark session
spark.stop()
