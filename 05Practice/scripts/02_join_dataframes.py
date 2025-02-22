import time
from memory_profiler import profile
import pandas as pd


@profile
def pandas_merge():
    # Load CSV files
    start = time.time()
    sales_df = pd.read_csv("../datasets/sales/sales.csv")
    products_df = pd.read_csv("../datasets/sales/products.csv")
    employees_df = pd.read_csv("../datasets/sales/employees.csv")
    customers_df = pd.read_csv("../datasets/sales/customers.csv")
    countries_df = pd.read_csv("../datasets/sales/countries.csv")
    cities_df = pd.read_csv("../datasets/sales/cities.csv")
    categories_df = pd.read_csv("../datasets/sales/categories.csv")

    # Perform Joins
    # https://pandas.pydata.org/docs/reference/api/pandas.merge.html
    merged_df = sales_df.merge(products_df, on="ProductID", how="left") \
        .merge(employees_df, left_on="SalesPersonID", right_on="EmployeeID", how="left") \
        .merge(customers_df, on="CustomerID", how="left") \
        .merge(cities_df, left_on="CityID_x", right_on="CityID", how="left") \
        .merge(countries_df, on="CountryID", how="left") \
        .merge(categories_df, on="CategoryID", how="left")

    # For example, rename 'CityName' to 'CustomerCityName' and 'CountryName' to 'CustomerCountryName'
    merged_df = merged_df.rename(columns={
        "CityName": "CustomerCityName",
        "CountryName": "CustomerCountryName"
    })

    # Finally, select the desired columns in the specified order.
    final_columns = [
        "ProductID", "SalesID", "SalesPersonID", "CustomerID", "Quantity", "Discount", "TotalPrice",
        "SalesDate", "ProductName", "Price", "CategoryID", "Class", "ModifyDate", "Resistant",
        "IsAllergic", "VitalityDays", "CustomerCityName", "CountryID", "CustomerCountryName"
    ]

    final_df = merged_df[final_columns]

    # Optionally, inspect the final joined DataFrame
    print(final_df.head(100))

    end = time.time()
    print("Pandas merge execution time: {:.2f} seconds".format(end - start))


if __name__ == "__main__":
    pandas_merge()
