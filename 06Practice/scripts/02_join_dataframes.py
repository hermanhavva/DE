import time
from memory_profiler import profile
import polars as pl


@profile
def polars_merge():
    # Load CSV files using Polars
    start = time.time()
    sales_df = pl.read_csv("../../datasets_ext/sales/sales.csv")
    products_df = pl.read_csv("../../datasets_ext/sales/products.csv")
    employees_df = pl.read_csv("../../datasets_ext/sales/employees.csv")
    customers_df = pl.read_csv("../../datasets_ext/sales/customers.csv")
    countries_df = pl.read_csv("../../datasets_ext/sales/countries.csv")
    cities_df = pl.read_csv("../../datasets_ext/sales/cities.csv")
    categories_df = pl.read_csv("../../datasets_ext/sales/categories.csv")

    # Perform Joins using Polars join (chain the joins similar to pandas merge)
    merged_df = sales_df.join(products_df, on="ProductID", how="left") \
        .join(employees_df, left_on="SalesPersonID", right_on="EmployeeID", how="left") \
        .join(customers_df, on="CustomerID", how="left") \
        .join(cities_df, on="CityID", how="left") \
        .join(countries_df, on="CountryID", how="left") \
        .join(categories_df, on="CategoryID", how="left")

    # Rename columns: rename 'CityName' to 'CustomerCityName' and 'CountryName' to 'CustomerCountryName'
    merged_df = merged_df.rename({
        "CityName": "CustomerCityName",
        "CountryName": "CustomerCountryName"
    })

    # Finally, select the desired columns in the specified order.
    final_columns = [
        "ProductID", "SalesID", "SalesPersonID", "CustomerID", "Quantity", "Discount", "TotalPrice",
        "SalesDate", "ProductName", "Price", "CategoryID", "Class", "ModifyDate", "Resistant",
        "IsAllergic", "VitalityDays", "CustomerCityName", "CountryID", "CustomerCountryName"
    ]
    final_df = merged_df.select(final_columns)

    # Optionally, inspect the final joined DataFrame
    print(final_df.head(100))

    end = time.time()
    print("Polars merge execution time: {:.2f} seconds".format(end - start))


if __name__ == "__main__":
    polars_merge()
