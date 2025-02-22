import pandas as pd

# Replace '../datasets/sales/sales.csv' with your actual file path
df = pd.read_csv('../datasets/sales/sales.csv')

# Display the first few rows
print(df.head())

# Get information on data types and non-null counts
print(df.info())

# Get descriptive statistics for numerical columns
print(df.describe())

# Data Cleaning
# Check for missing values in each column
print(df.isnull().sum())

# Example: If there's a 'Date' column, convert it to datetime
if 'SalesDate' in df.columns:
    df['SalesDate'] = pd.to_datetime(df['SalesDate'])

# Optionally, drop duplicates
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html
df = df.drop_duplicates()