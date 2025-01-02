## What is DuckDB
 - Info https://duckdb.org/
 - Getting started https://duckdb.org/docs/index 

## How to install
 - Command Line https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=win&download_method=package_manager&architecture=x86_64
 - Python https://duckdb.org/docs/installation/?version=stable&environment=python

## How to work with Duckdb
If you want to create a database that will store data, you need to perform the following steps:
* Run `duckdb my_new_database_name.duckdb` , where `my_new_database_name` is the name of the file that you come up with yourself.
* The next `command is select version();` .
* Exit the newly created database (roughly speaking, it's just a file my_new_database_name.duckdb ) `Cntrl+C` , or `Cntrl+D`, or `Command+C`, or `Command+D`.
* To connect to the database from DBeaver or another IDE, we will need the path to the file `my_new_database_name.duckdb`
