-- Full dataset info you can find here - https://www.kaggle.com/datasets/shivamb/netflix-shows?resource=download
-- For more info use https://duckdb.org/docs/guides/file_formats/csv_import.html
select *
from read_csv('path_to_your_csv/netflix_titles.csv')
limit 100;