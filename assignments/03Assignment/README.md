# 03 Assignment

#### Query optimization in MySQL (5 points):

1. Requirements for dataset: 3 tables, one of which has more than a million records. 
   You can use [this](https://www.kaggle.com/datasets/andrexibiza/grocery-sales-dataset/data) dataset or find or generate another one.
2. Generate **unoptimized** query using AI (ChatGPT or other AI assistant).
3. Provide step-by-step optimization of you generated query using:
   - code refactoring with adding CTE.
   - optimization via adding indexes.
4. Provide execution plans comparison.
5. Be able to explain your solution using the correct terminology.


#### Additional: Query optimization in DuckDB (1 point):

Optimize this script:

```
SELECT count(*)
FROM (
  SELECT type, repo.name, actor.login,
    JSON_EXTRACT_STRING(payload, '$.action') as event
  FROM read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  )
  WHERE event = 'opened'
  AND TYPE = 'IssuesEvent'
  ;
  
 
SELECT count(*)
FROM (
  SELECT type, repo.name, actor.login,
    JSON_EXTRACT_STRING(payload, '$.action') as event
  FROM read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  )
  WHERE event = 'closed'
  AND TYPE = 'IssuesEvent'
  ;
  
 SELECT count(*)
FROM (
  SELECT type, repo.name, actor.login,
    JSON_EXTRACT_STRING(payload, '$.action') as event
  FROM read_json_auto('https://data.gharchive.org/2025-02-14-12.json.gz')
  )
  WHERE event = 'reopened'
  AND TYPE = 'IssuesEvent'
  ;
```

##### Script purpose:
Count of issues opened, closed, and reopened. 
Note, that we need only IssuesEvent event type (TYPE = 'IssuesEvent').
