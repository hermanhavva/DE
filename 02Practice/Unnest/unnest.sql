-- Create a DuckDB table and load the CSV
--DROP TABLE kafka_data;
CREATE TABLE kafka_data AS
SELECT *
FROM read_csv('path_to_your_datasets/datasets/complex_json_kafka.csv');

SELECT * FROM kafka_data;


--DROP TABLE kafka_parsed_data;
CREATE TABLE kafka_parsed_data AS
WITH parsed_data AS (
    SELECT
        metadata::JSON AS metadata_json,
        raw_data::JSON AS raw_json
    FROM kafka_data
),
expanded_data AS (
    SELECT
        json_extract_string(metadata_json, '$.event_id') AS event_id,
        json_extract_string(metadata_json, '$.event_timestamp') AS event_timestamp,
        json_extract_string(raw_json, '$.user.name') AS user_name,
        json_extract_string(raw_json, '$.action') AS action,
        json_extract_string(raw_json, '$.details.location') AS location,
        json_extract_string(raw_json, '$.details.device') AS device,
        json_extract(raw_json, '$.events') AS events_array
    FROM parsed_data
    )
 SELECT * FROM expanded_data;


SELECT * FROM kafka_parsed_data;


-- Unnesting array
SELECT * EXCLUDE(events_array),
  UNNEST(from_json(
    json(events_array),
    '["INTEGER"]'
  )) as event_num
FROM kafka_parsed_data
