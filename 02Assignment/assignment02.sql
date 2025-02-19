create schema assignment2;


CREATE TABLE assignment2.raw_data AS select * from read_csv('https://raw.githubusercontent.com/hermanhavva/DE/refs/heads/assignment02/02Assignment/dataset.json');

SELECT * FROM assignment2.raw_data;


SELECT * FROM assignment2.parsed_data;


DROP TABLE assignment2.parsed_data;
DROP TABLE assignment2.parsed_data;

CREATE TABLE assignment2.parsed_data AS
WITH parsed_data AS (
    SELECT
        metadata::JSON AS metadata_json,
        raw_data::JSON AS raw_json
    FROM assignment2.raw_data 
),
expanded_data AS (
    SELECT
        json_extract_string(metadata_json, '$.event_id') AS event_id,
        json_extract_string(metadata_json, '$.event_timestamp') AS event_timestamp,
        json_extract_string(metadata_json, '$.source') AS data_source,
        json_extract_string(raw_json, '$.user.id') AS user_id,
        json_extract_string(raw_json, '$.user.name') AS user_name,
        json_extract_string(raw_json, '$.user.email') as user_email,
        json_extract_string(raw_json, '$.action') AS action,
        json_extract_string(raw_json, '$.details.device') AS device,
        json_extract(raw_json, '$.events') AS events_arr
    FROM parsed_data
    )
SELECT * FROM expanded_data;

SELECT * from assignment2.parsed_data;

SELECT pd.events_arr FROM assignment2.parsed_data pd;



SELECT * EXCLUDE(events_arr),  -- somehow the exclude clouse does not support the aliases
  UNNEST(from_json(json(events_arr), '["INTEGER"]')) as event_num
FROM assignment2.parsed_data pd;

