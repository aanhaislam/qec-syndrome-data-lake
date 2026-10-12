-- Run these in the Athena query editor, one statement at a time.
-- Replace YOUR_BUCKET with your bucket name everywhere below.

CREATE DATABASE IF NOT EXISTS qec_lake;

-- Fact table, one row per decoded shot, partitioned by code distance
CREATE EXTERNAL TABLE IF NOT EXISTS qec_lake.shots (
  run_id               STRING,
  shot_index           INT,
  physical_error_rate  DOUBLE,
  syndrome_weight      INT,
  actual_flip          BOOLEAN,
  predicted_flip       BOOLEAN,
  logical_error        BOOLEAN
)
PARTITIONED BY (distance INT)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/qec/shots_parquet/';

MSCK REPAIR TABLE qec_lake.shots;

-- Same data as unpartitioned CSV, kept only to measure what partitioning saves
CREATE EXTERNAL TABLE IF NOT EXISTS qec_lake.shots_csv (
  run_id               STRING,
  shot_index           INT,
  physical_error_rate  DOUBLE,
  syndrome_weight      INT,
  actual_flip          BOOLEAN,
  predicted_flip       BOOLEAN,
  logical_error        BOOLEAN,
  distance             INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://YOUR_BUCKET/qec/shots_csv/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Dimension table, one row per (distance, error rate) run
CREATE EXTERNAL TABLE IF NOT EXISTS qec_lake.runs (
  run_id               STRING,
  distance             BIGINT,
  rounds               BIGINT,
  physical_error_rate  DOUBLE,
  shots                BIGINT,
  num_detectors        BIGINT,
  logical_errors       BIGINT,
  logical_error_rate   DOUBLE,
  seconds              DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/qec/runs/';


-- 1. Rows loaded per distance
SELECT distance, COUNT(*) AS shots, COUNT(DISTINCT run_id) AS runs
FROM qec_lake.shots
GROUP BY distance
ORDER BY distance;

-- 2. Logical error rate per distance and physical error rate, computed from raw shots
SELECT distance, physical_error_rate,
       COUNT(*) AS shots,
       ROUND(AVG(CASE WHEN logical_error THEN 1.0 ELSE 0.0 END), 5) AS logical_error_rate
FROM qec_lake.shots
GROUP BY distance, physical_error_rate
ORDER BY physical_error_rate, distance;

-- 3. Threshold, where a bigger code stops helping (d = 7 versus d = 3)
SELECT a.physical_error_rate,
       a.logical_error_rate AS ler_d3,
       b.logical_error_rate AS ler_d7,
       CASE WHEN b.logical_error_rate < a.logical_error_rate
            THEN 'below threshold' ELSE 'above threshold' END AS regime
FROM qec_lake.runs a
JOIN qec_lake.runs b
  ON a.physical_error_rate = b.physical_error_rate
WHERE a.distance = 3 AND b.distance = 7
ORDER BY a.physical_error_rate;

-- 4. Decoder failures by syndrome weight, joined to run metadata
SELECT r.distance, s.syndrome_weight,
       COUNT(*) AS shots,
       SUM(CASE WHEN s.logical_error THEN 1 ELSE 0 END) AS failures
FROM qec_lake.shots s
JOIN qec_lake.runs r ON s.run_id = r.run_id
WHERE r.physical_error_rate = 0.006
GROUP BY r.distance, s.syndrome_weight
HAVING SUM(CASE WHEN s.logical_error THEN 1 ELSE 0 END) > 0
ORDER BY r.distance, s.syndrome_weight
LIMIT 50;

-- 5. Partitioning test. Run both, then compare "Data scanned" under each result.
SELECT physical_error_rate, AVG(syndrome_weight) AS mean_weight
FROM qec_lake.shots_csv
WHERE distance = 7
GROUP BY physical_error_rate;

SELECT physical_error_rate, AVG(syndrome_weight) AS mean_weight
FROM qec_lake.shots
WHERE distance = 7
GROUP BY physical_error_rate;
