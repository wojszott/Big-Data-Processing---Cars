-- hive.hql
-- Uruchamiaj przez run_hive.sh żeby przekazać hiveconf: input_dir3, input_dir4, output_dir6

-- Usuń wcześniejsze definicje wynikowe
DROP TABLE IF EXISTS final_result;
DROP VIEW IF EXISTS feature_stats;
DROP VIEW IF EXISTS joined_data;
DROP VIEW IF EXISTS cars_exploded;
DROP VIEW IF EXISTS rentals_summary;

-- 1. Dane z MapReduce

CREATE EXTERNAL TABLE IF NOT EXISTS rentals_summary_raw(line STRING)
LOCATION '${hiveconf:input_dir3}';

-- Widok parsujący linię MapReduce na kolumny
CREATE OR REPLACE VIEW rentals_summary AS
SELECT
split(split(line, '\t')[0], ',')[0] AS car_id,
CAST(split(split(line, '\t')[0], ',')[1] AS INT) AS rental_year,
CAST(split(split(line, '\t')[1], ',')[0] AS INT) AS total_rentals,
CAST(split(split(line, '\t')[1], ',')[1] AS DOUBLE) AS completed_ratio
FROM rentals_summary_raw
WHERE line IS NOT NULL AND trim(line) <> '';

-- 2. Dane samochodów (datasource4) jako EXTERNAL TABLE

CREATE EXTERNAL TABLE IF NOT EXISTS cars_dict (
  car_id STRING,
  make STRING,
  model STRING,
  year STRING,
  features STRING,
  category STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir4}';

-- 3. Widok rozbijający listę cech

CREATE OR REPLACE VIEW cars_exploded AS
SELECT
car_id,
TRIM(feature) AS feature
FROM cars_dict
LATERAL VIEW explode(split(features, ';')) f AS feature
WHERE features IS NOT NULL AND trim(features) <> '';

-- 4. Widok łączący dane MapReduce z cechami

CREATE OR REPLACE VIEW joined_data AS
SELECT
e.feature,
r.rental_year,
r.total_rentals,
r.completed_ratio
FROM rentals_summary r
JOIN cars_exploded e
ON r.car_id = e.car_id;

-- 5. Widok agregacji
CREATE OR REPLACE VIEW feature_stats AS
SELECT
feature,
rental_year,
AVG(total_rentals) AS feature_year_avg_rentals,
AVG(completed_ratio) AS feature_year_avg_completed_ratio
FROM joined_data
GROUP BY feature, rental_year;


-- 6. Tabela wynikowa

DROP TABLE IF EXISTS final_result;
CREATE EXTERNAL TABLE IF NOT EXISTS final_result (
  line STRING
)
STORED AS TEXTFILE
LOCATION '${hiveconf:output_dir6}';

-- 7. Średnie po cechach

DROP VIEW IF EXISTS feature_avg;
CREATE VIEW feature_avg AS
SELECT
  feature,
  AVG(feature_year_avg_rentals) AS avg_rentals_all_years
FROM feature_stats
GROUP BY feature;


-- 8. Wstaw wynik końcowy jako JSON

INSERT OVERWRITE TABLE final_result
SELECT
  concat(
    '{',
    '"feature":"', fs.feature, '",',
    '"rental_year":', fs.rental_year, ',',
    '"feature_year_avg_rentals":', round(fs.feature_year_avg_rentals, 2), ',',
    '"feature_year_avg_completed_ratio":', round(fs.feature_year_avg_completed_ratio, 2), ',',
    '"above_avg_rentals":',
      CASE
        WHEN fs.feature_year_avg_rentals > fa.avg_rentals_all_years THEN 'true'
        ELSE 'false'
      END,
    '}'
  ) AS line
FROM feature_stats fs
JOIN feature_avg fa
  ON fs.feature = fa.feature;