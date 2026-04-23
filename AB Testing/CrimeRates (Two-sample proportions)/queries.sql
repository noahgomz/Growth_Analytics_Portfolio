WITH base AS (
SELECT unique_key, EXTRACT(YEAR FROM timestamp) as crime_date_year, timestamp AS crime_date, clearance_date, clearance_status,
    CASE
      WHEN clearance_status = 'Not cleared' THEN 'No'
      WHEN clearance_status IS NULL THEN NULL ELSE
      'Yes'
    END clearance_flag,
  description, primary_type, district,
  COUNT(*) OVER () as totalCrimes
FROM `bigquery-public-data.austin_crime.crime`
),
clearanceCounts AS (
SELECT clearance_flag, count(*)
FROM base
GROUP BY clearance_flag
ORDER BY clearance_flag
),
categories AS (
SELECT crime_date_year, primary_type,
    CASE
      WHEN REGEXP_CONTAINS(primary_type, 'theft') OR REGEXP_CONTAINS(primary_type, 'Theft') THEN 'Theft'
      ELSE 'Violent/Other'
    END cat,
  clearance_flag, COUNT(clearance_flag) AS numCrimes,
  SUM(COUNT(clearance_flag)) OVER (PARTITION BY crime_date_year, primary_type) as total
FROM base
WHERE clearance_flag IS NOT NULL
GROUP BY crime_date_year, primary_type, clearance_flag
ORDER BY crime_date_year, total DESC, primary_type, clearance_flag DESC
)

SELECT cat, clearance_flag, sum(numCrimes) AS crimes,
  SUM(sum(numCrimes)) OVER (PARTITION BY cat) as total
FROM categories
GROUP BY cat, clearance_flag
