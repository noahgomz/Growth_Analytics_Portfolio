WITH btable AS (
  SELECT bike_id, bike_type,
    COUNT(distinct date_trunc(start_time, MONTH)) as numMonthsActive,
    COUNT(trip_id) as numTrips,
    ROUND(COUNT(trip_id) / COUNT(distinct date_trunc(start_time, MONTH)),2) as tripsPerMonth
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE bike_id <> '198BB'
  GROUP BY bike_id, bike_type
  ORDER BY COUNT(distinct bike_type) DESC
),
scoringInputs AS (
  SELECT bike_type,
    ROUND(SUM(tripsPerMonth),2) numTrips,
    COUNT(*) numBikes,
    ROUND(SUM(tripsPerMonth) / COUNT(*),2) as avgTripsPerMonth,
    STDDEV_SAMP(tripsPerMonth) as stdSamp
  FROM btable
  GROUP BY bike_type
),
scoringSingleLine AS (
  SELECT
    MAX(CASE WHEN bike_type = 'electric' THEN 'electric' END)as electric,
    MAX(CASE WHEN bike_type = 'electric' THEN numTrips END)as elecNumTrips,
    MAX(CASE WHEN bike_type = 'electric' THEN numBikes END)as elecNumBikes,
    MAX(CASE WHEN bike_type = 'electric' THEN avgTripsPerMonth END)as elecAvgTripsPerDay,
    MAX(CASE WHEN bike_type = 'electric' THEN stdSamp END)as elecSTDsamp,
    MAX(CASE WHEN bike_type = 'classic' THEN 'classic' END)as classic,
    MAX(CASE WHEN bike_type = 'classic' THEN numTrips END)as classicNumTrips,
    MAX(CASE WHEN bike_type = 'classic' THEN numBikes END)as classicNumBikes,
    MAX(CASE WHEN bike_type = 'classic' THEN avgTripsPerMonth END)as classicAvgTripsPerDay,
    MAX(CASE WHEN bike_type = 'classic' THEN stdSamp END)as classicSTDsam
    /*
    CASE WHEN bike_type = 'electric' THEN 'electric' END as electric,
    CASE WHEN bike_type = 'electric' THEN numTrips END as elecNumTrips,
    CASE WHEN bike_type = 'electric' THEN numBikes END as elecNumBikes,
    CASE WHEN bike_type = 'electric' THEN avgTripsPerMonth END as elecAvgTripsPerDay,
    CASE WHEN bike_type = 'electric' THEN stdSamp END as elecSTDsamp,
    CASE WHEN bike_type = 'classic' THEN 'classic' END as classic,
    CASE WHEN bike_type = 'classic' THEN numTrips END as classicNumTrips,
    CASE WHEN bike_type = 'classic' THEN numBikes END as classicNumBikes,
    CASE WHEN bike_type = 'classic' THEN avgTripsPerMonth END as classicAvgTripsPerDay,
    CASE WHEN bike_type = 'classic' THEN stdSamp END as classicSTDsamp
    */
  FROM scoringInputs
)

SELECT (elecAvgTripsPerDay - classicAvgTripsPerDay) / SQRT(POWER(classicSTDsam,2) / classicNumBikes + POWER(elecSTDsamp,2) / elecNumBikes) as zScore, *
FROM scoringSingleLine
