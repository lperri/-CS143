SELECT trip_id,
       user_id,
           CASE
               WHEN SUM( 1.00 +
                       EXTRACT(minute FROM date_trunc('minute',(trip_length+'00:00:59')))*0.15 +
                       EXTRACT(hour FROM date_trunc('hour',trip_length))*60.00*0.15
                     )::decimal(5,2) > 100.00
               THEN '100.00'
               ELSE SUM( 1.00 +
                       EXTRACT(minute FROM date_trunc('minute',(trip_length+'00:00:59')))*0.15 +
                       EXTRACT(hour FROM date_trunc('hour',trip_length))*60.00*0.15
                     )::decimal(5,2)
           END AS charge
FROM (
    SELECT hw2.trip_start.trip_id,hw2.trip_start.user_id,COALESCE(SUM(hw2.trip_end.time-hw2.trip_start.time),'24:00:00') AS trip_length
    FROM hw2.trip_start
    FULL JOIN hw2.trip_end ON hw2.trip_start.trip_id = hw2.trip_end.trip_id AND hw2.trip_start.user_id = hw2.trip_end.user_id
    GROUP BY hw2.trip_start.trip_id,hw2.trip_start.user_id
    ) AS subq
GROUP BY trip_id,user_id
LIMIT 5;
