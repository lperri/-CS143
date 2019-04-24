/* 1(a) */
SELECT highway,area 
FROM hw2.caltrans
WHERE condition LIKE '%CLOSED%' AND (condition LIKE '%DUE TO SNOW%' OR condition LIKE '%FOR THE WINTER%')
ORDER BY highway DESC, area DESC
LIMIT 20;
/* result 
 highway |                      area                       
---------+-------------------------------------------------
 US395   | IN THE CENTRAL CALIFORNIA AREA & SIERRA NEVADA
 US395   | IN THE CENTRAL CALIFORNIA AREA & SIERRA NEVADA
 US395   | IN THE CENTRAL CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA
*/





/* 1(b) */

/* by running the query:
    SELECT DISTINCT reported FROM hw2.caltrans; 
I found that there are 353 days of the year represented in the data. */

SELECT highway, area, percentage
FROM (
    SELECT highway, area, COUNT(reported)/353 AS percentage
    FROM hw2.caltrans
    WHERE condition LIKE '%CLOSED%' AND (condition LIKE '%DUE TO SNOW%' OR condition LIKE '%FOR THE WINTER'
    ORDER BY highway DESC, area DESC
    )
ORDER BY percentage DESC
LIMIT 5;

/* result 
 highway |                      area                       |       percentage       
---------+-------------------------------------------------+------------------------
 SR120   | IN THE CENTRAL CALIFORNIA AREA & SIERRA NEVADA  | 0.89518413597733711048
 SR89    | IN THE NORTHERN CALIFORNIA AREA & SIERRA NEVADA | 0.76770538243626062323
 SR203   | IN THE CENTRAL CALIFORNIA AREA & SIERRA NEVADA  | 0.63456090651558073654
 SR108   | IN THE CENTRAL CALIFORNIA AREA & SIERRA NEVADA  | 0.57507082152974504249
 SR4     | IN THE CENTRAL CALIFORNIA AREA                  | 0.56657223796033994334
*/





/* 2(a) 
A cross join is not a type of inner join. */





/* 3(a) */
SELECT hw2.trip_start.trip_id,hw2.trip_start.user_id,COALESCE(SUM(hw2.trip_end.time-hw2.trip_start.time),'24:00:00') AS trip_length 
FROM hw2.trip_start 
FULL JOIN hw2.trip_end ON hw2.trip_start.trip_id = hw2.trip_end.trip_id AND hw2.trip_start.user_id = hw2.trip_end.user_id 
GROUP BY hw2.trip_start.trip_id,hw2.trip_start.user_id 
LIMIT 5;

/* result
 trip_id | user_id | trip_length
---------+---------+-------------
       0 |   20685 | 00:01:12
       2 |   34808 | 00:02:59
       3 |   25463 | 24:00:00
       4 |   26965 | 00:01:34
       5 |     836 | 00:00:51 
*/





/* 3(b) */
SELECT trip_id,
       user_id,
           CASE
               WHEN SUM( 1.00 +
                       EXTRACT(minute FROM date_trunc('minute',(trip_length+'00:00:59')))*0.15 +
                       EXTRACT(hour FROM date_trunc('hour',trip_length))*60.00*0.15
                     )::decimal(5,2) > 100
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
/* result 
 trip_id | user_id | charge
---------+---------+--------
       0 |   20685 |   1.30
       2 |   34808 |   1.45
       3 |   25463 | 100.00
       4 |   26965 |   1.30
       5 |     836 |   1.15
*/





/* 3(c) */

SELECT user_id,
       SUM(charge) as monthly_charge
FROM (
    SELECT trip_id,
           user_id,
	   month,
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
	 SELECT hw2.trip_start.trip_id,
	 	hw2.trip_start.user_id,
	 	COALESCE(SUM(hw2.trip_end.time-hw2.trip_start.time),'24:00:00') AS trip_length,
		EXTRACT(month FROM hw2.trip_start.time) as month,
                EXTRACT(year FROM hw2.trip_start.time) as year
	 FROM hw2.trip_start
         FULL JOIN hw2.trip_end ON hw2.trip_start.trip_id = hw2.trip_end.trip_id AND hw2.trip_start.user_id = hw2.trip_end.user_id
         GROUP BY hw2.trip_start.user_id,hw2.trip_start.trip_id
	 HAVING EXTRACT(month FROM hw2.trip_start.time) = 3 AND EXTRACT(year FROM hw2.trip_start.time) = 2018
         
    ) AS subq1 GROUP BY user_id,trip_id,month 
    
) AS subq2 GROUP BY user_id
LIMIT 5;

/* result
 user_id | monthly_charge
---------+----------------
       0 |         105.50
       1 |           4.05
       2 |         314.05
       3 |          11.90
       4 |         210.55 
     
Reading from the table, we see that user2 spent $314.05 in March,2018.
*/





/* 3(d)
We would need a self-join which would join on trip_id. */
