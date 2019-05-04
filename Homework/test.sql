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
   	             )::decimal(5,2) > 100 
   	       THEN '100'
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
