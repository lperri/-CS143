/* HOMEWORK 1 SQL Queries */

/* 1a */
SELECT EXTRACT(hour FROM DateTime) AS hour,
       SUM(Throughput) AS trips
FROM hw1.rides2017
GROUP BY hour;



/* 1b */
/*
original attempt--leaving it commented so I can refer back while studying, error was that EXTRACT(isodow FROm DateTime) must be used either in GROUP BY statement or AGG fcn
SELECT EXTRACT(isodow FROM DateTime) AS day,
       Origin AS station_code,
       SUM(Throughput) AS weekday_trips
FROM hw1.rides2017
GROUP BY station_code
HAVING EXTRACT(isodow FROM DateTime) BETWEEN 1 AND 5
ORDER BY weekday_trips DESC
LIMIT 2;
*/

SELECT Origin AS station_code,
       SUM(Throughput) AS weekday_trips
FROM hw1.rides2017
WHERE EXTRACT(isodow FROM DateTime) BETWEEN 1 and 5
GROUP BY station_code
ORDER BY weekday_trips DESC
LIMIT 2;

/* Answer:
MONT: 3751634
EMBR: 3542766
*/



/* 1c */
SELECT Origin AS station_code,
       AVG(Throughput) as average_Trips
FROM hw1.rides2017
WHERE EXTRACT(isodow FROM DateTime) = 1 AND EXTRACT(hour FROM DateTime) BETWEEN 7 and 10
GROUP BY station_code
ORDER BY average_trips DESC
LIMIT 5;

/* Answer:
BALB: 34.845...
DUBL: 33.081...
PHIL: 32.844...
24TH: 31.900...
FRMT: 31.535...*/

