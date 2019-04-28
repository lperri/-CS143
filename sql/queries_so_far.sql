DROP TABLE IF EXISTS temp_table CASCADE;
DROP TABLE IF EXISTS temp_table_joined CASCADE;
DROP TABLE IF EXISTS practice_tfidf CASCADE;

SELECT token,COUNT(song_id) AS doc_freq
INTO TEMPORARY temp_table
FROM token
GROUP BY token;

ALTER TABLE temp_table
ALTER COLUMN doc_freq TYPE int;

SELECT token.song_id, token.count AS term_freq, token.token, temp_table.doc_freq
INTO TEMPORARY temp_table_joined
FROM token
LEFT JOIN temp_table ON token.token = temp_table.token;

WITH variable_j AS 
    (SELECT COUNT(song_id) AS j FROM song)
SELECT song_id,token,(term_freq*LOG(j/doc_freq)) AS score INTO TEMPORARY practice_tfidf FROM temp_table_joined,variable_j limit 20;


