\copy project1.artist FROM '/home/cs143/data/artist.csv' CSV DELIMITER ',' QUOTE '"';
\copy project1.song FROM '/home/cs143/data/song.csv' CSV DELIMITER ',' QUOTE '"';
\copy project1.token FROM '/home/cs143/data/token.csv' CSV DELIMITER ',' QUOTE '"';

