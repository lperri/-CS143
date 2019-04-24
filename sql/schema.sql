DROP SCHEMA IF EXISTS project1 CASCADE;
CREATE SCHEMA IF NOT EXISTS project1;


CREATE TABLE project1.artist(
    artist_id smallserial NOT NULL,
    artist_name varchar(50) NOT NULL,
    UNIQUE(artist_id),
    PRIMARY KEY (artist_id)
);


CREATE TABLE project1.song(
    song_id serial NOT NULL,
    artist_id smallserial NOT NULL,
    song_name varchar(80),
    url_suffix varchar(110),
    UNIQUE(url_suffix),
    UNIQUE(song_id),
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES project1.artist
);


CREATE TABLE project1.token(
    song_id integer NOT NULL,
    term varchar(110) NOT NULL,
    term_frequency smallint DEFAULT 0,
    PRIMARY KEY(song_id,term)
);



CREATE TABLE project1.tfidf(
    song_id integer NOT NULL,
    term varchar(110) NOT NULL,
    score double precision DEFAULT 0 CHECK (score >= 0),
    FOREIGN KEY (song_id,term) REFERENCES project1.token(song_id,term),
    PRIMARY KEY (song_id,term)
);

