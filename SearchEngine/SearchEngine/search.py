#!/usr/bin/python3

import psycopg2
import re
import string
import sys

_PUNCTUATION = frozenset(string.punctuation)

def _remove_punc(token):
    """Removes punctuation from start/end of token."""
    i = 0
    j = len(token) - 1
    idone = False
    jdone = False
    while i <= j and not (idone and jdone):
        if token[i] in _PUNCTUATION and not idone:
            i += 1
        else:
            idone = True
        if token[j] in _PUNCTUATION and not jdone:
            j -= 1
        else:
            jdone = True
    return "" if i > j else token[i:(j+1)]

def _get_tokens(query):
    rewritten_query = []
    tokens = re.split('[ \n\r]+', query)
    for token in tokens:
        cleaned_token = _remove_punc(token)
        if cleaned_token:
            if "'" in cleaned_token:
                cleaned_token = cleaned_token.replace("'", "''")
            rewritten_query.append(cleaned_token)
    return rewritten_query



def search(query, query_type, iteration=1,offset=0):
    rewritten_query = _get_tokens(query)

    if iteration == 1:
        drop_mat_view = "DROP MATERIALIZED VIEW IF EXISTS mat_view_results;"
    
    per_page = 20
    
    if query_type == "and" and iteration == 1:
        rewritten_query_joined = ") AND song_id IN (SELECT song_id FROM tfidf WHERE token=".join("'{}'".format(w) for w in rewritten_query)
        full_query = "CREATE MATERIALIZED VIEW mat_view_results AS SELECT song.song_name AS song_name,artist.artist_name AS artist_name,song.page_link AS link,SUM(subquery.score) FROM (SELECT song_id,token,score FROM tfidf WHERE song_id IN (SELECT song_id FROM tfidf WHERE token={}) GROUP BY song_id,token) AS subquery JOIN song ON subquery.song_id=song.song_id JOIN artist ON song.artist_id=artist.artist_id GROUP BY song.song_name,artist.artist_name,song.page_link ORDER BY SUM(subquery.score) DESC;".format(rewritten_query_joined) + "SELECT COUNT(*) FROM mat_view_results;"
        pag_query = "CREATE MATERIALIZED VIEW mat_view_results AS SELECT song.song_name AS song_name,artist.artist_name AS artist_name,song.page_link AS link,SUM(subquery.score) FROM (SELECT song_id,token,score FROM tfidf WHERE song_id IN (SELECT song_id FROM tfidf WHERE token={}) GROUP BY song_id,token) AS subquery JOIN song ON subquery.song_id=song.song_id JOIN artist ON song.artist_id=artist.artist_id GROUP BY song.song_name,artist.artist_name,song.page_link ORDER BY SUM(subquery.score) DESC;".format(rewritten_query_joined) + "SELECT * FROM mat_view_results LIMIT {} OFFSET {};".format(per_page,offset)

    
    elif query_type == "or" and iteration == 1:
        rewritten_query_joined = " OR token = ".join("'{}'".format(w) for w in rewritten_query)
        full_query = "CREATE MATERIALIZED VIEW mat_view_results AS SELECT subquery.page_link,a.artist_name FROM (SELECT l.song_id,l.artist_id,l.page_link,SUM(r.score) FROM song AS l JOIN tfidf AS r ON l.song_id = r.song_id WHERE token = {} GROUP BY l.song_id ORDER BY SUM(score) DESC) as subquery JOIN artist AS a ON subquery.artist_id = a.artist_id;".format(rewritten_query_joined) + "SELECT COUNT(*) FROM mat_view_results;" 
        pag_query = "CREATE MATERIALIZED VIEW mat_view_results AS SELECT subquery.page_link,a.artist_name FROM (SELECT l.song_id,l.artist_id,l.page_link,SUM(r.score) FROM song AS l JOIN tfidf AS r ON l.song_id = r.song_id WHERE token = {} GROUP BY l.song_id ORDER BY SUM(score) DESC) as subquery JOIN artist AS a ON subquery.artist_id = a.artist_id;".format(rewritten_query_joined) + "SELECT * FROM mat_view_results LIMIT {} OFFSET {};".format(per_page,offset) 
    
    else:
        query = "SELECT * FROM mat_view_results LIMIT {} OFFSET {};".format(per_page,offset)
    
    connection = None 
    try:
        connection = psycopg2.connect(user='cs143',
            password='cs143',
            host='localhost',
            port='5432',
            database='searchengine')
        cursor = connection.cursor()
        if drop_mat_view:
            cursor.execute(drop_mat_view)
#            cursor.execute(full_query)
#            num_lines = (cursor.fetchall())[0][0]
        cursor.execute(pag_query)
        connection.commit()
        rows = cursor.fetchall() 
        cursor.close()
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
#    if num_lines:
#        return num_lines,rows
#    else:
        return rows

if __name__ == "__main__":
    if len(sys.argv) > 2:
        result = search(' '.join(sys.argv[2:]), sys.argv[1].lower())
        print(result)
    else:
        print("USAGE: python3 search.py [or|and] term1 term2 ...")

