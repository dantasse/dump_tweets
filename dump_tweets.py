#!/usr/bin/env python

# Trying to dump some tweets from the database to compute stuff on them easily.

import csv, collections, argparse, psycopg2, psycopg2.extras, ppygis

parser = argparse.ArgumentParser()
parser.add_argument('--table', default='tweet_pgh')
parser.add_argument('--output_file', default='/data/temp/tweets.json')
args = parser.parse_args()

psql_conn = psycopg2.connect("dbname='tweet'")
psycopg2.extras.register_hstore(psql_conn)
pg_cur = psql_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

pg_cur.execute("SELECT text,ST_ASGEOJSON(coordinates),user_screen_name,created_at FROM tweet_pgh limit 10;")

# freqs = defaultdict(lambda: defaultdict(int)) #freqs[nghd][word]
# TF = {}
# IDF = defaultdict(int)
# TFIDF = {}
# uniq_users_per_word = defaultdict(lambda: defaultdict(set))
#             #uniq_users_per_word[nghd][word]
# entropy = defaultdict(lambda: defaultdict(int))

counter = 0
for row in pg_cur:
    counter += 1
    if (counter % 10000) == 0:
        print str(counter) + ' tweets processed'
    print row
