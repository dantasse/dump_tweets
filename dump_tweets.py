#!/usr/bin/env python

# Trying to dump some tweets from the database to compute stuff on them easily.

import csv, collections, argparse, psycopg2, psycopg2.extras, ppygis, json

parser = argparse.ArgumentParser()
parser.add_argument('--table', default='tweet_pgh')
parser.add_argument('--output_file', default='/data/temp/tweets.csv')
args = parser.parse_args()

psql_conn = psycopg2.connect("dbname='tweet'")
psycopg2.extras.register_hstore(psql_conn)
pg_cur = psql_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

pg_cur.execute("SELECT text,ST_ASGEOJSON(coordinates),user_screen_name,created_at FROM tweet_pgh;")

writer = csv.writer(open(args.output_file, 'w'))
counter = 0
for row in pg_cur:
    counter += 1
    if (counter % 1000) == 0:
        print str(counter) + ' tweets processed'
    datetime = row[3]
    date = datetime.strftime('%Y-%m-%d')
    time = datetime.strftime('%H:%M:%S')
    point = json.loads(row[1])
    lat = point['coordinates'][0]
    lon = point['coordinates'][1]
    row.append(str(lat))
    row.append(str(lon))
    row.append(date)
    row.append(time)
    del row[1]
    del row[2]
    del row[0] # ugh text is a mess.
    writer.writerow(row)
