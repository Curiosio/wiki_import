#!/bin/python3

from collections import Counter
import os
import argparse
import subprocess
import datetime
import calendar
import random
import psycopg2
import requests
import urllib

# REMOTE_PATH = 'https://dumps.wikimedia.org/other/pagecounts-raw/%(year)04d/%(year)04d-%(month)02d/pagecounts-%(year)04d%(month)02d%(day)02d-%(hour)02d0000.gz'
REMOTE_PATH = 'https://dumps.wikimedia.org/other/pageviews/%(year)04d/%(year)04d-%(month)02d/pageviews-%(year)04d%(month)02d%(day)02d-%(hour)02d0000.gz'
LOCAL_PATH = 'pageviews-%(year)04d%(month)02d%(day)02d-%(hour)02d0000.gz'


def setup_db(connection_string):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS wp.wikistats')
    cursor.execute('CREATE TABLE wp.wikistats ('
                    '    title TEXT PRIMARY KEY,'
                    '    viewcount INTEGER'
                    ')')
    return conn, cursor


def fetch_dumps(dump_dir, dumps_to_fetch):
    # don't try anything in the last month, it might not be online yet
    last_date = datetime.datetime.today() - datetime.timedelta(30)
    year = last_date.year
    if last_date.month <= 2:
        year -= 1
    if calendar.isleap(year):
        days = 366
    else:
        days = 365
    for i in range(dumps_to_fetch):
        local_path = None
        remote_path = None
        while not local_path or os.path.isdir(local_path):
            random_day = last_date - datetime.timedelta(days=random.randint(1, days))
            random_hour = random.randint(1, 24)
            d = {'year': random_day.year, 'month': random_day.month, 'day': random_day.day, 'hour': random_hour}
            remote_path = REMOTE_PATH % d
            local_path = os.path.join(dump_dir, LOCAL_PATH % d)
        print('getting', local_path)
        data = requests.get(remote_path).content
        with open(local_path, 'wb') as fout:
            fout.write(data)


def fetch_dumps_days(dump_dir, start_date, days):
    hour = datetime.timedelta(hours=1)
    last_date = datetime.datetime.strptime(start_date, '%Y%m%d') - hour
    print(last_date, last_date - hour)
    for i in range(days * 24):
        d = {'year': last_date.year, 'month': last_date.month, 'day': last_date.day, 'hour': last_date.hour}
        remote_path = REMOTE_PATH % d
        local_path = os.path.join(dump_dir, LOCAL_PATH % d)
        print('getting', local_path)
        data = requests.get(remote_path).content
        with open(local_path, 'wb') as fout:
            fout.write(data)
        last_date = last_date - hour

def main(dump_dir, cursor, dumps_to_fetch, start_date):
    if dumps_to_fetch > 0:
        fetch_dumps_days(dump_dir, start_date, dumps_to_fetch)

    c = Counter()
    for fn in os.listdir(dump_dir):
        if fn.endswith('.gz'):
            print(fn)
            path = os.path.join(dump_dir, fn)
            for line in subprocess.Popen(['zcat'], stdin=open(path, 'r'), stdout=subprocess.PIPE).stdout:
                line = line.decode('utf-8')
                if line.startswith('en '):
                    bits = line.split(' ')
                    _, wikipedia_id, count, size = bits
                    if not ':' in wikipedia_id:
                        try:
                            title = urllib.parse.unquote(wikipedia_id).replace('_', ' ')
                        except UnicodeDecodeError:
                            continue
                        c[title] += int(count)
    for k, v in c.items():
        try:
            cursor.execute("INSERT INTO wp.wikistats (title, viewcount) VALUES (%s, %s)", (k, v))
        except:
            print(k, v)
            raise

    import pprint
    pprint.pprint(c.most_common(25))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import wikidata into postgress')
    parser.add_argument('postgres', type=str, help='postgres connection string')
    parser.add_argument('dumps_to_fetch', type=int, default=0,
            help='randomly fetch this amount of dumps from the last year')
    parser.add_argument('start_date', type=str, help='YYYYMMDD formatted date to load stats to (last date)')
    parser.add_argument('dumps', type=str, help='directory where the downloaded page counts are stored')

    args = parser.parse_args()
    conn, cursor = setup_db(args.postgres)

    if not os.path.isdir(args.dumps):
        os.makedirs(args.dumps)

    main(args.dumps, cursor, args.dumps_to_fetch, args.start_date)

    conn.commit()

