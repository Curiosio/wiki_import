#!/bin/python3

import argparse
import sys
import json
import os
import sys
from datetime import date
from datetime import timedelta

from subprocess import call


THIS_DIR = os.path.abspath(os.path.dirname(__file__))
if not THIS_DIR:
    THIS_DIR = './'
if not THIS_DIR.endswith('/'):
    THIS_DIR = THIS_DIR + '/'
sys.path.append(THIS_DIR)
import wd_updater as Updater


PROPS_FILE = '/properties.json'
MAXREVID = '/maxrevid.txt'

BASE_URL = 'https://dumps.wikimedia.org/other/incr/wikidatawiki/'
STATUS_URL = BASE_URL + '%s/status.txt'
MAXREVID_URL = BASE_URL + '%s/maxrevid.txt'
DUMP_URL = BASE_URL + '%s/wikidatawiki-%s-pages-meta-hist-incr.xml.bz2'

MAXREVID_FILE = '/wikidatawiki-%s-maxrevid.txt'
STATUS_FILE = '/wikidatawiki-%s-status.txt'
DUMP_FILE = '/wikidatawiki-%s-pages-meta-hist-incr.xml.bz2'

STATUS_DONE = 'done:all'


def download_status(version, dump_path):
    # save revision id for the future in case dump have to be reloaded
    file_path = dump_path + STATUS_FILE % version
    params = ['wget', '-nv', '-O', file_path, STATUS_URL % version]
    call(params)

    status = ''
    with open(file_path, 'r') as f:
        status = f.read()

    return status.strip()


def download_revid(version, dump_path):
    # save revision id for the future in case dump have to be reloaded
    file_path = dump_path + MAXREVID_FILE % version
    if not os.path.isfile(file_path):
        params = ['wget', '-nv', '-O', file_path, MAXREVID_URL % version]
        call(params)
    else:
        print('File %s already exists, skip downloading' % file_path, flush=True)

    rev_id = ''
    with open(file_path, 'r') as f:
        rev_id = f.read()

    return rev_id


def download(version, dump_path):
    file_path = dump_path + DUMP_FILE % version
    if not os.path.isfile(file_path):
        params = ['wget', '-nv', '-O', file_path, DUMP_URL % (version, version)]
        call(params)
    else:
        print('File %s already exists, skip downloading' % file_path, flush=True)


def update(version, dump_path, conn_str, schema, id_name_map):
    if not conn_str or len(conn_str) == 0:
        return

    file_path = dump_path + DUMP_FILE % version
    conn, cursor = Updater.setup_db(conn_str)
    Updater.parse(file_path, id_name_map, conn, cursor, schema)
    conn.commit()


def main(max_days, max_rev_id, dump_path, conn_str, schema):
    id_name_map = {}
    # this file is required for updates
    # it is created by main WD import script during first time dump import
    props_path = dump_path + PROPS_FILE
    if os.path.isfile(props_path):
        print('loading properties from file', flush=True)
        id_name_map = json.load(open(props_path))
    else:
        print('ERROR: properties.json file is missing', flush=True)
        exit(-1)
    print('Loading dumps for', max_days, 'days', max_rev_id, dump_path, flush=True)

    day = timedelta(days=1)
    start_date = date.today() - timedelta(days=max_days)
    today = date.today()
    while start_date <= today:
        # check dump status (if it exists and is ready)
        date_str = start_date.strftime('%Y%m%d')

        # check if this dump has any updates
        rev_id_str = download_revid(date_str, dump_path)
        rev_id = int(rev_id_str)
        if rev_id > max_rev_id:
            # check if dump is ready to use
            status = download_status(date_str, dump_path)
            if status == STATUS_DONE:
                # download dump
                download(date_str, dump_path)

                # parse and load dump into DB
                update(date_str, dump_path, conn_str, schema, id_name_map)

                max_rev_id = rev_id
                write_revid(dump_path, rev_id)
            else:
                print('Skip %s dump as dumping process is not done yet, status: %s' % (date_str, status), flush=True)
        else:
            print('Skip %s dump as DB already contains that revision' % date_str, flush=True)

        start_date += day

    return max_rev_id


def read_revid(path):
    rev_id = 0
    if os.path.isfile(path + MAXREVID):
        with open(path + MAXREVID, 'r') as f:
            rev_id = int(f.read())
    return rev_id


def write_revid(path, rev_id):
    with open(path + MAXREVID, 'w') as f:
        f.write(str(rev_id))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download wikidata incremental dump into specfied location')
    parser.add_argument('max_days', type=int, help='Max days to load dumps for. Usually not more than 15 are available')
    parser.add_argument('dump_path', type=str, help='Location where to save BZipped wikipedia dumps')
    parser.add_argument('postgres', type=str, help='postgres connection string')
    parser.add_argument('schema', type=str, help='DB schema containing wikidata tables')

    args = parser.parse_args()

    max_rev_id = read_revid(args.dump_path)

    main(args.max_days, max_rev_id, args.dump_path, args.postgres, args.schema)
