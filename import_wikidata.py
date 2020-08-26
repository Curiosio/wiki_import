#!/usr/bin/env python

from collections import defaultdict

import argparse
import subprocess
import json
import os
import re

import psycopg2
from psycopg2 import extras

DATE_PARSE_RE = re.compile(r'([-+]?[0-9]+)-([0-9][0-9])-([0-9][0-9])T([0-9][0-9]):([0-9][0-9]):([0-9][0-9])Z?')

def setup_db(connection_string):
  conn = psycopg2.connect(connection_string)
  cursor = conn.cursor()
  cursor.execute('CREATE SCHEMA IF NOT EXISTS import;')
  cursor.execute('DROP TABLE IF EXISTS import.wikidata;')
  cursor.execute('CREATE TABLE import.wikidata ('
                 '    wikipedia_id TEXT PRIMARY KEY,'
                 '    title TEXT,'
                 '    wikidata_id TEXT,'
                 '    description TEXT,'
                 '    labels JSONB,'
                 '    sitelinks JSONB,'
                 '    properties JSONB'
                 ');')
  cursor.execute('DROP TABLE IF EXISTS import.id2name;')
  cursor.execute('CREATE TABLE import.id2name ('
                 '    id TEXT PRIMARY KEY,'
                 '    title TEXT,'
                 '    CONSTRAINT id2name_wikidata_id UNIQUE (id)'
                 ');')

  conn.commit()
  return conn, cursor


def parse_wikidata(line):

    line = line.decode("utf-8").strip()
    if line and line[0] == '{':
      if line[-1] == ',':
        line = line[:-1]
      return json.loads(line)


def map_value(value, id_name_map):
  if not value or not 'type' in value or not 'value' in value:
    return None
  typ = value['type']
  value = value['value']
  if typ == 'string':
    return value
  elif typ == 'wikibase-entityid':
    entitiy_id = value['id']
    return id_name_map.get(entitiy_id)
  elif typ == 'time':
    time_split = DATE_PARSE_RE.match(value['time'])
    if not time_split:
      return None
    year, month, day, hour, minute, second = map(int, time_split.groups())
    if day == 0:
      day = 1
    if month == 0:
      month = 1
    return '%04d-%02d-%02dT%02d:%02d:%02d' % (year, month, day, hour, minute, second)
  elif typ == 'quantity':
    return float(value['amount'])
  elif typ == 'monolingualtext':
    return value['text']
  elif typ == 'globecoordinate':
    lat = value.get('latitude')
    lng = value.get('longitude')
    if lat or lng:
      res = {'lat': lat, 'lng': lng}
      globe = value.get('globe', '').rsplit('/', 1)[-1]
      if globe != 'Q2' and globe in id_name_map:
        res['globe'] = globe
      if value.get('altitude'):
        res['altitude'] = value['altitude']
      return res

  return None


def main(dump, cursor, conn):
  """We do two scans:
     - first collect the id -> name / wikipedia title
     - then store the actual objects with a json property.
     The first step takes quite a bit of memory (5Gb) - could possibly be done using a temporary table in postgres.
  """
  id_name_map = {}
  if os.path.isfile('properties.json'):
      print('loading properties from file')
      id_name_map = json.load(open('properties.json'))
  else:
    c = 0
    skip = 0
    for line in subprocess.Popen(['bzcat'], stdin=open(dump, 'r'), stdout=subprocess.PIPE).stdout:
        d = parse_wikidata(line)
        if not d:
            print('Failed to parse', line[0])
            continue
        c += 1
        if c % 1000 == 0:
          print(c, skip)
        if d.get('sitelinks') and d['sitelinks'].get('enwiki'):
          value = d['sitelinks']['enwiki']['title']
        elif d['labels'].get('en'):
          value = id_name_map[d['id']] = d['labels']['en']['value']
        else:
          skip += 1
          continue
        id_name_map[d['id']] = value

    json.dump(id_name_map, open('properties.json', 'w'))

  wp_ids = set()
  c = 0
  rec = 0
  dupes = 0
  for line in subprocess.Popen(['bzcat'], stdin=open(dump, 'r'), stdout=subprocess.PIPE).stdout:
    d = parse_wikidata(line)
    if not d:
        continue
    c += 1
    if c % 1000 == 0:
      print(c, rec, dupes)
    if c % 10000 == 0:
      conn.commit()

    labels = [d['labels'][x]['value'] for x in d.get('labels', {})]
    sitelinks = [d.get('sitelinks')[x]['title'] for x in d.get('sitelinks', {})]

    wikipedia_id = d.get('sitelinks', {}).get('enwiki', {}).get('title')
    title = d['labels'].get('en', {}).get('value')
    description = d['descriptions'].get('en', {}).get('value')
    wikidata_id = d['id']
    properties = {}
    properties['sitelinks'] = d.get('sitelinks')
    properties['labels'] = d.get('labels')

    if wikipedia_id and title:
      # There are some duplicate wikipedia_id's in there. We could make wikidata_id the primary key
      # but that doesn't fix the underlying dupe
      if wikipedia_id in wp_ids:
        dupes += 1
        continue
      wp_ids.add(wikipedia_id)
      # Properties are mapped in a way where we create lists as values for wiki entities if there is more
      # than one value. For other types, we always pick one value. If there is a preferred value, we'll
      # pick that one.
      # Mostly this does what you want. For filtering on colors for flags it alllows for the query:
      #   SELECT title FROM wikidata WHERE properties @> '{"color": ["Green", "Red", "White"]}'
      # However, if you'd want all flags that have Blue in them, you'd have to check for just "Blue"
      # and also ["Blue"].
      for prop_id, claims in d['claims'].items():
        prop_name = id_name_map.get(prop_id)
        if prop_name:
          ranks = defaultdict(list)
          for claim in claims:
            mainsnak = claim.get('mainsnak')
            if mainsnak:
              data_value = map_value(mainsnak.get('datavalue'), id_name_map)
              if data_value:
                lst = ranks[claim['rank']]
                if mainsnak['datavalue'].get('type') != 'wikibase-entityid':
                  del lst[:]
                lst.append(data_value)
          for r in 'preferred', 'normal', 'depricated':
            value = ranks[r]
            if value:
              if len(value) == 1:
                value = value[0]
              else:
                value = sorted(value)
              properties[prop_name] = value
              break


      rec += 1
      cursor.execute('INSERT INTO import.wikidata (wikipedia_id, title, wikidata_id, labels, sitelinks, description, properties) VALUES (%s, %s, %s, %s, %s, %s, %s)',
              (wikipedia_id, title, wikidata_id, extras.Json(labels), extras.Json(sitelinks), description, extras.Json(properties)))


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Import wikidata into postgress')
  parser.add_argument('postgres', type=str,
                      help='postgres connection string')
  parser.add_argument('dump', type=str,
                      help='BZipped wikipedia dump')

  args = parser.parse_args()
  conn, cursor = setup_db(args.postgres)

  main(args.dump, cursor, conn)


  cursor.execute('CREATE INDEX wd_wikidata_wikidata_id ON import.wikidata(wikidata_id)')
  cursor.execute('CREATE INDEX wd_wikidata_properties ON import.wikidata USING gin(properties)')
  cursor.execute('CREATE INDEX wd_wikidata_labels ON import.wikidata USING gin(labels)')
  cursor.execute('CREATE INDEX wd_wikidata_sitelinks ON import.wikidata USING gin(sitelinks)')
  conn.commit()
  cursor.execute('DROP TABLE IF EXISTS import.geo')
  cursor.execute('CREATE TABLE import.geo ('
                 '    wikidata_id TEXT,'
                 '    geometry geometry(POINT, 4326),'
                 '    CONSTRAINT wd_geo_unique UNIQUE (wikidata_id)'
                 ')')
  cursor.execute('INSERT into import.geo (wikidata_id, geometry) '
                'SELECT wikidata_id, ST_SETSRID(ST_MAKEPOINT((properties->\'coordinate location\'->>\'lng\')::DECIMAL, '
                '(properties->\'coordinate location\'->>\'lat\')::DECIMAL), 4326) AS geometry '
                'FROM import.wikidata WHERE properties->\'coordinate location\' IS NOT NULL;'
                )
  cursor.execute('CREATE INDEX wd_geo_geometry ON import.geo USING gist (geometry) TABLESPACE pg_default;')
  conn.commit()
  cursor.execute('DROP TABLE IF EXISTS import.labels')
  cursor.execute('CREATE TABLE import.labels ('
                 '    wikidata_id TEXT,'
                 '    label TEXT,'
                 '    CONSTRAINT wd_label_unique UNIQUE (wikidata_id, label)'
                 ')'
                 )
  cursor.execute('INSERT INTO import.labels (wikidata_id, label) SELECT wikidata_id, jsonb_array_elements_text(labels) '
                 'FROM import.wikidata ON CONFLICT DO NOTHING;'
                )
  cursor.execute('CREATE INDEX wd_wikidata_labels_trgm ON import.labels USING gist (label COLLATE pg_catalog."default" gist_trgm_ops) TABLESPACE pg_default;')
  conn.commit()

  cursor.execute('DROP TABLE IF EXISTS import.instance')
  cursor.execute('CREATE TABLE import.instance ('
                 '    wikidata_id TEXT,'
                 '    instance_of TEXT,'
                 '    CONSTRAINT wd_instance_unique UNIQUE (wikidata_id)'
                 ');'
                 )
  cursor.execute('INSERT INTO import.instance (wikidata_id, instance_of) '
                 'SELECT wikidata_id, lower(properties->>\'instance of\')::jsonb '
                 'FROM import.wikidata WHERE jsonb_typeof(properties->\'instance of\') = \'array\';'
                 )
  cursor.execute('INSERT INTO import.instance (wikidata_id, instance_of) '
                 'SELECT wikidata_id, jsonb_build_array(lower(properties->>\'instance of\')) '
                 'FROM import.wikidata WHERE jsonb_typeof(properties->\'instance of\') = \'string\';'
                )
  cursor.execute('CREATE INDEX wd_wikidata_instance ON import.instance USING gist (instance_of COLLATE pg_catalog."default" gist_trgm_ops) TABLESPACE pg_default;')

  conn.commit()
