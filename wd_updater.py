#!/bin/python3

import argparse
import subprocess
import xml.sax

from collections import defaultdict
import mwparserfromhell
import psycopg2
from psycopg2 import extras
import re
import json
import os


DATE_PARSE_RE = re.compile(r'([-+]?[0-9]+)-([0-9][0-9])-([0-9][0-9])T([0-9][0-9]):([0-9][0-9]):([0-9][0-9])Z?')


def setup_db(connection_string):
  conn = psycopg2.connect(connection_string)
  cursor = conn.cursor()

  return conn, cursor


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


def parse_props(d, id_name_map):
  if type(d) != dict:
    return None, None, None, None, None, None
  wikidata_id = d.get('id')
  labels = None
  title = None
  try:
    labels = [d['labels'][x]['value'] for x in d.get('labels', {})]
    title = d['labels'].get('en', {}).get('value')
  except:
    pass

  sitelinks = None
  wikipedia_id = None
  try:
    sitelinks = [d.get('sitelinks')[x]['title'] for x in d.get('sitelinks', {})]
    wikipedia_id = d.get('sitelinks', {}).get('enwiki', {}).get('title')
  except:
    return None, None, None, None, None, None

  description = None
  try:
    description = d['descriptions'].get('en', {}).get('value')
  except:
    pass

  properties = {}
  properties['sitelinks'] = d.get('sitelinks')
  properties['labels'] = d.get('labels')

  if wikipedia_id and title and type(d['claims']) == dict:
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

    return wikipedia_id, title, labels, sitelinks, description, properties

  return None, None, None, None, None, None


def update_DB(wikipedia_id, title, wikidata_id, labels, sitelinks, description, properties, conn, cursor, schema):
  cursor.execute('INSERT INTO %s.wikidata (wikipedia_id, title, wikidata_id, labels, sitelinks, description, properties)' % schema +
                 'VALUES (%s, %s, %s, %s, %s, %s, %s)'
                 'ON CONFLICT (wikidata_id) DO UPDATE SET title = EXCLUDED.title, labels = EXCLUDED.labels, sitelinks = EXCLUDED.sitelinks,'
                 'description = EXCLUDED.description, properties = EXCLUDED.properties;',
                (wikipedia_id, title, wikidata_id, extras.Json(labels), extras.Json(sitelinks), description, extras.Json(properties)))

  cursor.execute('INSERT into %s.geo (wikidata_id, geometry) ' % schema +
                'SELECT wikidata_id, ST_SETSRID(ST_MAKEPOINT((properties->\'coordinate location\'->>\'lng\')::DECIMAL, '
                '(properties->\'coordinate location\'->>\'lat\')::DECIMAL), 4326) AS geometry '
                'FROM %s.wikidata ' % schema +
                'WHERE properties->\'coordinate location\' IS NOT NULL AND wikidata_id = %s ' % wikidata_id +
                'ON CONFLICT (wikidata_id) DO UPDATE SET geometry = EXCLUDED.geometry;'
                )

  cursor.execute('INSERT INTO %s.labels (label, wikidata_id) SELECT distinct(jsonb_array_elements_text(labels)), wikidata_id ' % schema +
                 'FROM %s.wikidata ' % schema +
                 'WHERE wikidata_id = %s ON CONFLICT (wikidata_id, label) DO NOTHING;' % wikidata_id
                )

  cursor.execute('INSERT INTO %s.instance (wikidata_id, instance_of) ' % schema +
                 'SELECT wikidata_id, lower(properties->>\'instance of\')::jsonb '
                 'FROM %s.wikidata ' % schema +
                 'WHERE jsonb_typeof(properties->\'instance of\') = \'array\' AND wikidata_id = %s ' % wikidata_id +
                 'ON CONFLICT (wikidata_id) DO UPDATE SET instance_of = EXCLUDED.instance_of;'
                )
  cursor.execute('INSERT INTO %s.instance (wikidata_id, instance_of) ' % schema +
                 'SELECT wikidata_id, jsonb_build_array(lower(properties->>\'instance of\')) '
                 'FROM %s.wikidata ' % schema +
                 'WHERE jsonb_typeof(properties->\'instance of\') = \'string\' AND wikidata_id = %s ' % wikidata_id +
                 'ON CONFLICT (wikidata_id) DO UPDATE SET instance_of = EXCLUDED.instance_of;'
                )


class WikiXmlHandler(xml.sax.handler.ContentHandler):
  def __init__(self, cursor, conn, schema, id_name_map):
    xml.sax.handler.ContentHandler.__init__(self)
    self._db_cursor = cursor
    self._db_conn = conn
    self._db_schema = schema
    self._id_name_map = id_name_map
    self._count = 0
    self.reset()


  def reset(self):
    self._buffer = []
    self._state = None
    self._values = {}


  def startElement(self, name, attrs):
    if name in ('title', 'text', 'id'):
      self._state = name


  def endElement(self, name):
    if name == self._state:
      if name not in self._values: self._values[name] = ''.join(self._buffer)
      self._state = None
      self._buffer = []

    if name == 'page':
      try:
        qcode = self._values['title']
        data = self._values['text']
        data = json.loads(data)

        wikipedia_id, title, labels, sitelinks, description, properties = parse_props(data, self._id_name_map)
        # print(wikipedia_id, title, qcode, description)
        if wikipedia_id:
            update_DB(wikipedia_id, title, qcode, labels, sitelinks, description, properties, self._db_conn, self._db_cursor, self._db_schema)

        self._count += 1
        if self._count % 100000 == 0:
            print(self._count, qcode)
            self._db_conn.commit()
      except mwparserfromhell.parser.ParserError:
        print('mwparser error for:', self._values['title'])
      except ValueError:
        # print('failed to parse json', qcode)
        pass
      self.reset()


  def characters(self, content):
    if self._state:
      self._buffer.append(content)


def parse(dump, props_dir, conn, cursor, schema):

  id_name_map = {}
  # this file is required for updates
  # it is created by main WD import script during first time dump import
  props_path = props_dir + '/properties.json'
  if os.path.isfile(props_path):
      print('loading properties from file')
      id_name_map = json.load(open(props_path))
  else:
      print('ERROR: properties.json file is missing')
      exit(-1)

  parser = xml.sax.make_parser()
  xmlHandler = WikiXmlHandler(cursor, conn, schema, id_name_map)
  parser.setContentHandler(xmlHandler)

  for line in subprocess.Popen(['bzcat'], stdin=open(dump, 'r'), stdout=subprocess.PIPE).stdout:
    try:
      parser.feed(line)
    except StopIteration:
      break


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Import wikidata incremental dump into existing postgress DB')
  parser.add_argument('postgres', type=str, help='postgres connection string')
  parser.add_argument('schema', type=str, help='DB schema containing wikidata tables')
  parser.add_argument('dump', type=str, help='BZipped wikipedia dump')

  args = parser.parse_args()
  print('Setup db')
  conn, cursor = setup_db(args.postgres)

  print('Parsing...')
  parse(args.dump, '', conn, cursor, args.schema)

  conn.commit()
