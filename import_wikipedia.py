#!/bin/python3

import argparse
import subprocess
import xml.sax

import mwparserfromhell
import psycopg2
import re

CAT_PREFIX = 'Category:'
INFOBOX_PREFIX = 'infobox '

RE_GENERAL = re.compile('(.+?)(\ (in|of|by)\ )(.+)')

def setup_db(connection_string):
  conn = psycopg2.connect(connection_string)
  cursor = conn.cursor()
  cursor.execute('CREATE SCHEMA IF NOT EXISTS wp;')
  cursor.execute('DROP TABLE IF EXISTS wp.wikipedia')
  cursor.execute('CREATE TABLE wp.wikipedia ('
                 '    id integer,'
                 '    title TEXT PRIMARY KEY,'
                 '    infobox TEXT,'
                 '    wikitext TEXT,'
                 '    templates TEXT[] NOT NULL DEFAULT \'{}\','
                 '    categories TEXT[] NOT NULL DEFAULT \'{}\','
                 '    general TEXT[] NOT NULL DEFAULT \'{}\''
                 ')')

  return conn, cursor


def make_tags(iterable):
  return list(set(x.strip().lower() for x in iterable if x and len(x) < 256))


def strip_template_name(name):
  return name.strip_code().strip()


def extact_general(category):
  m = RE_GENERAL.match(category)
  if m:
    return m.groups()[0]
  return None


class WikiXmlHandler(xml.sax.handler.ContentHandler):
  def __init__(self, cursor):
    xml.sax.handler.ContentHandler.__init__(self)
    self._db_cursor = cursor
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
        wikicode = mwparserfromhell.parse(self._values['text'])
        templates = make_tags(strip_template_name(template.name) for template in wikicode.filter_templates())
        infobox = None
        for template in templates:
          if template.startswith(INFOBOX_PREFIX):
            infobox = template[len(INFOBOX_PREFIX):]
            break
        if len(infobox or '') > 1024 or len(self._values['title']) > 1024:
          print('Too long')
          raise mwparserfromhell.parser.ParserError('too long')
        categories = make_tags(l.title[len(CAT_PREFIX):] for l in wikicode.filter_wikilinks() if l.title.startswith(CAT_PREFIX))
        general = make_tags(extact_general(x) for x in categories)
        # even though we shouldn't get dupes, sometimes wikidumps are faulty:
        # print(self._values['title'], self._values['id'], infobox, templates, categories, general);
        self._db_cursor.execute('INSERT INTO wp.wikipedia (id, title, infobox, wikitext, templates, categories, general) VALUES (%s, %s, %s, %s, %s, %s, %s)  ON CONFLICT DO NOTHING',
                                (self._values['id'], self._values['title'], infobox, self._values['text'], templates, categories, general))
        self._count += 1
        if self._count % 100000 == 0:
          print(self._count)
      except mwparserfromhell.parser.ParserError:
        print('mwparser error for:', self._values['title'])
      self.reset()

  def characters(self, content):
    if self._state:
      self._buffer.append(content)


def main(dump, cursor):
  parser = xml.sax.make_parser()
  parser.setContentHandler(WikiXmlHandler(cursor))
  for line in subprocess.Popen(['bzcat'], stdin=open(dump, 'r'), stdout=subprocess.PIPE).stdout:
    try:
      parser.feed(line)
    except StopIteration:
      break


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Import wikipedia into postgress')
  parser.add_argument('postgres', type=str,
                      help='postgres connection string')
  parser.add_argument('dump', type=str,
                      help='BZipped wikipedia dump')

  args = parser.parse_args()
  conn, cursor = setup_db(args.postgres)

  main(args.dump, cursor)
  cursor.execute('CREATE INDEX wp_wikipedia_infobox ON wp.wikipedia(infobox)')
  cursor.execute('CREATE INDEX wp_wikipedia_templates ON wp.wikipedia USING gin(templates)')
  cursor.execute('CREATE INDEX wp_wikipedia_categories ON wp.wikipedia USING gin(categories)')
  cursor.execute('CREATE INDEX wp_wikipedia_general ON wp.wikipedia USING gin(general)')

  conn.commit()

