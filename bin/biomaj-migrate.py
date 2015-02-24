#!/usr/bin/python

import os,sys
import argparse
import pkg_resources
import logging
import re
import fnmatch

import MySQLdb

from biomaj.bank import Bank
from biomaj.config import BiomajConfig
from biomaj.workflow import UpdateWorkflow, RemoveWorkflow, Workflow

def migrate_bank(cur, bank):
    cur.execute("SELECT path, session, creation, remove from productionDirectory WHERE ref_idbank="+str(bank['dbid']))
    bank['productionDirectory'] = []
    for row in cur.fetchall():
      if not row[3]:
        bank['productionDirectory'].append({
          'session': row[1], 'path': row[0],
          'creation': row[2]
      })
    for prod in bank['productionDirectory']:
      if prod['session']:
        cur.execute("SELECT ref_idupdateBank from session WHERE idsession="+str(prod['session']))
        for row in cur.fetchall():
          prod['ref_idupdateBank'] = row[0]
    for prod in bank['productionDirectory']:
      if 'ref_idupdateBank' in prod:
        cur.execute("SELECT updateRelease from updateBank where idupdateBank="+str(prod['ref_idupdateBank']))
        for row in cur.fetchall():
          prod['release'] = row[0]
          prod['remoterelease'] = row[0]
          pathelts = prod['path'].split('/')
          release_dir = pathelts[len(pathelts)-1]
          prod['prod_dir'] = release_dir
          pattern = re.compile(".*__(\d+)$")
          relmatch = pattern.match(release_dir)
          if relmatch:
            prod['release'] = prod['release']+'__'+relmatch.group(1)
        b = Bank(bank['name'], no_log=True)
        prod_present = False
        for p in b.bank['production']:
          if p['release'] == prod['release']:
            logging.warn('Prod release already imported: '+b.name+":"+p['release'])
            continue
        b.load_session(UpdateWorkflow.FLOW)
        b.session.set('action','update')
        b.session.set('release', prod['release'])
        b.session.set('remoterelease', prod['remoterelease'])
        b.session._session['status'][Workflow.FLOW_OVER] = True
        b.session._session['update'] = True
        b.save_session()
        # Listing files ?
        root_files = []
        if os.path.exists(prod['path']):
          root_files = os.listdir(prod['path'])
        for root_file in root_files:
          if root_file.startswith('listing.'):
            fileName, fileExtension = os.path.splitext(root_file)
            f = open(os.path.join(prod['path'], root_file), 'r')
            listing = f.read()
            f.close()
            f = open(os.path.join(prod['path'], 'listingv1'+fileExtension), 'w')
            listing = "{\"files\": [], \"name\": \""+fileExtension.replace('.','')+"\"," +listing+"}"
            f.write(listing)
            f.close()
        # Current link?
        pathelts = prod['path'].split('/')
        del pathelts[-1]
        current_link = os.path.join('/'.join(pathelts),'current')
        if os.path.lexists(current_link):
          b.bank['current'] = b.session._session['id']
          b.banks.update({'name': b.name},
                      {
                      '$set': {'current': b.session._session['id']}
                      })

def main():

  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--config', dest="config",help="Biomaj3 Configuration file")
  parser.add_argument('-o', '--oldconfig', dest="oldconfig",help="Old configuration file")
  parser.add_argument('-u', '--user', dest="user", help="MySQL user to override global properties")
  parser.add_argument('-p', '--password', dest="password", help="MySQL password to override global properties")
  parser.add_argument('-l', '--host', dest="host", help="MySQL host to override global properties")
  parser.add_argument('-d', '--database', dest="database", help="MySQL database to override global properties")


  args = parser.parse_args()

  biomajconfig = {}
  banks = []
  with open(args.oldconfig,'r') as old:
    for line in old:
      vals = line.split('=')
      key = None
      if len(vals) > 1:
        biomajconfig[vals[0].strip()] = vals[1].strip()

  BiomajConfig.load_config(args.config, allow_user_config=False)
  db_properties_dir = os.path.dirname(args.oldconfig)
  if db_properties_dir == os.path.dirname(args.config):
    logging.error("Bank properties use the same directory, please use a different conf.dir")
    sys.exit(1)

  data_dir = biomajconfig['data.dir']
  if data_dir.endswith('/'):
    data_dir = data_dir[:-1]

  if not os.path.dirname(data_dir) == os.path.dirname(BiomajConfig.global_config.get('GENERAL','data.dir')):
    logging.error('Data dirs are different, please use the same data dirs')
    sys.exit(1)

  prop_files = []
  for root, dirnames, filenames in os.walk(db_properties_dir):
    for filename in fnmatch.filter(filenames, '*.properties'):
      if filename != 'global.properties':
        prop_files.append(os.path.join(root, filename))

  if not os.path.exists(BiomajConfig.global_config.get('GENERAL','conf.dir')):
    os.makedirs(BiomajConfig.global_config.get('GENERAL','conf.dir'))
  for prop_file in prop_files:
    newpropfile = os.path.join(BiomajConfig.global_config.get('GENERAL','conf.dir'),os.path.basename(prop_file))
    newprop = open(newpropfile,'w')
    #logging.warn("manage "+prop_file+" => "+newpropfile)
    newprop.write("[GENERAL]\n")
    with open(prop_file,'r') as props:
      for line in props:
        if not (line.startswith('*') or  line.startswith('/*')):
          newprop.write(line.replace('\\\\','\\').replace('db.source','depends'))
    newprop.close()
    b = Bank(os.path.basename(prop_file).replace('.properties',''),no_log=True)
    banks.append(b.name)

    #database.url=jdbc\:mysql\://genobdd.genouest.org/biomaj_log
  vals = biomajconfig['database.url'].split('/')
  urllen = len(vals)
  db_name = vals[urllen-1]
  if args.database:
    db_name = args.database
  db_host = vals[urllen -2]
  if args.host:
    db_host = args.host
  db_user = biomajconfig['database.login']
  if args.user:
    db_user = args.user
  db_password = biomajconfig['database.password']
  if args.password:
    db_password = args.password
  db = MySQLdb.connect(host=db_host, # your host, usually localhost
                   user=db_user, # your username
                    passwd=db_password, # your password
                    db=db_name) # name of the data base
  cur = db.cursor()
  oldbanks = {}
  cur.execute("SELECT name,idbank from bank")
  for row in cur.fetchall():
    oldbanks[row[0]] = { "dbid": row[1], "name": row[0] }
  for bank,value in oldbanks.iteritems():
    migrate_bank(cur, value)

if __name__ == '__main__':
    main()
