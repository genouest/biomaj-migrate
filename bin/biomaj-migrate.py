#!/usr/bin/python

import os
import sys
import argparse
import humanfriendly
import datetime
import time
import logging
import re
import fnmatch
import mysql.connector
from mysql.connector import errorcode

from biomaj.bank import Bank
from biomaj.config import BiomajConfig
from biomaj.workflow import UpdateWorkflow, RemoveWorkflow, Workflow


def migrate_bank(cur, bank, history=False):
    """
    Migrate old MySQL information to the new MongoDB
    :param cur: MySQL cursor
    :type cur: MySQL cursor
    :param bank: Bank name
    :type bank: String
    :param history: Keep bank history
    :type history: Boolean, default False
    :return:
    """
    query = "SELECT p.path, p.session, p.creation, p.remove, p.size, u.updateRelease, s.logfile, s.status "
    query += "FROM productionDirectory p "
    query += "JOIN updateBank u on u.idLastSession = p.session JOIN bank b on b.idbank = p.ref_idbank "
    query += "LEFT JOIN session s ON s.idsession = u.idLastSession "
    query += "WHERE b.name='" + str(bank) + "' "
    if not history:
        query += "AND p.remove IS NULL "
    query += "ORDER BY p.creation ASC"

    cur.execute(query)
    banks = []
    not_prod = {}
    for row in cur.fetchall():
        banks.append({
            'path': row[0],
            'session': row[1],
            'creation': row[2],
            'remove': row[3],
            'size': humanfriendly.parse_size(row[4].replace(',', '.')),
            'release': row[5],
            'remoterelease': row[5],
            'logfile': row[6],
            'status': row[7]
        })
        # If we want to keep the history we will need to delete from 'production',
        # session(s) which have been tagged as 'removed', so row[3] is a date
        if row[3] and history:
            sess = time.mktime(datetime.datetime.strptime(str(row[2]), "%Y-%m-%d %H:%M:%S").timetuple())
            not_prod[sess] = True

    for prod in banks:

        pathelts = prod['path'].split('/')
        release_dir = pathelts[len(pathelts) - 1]
        prod['prod_dir'] = release_dir
        pattern = re.compile(".*__(\d+)$")
        relmatch = pattern.match(release_dir)
        if relmatch:
            prod['release'] = prod['release'] + '__' + relmatch.group(1)

        # We create the session id from the productionDirectory.creation field
        session_id = time.mktime(datetime.datetime.strptime(str(prod['creation']), "%Y-%m-%d %H:%M:%S").timetuple())
        session_exists = False
        b = Bank(bank, no_log=True)

        # We check we did not already imported this session into the database
        for s in b.bank['sessions']:
            if s['id'] == session_id:
                logging.warn('Session already imported: ' + b.name + ':' + str(prod['creation']))
                session_exists = True
                break  # No need to continue
        if session_exists:
            continue

        for p in b.bank['production']:
            if p['release'] == prod['release']:
                logging.warn('Prod release already imported: ' + b.name + ":" + p['release'])
                continue
        b.load_session(UpdateWorkflow.FLOW)
        b.session.set('prod_dir', prod['prod_dir'])
        b.session.set('action', 'update')
        b.session.set('release', prod['release'])
        b.session.set('remoterelease', prod['remoterelease'])
        # Biomaj >= 3.0.14 introduce new field in sessions 'workflow_status'
        # Set production size from productionDirectory.size field
        b.session.set('workflow_status', True if prod['status'] else False)
        b.session.set('fullsize', prod['size'])
        b.session._session['status'][Workflow.FLOW_OVER] = True
        b.session._session['update'] = True
        # We set the session.id (timestamp) with creation field from productionDirectory table
        b.session.set('id', session_id)
        b.save_session()
        # We need set update the field 'last_update_time' to the time the bank has been created
        # because 'save_session' set this value to the time it is called
        b.banks.update({'name': b.name, 'sessions.id': session_id},
                       {'$set': {'sessions.$.last_update_time': session_id}})
        # Keep trace of the logfile. We need to do a manual update
        if prod['logfile'] and os.path.exists(prod['logfile']):
            b.banks.update({'name': b.name, 'sessions.id': session_id},
                           {'$set': {'sessions.$.log_file': prod['logfile']}})
        # Due to the way save_session set also the production, to exclude last session
        # from the production entries, we need to loop over each production entries
        if history:
            # If we want to keep history, we also need to keep trace of the time session/update has been deleted
            # from the database/disk
            if prod['remove']:
                removed = time.mktime(datetime.datetime.strptime(str(prod['remove']), "%Y-%m-%d %H:%M:%S").timetuple())
                b.banks.update({'name': b.name, 'sessions.id': session_id},
                               {'$set': {'sessions.$.deleted': removed}})
            for production in b.bank['production']:
                if production['session'] in not_prod:
                    b.banks.update({'name': b.name, 'production.session': production['session']},
                                   {'$pull': {'production': {'session': production['session']}}})

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
                f = open(os.path.join(prod['path'], 'listingv1' + fileExtension), 'w')
                listing = "{\"files\": [], \"name\": \"" + fileExtension.replace('.', '') + "\"," + listing + "}"
                f.write(listing)
                f.close()
        # Current link?
        pathelts = prod['path'].split('/')
        del pathelts[-1]
        current_link = os.path.join('/'.join(pathelts), 'current')
        if os.path.lexists(current_link):
            b.bank['current'] = b.session._session['id']
            b.banks.update({'name': b.name},
                           {'$set': {'current': b.session._session['id']}})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest="config", help="Biomaj3 Configuration file")
    parser.add_argument('-o', '--oldconfig', dest="oldconfig", help="Old configuration file")
    parser.add_argument('-u', '--user', dest="user", help="MySQL user to override global properties")
    parser.add_argument('-p', '--password', dest="password", help="MySQL password to override global properties")
    parser.add_argument('-l', '--host', dest="host", help="MySQL host to override global properties")
    parser.add_argument('-d', '--database', dest="database", help="MySQL database to override global properties")
    parser.add_argument('-H', '--keep_history', dest="history", action="store_true", default=False,
                        help="Keep bank history, not only production")

    args = parser.parse_args()

    biomajconfig = {}
    banks = []
    with open(args.oldconfig, 'r') as old:
        for line in old:
            vals = line.split('=')
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

    if not os.path.dirname(data_dir) == os.path.dirname(BiomajConfig.global_config.get('GENERAL', 'data.dir')):
        logging.error('Data dirs are different, please use the same data dirs')
        sys.exit(1)

    prop_files = []
    for root, dirnames, filenames in os.walk(db_properties_dir):
        for filename in fnmatch.filter(filenames, '*.properties'):
            if filename != 'global.properties':
                prop_files.append(os.path.join(root, filename))

    if not os.path.exists(BiomajConfig.global_config.get('GENERAL', 'conf.dir')):
        os.makedirs(BiomajConfig.global_config.get('GENERAL', 'conf.dir'))
    for prop_file in prop_files:

        propbankconfig = {}
        with open(prop_file, 'r') as old:
            for line in old:
                vals = line.split('=')
                if len(vals) > 1:
                    propbankconfig[vals[0].strip()] = vals[1].strip()

        newpropfile = os.path.join(BiomajConfig.global_config.get('GENERAL', 'conf.dir'), os.path.basename(prop_file))
        newprop = open(newpropfile, 'w')
        # logging.warn("manage "+prop_file+" => "+newpropfile)
        newprop.write("[GENERAL]\n")
        with open(prop_file, 'r') as props:
            for line in props:
                if not (line.startswith('*') or line.startswith('/*')):
                    # Replace config variables with new syntax ${xx} => %(xx)s, not other env variables
                    pattern = re.compile("\$\{([a-zA-Z0-9-_.]+)\}")
                    varmatch = pattern.findall(line)
                    if varmatch:
                        for match in varmatch:
                            if match in biomajconfig or match in propbankconfig:
                                line = line.replace('${' + match + '}', '%(' + match + ')s')
                newprop.write(line.replace('\\\\', '\\').replace('db.source', 'depends'))
        newprop.close()
        b = Bank(os.path.basename(prop_file).replace('.properties', ''), no_log=True)
        banks.append(b.name)

    # database.url=jdbc\:mysql\://genobdd.genouest.org/biomaj_log
    vals = biomajconfig['database.url'].split('/')
    urllen = len(vals)
    db_name = vals[urllen - 1]
    if args.database:
        db_name = args.database
    db_host = vals[urllen - 2]
    if args.host:
        db_host = args.host
    db_user = biomajconfig['database.login']
    if args.user:
        db_user = args.user
    db_password = biomajconfig['database.password']
    if args.password:
        db_password = args.password

    try:
        cnx = mysql.connector.connect(host=db_host, database=db_name,
                                      user=db_user, password=db_password)
        cur = cnx.cursor()
        cur.execute("SELECT name FROM bank")
        for row in cur.fetchall():
            migrate_bank(cur, row[0], history=args.history)
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Wrong username or password: %s" % error.msg)
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist: %s" % error.msg)
        else:
            print("Unknown error: %s" % error)
    finally:
        cnx.close()


if __name__ == '__main__':
    main()
