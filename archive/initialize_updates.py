#!/usr/bin/env python

"""This script automatically generates a data scan report.

For usage information, see `quick_guide.txt`.
"""

# from __future__ import print_function, division, unicode_literals
import os
import sys
import json
from io import open
from fnmatch import filter

import pandas as pd
import MySQLdb

from data_error import DataError


DEFAULT_CONFIG = {
    'client': None,
    'client_dir': None,
    'client_id': None,
    'source_id': None,
    'current_dir': None,
    'refresh': None,
    'old_dirs': [],
    'new_dirs': [],
    'database': None,
    'recipients': None,
    'sender_email': os.getenv('SERVICE_EMAIL_USER'),
    'sender_email_password': os.getenv('SERVICE_EMAIL_PASS'),
    'db_host': 'mysql.test',
    'db_user': os.getenv('DB_TEST_USER'),
    'db_pass': os.getenv('DB_TEST_PASS'),
    'datetime_format': '%Y-%m-%d %I:%M%p',
    }

def set_config(**kwargs):
    """Modify `DEFAULT_CONFIG` using `CLIENT_CONFIG.json`"""

    config = DEFAULT_CONFIG
    config.update(kwargs)

    # Open the client config file if possible
    ## CLIENT_CONFIG_portal_test2 instead of Client config.
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'CLIENT_CONFIG_portal_test2.json')
    client_config = {}
    if os.path.exists(config_path):
        with open(config_path, mode='r') as config_file:
            client_config = json.load(config_file).get(config['client'], {})
    else:
        print("WARNING. Unable to access CLIENT_CONFIG.json at {}."
              "Continuing with default settings.".format(config_path)
        )

    # We NEED to have the client name before continuing
    if config['client'] is None:
        raise DataError('Need to specify a client name!')

    # Determine the client's primary directory or throw an Error
    if config['client_dir'] is None:
        config['client_dir'] = client_config.get(
            'client_dir', os.path.join('/home/customers/', config['client']))
    if not os.path.exists(config['client_dir']):
        raise OSError('Could not find the client directory!\n{}'.format(config['client_dir']))

    # Determine the client's appropriate 'current' directory or throw an Error
    if config['current_dir'] is None:
        config['current_dir'] = client_config.get(
            'current_dir', os.path.join(config['client_dir'], 'current/'))
    if not os.path.exists(config['current_dir']):
        raise OSError('Could not find the current directory!\n{}'.format(config['current_dir']))

    # See what else can be found in client_config
    if config['database'] is None:
        config['database'] = client_config.get('database', None)

    ## HH update addition
    if config['client_id'] is None:
        config['client_id'] = client_config.get('client_id', None)

    # Data Source
    if config['source_id'] is None:
        config['source_id'] = client_config.get('source_id', None)
    # ... can add more parameters here
    return config


def con_db(config):
    """Connects to database."""
    # Try to connect to the database
    db_conn = None
    try:
        db_name = config.get('database')
        if db_name is None:
            # db_name = '{}_staging'.format(config['client'])
            db_name = 'portal_test2'
        db_conn = MySQLdb.connect(host=config['db_host'],
                                  user=config['db_user'],
                                  passwd=config['db_pass'],
                                  port=3306,
                                  db=db_name)
        print('Successful connection to db: '+str(db_name))
    except:
        print('Could not connect to the database! Continuing without it...')

    # Get the tables that will be helpful for the data scan
    if db_conn is not None:
        dim_issue = pd.read_sql_query('SELECT * FROM `dim_issue`;', con=db_conn)
        dim_file = pd.read_sql_query('SELECT * FROM `dim_file`;', con=db_conn)
        dim_file_filename = pd.read_sql_query(
            'SELECT * FROM `dim_file_filename`;', con=db_conn)
    else:
        _here = os.path.dirname(os.path.abspath(__file__))
        dim_issue = pd.read_csv(os.path.join(_here, 'setup/dim_issue.csv'))
        dim_file = pd.read_csv(os.path.join(_here, 'setup/dim_file.csv'))
        dim_file_filename = pd.read_csv(os.path.join(_here, 'setup/dim_file_filename.csv'))

    return db_conn, dim_issue, dim_file, dim_file_filename


def find_custom_scan(config):
    """ """
    # If the client has their own custom data scan, we want to run that one
    # instead of the "master" data scan.

    # First, determine the path of the correct scan to run
    data_scan_path = os.path.abspath(__file__)
    for root, subdirectories, files in os.walk(config['client_dir']):
        for filename in filter(files, 'datascan.py'):
            data_scan_path = os.path.join(root, filename)
            break

    # If this is true, then we are running the wrong file and need to switch
    # NOTE: os.path.realpath is necessary because /home/customers is a symlink
    if os.path.realpath(__file__) != os.path.realpath(data_scan_path):

        # NOTE: The mechanism for importing a module from a specific location is
        #       completely different between Python 2 and 3. I tried to make
        #       this compatible with both, but I have not tested it on 3. If you
        #       are trying to run this file on Python 3, you may need to fix a
        #       bug or error in this section.
        try:
            if sys.version_info.major == 3 and sys.version_info.minor >= 5:
                from importlib.util import spec_from_file_location, \
                    module_from_spec
                spec = spec_from_file_location('custom_datascan',
                                               data_scan_path)
                mod = module_from_spec(spec)
                spec.loader.exec_module(mod)
            elif sys.version_info.major == 2:
                from imp import load_source
                load_source('custom_datascan', data_scan_path)

            # Now import that custom data scan, run it, and exit
            import custom_datascan
            return True, custom_datascan

        except ImportError:
            print('Found custom data scan for {}, but could not run it. '
                  'Defaulting to the generic "master" data scan.'
                  .format(config['client']))
            return False, None

    return False, None
