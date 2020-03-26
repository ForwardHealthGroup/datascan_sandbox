#!/usr/bin/env python

# HH 3/2/2020

import os
import sys
import json
import io # open
from fnmatch import filter

import pandas as pd
import MySQLdb

# from data_error import DataError

DEFAULT_CONFIG = {
    'client': None,
    'client_dir': None,
    'client_id': None,
    'normalization_script': None,
    'source_id': None,
    'current_dir': None,
    'refresh': None,
    'old_dirs': [],
    'new_dirs': [],
    'database': None,
    # 'recipients': None,
    # 'sender_email': os.getenv('SERVICE_EMAIL_USER'),
    # 'sender_email_password': os.getenv('SERVICE_EMAIL_PASS'),
    'db_host': 'mysql.test',
    'db_user': os.getenv('DB_TEST_USER'),
    'db_pass': os.getenv('DB_TEST_PASS'),
    'datetime_format': '%Y-%m-%d %I:%M%p',
    }

def con_db(config):
    """Connects to database."""
    # Try to connect to the database
    db_conn = None
    try:
        db_name = config['database']
        if db_name is None:
            # db_name = '{}_staging'.format(config['client'])
            db_name = 'portal_test2'
        db_conn = MySQLdb.connect(host=config['db_host'],
                                  user=config['db_user'],
                                  passwd=config['db_pass'],
                                  port=3306,
                                  db=db_name)
        print('Successful connection to db: '+str(db_name))
        return db_conn
    except:
        print('Could not connect to the database! Continuing without it...')

def find_old_data(config, db_conn):
    # Get the tables that will be helpful for the data scan
    # Client ID ?
    if db_conn is not None:
        dim_issue = pd.read_sql_query('SELECT * FROM `dim_issue`;', con=db_conn)
        dim_file = pd.read_sql_query('SELECT * FROM `dim_file`;', con=db_conn) # CLIENT ID
        dim_file_filename = pd.read_sql_query(
            'SELECT * FROM `dim_file_filename` where client_id = {};'.format(config['client_id']), con=db_conn)
        fact_dscan_previous = pd.read_sql_query(
            'SELECT * FROM `fact_datascan` where client_id = {};'.format(config['client_id']), con=db_conn)
        fact_dscan_refresh = pd.read_sql_query(
            'SELECT * FROM `fact_datascan_refresh` where client_id = {};'.format(config['client_id']), con=db_conn)
    else:
        _here = os.path.dirname(os.path.abspath(__file__))
        dim_issue = pd.read_csv(os.path.join(_here, 'setup/dim_issue.csv'))
        dim_file = pd.read_csv(os.path.join(_here, 'setup/dim_file.csv'))
        dim_file_filename = pd.read_csv(os.path.join(_here, 'setup/dim_file_filename.csv'))
    print()

    return dim_issue, dim_file, dim_file_filename, fact_dscan_previous, fact_dscan_refresh
