#!/usr/bin/env python


import pchc_standardize_filenames as pchc

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

def set_config(**kwargs):
    """Modify `DEFAULT_CONFIG` using `FILE_NORMAL_CONFIG.json`"""

    config = DEFAULT_CONFIG
    config.update(kwargs)

    # Open the normal file config if possible
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'FILE_NORMAL_CONFIG.json')
    client_config = {}
    if os.path.exists(config_path):
        with io.open(config_path, mode='r') as config_file:
            client_config = json.load(config_file).get(config['client'], {})
    else:
        print("WARNING. Unable to access FILE_NORMAL_CONFIG.json at {}."
              "Continuing with default settings.".format(config_path)
        )

    # We NEED to have the client name before continuing
    # if config['client'] is None:
        # raise DataError('Need to specify a client name!')

    # # Determine the client's primary directory or throw an Error
    # if config['client_dir'] is None:
    #     config['client_dir'] = client_config.get(
    #         'client_dir', os.path.join('/home/customers/', config['client']))
    # if not os.path.exists(config['client_dir']):
    #     raise OSError('Could not find the client directory!\n{}'.format(config['client_dir']))
    #
    # # Determine the client's appropriate 'current' directory or throw an Error
    # if config['current_dir'] is None:
    #     config['current_dir'] = client_config.get(
    #         'current_dir', os.path.join(config['client_dir'], 'current/'))
    # if not os.path.exists(config['current_dir']):
    #     raise OSError('Could not find the current directory!\n{}'.format(config['current_dir']))

    # See what else is in config.
    if config['database'] is None:
        config['database'] = client_config.get('database', None)

    if config['normalization_script'] is None:
        config['normalization_script'] = client_config.get('normalization_script', None)

    ## HH update addition
    if config['client_id'] is None:
        config['client_id'] = client_config.get('client_id', None)

    # Data Source
    if config['source_id'] is None:
        config['source_id'] = client_config.get('source_id', None)
    # ... can add more parameters here
    return config

def standardize_files(config):
    # accepts config tells this script which other script to run.
    if config['normalization_script'] == 'pchc_standardize_filenames':
        stdz_files = pchc.pchc_stand(config)
    # print(std_files)
    return(stdz_files)
    # has path . needs to use path within pchc_standardize_filenames

def files_as_df(config, stdz_files):
    path = config['new_dirs'][0]
    os.chdir(path)
    files = os.listdir(path)
    paths_to_file = []
    for file in files:
        file_path = '{}{}'.format(path,file)
        paths_to_file.append(file_path)
    print(paths_to_file)
    df_files = pd.DataFrame(list(zip(stdz_files, files, paths_to_file)),columns=['stdz_file','orig_file', 'orig_file_path'])
    return(df_files)
    # for index,value in enumerate(files):
    #     print('{},{}{}'.format(path,value))
    #     print('')


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
    except:
        print('Could not connect to the database! Continuing without it...')

    # Get the tables that will be helpful for the data scan
    # Client ID ?
    if db_conn is not None:
        dim_issue = pd.read_sql_query('SELECT * FROM `dim_issue`;', con=db_conn)
        dim_file = pd.read_sql_query('SELECT * FROM `dim_file`;', con=db_conn) # CLIENT ID
        dim_file_filename = pd.read_sql_query(
            'SELECT * FROM `dim_file_filename`;', con=db_conn) # CLIENT ID
        fact_dscan_previous_results = pd.read_sql_query(
            'SELECT * FROM `fact_datascan` where client_id = {};'.format(config['client_id']), con=db_conn) # CLIENT ID
    else:
        _here = os.path.dirname(os.path.abspath(__file__))
        dim_issue = pd.read_csv(os.path.join(_here, 'setup/dim_issue.csv'))
        dim_file = pd.read_csv(os.path.join(_here, 'setup/dim_file.csv'))
        dim_file_filename = pd.read_csv(os.path.join(_here, 'setup/dim_file_filename.csv'))
    print()

    return db_conn, dim_issue, dim_file, dim_file_filename, fact_dscan_previous_results
