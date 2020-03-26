#!/usr/bin/env python3

import MySQLdb
import sys
import os
import pandas as pd
import json
from io import open
from pathlib import Path
# from fnmatch import filter

"""
Initalizes the datascan.
- Decides if datascan is manual or automatic
- Finds origin of the files in question, client, and source
- Also connects to the database and will display information present to assist
with client/source selection if the datascan is manually ran.

Returns db connection , final file names
Returns the variables manual, client, source, and download information. (?)
"""

# todo: kwargs??
# manual/ auto ?
# def set_config(**kwargs):

def con_db(config):
    """Connects to database."""
    # Try to connect to the database
    print('RUNNING FXN: con_db\n')
    db_conn = None
    print(config)
    try:
        db_conn = MySQLdb.connect(host=config['db_host'],
                                  user=config['db_user'],
                                  passwd=config['db_pass'],
                                  port=3306,
                                  db=config['database'])
        print('Successful connection to db: '+str(config['database']))
    except Exception as e:
        print(e)
        print('Could not connect to the database. Sorry')
        sys.exit(0)
    return db_conn

def set_config(db_conn, config):



    """
    GET RID OF get_source


    local functions

    get_client(db_conn, config),
    get_directory_path(config),
    get_source(db_conn, config), and
    cleanup_dir(config, path)

    are used to update the configuration
    and prepare files to be scanned.

    get_client grabs client,
    get_directory_path grabs path to files to be scanned,
    get_source maps each file in the path to a source.
    """
    print ('RUNNING FXN: set_config\n')
    config.update(manual = 1)
    manual = config['manual']
    db_cursor = db_conn.cursor()

    def get_client(db_conn, config):

        """
        Interactive client locator.
        """
        print ('RUNNING sub-FXN: get_client\n')
        if config['client_id'].isna():
            query = 'SELECT id, alias, descriptive_name from {}'.format(config['clients_db'])
            try:
                clients_df = pd.read_sql(query, db_conn)
            except Exception as e:
                print(e)
                print('Could not execute query in the client db. Sorry')
                sys.exit(0)

            # Gather ids, convert to str for matching porpoises.
            client_ids = [str(i) for i in list(clients_df['id'])]
            if len(client_ids) == 0:
                print('There are no clients in the table {}.'.format(config['clients_db']) )
                sys.exit(0)
            else:
                if config['manual'] == 1:
                    print('Manual scan selected.\n')
                    client_id = input('Input client_id. [type v to view all client info]: ')
                    while client_id not in client_ids:
                        if client_id == 'v':
                            print('\nYou may have to expand your window to view all details.')
                            print(clients_df)
                            client_id = input('Input client_id. [type v to view all client info]: ')
                            print('\n')
                        else:
                            client_id = input('Please input "v" or an existing client_id: ')
                            print('\n')
            print('client_id selected: ', client_id)
            client_name = clients_df.loc[clients_df['id']==int(client_id), 'alias'].iloc[0]
            print('client selected: ', client_name)
            print('\n')
            config.update(client_id = client_id, client = client_name)

    get_client(db_conn, config)

    def get_directory_path(config):
        """
        Interactive directory locator.
        """
        print ('RUNNING sub-FXN: get_directory_path\n')

        print('Interactive directory locator... sorry, no tab complete.')
        print("Do not worry if you're unsure of the exact path.\n")
        #
        path_valid = 0
        path_success = 0
        while (path_valid == 0) & (path_success == 0):
            path_1 = ''
            try:
                path_1 = input('Please input path to files: ')
                os.chdir(path_1)
                path_valid = 1
            except Exception as e:
                print(e)
                print('Please enter a valid path.')
            path = path_1
            while (path_valid == 1) & (path_success == 0):

                print('Listing directory...')
                os.system('ls -l {}'.format(path))
                option = ''
                print('\nCurrent path:',path)
                print('\nOptions: [u]-use this path, [a]-append to path, [r]-restart')
                while option not in ['u','a','r']:
                    print('Please type u , a , or r as necessary.')
                    option = input('Option: ')
                if option == 'u':
                    print('Using this path: ', path,'\n')
                    path_success = 1
    #   #   #
                elif option == 'a':
                    try:
                        path = path+input('Append to {}'.format(path))
                        os.chdir(path)
                    except Exception as e:
                        print('Sorry, invalid path.')
                        path = path_1
                        print(e)
                        # path_valid = 0
                elif option == 'r':
                    path_valid = 0
                    print('Starting over...\n')
        if path[-1] != '/':
            path = path+'/'
        config['directory_path'] = path
    get_directory_path(config)

    def get_source(db_conn, config):

        """
        Interactive source locator for client.
        """

        print ('RUNNING sub-FXN: get_source\n')
        # This is similar to the query for the dcds-view in the db. Yes, it is ugly.
        query = 'SELECT  `a`.`id` AS `id`, \
        `b`.`alias` AS `client_name`,\
        `a`.`active` AS `active`,\
        `e`.`name` AS `ds_master`,\
        `c`.`name` AS `ds_system`,\
        `d`.`name` AS `ds_origin`\
        FROM ((((`dim_client_data_source` `a` join `dim_client_master` `b`) join `dim_data_source_system_master` `c`) join `dim_data_source_origin_master` `d`) join `dim_data_source_master` `e`) where ((`a`.`data_source_id` = `e`.`id`) and (`a`.`client_id` = `b`.`id`) and (`a`.`data_source_system_id` = `c`.`id`) and (`a`.`data_source_origin_id` = `d`.`id`))\
        and a.client_id = {};'.format(config['client_id'])
        try:
            sources_df = pd.read_sql(query, db_conn)
        except Exception as e:
            print(e)
            print('Could not execute query in the client db. Sorry')
            sys.exit(0)
        source_ids = [str(i) for i in list(sources_df['id'])]

        # Check number of source ids.
        if len(source_ids) == 0:
            print('There are no source ids for this client in the table {}.'.format(config['sources_db']))
            sys.exit(0)
        elif len(source_ids) == 1:
            print('There is only one source_id for this client. Using it!')
            source_id = source_ids[0]

        else:
            if config['manual'] == 1:
                print('Possible source_ids: ')
                print('You may have to expand your window to view all details.')
                print(sources_df)
                source_id = input('Input source_id. [type v to repeat view]: ')
                print('\n')
                while source_id not in source_ids:
                    if source_id == 'v':
                        print('You may have to expand your window to view all details.')
                        print(sources_df)
                        source_id = input('Input source_id. [type v to repeat view]: ')
                        print('\n')
                    else:
                        source_id = input('Please input "v" or an existing source_id: ')
                        print('\n')
        print('source_id selected: ', source_id)
        print('\n')
        config.update(source_id = source_id)

    get_source(db_conn, config)

    def cleanup_dir(config, path):
        """
        The datascan only scans files in csv,txt format.
        IF there are extraneous sub-directories, or other file types
        - The sub-directories are pointed out to the User
        - Excel files are attempted to convert into .txt or .csvs
            - Non-excels are left as is. Up to user to fix.
            - If a file is converted, its original version is stored in location:
            /$path/raw_files
        """

        print ('RUNNING sub-FXN: cleanup_dir')
            #   #

        print('\nNow handling extraneous directories or files...')
        subdirs = next(os.walk(path))[1]
        files_only = 0
        final_files = []
        # while (files_only == 0) & (finalize == 0):
        while files_only == 0:
            # Get rid of sub-directories
            if len(subdirs) != 0:
                print('\nthe sub-directories detected:')
                for i in subdirs:
                    print(i)
                print('\nThe Datascan only handles files.')
                exclude_dirs = ''
                while exclude_dirs not in['y','n']:
                    exclude_dirs = 'y'
                    # exclude_dirs = input('Exclude sub-directories? [y/n] (n quits scan): ')
                if exclude_dirs == 'y':
                    print('\nExcluding sub-directories.')
                    files_only = 1
                elif exclude_dirs == 'n':
                    print('You must modify the folder you are working with.')
                    print('Modify and start over.')
                    sys.exit(0)
            # Only want files - Create path for files in need of conversion - OK if empty.
            try:
                print(path)
                # statement = 'cd {} ; mkdir ./raw_files'.format(path)
                # print('STATEMENT: ', statement)
                os.chdir(path)
                os.system('mkdir ./raw_files')
            except:
                print('\nraw_files directory already exists.')
                continue
            files = next(os.walk(path))[2]
            files_to_fix = []
            print('\nNon - txt/csv(s) detected:')
            for file in files:
                if file.lower().split('.')[-1] not in ['txt','csv']:
                    print(file)
                    files_to_fix.append(file)
            if len(files_to_fix) > 0:
                print('\nThe datascan works best with csv and txt files.')
                fix_files = ''
                while fix_files not in ['y','n']:
                    fix_files = 'y'
                    # fix_files = input('Try to automatically fix file types? [y/n] (n quits scan): ')

                if fix_files == 'n':
                    print('You must modify the files you are working with.')
                    print('Modify and start over.')
                    sys.exit(0)

                elif fix_files == 'y':
                    print('Attempting to fix files')

                    def csvs_txts_only(files_to_fix, path):
                        for file in files_to_fix:
                            original_path = path+file

                            if file.lower().split('.')[-1] in ['xlsx', 'xls']:
                                try:
                                    copy_file = 'cp {} ./raw_files/'.format(original_path)
                                    os.system(copy_file)
                                    print('{} has been copied to {}raw_files'.format(file, path) )
                                except Exception as e:
                                    print('{} was not copied...'.format(file) )
                                    continue

                                print('\nExcel file detected; pick row in which header begins... (May have to exit datascan to edit if not clear.)\n')
                                header_row = ''
                                correct = ''
                                while correct not in ['y','n']:
                                    try:
                                        # Print the first 11 rows to find the header.
                                        if header_row == '':
                                            excel_df = pd.read_excel(original_path, nrows = 11)
                                        else:
                                            excel_df = pd.read_excel(original_path, nrows = 11, header = int(header_row) )
                                    except Exception as e:
                                        print(e)
                                        print('Invalid Excel File... exiting.')
                                        sys.exit(0)
                                    print(excel_df)
                                    correct = 'y'
                                    # correct = input('Is this the right header? [y/n]: ')
                                    if correct == 'y':
                                        continue
                                    if correct == 'n':
                                        header_row = ''
                                        possible_new_header_rows = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 'r']
                                        # Need string to avoid input issues.
                                        possible_new_header_rows = [str(n) for n in possible_new_header_rows]
                                        # I would hope the header is in the first 11 rows of the excel file...
                                        while header_row not in possible_new_header_rows:
                                            header_row = input('What is the correct new header row? (use index shown), (input "r" for restart) ')
                                        print('Breaking header_row loop')
                                        # Restart top loop
                                        if header_row == 'r':
                                            correct = ''
                                            header_row = ''
                                        # go to top, header_row passed to pd read excel.
                                        else:
                                            correct = ''
                                            header_row = str(int(header_row)+1)
                                new_file_csv = file.split('.')[-2]+'.csv'
                                # If for some reason the excel already exists as a csv, continue on.
                                if os.path.exists(new_file_csv):
                                    continue
                                else:
                                    try:
                                        excel_df.to_csv(new_file_csv, index = False)
                                        # os.system()
                                    except:
                                        print('Error in converting excel df to a csv.')
                                        sys.exit(0)

                            else:
                                print('This file extension:', file.split('.')[-1], '- is not supported. This file will not be scanned and will be "pended".')
                        # This is the top for-loops indentation. Below is not associated with top for-loop.
                        for root, dirs, files in os.walk(path+'raw_files', topdown=False):
                            print('\nPath of copied files: ',root)
                            for file in files:
                                print(file)
                        # Check if files copied correctly.
                        keep_going = 'y'
                        while keep_going not in ['y','n']:
                            keep_going = input('Did all your files get copied correctly? [y/n]: ')
                        if keep_going == 'n':
                            print('\nPlease fix your mistake...')
                            sys.exit(0)

                        #remove non-csv/txt files, after copying and converting.
                        remove_old = 'y'
                        while remove_old not in ['y','n']:
                            remove_old = input('Remove original files from folder being scanned? [y/n]: ')
                        if remove_old == 'y':
                            print('\nRemoving original files...')
                            for file in files_to_fix:
                                if file.split('.')[-1] == 'xlsx':
                                    print('REMOVING FILE: ', file)
                                    os.system('rm -f {}'.format(file))
                        else:
                            print('Please update folder being scanned. ')

                            sys.exit(0)
                    # csvs_txts_only fxn original indentation
                    csvs_txts_only(files_to_fix, path)

        # ORIGINAL WHILE LOOP INDENTATION
            print('\nAll files are now .csv or .txt and all directories have been excluded from datascan.\n')
            updated_files = next(os.walk(path))[2]

            # Only returns csvs and txts
            final_files = [i for i in updated_files if i.lower().split('.')[-1] in ['txt','csv']]
            # for i in updated_files:
            #     if i.lower().split
            # break while loop.
        return(final_files)

# import pandas as pd
# df = pd.read_{from_type}({path_to_file})
# df.to_csv({output_path},sep='|',index=False)


        # config.update(directory_path = path)
    final_files = cleanup_dir(config, config['directory_path'])
    return(final_files)
            # os.listdir(initial_path)

# file_fixer
