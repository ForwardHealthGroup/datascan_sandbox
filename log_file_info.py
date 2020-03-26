# #!/usr/bin/env python3
#
# import MySQLdb
# import sys
# import os
# import pandas as pd
# import json
# from io import open
# # from fnmatch import filter
#
# """
# Log file info including:
# download, raw filenames, standardized filenames, column names.
# Cumulative info logged in fact_data_load as:
# id, client_id, source_id, download_id, file_id, stdz_file_id .
# """
#
# def log_info(db_conn, config, final_files):
#     """
#     Uses the files from initialize_scan.py and logs info.
#     """
#     db_cursor = db_conn.cursor()
#     # def update_dcds_download():
#     #     query = 'INSERT IGNORE INTO dim_client_data_source_download\
#     #             select '
#
#     def update_dcds_file_name():
#         query = 'INSERT IGNORE INTO dim_client_data_source_file_name(file_name, client_id, source_id)\
#                 select {},{},{}'.format(file_name, client_id, source_id)
#
#         try:
#             file_name_df = pd.read_sql(query, db_conn)
#         except Exception as e:
#             print(e)
#             print('Could not execute query in the client db. Sorry')
#             sys.exit(0)
#         print(file_name_df)

#!/usr/bin/env python3

import MySQLdb
import sys
import os
import pandas as pd
import json
from io import open
import warnings
# Ignore duplicate entry warnings.
warnings.filterwarnings("ignore", category = MySQLdb.Warning)
# from fnmatch import filter
import client_standardize_filenames # standardize_files

"""
Log file info including:
download, raw filenames, standardized filenames, column names.
Cumulative info logged in fact_data_load as:
id, client_id, source_id, download_id, file_id, stdz_file_id .
"""

# def log_info(db_conn, config, final_files):
def insert_download_file_info(db_conn, config, final_files):
    """
    Uses the files from initialize_scan.py and logs into:
        dcds_download, dcds_file_name, dcds_stdz_file_name,
        dcds_file_set, dcds_field_name.
        - This info is then logged into fact_data_load in the form of:
            id, client_id, source_id, download_id, file_id, stdz_file_id.
    """
    print ('RUNNING FXN: insert_download_file_info')
    db_cursor = db_conn.cursor()
    # def update_dcds_download():
    #     query = 'INSERT IGNORE INTO dim_client_data_source_download\
    #             select '

    def update_dcds_file_name(db_cursor, config, final_files):
        print ('RUNNING sub-FXN: update_dcds_file_name')
        """
        Insert ignore into dcds_file_name
        """
        # Problem: Want to track newly inserted files
        # Solution 1a: build query like
        #       INSERT IGNORE INTO ...
        #       VALUES (a1,b1,c1),
        #              (a2,b2,c2),
        #              ...,
        #              (an,bn,cn),
        # This query will ensure that all new files will have the same timestamp.

        # Solution 1b: Compare max timestamp before and after insert to see which files are new.
        # 1b (part 1). Grab before.
        def get_max_timestamp(before):
            # Finds the maximum date_created timestamp from dcds_file_name.
            max_timestamp = 'SELECT max(date_created) FROM dim_client_data_source_file_name'
            db_cursor.execute(max_timestamp)
            max_time = db_cursor.fetchall()
            # (('yyyy-mm-dd hh:mm:ss',),)
            # This is a tuple... The [0][0] index grabs the value we want.
            max_time = max_time[0][0]
            if before == True:
                if max_time is None:
                    max_time = pd.to_datetime('1970-01-01')
            return(max_time)

        max_before = get_max_timestamp(before = True)

        list_of_inserts= []
        # 1a. Create insert query:
        try:
            for file_name in final_files:
                query_start = 'INSERT IGNORE INTO dim_client_data_source_file_name (file_name, client_id, source_id)'
                # Conv. to String-type for joining string together later. Does not mess up insert.
                list_of_inserts.append([ "'"+file_name+"'", str(config['client_id']), str(config['source_id']) ])
            # Convert list of lists into list of strings where each item in list has one triad of values.
            list_of_inserts_2 = [', '.join(x) for x in list_of_inserts]
            # Separate values to match insert values syntax.
            string_of_inserts = '), ('.join(list_of_inserts_2)
            # Add beginning and end of "VALUES ( string_of_inserts )" syntax.
            final_values_string = 'VALUES ('+string_of_inserts+');'
            final_query = query_start+' '+final_values_string
            # print('INSERT STATEMENT: ',final_query)
            db_cursor.execute(final_query)
            db_conn.commit()
        except Exception as e:
            print(e)
            print('Could not execute insert query for dim_client_data_source_file_name. Sorry')
            db_conn.rollback()
            db_conn.close()
            sys.exit(0)

        # 1b. (part 2)
        max_after = get_max_timestamp(before = False)
        if (max_after > max_before):
            print('\nNew files in dcds_filename: \n')
            new_files_query = 'SELECT * FROM dim_client_data_source_file_name \
                            WHERE date_created = "{}" ;'.format(max_after)
            new_files_df = pd.read_sql(new_files_query, db_conn)
            print(new_files_df)

        db_conn.commit()
    update_dcds_file_name(db_cursor=db_cursor, config=config, final_files=final_files)

    def update_dcds_stdz_file_name(db_cursor, config, final_files):
#        Standardize files. Looks in fact_data_load for the combination of file_id and stdz_file_id.
#         If exists, uses that stdz_file_id.
#         If not, needs to attempt standardization.
#         Also, needs to figure out file_set_id after standardization occurs.
#         Create an extraneous function... The function will take in
        print ('RUNNING sub-FXN: update_dcds_stdz_file_name\n')
        # Goal: Create dataframe with this info:
        # file_id, file_name, stdz_file_id, stdz_file_name

        # Part1: grab file_id and name from previous step.
        # Part2: Use fact_data_load to match previously existing file_ids with stdz_file_ids
        # Part3: Standardize new file_ids.

        # Part1: grab file_id and name from previous step.
        print('Gathering file_id and file_name from dcds_file_name.\n')
        file_list = []
        for file_name in final_files:
            query = 'SELECT id, file_name FROM dim_client_data_source_file_name \
                   WHERE client_id = {} and source_id = {} and file_name = "{}";\
                   '.format(config['client_id'], config['source_id'], file_name)
            db_cursor.execute(query)
            ids_files = db_cursor.fetchall()
            #[0][0] is id, [0][1] is file_name
            try:
                file_list.append([ids_files[0][0], ids_files[0][1]])
            except:
                print('File name ',file_name,' does not exist in dcds_file_name.')
        file_df = pd.DataFrame(file_list, columns = ['file_id', 'file_name'])
        # file_df.columns = ['file_id','file_name']
        print('Original list of files: \n')
        print(file_df)

        # Part2: Use fact_data_load to match previously existing file_ids with stdz_file_ids
        print('\nSearching fact_data_load for previously existing file_id, stdz_file_id pairs.')
        stdz_file_list = []
        print()
        for file_id in file_df['file_id'].tolist():
            query = 'SELECT file_id, stdz_file_id FROM fact_data_load \
                    WHERE file_id = {} AND client_id = {} AND source_id = {} \
                    GROUP BY client_id, source_id, download_id, file_id, stdz_file_id \
                    ORDER BY id desc, download_id desc \
                    '.format(file_id, config["client_id"], config["source_id"])
            db_cursor.execute(query)
            ids_stdzfiles = db_cursor.fetchall()
            #[0][0] is file_id, [0][1] is stdz_file_id
            try:
                stdz_file_list.append([ids_stdzfiles[0][0], ids_stdzfiles[0][1]] )
            except Exception as e:
                # print(e)
                continue
                # print('data load for file id ',file_id,' does not exist')
        file_stdzfile_ids_df = pd.DataFrame(stdz_file_list, columns = ['file_id','stdz_file_id'])
        # file_stdzfile_ids_df.columns = ['file_id','stdz_file_id']

        # Merge file_df with stdz_file_df on file_id, keep null where no match.
        print('Attempting to match file_id with previously existing stdz_file_id using fact_data_load\n')
        client_stdz_df = pd.merge(file_df, file_stdzfile_ids_df, how='outer', on='file_id')
        print('CLIENT STDZ DF:')
        print(client_stdz_df)
        count_not_match = client_stdz_df['stdz_file_id'].isna().sum()
        count_total = len(client_stdz_df['stdz_file_id'])
        count_match = count_total - count_not_match
        print('{} out of {} files were matched with an existing stdz_file_id in fact_data_load.\n'.format(count_match, count_total) )

        # Part3: Standardize new file_ids.
        # NOTE: There will likely be some cases where standardization will be imperfect
        # -for each file_name for the client.
        # Possible solution: Search in fact_datascan and dcds_field_name for matches...
        # Also, may be smart to add a manual override of some sort,
        # where the stdz file name is just the file name itself...
        if count_match == 0:
            print('Standardizing new file_ids using client/source-specific normalization script.\n')
            # Perform standardization on non-standardized files with client stdz. method.
            # Returns standardized file names, with no ID yet.
            client_stdz_df = client_standardize_filenames\
                            .client_standardize_files(db_cursor=db_cursor, config=config, fact_data_load_df=client_stdz_df)
            print('Final Standardized file names:')
            print(client_stdz_df)
            print('\nUpdating dcds_stdz_file_name with stdz_file_ids/names which did not previously exist in fact_data_load.')
            print('\nUpdating dataframe with newly inserted stdz_file_ids.')
            for row in client_stdz_df.itertuples():
                # print(row)
                # If stdz_file id is null, insert stdz_file_name into dcds_stdz_file_name
                if pd.isna(row.stdz_file_id):
                    query = 'INSERT IGNORE INTO dim_client_data_source_stdz_file_name (stdz_file_name, client_id, source_id) \
                            VALUES ("{}", {}, {})\
                            '.format(row.stdz_file_name, config["client_id"], config["source_id"])
                    db_cursor.execute(query)
                    db_conn.commit()

                    query = 'SELECT id, stdz_file_name FROM dim_client_data_source_stdz_file_name \
                            WHERE stdz_file_name = "{}" AND client_id = {} AND source_id = {} ;\
                            '.format(row.stdz_file_name, config["client_id"], config["source_id"])
                    db_cursor.execute(query)
                    ids_stdzfiles = db_cursor.fetchall()
                    # at module updates the dataframe.
                    client_stdz_df.at[row.Index, 'stdz_file_id'] = ids_stdzfiles[0][0]

        return(client_stdz_df)


    fact_data_load_df = update_dcds_stdz_file_name(db_cursor=db_cursor, config=config, final_files=final_files)
    return(fact_data_load_df)

def fact_data_load(db_conn, config, fact_data_load_df):
    """
    Uses the files from initialize_scan.py and logs into:
        dcds_download, dcds_file_name, dcds_stdz_file_name,
        dcds_file_set, dcds_field_name.
        - This info is then logged into fact_data_load in the form of:
            id, client_id, source_id, download_id, file_id, stdz_file_id.
    """
    print ('\nRUNNING FXN: fact_data_load')
    db_cursor = db_conn.cursor()
    print(fact_data_load_df)
    for row in fact_data_load_df.itertuples():
        query = 'INSERT IGNORE INTO fact_data_load (client_id, source_id, download_id, file_id, stdz_file_id) \
                VALUES ({},{},{},{},{})\
                '.format(config["client_id"], config["source_id"], 0, row.file_id, row.stdz_file_id)
        db_cursor.execute(query)
        db_conn.commit()
        # view fact_data_load insert..
    query = 'SELECT * FROM fact_data_load \
            WHERE client_id = {} AND source_id = {} AND download_id = 0 \
            '.format(config["client_id"], config["source_id"])
    db_cursor.execute(query)
    # view_fact_data_load = db_cursor.fetchall()
    # print('FACT DATA LOAD FINAL RESULT :)')
    # print(view_fact_data_load)

def review_active_fileset(db_conn, config, fact_data_load_df):
    """
    Fileset: The expected set of files.

    Gives the user the ability to verify the fileset is correct
    and make updates if necessary.
    """
    # Options for fileset:
    # Explain fileset...
    # Checks if all files received are in the active set.
    # View active fileset and all files within it.
    # create new active fileset. (can append to it later...)
    # Where is the new active fileset?
    # Modify existing fileset. (add or delete.)
    print('\nRUNNING FXN: review_active_fileset')
    db_cursor = db_conn.cursor()

    # Check fileset vs. current files.
    # Check first-priority fileset ids first.
    query = 'SELECT id, is_active FROM dim_client_data_source_file_set \
             WHERE client_id = {} AND source_id = {} AND is_active >= 1 \
             GROUP BY id, is_active ORDER BY is_active ; \
            '.format(config["client_id"], config["source_id"])
    db_cursor.execute(query)
    id_active = db_cursor.fetchall()
    print('LENGTH OF ID_ACTIVE:')
    print(len(id_active))
    # Check ...
    print('Checking if files match previously existing fileset.')
    match = 0
    all_existing_file_sets = []
    for i in id_active:

        id = i[0]
        # Store file set ids for later
        all_existing_file_sets.append(id)
        active = i[1]
        print('id: ',id,' active: ',active)
        query = 'SELECT id, stdz_file_name, file_set_id \
                 FROM dim_client_data_source_stdz_file_name \
                 WHERE file_set_id = {}'.format(id)
        db_cursor.execute(query)
        ids = db_cursor.fetchall()
        print(ids)
        # Check if all stdz file ids match the stdz file ids from fact_data_load_df
        query_ids_list = sorted( [str(i[0]) for i in ids] )

        my_ids_list = sorted( [str(i) for i in fact_data_load_df['stdz_file_id'].tolist()] )
        print(query_ids_list)
        print(my_ids_list)
        if query_ids_list == my_ids_list:
            print('MATCH')
            match = 1
        else:
            print('NO MATCH...')
    if match == 0:
        print('No fileset matches found. Data inventory (fact_datascan_refresh) will be marked incomplete.')
        print('If the files are an expected full set of files per refresh, a new file set may be in order.')
        new_fileset = ''
        while new_fileset not in ['y','n']:
            new_fileset = input('Would you like to create a new file set? [y/n]: ')
        if new_fileset == 'y':
            # View existing filesets.
            # What priority should the fileset take?
            # Which files in the new fileset? - all files from set? only some? all + more previously existing?
            # options = u (use this set), a (append/add to), x (remove some), e (exit/no new fileset),
            # v (view this set), vv (view all existing sets and these files), vvv (view all stdz_file_ids).
            sql_existing_file_sets = ','.join(str(i) for i in all_existing_file_sets)
            print(sql_existing_file_sets)
            v_query = 'SELECT id, stdz_file_name, file_set_id FROM dim_client_data_source_stdz_file_name\
             WHERE file_set_id in ({}) ;'.format(sql_existing_file_sets)
            print(v_query)
            query_dict = {}

            # def choose_option(query_dict, possible_options = ['u', 'a', 'x', 'e', 'v', 'vv']):
            #     option = ''
            #     print(fact_data_load_df)
            #     original_options = possible_options
            #     # Initialize option choice. option variable updates throughout next while loop.
            #     while option not in original_options:
            #         print('Options: u (use this set), a (append/add), x (remove some), e (exit/no new fileset),')
            #         print('v (view all existing sets), vv (view all stdz_file_ids).')
            #         option = input('Option: ')
                # a and x are part of m...
            def recurse_options():
                # print('RUNNING SUB FXN: recurse_options')
                # Add to options stack as function goes. Sometimes, uses previous option.
                print(fact_data_load_df)
                options_stack = []
                option = 'start'
                options_stack.append(option)
                original_options = ['u', 'a', 'x', 'e', 'v', 'vv', 'vvv']
                done = 0
                while done == 0:
                    possible_options = original_options

                    # if option not in ['start','b','v','vv','vvv']:

                    # elif option in ['v','vv','vvv','b']:
                    #     options_stack.append(option)
                    # options_stack.append(options_stack[-1])
                    # option = options_stack[-1]

                    # if (options_stack[-1] == 'start') or (option == 'start'):
                    if option == 'start':
                        option = options_stack[0]
                    else:
                        options_stack.append(option)
                        option = options_stack[-1]
                    print('\nTOP OF LOOP')
                    # if option not in
                    # options_stack.append(option)
                        #Deleted afterward

                    print('Chosen options so far')
                    print(options_stack)
                    print('Choosing option')
                    print(option)
                    if option == 'start':
                        while option not in original_options:
                            print('Options: u (use this set), a (append/add), x (remove some), e (exit/no new fileset),')
                            print('v (view fact_data_load_df), vv (view all existing sets), vvv (view all stdz_file_ids).')
                            option = input('Choose Option: ')
                        print('START CHOSEN OPTION:')
                        print(option)
                        print('DONE WITH START LOOP')
                    elif option == 'u':
                        print('Using this set')
                        done = 1
                    elif option == 'a':
                        print('User appends stdz file ids to file_set.')
                        print('Options: b (back), c (choose ids),')
                        print('v (view fact_data_load_df), vv (view all existing sets), vvv (view all stdz_file_ids).')
                        possible_options = ['b', 'c', 'v', 'vv', 'vvv']
                        while option not in possible_options:
                            option = input('Choose Option: ')
                        # if option == 'b':
                        #     option
                        #     recurse_options(option)
                        # else:
                        #     recurse_options(option)
                        #     option = 'a'
                        #     recurse_options(option)

                    elif option == 'c':
                        print('Which stdz file ids to add/remove?')
                        print('Choose the file ids here...')
                    elif option == 'x':
                        print('User removes stdz file ids from file set.')
                    elif option == 'e':
                        print('No new fileset created.')
                        done = 1
                    elif option == 'b':
                        print('Chose to go back')
                        # delete b and previous option from stack.
                        print('Deleting previous item from stack.')
                        del options_stack[-2:]
                        option = options_stack[-1]
                    elif option in ['v','vv','vvv']:
                        print('THE DELETERS')
                        if option == 'v':
                            print('Viewing fact_data_load_df')
                            print(fact_data_load_df)
                        elif option == 'vv':
                            print('Viewing all existing file sets')
                        # v_df = pd.read_sql(query, db_conn)
                        # print(v_df)
                        elif option == 'vvv':
                            print('Viewing all stdz_file_ids')

                        print('OPTIONS STACK BEFORE V')
                        print(options_stack)
                        del options_stack[-1]
                        print('OPTIONS STACK AFTER V')
                        print(options_stack)
                        option = options_stack[-1]

                    print('OPTION AT END:')
                    print(option)
                    print('END OF LOOP\n')
                print('RETURN FILE SET')
            recurse_options()
            # print('RUNNING SUB FXN: choose_option')
            # choose_option(query_dict = '')

    else:

        print('File set match found. ')
