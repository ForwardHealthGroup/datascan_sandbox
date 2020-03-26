#!/usr/bin/env python3
import sys
import MySQLdb
import pandas as pd

# functions and sub fxns are listed in this manner:
# fxn[sub_fxn_1,sub_fxn_2,...]
import initialize_scan # con_db, set_config[get_client, get_source]
import log_file_info # log_info[update_dcds_download, update_dcds_file_name[get_max_timestamp], update_dcds_stdz_file_name]
                     # fact_data_load
# import standardize_filenames # standardize_files


DEFAULT_CONFIG = {
    'client': None,
    'client_dir': None,
    'client_id': None,
    'source_id': None,
    'directory_path': None,
    'refresh_id': None,
    'manual':0,
    'database': 'datateam',
    'db_host': 'mysql.test',
#     'db_user': os.getenv('DB_TEST_USER'),
#     'db_pass': os.getenv('DB_TEST_PASS'),
    'db_user': 'hhopkins',
    'db_pass': '7p!WUv&P^UhhO0',
    'datetime_format': '%Y-%m-%d %I:%M%p',
    'clients_db': 'dim_client_master',
    'sources_db': 'dim_client_data_source'
    }

# 0. Initialize data and update config

# Connect to database
db_conn = initialize_scan.con_db(config=DEFAULT_CONFIG)
print(db_conn)

# Manually setting variables within initialize_scan.set_config()
DEFAULT_CONFIG['client_id'] = 1003
DEFAULT_CONFIG['client'] = 'VNSNY'
# DEFAULT_CONFIG['source_id'] = 57
# DEFAULT_CONFIG['directory_path'] = '/data/customers/vnsny/current/test_old'
DEFAULT_CONFIG['directory_path'] = '/data/customers/vnsny/current/test_fix_files_folders/'

# Find client, source, and location of data.
# set_config updates the config and outputs the final files.
# Associate each file with client, source pair...
final_files = initialize_scan.set_config(db_conn=db_conn, config=DEFAULT_CONFIG)
print('\nFinal files: ')
print(final_files,'\n')
print('\nDEFAULT_CONFIG:')
print(DEFAULT_CONFIG,'\n')

# 1. Log File Info.
# # log dcds download, file_name, stdz_file_name, file_set, field_name info.
# fact_data_load_df = log_file_info.log_info(db_conn=db_conn, config=DEFAULT_CONFIG, final_files=final_files)
# # log dcds download, file_name, stdz_file_name, file_set
fact_data_load_df = log_file_info.insert_download_file_info(db_conn=db_conn, config=DEFAULT_CONFIG, final_files=final_files)
# # populate fact_data_load with id, client_id, source_id, download_id, file_id, stdz_file_id.
log_file_info.fact_data_load(db_conn=db_conn, config=DEFAULT_CONFIG, fact_data_load_df=fact_data_load_df)
# # Update dim_client_data_source_file_set
log_file_info.review_active_fileset(db_conn=db_conn, config=DEFAULT_CONFIG, fact_data_load_df=fact_data_load_df)

db_conn.close()








# OLD DATASCAN :

######################################

# import sys
# from argparse import ArgumentParser
# from datetime import datetime
# import MySQLdb
#
# # initialize_updates instead of initialize
# # import sand_initialize_updates # set_config, con_db, find_custom_scan
# import file_setup # find_files, match_files
#
# # import out_file_updates # prep_file, print_header, print_compare
# # import finalize # update_db, send_email
# # from data_error import DataError
# from issue_tracker_updates import IssueTracker
#
# import normalize_filenames #set_config, standardize_files, con_db
# import find_past_datascans # con_db, find_old_data
#
# def scan(**kwargs):
#     """Runs the data scan inventory process from start to finish.
#     Executes Statistics script if data scan process is successful.
#
#     This function uses arguments passed to it.
#     If the script is run automatically it accepts the filepath of new data
#     in the form of /data/$client/current/$source/yyyymm.
#     $client and $source are stored as system variables.
#
#     TABLE OF CONTENTS:
#     0. Initialize data and get config
#     1. Log File
#     2a. Compare files with previous results
#     2b. Update database with Inventory results
#     3. If Inventory data checks out, run Statistics for files
#     Final. Display results in portal
#     """
#
#     """ 0. Initialize data and get config
#     This step depends on the automaticity of the datascan ran.
#     -- The config is utilized in each case.
#     - If automatic, the client and source are automatically located from the path
#     - If manual, the user is prompted for information.
#     """
#
#
#
#
# def scan(**kwargs):
#     """Runs the entire data scan process from start to finish.
#
#     This function will use any and all arguments passed to it. For any arguments
#     not passed, the default values specified in `DEFAULT_CONFIG` will be used
#     instead. It will also try and find settings in `CLIENT_CONFIG.json` for the
#     given client.
#
#     TABLE OF CONTENTS:
#       1. GET THE CONFIG SETTINGS
#       2. RUN THE RIGHT DATA SCAN
#       3. CONNECT TO THE DATABASE
#       4. FIND THE FILES
#       5. MATCH THE FILES
#       6. PREPARE TO PRINT THE RESULTS
#       7. PRINT THE DATA SCAN HEADER
#       8. PRINT THE DATA SCAN COMPARISONS
#       9. CLEANUP
#       10. SEND THE EMAIL
#       11. END OF DATA SCAN
#
#     Args:
#         **kwargs: See `DEFAULT_CONFIG` for a list of possible parameters.
#     """
#
#     # Check for custom datascan, run if exists ---------------------------------
#     # custom_tf, custom_datascan = sand_initialize_updates.find_custom_scan(config)
#     # if custom_tf:
#     #     print('Found custom data scan for {}'.format(config['client']))
#     #     custom_datascan.scan(**config)
#     #     sys.exit(0)
#
#     # Setup config -------------------------------------------------------------
#     config = normalize_filenames.set_config(**kwargs)
#     print('Printing config')
#     print(config)
#
#     # Initialize objects & standardize file names -------------------------------------------------------
#     print('Beginning data scan: {}\n'.format(config['client'].upper()))
#     run_datetime = datetime.now().strftime(config['datetime_format'])
#
#     stdz_files = normalize_filenames.standardize_files(config)
#     print('standardized files: ',stdz_files)
#     df_files = normalize_filenames.files_as_df(config, stdz_files)
#     print(df_files)
#
#     # Connect to DB and find old datascan data.
#     db_conn = find_past_datascans.con_db(config)
#     dim_issue, dim_file, dim_file_filename, fact_dscan_previous, fact_dscan_refresh = find_past_datascans.find_old_data(config,db_conn)
#     print(fact_dscan_previous)
#     print('fact dscan_refresh: ')
#     print(fact_dscan_refresh)
#     # print(dim_file)
#     # print(dim_file_filename)
#
#     #IssueTracker use for inserting into fact_datascan later.
#     issue_tracker = IssueTracker(dim_issue)
#
#     # Find and match files -----------------------------------------------------
#     # new_files is a dataframe with stdz_file_name, orig_file_name, path
#     new_files = file_setup.find_files(config)
#     print('new Files: \n')
#     print(new_files)
#     for i in new_files:
#         print(i)
#     # print('Old Files: \n')
#     # print(old_files)
#     # print('Old files are from: {}'.format(', '.join(config['old_dirs'])))
#     print('New files are from: {}\n'.format(', '.join(config['new_dirs'])))
#
#     # Final pairs
#     # final_pairs = file_setup.match_files(new_files, dim_file_filename, dim_file, fact_dscan_previous, fact_dscan_refresh, db_conn)
#     #
#     # # Perform comparison, print to output file ---------------------------------
#     # output = out_file_updates.prep_file(config, run_datetime)
#     # out_file_updates.print_header(config, run_datetime, output, final_pairs)
#     # out_file_updates.print_compare(config, final_pairs, output, issue_tracker, run_datetime)
#     # output.close()
#     # print('\nData scan saved to {}'.format(output.name))
#     #
#     # # Write results to database ------------------------------------------------
#     # finalize.update_db(config, db_conn, issue_tracker, dim_file_filename, final_pairs)
#     # db_conn.commit()
#     # db_conn.close()
#     #
#     # # Retrieve problem, warning messages ---------------------------------------
#     # problems = ['- {}'.format(msg) for msg in issue_tracker.get_messages(2)]
#     # warnings = ['- {}'.format(msg) for msg in issue_tracker.get_messages(1)]
#     #
#     # # Email results ------------------------------------------------------------
#     # finalize.send_email(issue_tracker, config, output, problems, warnings)
#     #
#     # # Raise an error if there were problems ------------------------------------
#     # if problems:
#     #     raise DataError('\nProblems were identified during the data scan. '
#     #                     'Review scan output for details.'
#     #                     )
#
# def main():
#     """Parses command line arguments and passes them to `scan(...)`."""
#
#     parser = ArgumentParser()
#     parser.add_argument(action='store', dest='client', help='client name')
#     parser.add_argument('-r', action='store', dest='refresh',
#                         help='specify the refresh name formatted as YYYYMM')
#     parser.add_argument('-n', action='append', dest='new_dirs', default=[],
#                         help='add a directory containing new files to analyze')
#     parser.add_argument('-o', action='append', dest='old_dirs', default=[],
#                         help='add a directory containing old files to analyze')
#     parser.add_argument('-db', action='store', dest='database',
#                         help='specify the name of the database to use')
#     parser.add_argument('-e', action='append', dest='recipients',
#                         help='add an email address to receive the results')
#     kwargs = vars(parser.parse_args())  # This is a dict of {arg: value} pairs
#
#     scan(**kwargs)
#     # try:
#     #     scan(**kwargs)
#     # # except dog as e:
#     #     print(e.message)
#     #     sys.exit(1)
#
#
# if __name__ == '__main__':
#     main()
#     print('\n')
