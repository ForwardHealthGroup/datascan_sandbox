#!/usr/bin/env python

import sys
from argparse import ArgumentParser
from datetime import datetime
import MySQLdb

# initialize_updates instead of initialize
# import sand_initialize_updates # set_config, con_db, find_custom_scan
import file_setup # find_files, match_files

# import out_file_updates # prep_file, print_header, print_compare
# import finalize # update_db, send_email
# from data_error import DataError
from issue_tracker_updates import IssueTracker

import normalize_filenames #set_config, standardize_files, con_db
import find_past_datascans # con_db, find_old_data

def scan(**kwargs):
    """Runs the entire data scan process from start to finish.

    This function will use any and all arguments passed to it. For any arguments
    not passed, the default values specified in `DEFAULT_CONFIG` will be used
    instead. It will also try and find settings in `CLIENT_CONFIG.json` for the
    given client.

    TABLE OF CONTENTS:
      1. GET THE CONFIG SETTINGS
      2. RUN THE RIGHT DATA SCAN
      3. CONNECT TO THE DATABASE
      4. FIND THE FILES
      5. MATCH THE FILES
      6. PREPARE TO PRINT THE RESULTS
      7. PRINT THE DATA SCAN HEADER
      8. PRINT THE DATA SCAN COMPARISONS
      9. CLEANUP
      10. SEND THE EMAIL
      11. END OF DATA SCAN

    Args:
        **kwargs: See `DEFAULT_CONFIG` for a list of possible parameters.
    """

    # Check for custom datascan, run if exists ---------------------------------
    # custom_tf, custom_datascan = sand_initialize_updates.find_custom_scan(config)
    # if custom_tf:
    #     print('Found custom data scan for {}'.format(config['client']))
    #     custom_datascan.scan(**config)
    #     sys.exit(0)

    # Setup config -------------------------------------------------------------
    config = normalize_filenames.set_config(**kwargs)
    print('Printing config')
    print(config)

    # Initialize objects & standardize file names -------------------------------------------------------
    print('Beginning data scan: {}\n'.format(config['client'].upper()))
    run_datetime = datetime.now().strftime(config['datetime_format'])

    stdz_files = normalize_filenames.standardize_files(config)
    print('standardized files: ',stdz_files)
    df_files = normalize_filenames.files_as_df(config, stdz_files)
    print(df_files)

    # Connect to DB and find old datascan data.
    db_conn = find_past_datascans.con_db(config)
    dim_issue, dim_file, dim_file_filename, fact_dscan_previous, fact_dscan_refresh = find_past_datascans.find_old_data(config,db_conn)
    print(fact_dscan_previous)
    print('fact dscan_refresh: ')
    print(fact_dscan_refresh)
    # print(dim_file)
    # print(dim_file_filename)

    #IssueTracker use for inserting into fact_datascan later.
    issue_tracker = IssueTracker(dim_issue)

    # Find and match files -----------------------------------------------------
    # new_files is a dataframe with stdz_file_name, orig_file_name, path
    new_files = file_setup.find_files(config)
    print('new Files: \n')
    print(new_files)
    for i in new_files:
        print(i)
    # print('Old Files: \n')
    # print(old_files)
    # print('Old files are from: {}'.format(', '.join(config['old_dirs'])))
    print('New files are from: {}\n'.format(', '.join(config['new_dirs'])))

    # Final pairs
    # final_pairs = file_setup.match_files(new_files, dim_file_filename, dim_file, fact_dscan_previous, fact_dscan_refresh, db_conn)
    #
    # # Perform comparison, print to output file ---------------------------------
    # output = out_file_updates.prep_file(config, run_datetime)
    # out_file_updates.print_header(config, run_datetime, output, final_pairs)
    # out_file_updates.print_compare(config, final_pairs, output, issue_tracker, run_datetime)
    # output.close()
    # print('\nData scan saved to {}'.format(output.name))
    #
    # # Write results to database ------------------------------------------------
    # finalize.update_db(config, db_conn, issue_tracker, dim_file_filename, final_pairs)
    # db_conn.commit()
    # db_conn.close()
    #
    # # Retrieve problem, warning messages ---------------------------------------
    # problems = ['- {}'.format(msg) for msg in issue_tracker.get_messages(2)]
    # warnings = ['- {}'.format(msg) for msg in issue_tracker.get_messages(1)]
    #
    # # Email results ------------------------------------------------------------
    # finalize.send_email(issue_tracker, config, output, problems, warnings)
    #
    # # Raise an error if there were problems ------------------------------------
    # if problems:
    #     raise DataError('\nProblems were identified during the data scan. '
    #                     'Review scan output for details.'
    #                     )

def main():
    """Parses command line arguments and passes them to `scan(...)`."""

    parser = ArgumentParser()
    parser.add_argument(action='store', dest='client', help='client name')
    parser.add_argument('-r', action='store', dest='refresh',
                        help='specify the refresh name formatted as YYYYMM')
    parser.add_argument('-n', action='append', dest='new_dirs', default=[],
                        help='add a directory containing new files to analyze')
    parser.add_argument('-o', action='append', dest='old_dirs', default=[],
                        help='add a directory containing old files to analyze')
    parser.add_argument('-db', action='store', dest='database',
                        help='specify the name of the database to use')
    parser.add_argument('-e', action='append', dest='recipients',
                        help='add an email address to receive the results')
    kwargs = vars(parser.parse_args())  # This is a dict of {arg: value} pairs

    scan(**kwargs)
    # try:
    #     scan(**kwargs)
    # # except dog as e:
    #     print(e.message)
    #     sys.exit(1)


if __name__ == '__main__':
    main()
    print('\n')
