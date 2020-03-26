#!/usr/bin/env python

from __future__ import print_function, division, unicode_literals
import os
from io import open
from datetime import datetime
from collections import namedtuple
from scripts.data_utilities import read_file, find_date_columns, fix_date_column


def prep_file(config, run_datetime):
    """Create output file object for txt report."""

    # Set the file name of the txt file report
    if config['refresh'] is not None:
        client = config['client']
        description = config['refresh'] + "_" + run_datetime
    else:
        client = config['client'].upper()
        description = run_datetime
    fname = '{}_data_scan_{}.txt'.format(client, description)

    # Create the output object to write the data scan results to
    out_path = os.path.join(config['current_dir'], 'data_scans/')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # Open output file for writing report to
    output_file = os.path.join(out_path, fname)
    output = open(output_file, mode='w+', encoding='utf8')

    return output


def print_header(config, run_datetime, output, final_pairs):
    """ """
    # Print the file header
    lines = []
    if config['refresh'] is not None:
        lines.append('{} data scan for {} refresh run on date {}.'
                     .format(config['client'].upper(), config['refresh'],
                             run_datetime))
    else:
        lines.append('{} data scan run on date {}.'
                     .format(config['client'], run_datetime))
        lines.append('Old files are from: {}'
                     .format(', '.join(config['old_dirs'])))
        lines.append('New files are from: {}'
                     .format(', '.join(config['new_dirs'])))
    bar = '-' * max([len(line) for line in lines])
    print('\n'.join([bar] + lines + [bar]), file=output)
    print('', file=output)  # whitespace

    # Print the table header
    width = max([len(p['old_file']) if p['old_file'] is not None else 15
                 for p in final_pairs])
    print('   OLD FILES{}\t   NEW FILES'.format(' ' * max(0, (width - 12))),
          file=output)

    # Print the table of files
    for pair in final_pairs:
        if pair['old_file'] is None:
            formatting = '', ' ' * width, pair['new_file']
        elif pair['new_file'] is None:
            formatting = pair['old_file'], '', ''
        else:
            formatting = pair['old_file'], \
                         ' ' * (width - len(pair['old_file'])), pair['new_file']
        print('{}{}\t{}'.format(*formatting), file=output)
    print('', file=output)  # whitespace


def print_compare(config, final_pairs, output, issue_tracker, run_datetime):
    """
    Perform file comparison for each pair.
        - Print results to output file
        - Add issues to issue tracker

    Keep in mind that either the old file or the new file might be None if
    this pair represents an unmatched file.
    """
    File = namedtuple('File', ['fname', 'desc', 'id', 'path', 'required', 'count', 'data'])

    print("Analyzing Files: ")
    for pair in final_pairs:

        # Declare which files are being analyzed
        declare_files(pair, output)

        # Read files to dataframes
        old_data, new_data = open_file_pair(pair)

        # Get file counts, setup files
        old_count, new_count, variance = row_count(old_data, new_data)

        old = new = None
        if new_data is not None:
            new = File(pair['new_file'],
                       'new',
                       pair['new_id'],
                       pair['new_path'],
                       pair['new_required'],
                       new_count,
                       new_data)
        if old_data is not None:
            old = File(pair['old_file'],
                       'old',
                       pair['old_id'],
                       pair['old_path'],
                       pair['old_required'],
                       old_count,
                       old_data)


        # Calculate old_count, new_count, variance
        # Append these results to the pair-dictionary for portability

        # Next, analyze the new and old files individually
        if new is not None:
            analyze_date_cols(new, output)

        if old is not None:
            analyze_date_cols(old, output)

        # Print the row count and column changes if this is a matched pair
        if old is not None and new is not None:
            print('{:.2%} percent change in the row count since last refresh.\n'
                  .format(variance), file=output)

            # ISSUE: >5% drop in row count
            if variance < -0.05:
                msg = 'The number of rows in {} fell by {:.2%}.' \
                    .format(new.fname, -variance)
                issue_tracker.add_issue(filename=new.fname,
                                        file_id=new.id,
                                        run_date=run_datetime,
                                        refresh=config['refresh'],
                                        old_count=old_count,
                                        new_count=new_count,
                                        variance=variance,
                                        issue_id=5,
                                        message=msg,
                                        source_id=config['source_id'],
                                        client_id=config['client_id'])

            # ISSUE: <5% drop in row count
            elif variance < 0.:
                msg = 'The number of rows in {} decreased by {:.2%}' \
                    .format(new.fname, -variance)
                issue_tracker.add_issue(filename=new.fname,
                                        file_id=new.id,
                                        run_date=run_datetime,
                                        refresh=config['refresh'],
                                        old_count=old_count,
                                        new_count=new_count,
                                        variance=variance,
                                        issue_id=6,
                                        message=msg,
                                        source_id=config['source_id'],
                                        client_id=config['client_id'])

            # Print column alignment
            col_matches = [False] * max(len(old.data.columns), len(new.data.columns))
            for index in range(0, min(len(old.data.columns), len(new.data.columns))):
                col_matches[index] = old.data.columns[index].replace('\x00', '') \
                                     == new.data.columns[index].replace('\x00', '')
            print('Match column names: {}\n'.format(
                ' '.join([str(b).upper() for b in col_matches])), file=output)

            # ISSUE: One or more columns changed
            if not all(col_matches):
                msg = 'The columns in {} did not match the previous file.' \
                    .format(new.fname)
                issue_tracker.add_issue(filename=new.fname,
                                        file_id=new.id,
                                        run_date=run_datetime,
                                        refresh=config['refresh'],
                                        old_count=old_count,
                                        new_count=new_count,
                                        variance=variance,
                                        issue_id=4,
                                        message=msg,
                                        source_id=config['source_id'],
                                        client_id=config['client_id'])

        # Add a neutral issue for a successfully matched pair of files
        if pair['old_file'] is not None and pair['new_file'] is not None:
            msg = 'The new file {} was matched with {}.' \
                .format(new.fname, old.fname)
            issue_tracker.add_issue(filename=new.fname,
                                    file_id=new.id,
                                    run_date=run_datetime,
                                    refresh=config['refresh'],
                                    old_count=old_count,
                                    new_count=new_count,
                                    variance=variance,
                                    issue_id=1,
                                    message=msg,
                                    source_id=config['source_id'],
                                    client_id=config['client_id'])
        elif pair['old_file'] is None:
            msg = 'Could not find a match for {} in the previous refresh.' \
                .format(new.fname)
            issue_tracker.add_issue(filename=new.fname,
                                    file_id=new.id,
                                    run_date=run_datetime,
                                    refresh=config['refresh'],
                                    old_count=old_count,
                                    new_count=new_count,
                                    variance=variance,
                                    issue_id=3,
                                    message=msg,
                                    source_id=config['source_id'],
                                    client_id=config['client_id'])
        elif pair['new_file'] is None:
            msg = 'Could not find a match for the required file {}.' \
                .format(old.fname)
            issue_tracker.add_issue(filename=old.fname,
                                    file_id=old.id,
                                    run_date=run_datetime,
                                    refresh=config['refresh'],
                                    old_count=old_count,
                                    new_count=new_count,
                                    variance=variance,
                                    issue_id=2,
                                    message=msg,
                                    source_id=config['source_id'],
                                    client_id=config['client_id'])

def declare_files(pair, output):
    '''Print which files are compared'''

    if pair['new_file'] is not None:
        print('  {}'.format(pair['new_file']))
        print('-' * 40 + '\n{}\n'.format(pair['new_file']), file=output)
    else:
        print('  {}'.format(pair['old_file']))
        print('-' * 40 + '\n{}\n'.format(pair['old_file']), file=output)


def open_file_pair(pair):
    '''Read each file from pair into a df'''
    # Try to read the files and print a message if they can't be read
    error_message = 'The file {} could not be read for an unknown reason.'
    old = new = None
    if pair['old_path'] is not None:
        try:
            old = read_file(pair['old_path'])
        except:
            print(error_message.format(pair['old_file']), file=output)
            print(error_message.format(pair['old_file']))
    if pair['new_path'] is not None:
        try:
            new = read_file(pair['new_path'])
        except:
            print(error_message.format(pair['new_file']), file=output)
            print(error_message.format(pair['new_file']))

    return old, new


def row_count(old, new):
    '''Calculate row count for each file and variance between the two'''

    if old is None and new is None:
        # May not have been able to read the files
        return None, None, None
    elif old is None:
        old_count = None
        new_count = new.shape[0]
        variance = 1
    elif new is None:
        old_count = old.shape[0]
        new_count = None
        variance = -1
    else:
        old_count = old.shape[0]
        new_count = new.shape[0]
        variance = new_count / old_count - 1 if old_count > 0 else 1

    return old_count, new_count, variance


def analyze_date_cols(file, output):
    '''Identify date columns, find min and max dates.'''
    df = file.data
    date_cols = find_date_columns(df)
    print('The {} file {} has {} rows and {} date column(s).'
          .format(file.desc, file.fname, file.count, len(date_cols)), file=output)

    # For each date column, find the min and max dates
    for col in date_cols:
        _series = fix_date_column(df[col])
        _series = _series.sort_values().dropna()
        _series = _series[_series.str.contains('^\\d{4}-\\d{2}-\\d{2}|\\d{2}/\\d{2}/\\d{4}')]
        if _series.empty:
            print('The date column {} is empty.'.format(col),
                  file=output)
            continue
        print('The date column {} has dates {} through {}.'
              .format(col, min(_series), max(_series)), file=output)

    print('There are {} columns: {}\n'
          .format(df.shape[1], '|'.join(df.columns)), file=output)
