#!/usr/bin/env python
# from __future__ import print_function, division, unicode_literals
from collections import OrderedDict
import os
import re
from difflib import SequenceMatcher
from data_error import DataError
from scripts.data_utilities import list_files, list_folders
from scripts.data_utilities import read_file, find_date_columns, fix_date_column

# Looks for complete set of files for refresh.
# Looks at old refresh (if exists) in fact_datascan to find which normalized filenames to compare against.
# If not all expected files are received throws an error.

def find_files(config):
    """Searches for files in the client's /current/ directory,
        or another directory as indicated by `current_dir` in CLIENT_CONFIG."""

    # These dictionaries will contain {file name: absolute file path} pairs
    new_files = {}
    old_files = {}

    # --------------------------------------------------------------------------
    # Known new_dirs and old_dirs
    #       If the user manually specified old/new directories with the files,
    #       add those files to the dictionaries.
    # --------------------------------------------------------------------------
    # if config['old_dirs']:
    #     for d in config['old_dirs']:
    #         old_files.update(list_files(d, as_dict=True))

    if config['new_dirs']:
        for d in config['new_dirs']:
            new_files.update(list_files(d, as_dict=True))
    if new_files:
        return new_files
    # if new_files and old_files:
        # return new_files, old_files

    # --------------------------------------------------------------------------
    # Unknown either new_dirs or old_dirs
    # --------------------------------------------------------------------------
    def gen_folders(path):
        ''' Yields YYYYMM* sub-directories.'''
        print('path: ')
        print(path)
        for folder in os.listdir(path):
            key = folder[:6]
            is_dir = os.path.isdir(os.path.join(path, folder))
            dt_format = re.compile('\\d{6}').match(key)
            if is_dir and dt_format:
                yield key, folder

    # Organize current folders by YYYYMM groups in an OrderedDict
    current_dir = config['current_dir']
    print('current dir: ')
    print(current_dir)
    current_folders = OrderedDict()
    for key, folder in gen_folders(current_dir):
        # Update dictionary, append directory name to list
        if key not in current_folders:
            current_folders[key] = [folder]
        else:
            current_folders[key] += [folder]
    else:
        if not current_folders:
            raise DataError('\nCould not find folders starting with "YYYYMM" '
                            'in: {}'.format(current_dir))

    # Sort current folders by date
    current_folders = OrderedDict(sorted(current_folders.iteritems()))
    for key in current_folders: current_folders[key].sort()

    # Determine the refresh_month
    month_list = sorted(current_folders.keys(), reverse=True)
    refresh_month = config['refresh']
    if refresh_month is not None:
        for idx, month in enumerate(month_list):
            if refresh_month == month: break
        if idx < 1:
            raise DataError('\nInvalid refresh month specified: {}\n'
                            'Please use a different month'.format(refresh_month))
    else:
        idx = 1

    # Identify other relevant refresh months
    current_month, refresh_month, precede_month = (month_list[idx - 1],
                                                   month_list[idx],
                                                   month_list[idx + 1])
    config['refresh'] = refresh_month

    # Error Checking
    if precede_month is None:
        raise DataError('Could not identify the previous refresh.\n'
                        'Please manually specify the path for the old files.')
    if current_month is None:
        raise DataError('Could not identify the current/ directory.\n'
                        'Please manually specify the path for the new files.')

    # For testing
    # print("Current month: {}".format(current_month))
    # print("Refresh month: {}".format(refresh_month))
    # print("Precede month: {}".format(precede_month))

    # Find NEW FILES for this refresh ------------------------------------------
    # Default: Uses most reccent files in the current_month
    path = os.path.join(current_dir, current_folders[current_month][-1])
    config['new_dirs'].append(path)
    new_files.update(list_files(path, as_dict=True))

    # Find OLD FILES for this refresh ------------------------------------------
    # If the /customer/refresh/ directory exists, use it to find a
    # consolidated /customer_submission/ directory for an old refresh.
    refresh_dir = os.path.join(config['client_dir'], 'refresh/')
    if os.path.isdir(refresh_dir):
        def digit_sort(x):
            """Finds digits in a string and returns a sortable number."""
            found_digits = re.findall('\d+', x)
            priority = ''.join(found_digits) if found_digits else 0
            return int(priority)

        def customer_submission_iterator():
            """This will go through all possible customer submission folders."""
            for f in refresh_folders:
                if f.startswith('cycle') or f.startswith(precede_month):
                    for name in ['customer_submission/',
                                 'customer_submissions/',
                                 'claims_files/']:
                        yield os.path.join(refresh_dir, f, name)

        # Identify customer_submission directory of preceding refresh
        refresh_folders = list_folders(refresh_dir)
        refresh_folders.sort(reverse=True, key=digit_sort)
        for f in customer_submission_iterator():
            if os.path.exists(f):
                # NOTE: This is a custom rule for Albany which uses the files
                # from the YYYYMMDD folder inside customer submission rather
                # than just the files directly inside customer submission.
                subfolder = [s for s in os.listdir(f)
                             if not os.path.isfile(s) and s.startswith('20')]
                path = os.path.join(f, subfolder[0]) if subfolder else f
                break

    # if the `refresh` isn't accessible, use preceding data in `current`
    else:
        # NOTE: This is a custom case for Fresenius Nephrology
        #       Search for the most reccent preceding directory in /current/
        possible_folders = current_folders[refresh_month] + current_folders[current_month]
        possible_folders.sort(reverse=True)
        path = os.path.join(current_dir, possible_folders[1])

    config['old_dirs'].append(path)
    old_files.update(list_files(path, as_dict=True))

    return new_files, old_files


def match_files(new_files, dim_file_filename, dim_file, fact_dscan_previous, fact_dscan_refresh, db_conn):
    """ """
    print('Matching the files with each other...')
    print(db_conn)

    # This function takes the normalized filenames (if exist) and looks
    # for the last refresh (if successful) record or most recent in fact_datascan for the same normalized filename



def match_files(new_files, dim_file_filename, dim_file, fact_dscan_previous, fact_dscan_refresh, db_conn):
    """ """
    print('Matching the files with each other...')
    print(db_conn)

    # This section is a bit complicated; it matches the files based on the
    # similarity between their filenames. However, it does this in consultation
    # with the database if there is an active database connection.
    def string_similarity_score(file1, file2):
        """Scores the similarity between two file names on a scale [0, 1]."""
        if file1 is None or file2 is None:
            return 0
        regex = re.compile('\\.csv|\\.xls|\\.xlsx|.txt|.pipe|'
                           '\\d{1,4}\\D\\d{1,4}\\D\\d{1,4}\\D?|\\d{4,8}')
        return SequenceMatcher(None, regex.sub('', file1).lower(),
                               regex.sub('', file2).lower()).ratio()

    def get_file_id(filename):
        """Tries to find a file's ID in `dim_file_filename`."""
        if filename is None:
            return 0
        search = dim_file_filename.loc[
            dim_file_filename['filename'] == filename, 'file_id'].values
        return search[0] if len(search) > 0 else 0

    def get_required(file_id):
        """Tries to determine if a file ID is required or not."""
        if file_id not in dim_file.index:
            return True
        search = dim_file.loc[dim_file['id'] == file_id, 'required'].values
        return search[0] if len(search) > 0 else 0

    # Generate every possible file pair of new and old files
    # Also generate pairs of each file and `None` to represent unmatched files
    pairs = []
    old_files.update({None: None})
    new_files.update({None: None})
    for old_file, old_path in old_files.items():
        for new_file, new_path in new_files.items():
            old_id = get_file_id(old_file)
            new_id = get_file_id(new_file)
            new_folder = None if new_path is None else new_path.split('/')[-2]

            # This dictionary structure will be used to represent a pair of
            # files for the rest of the data scan.
            pair = {
                'old_file': old_file,
                'old_path': old_path,
                'new_file': new_file,
                'new_path': new_path,
                'old_id': old_id,
                'new_id': new_id,
                'old_required': get_required(old_id),
                'new_required': get_required(new_id),
                'new_folder': new_folder,
                'score': string_similarity_score(old_file, new_file)
            }
            pairs.append(pair)

    # Sort the pairs first by new folder, second by string similarity
    pairs.sort(key=lambda x: (x['new_folder'], x['score']), reverse=True)

    # Determine the matches
    final_pairs = []
    for pair in pairs:

        # If either of these files have been matched already, skip this pair
        if pair['old_file'] in [p['old_file'] for p in final_pairs] \
                and pair['old_file'] is not None:
            continue
        if pair['new_file'] in [p['new_file'] for p in final_pairs] \
                and pair['new_file'] is not None:
            continue

        # If this is an empty pair, skip it
        if pair['old_file'] is None and pair['new_file'] is None:
            continue

        # # If this pair only contains a non-required old file, skip it
        # if pair['new_file'] is None and not pair['old_required']:
        #     continue

        # If these files have different IDs which are not both 0, skip this pair
        if 0 != pair['old_id'] != pair['new_id'] != 0:
            continue

        # If the string similarity score is too low, skip this pair
        if pair['old_file'] is not None and pair['new_file'] is not None \
                and pair['score'] < 0.25:
            continue

        # If none of the above are true, update the IDs and finalize this match
        if pair['old_id'] != pair['new_id']:
            pair['old_id'] = max(pair['old_id'], pair['new_id'])
            pair['new_id'] = max(pair['old_id'], pair['new_id'])
        final_pairs.append(pair)

    # Sort the pairs so matches come first in alphabetical order
    final_pairs.sort(key=lambda x: (int(x['old_file'] is None),
                                    int(x['new_file'] is None),
                                    x['old_file'],
                                    x['new_file']))

    # Do a final check to make sure we didn't lose any new files
    found_new = set(new_files).difference([None])
    final_new = set([p['new_file'] for p in final_pairs]).difference([None])
    assert found_new == final_new

    # Stop if there's nothing to analyze
    assert final_pairs

    return final_pairs
