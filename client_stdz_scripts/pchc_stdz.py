#!/usr/bin/env python

import os
from glob import glob
import shutil
import sys


FILENAME_MAP = {
    'demo': 'demographics.txt',
    'blood pressure': 'bp.txt',
    'colonoscopy': 'colonoscopy.txt',
    'medication': 'medication.txt',
    'problems': 'problem.txt',
    'provider': 'provider.txt',
    '10. obs': 'observation.txt',
    'allergy': 'allergy.txt',
    'mam and pap': 'mams.txt',
    'orders': 'orders.txt',
    '7. encounters': 'encounters_1.pipe',
    '8. encounters': 'encounters_2.pipe',
    '9. encounters': 'encounters_3.pipe',
}


def match_filename(filename):
    '''Matches filename with keys from the FILENAME_MAP. Returns Success(Bool), key and value'''
    for key, value in FILENAME_MAP.items():
        if key in filename:
            # print(key)
            return True, key, value
    # print('NO MATCH IN FILENAME_MAP')
    return False, None, 'NO MATCH IN FILENAME_MAP'


def pchc_stand(config, printing = False):
    ''' Traverses eligible subdirectories,
        Copies files into customer_submission,
        Renames files according to FILENAME_MAP.

        Special Notes:
            -- Filenames not matching the keys in FILENAME_MAP do not get copied
            -- shutil.copy() WILL overwrite files, so it's important to be careful with the keys in FILENAME_MAP.
    '''

    # Instantiate customer_submission filepath
    # path = Path('/data/customers/{}/refresh/{}/customer_submission/'.format(customer, refresh_name))
    path = config['new_dirs'][0]

    # List eligible subdirectories within directory
    os.chdir(path)
    files = os.listdir(path)
    # subdirs = [dir for dir in os.listdir() if ]

    # print(subdirs)


    # Walk through eligible subdirs
    print("Converting filenames in {}".format(path))
    new_files = []
    for f in files:
        success, name_regex, new_name = match_filename(f.lower())
        white_space = ' ' * (45 - len(f))
        if success:
            # If we find a match, copy that file into customer_submission
            # with the new filenames
            # shutil.copy(str(f), str(path / new_name)
            # print('str(f)',str(f))
            # print(new_name)
            new_files.append(new_name)
            # print('path / new_name', str(path / new_name))
        else:
            print('ruh roh')
        if printing:
            print('\t{}{}--> {}'.format(f, white_space, new_name))
    return(new_files)

    # for dir in subdirs:
    #     print('\n' + str(dir))
    #     # Find files in the subdirs
    #     files = [f for f in dir.iterdir() if f.is_file()]
    #     # For each file, try to find a match in FILENAME_MAP
    #     for f in files:
    #         success, name_regex, new_name = match_filename(f.name.lower())
    #         white_space = ' ' * (45 - len(f.name))
    #         if success:
    #             # If we find a match, copy that file into customer_submission
    #             # with the new filenames
    #             # shutil.copy(str(f), str(path / new_name)
    #             print('str(f)',str(f))
    #             print('path / new_name', str(path / new_name))
    #         if printing:
    #             print('\t{}{}--> {}'.format(f.name, white_space, new_name))

if __name__ == "__main__":
    pchc_stand(config)
