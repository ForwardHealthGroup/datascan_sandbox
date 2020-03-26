#!/usr/bin/env python
import os
import subprocess

path_to_folder = '/data/customers/fresenius/current/acumen/post_june_refresh/'
os.chdir(path_to_folder)
os.getcwd()
list_of_folders = os.listdir(path_to_folder)
list_of_folders.sort()

client = 'fresenius-acumen'

for index, value in enumerate(list_of_folders):
#     print(index)
    print('\n')
    if index == len(list_of_folders) - 1:
        print('All Datascans Complete')
    else:
        files_new = list_of_folders[index+1]
        files_old = list_of_folders[index]
        refresh = list_of_folders[index+1][0:6]
        data_scan_command = 'python /home/development/automation/datascan/datascan.py {} -r {} -n {}{} -o {}{}'.format(client, refresh, path_to_folder, files_new, path_to_folder, files_old)
        print(data_scan_command)
        the_output = subprocess.check_output(data_scan_command, stderr=subprocess.STDOUT, shell=True )
    print(the_output)

print('I hope this worked!')
