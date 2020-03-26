#!/usr/bin/env python

"""This script calls the data scan for any clients with new data.

- USAGE FROM THE COMMAND LINE:
To run the data scan from the command line, simply type:
`python listener.py`. Add the argument "--nowait" to skip the 30 minute wait
after new files arrive.

Make sure that `listener.py`, `CLIENT_CONFIG.json`, and `datascan.py` are all in
the same directory.
"""

from __future__ import print_function
import os
import sys
import re
from copy import copy
from json import load
from time import sleep
from datetime import datetime, timedelta
from multiprocessing import Process
from subprocess import Popen

import datascan


def main():
    """Checks each client for new files and run the scripts if any are found."""
    # Load the dictionary of {clients: client information}
    json_location = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'CLIENT_CONFIG.json')
    with open(json_location, mode='r') as f:
        client_config = load(f)

    # Collect all the processes to be executed and run them in parallel
    processes = []

    # For each client
    for client, config in client_config.items():

        # Get the info for this client from the config file
        client_dir = config.get('client_dir',
                                '/home/customers/{}/'.format(client))
        recipients = config.get('recipients', [])
        database = config.get('database', None)
        current_dir = config.get('current_dir',
                                  os.path.join(client_dir, 'current/'))

        # Collect all the YYYYMMDD* directories in their current directory
        current_folders = []
        for item in os.listdir(current_dir):
            item_path = os.path.join(current_dir, item)
            if os.path.isfile(item_path) or not re.match('^\\d{6}.*', item):
                continue
            if os.listdir(item_path):
                current_folders.append(item_path)
        current_folders.sort(reverse=True, key=lambda x: os.path.getmtime(x))

        # We want to run the data scan only if there are file changes since the
        # last time it ran. So, we need to find the most recent change, if any,
        # in the period between 60 and 30 minutes in the past. The data scan
        # would have run 30 minutes after THAT change. Then, we only need to run
        # the data scan again if more files have changed since it last ran.
        last_run = datetime.now() - timedelta(minutes=30)
        for folder in current_folders:
            last_change = datetime.fromtimestamp(os.path.getmtime(folder))
            if datetime.now() - timedelta(minutes=60) < last_change \
                    and last_change < datetime.now() - timedelta(minutes=30):
                last_run = last_change + timedelta(minutes=30)
                break

        # Figure out if there are new files since the last scan
        for folder in current_folders:
            last_change = datetime.fromtimestamp(os.path.getmtime(folder))
            if last_change < last_run:
                continue

            # How long to wait for new files before running the data scan
            wait = last_change + timedelta(minutes=30) - datetime.now()
            wait = wait.seconds

            if len(sys.argv) > 1 and sys.argv[1] == '--nowait':
                wait = 0

            # Queue the data scan for this client
            p = Process(target=run_scripts, kwargs={'client': client,
                                                    'client_dir': client_dir,
                                                    'recipients': recipients,
                                                    'database': database,
                                                    'wait': wait})
            processes.append(p)
            break

    # Execute all the queued processes
    for p in processes:
        p.start()

    # Wait for everything to finish
    for p in processes:
        p.join()


def run_scripts(client, client_dir, recipients, database, wait=0):
    """Runs the data scan and all refresh scripts/commands for `client`.

    Args:
        client: Name of the client.
        client_dir: Path to the client's folder in `/home/customers/`.
        recipients: List of recipients for the data scan.
        database: Name of the test database host.
        wait: How many seconds to wait before running the data scan.
    """
    # Wait in case more files are being added
    sleep(wait)
    try:
        datascan.scan(client=client,
                      client_dir=client_dir,
                      database=database,
                      recipients=recipients)
    except Exception as e:
        print(e.message)
        return

    # ------------ INSERT ADDITIONAL SCRIPTS AFTER DATA SCAN HERE ------------ #

    # These are the parameters we have
    client = client
    client_dir = client_dir
    database = database
    recipients = recipients

    # Now we can run additional scripts here using those parameters if desired

    # Example
    if client == 'fake_client':
        Popen('cd /home/customers/; ls', shell=True)
        Popen('python path/to/script.py argument1 argument2', shell=True).wait()

    # ------------------------------------------------------------------------ #

    return


if __name__ == '__main__':
    main()
