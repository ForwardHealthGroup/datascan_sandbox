#!/usr/bin/env python

"""This script automatically generates a data scan report.

For usage information, see `quick_guide.txt`.
"""

import os
from io import open
import MySQLdb

# Email-Specific imports
from smtplib import SMTP_SSL
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.encoders import encode_base64

# issue_tracker_updates
from issue_tracker_updates import IssueTracker

def update_db(config, db_conn, issue_tracker, dim_file_filename, final_pairs):
    """ """

    # Insert information from the data scan into the database
    if db_conn is not None:
        print("Connecting to database with {}".format(db_conn))

        # Add the issues to `dim_issue`
        issue_tracker.insert_issues(db_conn)

        # Next, update the information in `dim_file_filename`
        dff = dim_file_filename  # It's a long variable name and I am lazy
        cursor = db_conn.cursor()
        insert_statement = 'INSERT INTO dim_file_filename ' \
                           '(file_id, filename, refresh) ' \
                           'VALUES ({}, "{}", "{}");'
        update_statement = 'UPDATE dim_file_filename SET file_id = {} ' \
                           'WHERE filename = "{}";'

        # For each pair, update the old/new filename's info in dim_file_filename
        for pair in final_pairs:
            if config['refresh'] is None:
                print("\nWARNING. Cannot update DB without specifying refresh. "
                      "Refresh variable currently set to '{}''. \nHint: To insert "
                      "into DB use [-r] option.".format(config['refresh']))
                break

            # See if old file is in dim...; if not, insert it; if so, update it
            lookup = dff.loc[dff['filename'] == pair['old_file'], 'file_id']
            if pair['old_file'] and len(lookup.values) == 0:
                cursor.execute(insert_statement.format(
                    pair['old_id'], pair['old_file'], config['refresh']))
            elif pair['old_file'] and lookup.values[0] != pair['old_id']:
                cursor.execute(update_statement.format(
                    pair['old_id'], pair['old_file']))

            # See if new file is in dim...; if not, insert it; if so, update it
            lookup = dff.loc[dff['filename'] == pair['new_file'], 'file_id']
            if pair['new_file'] and len(lookup.values) == 0:
                cursor.execute(insert_statement.format(
                    pair['new_id'], pair['new_file'], config['refresh']))
            elif pair['new_file'] and lookup.values[0] != pair['new_id']:
                cursor.execute(update_statement.format(
                    pair['new_id'], pair['new_file']))


def send_email(issue_tracker, config, output, problems, warnings):
    """ """

    # Check for relevant parameters
    if not config['sender_email'] or not config['recipients']:
        print("\nWARNING. Cannot send email. Missing necessary information.\n"
              "(Data scan is not currently configured to send emails "
              "when executed manually.)")
        return

    # Compose the message
    if problems:
        body = '---------- PROBLEMS: ----------\n{}\n\n' \
            .format('\n'.join(problems))
    else:
        body = 'The data scan completed with no problems.\n\n'
    if warnings:
        body += '---------- WARNINGS ----------\n{}\n\n' \
            .format('\n'.join(warnings))
    else:
        body += 'The data scan completed with no warnings.\n\n'

    # Construct the email
    msg = MIMEMultipart()
    msg['From'] = config['sender_email']
    msg['To'] = ', '.join(config['recipients'])
    msg['Subject'] = '{} Data Scan Complete: {} Problem{}'.format(
        config['client'].upper(), len(problems), 's'*(len(problems) != 1))
    msg.attach(MIMEText(body, 'plain'))

    # Give it the attachment
    attachment = open(output.name, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encode_base64(part)
    part.add_header('Content-Disposition',
                    "attachment; filename= {}".format(os.path.basename(output.name)))
    # Send the email
    server = SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    server.login(config['sender_email'], config['sender_email_password'])
    server.sendmail(from_addr=config['sender_email'],
                    to_addrs=config['recipients'],
                    msg=msg.as_string())
    server.close()
