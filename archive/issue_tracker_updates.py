#!/usr/bin/env python
from collections import OrderedDict
import pandas as pd

class IssueTracker(object):
    """Keeps track of issues throughout a data scan."""
    # def __init__(self, dim_issue):
    #     """Sets up the issue tracker using `dim_issue` to look up issue IDs."""
    #     fields = ['filename', 'file_id', 'run_date', 'refresh', 'old_count',
    #               'new_count', 'variance', 'issue_id', 'message', 'source_id']
    #     self.issues = OrderedDict([(col, []) for col in fields])
    #     self.dim_issue = dim_issue

    ## Add client id -- Match with out_file script.
    def __init__(self, dim_issue):
        """Sets up the issue tracker using `dim_issue` to look up issue IDs."""
        fields = ['filename', 'file_id', 'run_date', 'refresh', 'old_count', 'new_count',
                'variance', 'issue_id', 'message', 'source_id', 'client_id']
        self.issues = OrderedDict([(col, []) for col in fields])
        self.dim_issue = dim_issue

    def add_issue(self, **kwargs):
        """Adds an issue to the tracker."""
        for key, value in kwargs.items():
            if key in self.issues:
                self.issues[key].append(value)

    # def insert_issues(self, db_conn):
    #     """Inserts all logged issues into the database."""
    #     statement = 'INSERT INTO fact_datascan ' \
    #                 '(file, file_id, run_date, refresh, old_count,' \
    #                 'new_count, variance, issue_id, message, source_id) ' \
    #                 'VALUES ("{}", {}, "{}", "{}", {}, {}, {}, {}, "{}", {});'

    ## Add client_id -- todo: Add file_id ? -- Match with out_file script.
    def insert_issues(self, db_conn):
        """Inserts all logged issues into the database."""
        statement = 'INSERT INTO fact_datascan ' \
                    '(file, file_id, run_date, refresh, old_count, new_count,' \
                    'variance, issue_id, message, source_id, client_id) ' \
                    'VALUES ("{}", {}, "{}", "{}", {}, {}, {}, {}, "{}", {}, {});'
                    # Add
        cursor = db_conn.cursor()
        print('Entering self issues for loop:')
        print(self.issues)
        for index, issue in pd.DataFrame(self.issues).iterrows():
            if issue['refresh'] is None: continue
            issue = issue.where(pd.notnull(issue), 'NULL')
            issue['variance'] *= 100  # Variance should be in percentage form

            cursor.execute(statement.format(*issue))
        cursor.close()
        db_conn.commit()

    def lookup_severity(self, issue_id):
        """Returns the severity level of a given issue ID."""
        found = self.dim_issue.loc[self.dim_issue['id'] == issue_id, 'severity']
        return int(found.values[0]) if len(found.values) > 0 else 2

    def get_messages(self, severity):
        """Returns a list of all issue messages at a given severity level."""
        messages = []
        for idx, issue in pd.DataFrame(self.issues).iterrows():
            if self.lookup_severity(issue['issue_id']) == severity:
                messages.append(issue['message'])
        return messages
