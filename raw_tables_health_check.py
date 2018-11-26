#!/usr/bin/env python3

import datetime
import os
import sys

import pandas as pd

sys.path.insert(0, os.getcwd())

from component.utils.mail_sender import send_email
from component.utils.impala_db import ImpalaDB, DF_RESULT_CHECK_FIELD_NAME, STATUS_WRONG, DF_DB_NAME, DF_TABLE_NAME, \
    DF_ERROR_MESSAGE
from component.utils.report_generate import create_html_table

# impala raw database names list (separated by \n symbols)

IMPALA_RAW_DB_NAMES = os.environ['IMPALA_RAW_DB_NAMES']

EMAIL_SUBJECT_NAME = 'Impala RAW unregistered tables'
EMAIL_SUBJECT_ERROR_NAME = 'Register RAW running error'

QUERY_SHOW_TABLE_LIST_STATEMENT = 'SHOW TABLES IN {}'
QUERY_CHECK_TABLE_STATEMENT = 'SELECT 1 FROM {} LIMIT 1'

PROCESSED_LOG_TITLE = 'Impala RAW area tables health check on {}'
PROCESSED_ERROR_MESSAGE = 'Results have not been processed successfully {}'
PROCESSED_TABLES_FAILED_MESSAGE = 'Unregistered tables have been found.'
PROCESSED_SUCCESSFULL_MESSAGE = 'All tables are registered correctly.'

DF_RESULT_COLUMN_NAMES = [DF_DB_NAME, DF_TABLE_NAME, DF_ERROR_MESSAGE]
DF_REFULT_TITLE_NAMES = ['Database name', 'Failed table name', 'Message']


class ImpalaRawCheck():

    """ Collects specified databases and their tables.
        Excludes 'Hard deletes' tables.
        Checks if all tables are available. """

    def __init__(self):
        self.impala_db = ImpalaDB()
        try:
            self.connection = self.impala_db.connect()
            self.cursor = self.impala_db.get_cursor(self.connection)
        except Exception as e:
            send_email(EMAIL_SUBJECT_ERROR_NAME, PROCESSED_ERROR_MESSAGE.format(str(e)), False)
            sys.exit(PROCESSED_ERROR_MESSAGE.format(str(e)))

    def process(self):
        try:
            self.check_tables()
        except Exception as e:
            send_email(EMAIL_SUBJECT_ERROR_NAME, PROCESSED_ERROR_MESSAGE.format(str(e)), False)
            sys.exit(PROCESSED_ERROR_MESSAGE.format(str(e)))
        finally:
            self.impala_db.release_connect()

    def check_tables(self):

        current_date_time = datetime.datetime.now()
        print(PROCESSED_LOG_TITLE.format(current_date_time.strftime("%Y-%m-%d %H:%M")))

        db_names_list = IMPALA_RAW_DB_NAMES.split()
        all_db_tables = []

        for db_name in db_names_list:
            db_tables_df = self.impala_db.get_unregistered_tables(db_name)
            all_db_tables.append(db_tables_df)

        all_db_tables_df = pd.concat(all_db_tables)

        error_db_tables_df = all_db_tables_df[all_db_tables_df[DF_RESULT_CHECK_FIELD_NAME] == STATUS_WRONG]

        if not error_db_tables_df.empty:
            print(PROCESSED_TABLES_FAILED_MESSAGE)
            #print(error_db_tables_df)
            email_message = create_html_table(error_db_tables_df, DF_RESULT_COLUMN_NAMES, DF_REFULT_TITLE_NAMES)
            # fixme uncomment
            send_email(EMAIL_SUBJECT_NAME, email_message, True)
        else:
            print(PROCESSED_SUCCESSFULL_MESSAGE)


if __name__ == '__main__':
    impalaRawCheck = ImpalaRawCheck()
    impalaRawCheck.process()
