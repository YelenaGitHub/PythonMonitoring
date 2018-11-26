#!/usr/bin/env python3

import os
import sys

import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas

IMPALA_HOST_NAME = os.environ['IMPALA_HOST']
IMPALA_HOST_PORT = os.environ['IMPALA_PORT']
IMPALA_AUTH_MECHANISM = 'PLAIN'
IMPALA_USER_NAME = os.environ['SYSTEM_USER_USER']
IMPALA_USER_PASSWORD = os.environ['SYSTEM_USER_PASSWORD']

DATABASE_FAILED_CONNECTION_MESSAGE = 'Impala connection has not been established...'
DATABASE_FAILED_RETRIEVED_CURSOR = 'Impala database cursor has not been retrieved...'
DATABASE_FAILED_EXECUTE_STATEMENT = 'ERROR while executing {}'
DATABASE_FAILED_FETCH_STATEMENT = 'ERROR during fetching {}...'

PROCESSING_UNREGISTERED_TABLES = '\nUnregistered tables on {} :'
PROCESSING_HISTORICAL_TABLES = '\nHistorical tables on {} :'

PROCESSED_DB_NAME = "\nDatabase '{}' is processing:"


QUERY_SHOW_TABLE_LIST_STATEMENT = 'SHOW TABLES IN {}'
QUERY_CHECK_SELECT_STATEMENT = 'SELECT 1 FROM {} LIMIT 1'
QUERY_REFRESH_TABLE_STATEMENT = 'REFRESH {}'
QUERY_CREATE_HISTORICAL_TABLE_STATEMENT = 'CREATE TABLE IF NOT EXISTS %s (amount int, created timestamp)'
QUERY_COUNT_OF_RECORDS_STATEMENT = 'SELECT COUNT(1) FROM {}'
QUERY_INSERT_REC_COUNT_STATEMENT = 'INSERT INTO %s values (%s, current_timestamp())'
QUERY_OFFSET_REC_COUNT_STATEMENT = "select count(amount), count(distinct amount) from {} where trunc(created, 'DD') between trunc(current_timestamp() - interval 1 day, 'DD') and trunc(current_timestamp(), 'DD')"
QUERY_IS_TABLE_EXISTS = "SHOW TABLES IN {} LIKE '{}'"

STATUS_CORRECT = 'OK'
STATUS_WRONG = 'FAILED'

STATUS_PROCESSING_TABLE_MSG = '{} {} ...'

DF_DB_NAME = 'database'
DF_TABLE_NAME = 'name'
DF_RESULT_CHECK_FIELD_NAME = 'checked_status'
DF_ERROR_MESSAGE = 'error'

HD_ATTRIBUTE = '_hd'


class ImpalaDB():
    """ The class uses Cloudera impyla package for connecting to an Impala database """

    def __init__(self):
        self.host_name = IMPALA_HOST_NAME
        self.host_port = IMPALA_HOST_PORT
        self.user_name = IMPALA_USER_NAME
        self.user_password = IMPALA_USER_PASSWORD

    def connect(self):
        try:
            self.connect = connect(host=self.host_name,
                                   port=int(self.host_port),
                                   use_ssl = True,
                                   timeout = 100000,
                                   user=self.user_name,
                                   password=self.user_password,
                                   auth_mechanism = IMPALA_AUTH_MECHANISM)
            return self.connect
        except Exception as e:
            err_msg = DATABASE_FAILED_CONNECTION_MESSAGE + str(e)
            print(err_msg)
            raise Exception(e)

    def release_connect(self):
        self.connect.close()

    def get_cursor(self, connect):
        try:
            cursor = connect.cursor()
            return cursor
        except Exception as e:
            sys.exit(DATABASE_FAILED_RETRIEVED_CURSOR + str(e))

    def execute_query(self, statement):

        try:
            cursor = self.get_cursor(self.connect)
            cursor.execute(statement)
            return cursor
        except Exception as e:
            err_msg = DATABASE_FAILED_EXECUTE_STATEMENT.format(statement) + str(e)
            print(err_msg)
            raise Exception(e)

    def fetch_query(self, statement):

        try:
            cursor = self.execute_query(statement)
            return cursor.fetchall()
        except Exception as e:
            err_msg = DATABASE_FAILED_FETCH_STATEMENT.format(statement) + str(e)
            print(err_msg)
            raise Exception(e)

    def get_all_tables(self, db_name):

        """ Gets all tables in the database """

        print(PROCESSED_DB_NAME.format(db_name.upper()))

        cursor = self.execute_query(QUERY_SHOW_TABLE_LIST_STATEMENT.format(db_name))

        all_tables_df = as_pandas(cursor)
        all_tables_df[DF_DB_NAME] = db_name

        return all_tables_df

    def get_unregistered_tables(self, db_name):

        """ Gets tables that can not be selected.
            Hard deletes tables exclude from a result frame."""

        all_tables_df = self.get_all_tables(db_name)
        unregistered_tables_df = all_tables_df[~all_tables_df[DF_TABLE_NAME].str.endswith(HD_ATTRIBUTE)]

        unregistered_tables_df[[DF_RESULT_CHECK_FIELD_NAME, DF_ERROR_MESSAGE]] = \
            all_tables_df[DF_TABLE_NAME].apply(check_table, args=(db_name, self)).apply(pd.Series)

        return unregistered_tables_df

    def process_historical_table(self, db_name, table_name, table_name_prefix):
        """ 1. Create historical table if it's not exists.
            2. Refresh original table.
            3. Collect record count of an original table.
            4. Insert them into historical table. """

        print(PROCESSING_HISTORICAL_TABLES.format(db_name.upper()))

        historical_full_table_name = get_full_table_name(db_name, table_name_prefix + table_name)
        original_full_table_name = get_full_table_name(db_name, table_name)

        try:
            self.execute_query(QUERY_CREATE_HISTORICAL_TABLE_STATEMENT % historical_full_table_name)
        except Exception:
            print("Could not create table")
            return None
            
        self.refresh_table(original_full_table_name)

        self.process_historical_data(original_full_table_name, historical_full_table_name)

        #result_count = self.get_table_count_record(original_full_table_name)

        #if result_count:
        #    record_count = result_count[0][0]
        #    self.insert_record_count(historical_full_table_name, record_count)

        #historical_offset = self.get_historical_table_offset(historical_full_table_name)
        #if historical_offset:
        #    row_count = historical_offset[0][0]
        #    unique_row_count = historical_offset[0][1]

        #print("historical_offset: " + historical_offset)

    def refresh_table(self, table_name):
        self.execute_query(QUERY_REFRESH_TABLE_STATEMENT.format(table_name))

    def get_table_count_record(self, table_name):
        return self.fetch_query(QUERY_COUNT_OF_RECORDS_STATEMENT.format(table_name))

    def insert_record_count(self, table_name, record_count):
        self.execute_query(QUERY_INSERT_REC_COUNT_STATEMENT % (table_name, record_count))

    def get_historical_table_offset(self, table_name):
        """ Monitoring records ingesting """
        return self.fetch_query(QUERY_OFFSET_REC_COUNT_STATEMENT.format(table_name))

    def process_historical_data(self, original_full_table_name, historical_full_table_name):

        result_count = self.get_table_count_record(original_full_table_name)

        if result_count:
            record_count = result_count[0][0]
            self.insert_record_count(historical_full_table_name, record_count)

        historical_offset = self.get_historical_table_offset(historical_full_table_name)
        if historical_offset:
            row_count = historical_offset[0][0]
            unique_row_count = historical_offset[0][1]

    def is_table_exists(self, db_name, table_name):
        result = self.fetch_query(QUERY_IS_TABLE_EXISTS.format(db_name, table_name))
        if result:
            return True
        else:
            return False

def check_table(table_name, db_name, impala_db):

    err_message = None

    try:
        full_table_name = db_name + '.' + table_name
        impala_db.fetch_query(QUERY_CHECK_SELECT_STATEMENT.format(full_table_name))
        checked_status = STATUS_CORRECT

    except Exception as e:
        checked_status = STATUS_WRONG
        err_message = str(e)

    print(STATUS_PROCESSING_TABLE_MSG.format(table_name, checked_status))

    return checked_status, err_message


def get_full_table_name(db_name, table_name):
    return db_name + '.' + table_name

