#!/usr/bin/env python3

import os

from component.utils.impala_db import ImpalaDB
from component.utils.log_writer import selected_log_line

#import abc

IMPALA_RAW_DB_NAMES = os.environ['IMPALA_RAW_DB_NAMES']
IMPALA_UDL_DB_NAMES = os.environ['IMPALA_UDL_DB_NAMES']
IMPALA_EDM_DB_NAMES = os.environ['IMPALA_EDM_DB_NAMES']
IMPALA_DM_DB_NAMES = os.environ['IMPALA_DM_DB_NAMES']

RAW_LAYER = 'RAW'
UDL_LAYER = 'UDL'
EDM_LAYER = 'EDM'
DM_LAYER = 'DM'
PROCESSING_LAYER_MESSAGE = 'Processing %s layer...'
PROCESSING_UNREGISTERED_TABLES = '\nUnregistered tables on %s :'
PROCESSING_HISTORICAL_TABLE = '\nCheck ingesting of %s...'

NOT_FOUND_MESSAGE = "Not found {}."

# RAW historical tables
CALL2_VOD__C = 'call2_vod__c'
TIME_OFF_TERRITORY_VOD__C = 'time_off_territory_vod__c'
HISTORICAL_TABLE_NAME_PREFIX = 'xx'

IMPALA_RAW_HISTORICAL_TABLE_NAMES = [CALL2_VOD__C, TIME_OFF_TERRITORY_VOD__C]


class LayersCheck():
    """ The main class for all Impala layers processing """

    def __init__(self):
        self.impala_db = ImpalaDB()

        self.connection = self.impala_db.connect()
        self.cursor = self.impala_db.get_cursor(self.connection)

    def process(self):

        db_name_list = get_db_name_list()

        # selected_log_line(PROCESSING_LAYER_MESSAGE % RAW_LAYER)
        rawLayer = RawLayer(self.impala_db)
        for db_name in db_name_list:
            rawLayer.process(db_name)
            # self.check_raw_layer(db_name)

        self.cursor.close()
        self.connection.close()

    def check_raw_historical_data(self, cursor, db_name, table_name):
        print(PROCESSING_HISTORICAL_TABLE % table_name.upper())
        # process_historical_table(cursor, db_name, table_name)

    def process_layer(self, layer_name):
        selected_log_line(PROCESSING_LAYER_MESSAGE % layer_name)

    def get_db_name_list(self):
        return IMPALA_RAW_DB_NAMES.split()


class Layer():
    """ Base Layer class with the common processing logic """

    def __init__(self, impala_db, layer_name):
        selected_log_line(PROCESSING_LAYER_MESSAGE % layer_name)
        self.impala_db = impala_db

    #@abc.abstractmethod
    def process(self, db_name):
        pass

    def get_unregistered_tables(self, db_name):
        return self.impala_db.get_unregistered_tables(db_name)

    def process_historical_table(self, db_name, table_name):
        if self.impala_db.is_table_exists(db_name, table_name):
            self.impala_db.process_historical_table(db_name, table_name, HISTORICAL_TABLE_NAME_PREFIX)


class RawLayer(Layer):
    """Raw layer class for Impala RAW layer databases processing"""

    def __init__(self, impala_db):
        super().__init__(impala_db, RAW_LAYER)
        self.layer_name = RAW_LAYER

    def process(self, db_name):
        """ Look for the unregistered tables and process historical tables """
        # print(self.layer_name)
        unregistered_tables_list = self.get_unregistered_tables(db_name)

        # historical_table_list = IMPALA_RAW_HISTORICAL_TABLE_NAMES.split()
        for historical_table_name in IMPALA_RAW_HISTORICAL_TABLE_NAMES:
            self.process_historical_table(db_name, historical_table_name)

        if not unregistered_tables_list.empty:
            print(NOT_FOUND_MESSAGE.format('unregistered tables'))


def get_db_name_list():
    return IMPALA_RAW_DB_NAMES.split()


# if __name__ == '__main__':
#     layerCheck = LayersCheck()
#     layerCheck.process()
