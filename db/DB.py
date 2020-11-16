"""
General DB class to manage database connection
"""
__version__ = '0.1'
__author__ = 'Dat Nguyen'

from sqlalchemy import insert, select, create_engine, MetaData, Table
import logging
import logging.config
from os import environ as env


from sqlalchemy.exc import DBAPIError


class DB:
    # The global table names. Modify only this part whenever the ERD is changed.
    TB_NAMES = ['covid19_us_raw', 'covid19_global_raw']

    """Constructor
    Args:
        str_connection: string connection
    """
    def __init__(self, str_connection):
        #logging.config.fileConfig(fname='../log.conf')
        logging.basicConfig(filename='test.log', format='%(asctime)s  %(name)s  %(levelname)s: %(message)s',
                            level=logging.DEBUG)
        self.logger = logging.getLogger('dev')

        self.str_connection = str_connection
        try:
            self.engine = create_engine(str_connection, pool_recycle=3600)
            self.connection = self.engine.connect()
            self.logger.info('Connect database')

            self.metadata = MetaData()

            # Make the list of tables
            #self.table_list = [Table(name, self.metadata, autoload=True, autoload_with=self.engine) for name in DB.TB_NAMES]

            # Combine (name:table) pair in to dictionary
            #self.table_dict = {DB.TB_NAMES[i]: tb for i, tb in enumerate(self.table_list)}
        except DBAPIError as e:
            self.logger.error(f'Cannot connect to database using connection string {self.str_connection}')
            self.connection = None

    def get_table(self, table_name):
        """ Return Table instance for a given table name
            Args:
                table_name (str): table name in string
            Returns:
                Table instance of sqlalchemy
            """
        return self.table_dict[table_name]

    def get_conn(self):
        """
        Simple getter that return the connection
        :return: the database connection
        """
        return self.connection

    def close_connect(self):
        """
        Close the connection
        :return: None
        """
        self.connection.close()
        self.logger.info('Close database')

    def get_logger(self):
        return self.logger
