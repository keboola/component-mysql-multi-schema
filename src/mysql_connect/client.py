import logging
import time

import pymysql
import regex

DEFAULT_MAX_CHUNK_SIZE = 500000

READ_TIMEOUT = 1800

MAX_RETRIES = 2

RETRY_CODES = [2013]


class ClientError(Exception):
    """

    """


class Client:

    def __init__(self, host, port, user, password, max_chunk_size=DEFAULT_MAX_CHUNK_SIZE):
        """Creates a mysql client and initiates connection"""
        db_opts = {
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'read_timeout': READ_TIMEOUT
        }

        self.db = pymysql.connect(**db_opts)
        self.max_chunk_size = max_chunk_size

    def get_available_schemas(self):
        cur = self.__get_cursor()

        sql = 'SHOW SCHEMAS'
        rows = []
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except Exception as e:
            self.db.close()
            raise ClientError(f'Failed to retrieve schemas! {e}')

        return rows

    def get_schemas_by_pattern(self, pattern):
        schemas = self.get_available_schemas()
        return [s[0] for s in schemas if regex.search(pattern, s[0])]

    def get_table_data_buffered(self, table_name, schema, columns=None, row_limit=None, since_index=None,
                                sort_key_col=None, sort_key_type=None):
        cur = self.db.cursor()

        start = time.perf_counter()
        sql = self.__build_select_query(columns, sort_key_col, sort_key_type, since_index, row_limit, schema,
                                        table_name)
        rows = []
        col_names = []
        last_id = None
        try:
            if logging.DEBUG == logging.root.level:
                # wait before each message
                # time.sleep(0.5)
                logging.debug(f'Executing query: {sql}')
            cur = self.__try_execute(cur, sql, buffered=True)
            rows = cur.fetchall()
            if rows:
                if logging.DEBUG == logging.root.level:
                    logging.info(f'Fetched {len(rows)} rows from {schema}.{table_name}')
                for i in cur.description:
                    col_names.append(i[0])
                last_id = self._get_last_id(rows, col_names, sort_key_col)

            # timer
            elapsed = time.perf_counter() - start
            logging.debug(f'Query took: {elapsed:.5f}s')

        except pymysql.Error as e:
            if e.args[0] == 1146:
                logging.warning(f'Table {table_name} does not exist in schema {schema}, '
                                f'skipping!')
            else:
                raise ClientError(f'Failed to execute query {sql}!') from e
            pass
        except Exception as e:
            self.db.close()
            raise ClientError(f'Failed to execute query {sql}!') from e

        return rows, col_names, str(last_id)

    def get_table_data_chunks(self, table_name, schema, columns=None, row_limit=None, since_index=None,
                              sort_key_col=None, sort_key_type=None):
        cur = self.db.cursor(pymysql.cursors.SSCursor)

        start = time.perf_counter()
        sql = self.__build_select_query(columns, sort_key_col, sort_key_type, since_index, row_limit, schema,
                                        table_name)
        rows = []
        last_id = None
        try:
            if logging.DEBUG == logging.root.level:
                # wait before each message
                # time.sleep(0.5)
                logging.debug(f'Executing query: {sql}')
            cur = self.__try_execute(cur, sql, buffered=False)
            while True:
                rows = cur.fetchmany(self.max_chunk_size)
                if rows:
                    col_names = []
                    if logging.DEBUG == logging.root.level:
                        logging.info(f'Fetched {len(rows)} rows from {schema}.{table_name}')
                    for i in cur.description:
                        col_names.append(i[0])
                    last_id = self._get_last_id(rows, col_names, sort_key_col)
                    yield rows, col_names, str(last_id)
                else:
                    break
            # timer
            elapsed = time.perf_counter() - start
            logging.debug(f'Query took: {elapsed:.5f}s')

        except pymysql.Error as e:
            if e.args[0] == 1146:
                logging.warning(f'Table {table_name} does not exist in schema {schema}, '
                                f'skipping!')
            else:
                raise ClientError(f'Failed to execute query {sql}!') from e
            pass
        except Exception as e:
            self.db.close()
            raise ClientError(f'Failed to execute query {sql}!') from e

    def __build_select_query(self, columns, sort_key_col, sort_key_type, since_index, row_limit, schema, table_name):
        if columns and columns != []:
            columns = ','.join(columns)
        else:
            columns = '*'

        sql = f'SELECT {columns} FROM {schema}.{table_name}'

        if sort_key_col and since_index not in [None, 'None']:
            if sort_key_type == 'string':
                since_index = f"'{since_index}'"
            sql += f' WHERE {sort_key_col} >= {since_index} ORDER BY {sort_key_col}'
        elif sort_key_col:
            sql += f' ORDER BY {sort_key_col}'

        if row_limit:
            sql += f' LIMIT {row_limit}'
        return sql

    def get_table_row_count(self, table_name, schema, last_index, sort_key_col, sort_key_type):
        cur = self.db.cursor()
        sql = f"SELECT COUNT(*) as cnt, '{last_index}' as last_index, " \
              f"'{sort_key_col}' as sort_key_col  FROM {schema}.{table_name}"
        if sort_key_type == 'string':
            last_index = f"'{last_index}'"
        if last_index and last_index != 'None':
            sql += f' WHERE {sort_key_col} <= {last_index};'

        rows = []
        col_names = []
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if rows:
                for i in cur.description:
                    col_names.append(i[0])

        except Exception as e:
            self.db.close()
            raise ClientError(f'Failed to execute query {sql}! {e}')

        return rows, col_names

    def __try_execute(self, cursor, query, buffered=True):
        retries = 1
        retry = True
        while retry:
            try:
                cursor.execute(query)
                retry = False
            except pymysql.Error as e:
                if e.args[0] in RETRY_CODES and retries <= MAX_RETRIES:
                    logging.warning(f'Query failed retrying {retries}x')
                    time.sleep(2 ^ retries)
                    retries += 1
                    self.db.close()
                    self.db.connect()
                    if buffered:
                        cursor = self.db.cursor()
                    else:
                        cursor = self.db.cursor(pymysql.cursors.SSCursor)
                else:
                    raise e
        return cursor

    def __get_cursor(self):
        try:
            self.db.ping(reconnect=True)
        except pymysql.Error as err:
            logging.debug(f'Ping failed, reconnecting. {err}')
            # reconnect your cursor
            self.db.close()
            self.db.connect()
        return self.db.cursor()

    def _get_last_id(self, rows, col_names, index_column):
        if not index_column:
            return None
        pk_index = col_names.index(index_column)
        return rows[-1][pk_index]
