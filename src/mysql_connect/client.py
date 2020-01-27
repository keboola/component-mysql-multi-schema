import logging

import pymysql
import regex
import time


class ClientError(Exception):
    """

    """


class Client:

    def __init__(self, host, port, user, password):
        """Creates a mysql client and initiates connection"""
        db_opts = {
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }

        self.db = pymysql.connect(**db_opts)

    def get_available_schemas(self):
        cur = self.db.cursor()

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

    def get_table_data(self, table_name, schema, columns=None, row_limit=None, since_index=None,
                       sort_key_col=None, sort_key_type=None):
        cur = self.db.cursor()
        if columns and columns != []:
            columns = ','.join(columns)
        else:
            columns = '*'

        sql = f'SELECT {columns} FROM {schema}.{table_name}'

        if sort_key_col and since_index:
            if sort_key_type == 'string':
                since_index = f"'{since_index}'"
            sql += f' WHERE {sort_key_col} >= {since_index} ORDER BY {sort_key_col}'
        elif sort_key_col:
            sql += f' ORDER BY {sort_key_col}'

        if row_limit:
            sql += f' LIMIT {row_limit}'
        rows = []
        col_names = []
        last_id = None
        try:
            if logging.DEBUG == logging.root.level:
                # wait before each message
                # time.sleep(0.5)
                logging.debug(f'Executing query: {sql}')
            cur.execute(sql)
            rows = cur.fetchall()
            if rows:
                for i in cur.description:
                    col_names.append(i[0])
                last_id = self._get_last_id(rows, col_names, sort_key_col)
        except Exception as e:
            self.db.close()
            raise ClientError(f'Failed to execute query {sql}! {e}')

        return rows, col_names, str(last_id)

    def _get_last_id(self, rows, col_names, index_column):
        if not index_column:
            return None
        pk_index = col_names.index(index_column)
        return rows[-1][pk_index]
