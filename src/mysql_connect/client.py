import pymysql
import regex


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

        finally:
            self.db.close()
        return rows

    def get_schemas_by_pattern(self, pattern):
        schemas = self.get_available_schemas()
        return [s[0] for s in schemas if regex.search(pattern, s[0])]

    def get_table_data(self, table_name, schema, columns=None, row_limit=None, index_column=None, since_index=None):
        cur = self.db.cursor()
        if columns and columns != []:
            columns = ','.join(columns)
        else:
            columns = '*'

        sql = f'SELECT {columns} FROM {schema}.{table_name}'
        if index_column and since_index:
            sql += f' WHERE {index_column} >= {since_index} ORDER BY {index_column}'
        elif index_column:
            sql += f' ORDER BY {index_column}'

        if row_limit:
            sql += f' LIMIT {row_limit}'
        rows = []
        col_names = []
        last_id = None
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if rows:
                for i in cur.description:
                    col_names.append(i[0])
                last_id = self._get_last_id(rows, col_names, index_column)

        finally:
            self.db.close()

        return rows, col_names, last_id

    def _get_last_id(self, rows, col_names, index_column):
        pk_index = col_names.index(index_column)
        return rows[-1][pk_index]
