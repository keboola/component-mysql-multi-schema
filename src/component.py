'''
Template Component main class.

'''

import gzip

import base64
import csv
import json
import logging
import os
import sys
import time
from kbc.env_handler import KBCEnvHandler

from mysql_connect.client import Client

# configuration variables
KEY_DEST_BUCKET = 'dest_bucket'
KEY_ROW_LIMIT = 'row_limit'
KEY_PKEY = 'pkey'
# sort key column parameters
KEY_SORT_KEY = 'sort_key'
KEY_SORTKEY_TYPE = 'sort_key_type'
KEY_SORT_KEY_COL = 'col_name'

KEY_COLUMNS = 'columns'
KEY_NAME = 'name'
KEY_SCHEMA_PATTERN = 'schema_pattern'
KEY_SCHEMA_LIST = 'schema_list'
KEY_USER = 'user'
KEY_PASSWORD = '#password'
KEY_HOST = 'host'
KEY_PORT = 'port'
KEY_TABLES = 'tables'
KEY_INCREMENTAL_FETCH = 'incremental_fetch'
KEY_MAX_RUNTIME_SEC = 'max_runtime_sec'

KEY_VALIDATION_MODE = 'validation_mode'

# max runtime default 6.5hrs
MAX_RUNTIME_SEC = 21600
# #### Keep for debug
KEY_DEBUG = 'debug'
MANDATORY_PARS = [KEY_USER, KEY_PASSWORD, KEY_HOST, KEY_PORT, KEY_TABLES, [KEY_SCHEMA_PATTERN, KEY_SCHEMA_LIST]]


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, )
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True

        log_level = logging.DEBUG if debug else logging.INFO
        # setup GELF if available
        if os.getenv('KBC_LOGGER_ADDR', None):
            self.set_gelf_logger(log_level)
        else:
            self.set_default_logger(log_level)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)
        state = self.get_state_file()
        self.last_state = state if state else dict()
        # init execution timer
        self.start_time = time.perf_counter()
        self.max_runtime_sec = float(self.cfg_params.get(KEY_MAX_RUNTIME_SEC, MAX_RUNTIME_SEC))
        self._res_file_cache = dict()

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        cl = Client(params[KEY_HOST], params[KEY_PORT], params[KEY_USER], params[KEY_PASSWORD])
        schema_pattern = params.get(KEY_SCHEMA_PATTERN)
        schema_list = params.get(KEY_SCHEMA_LIST)
        if not schema_list:
            schemas = cl.get_schemas_by_pattern(schema_pattern)
        else:
            schemas = schema_list

        validation_mode = params.get(KEY_VALIDATION_MODE, False)
        # iterate through schemas
        last_state = self.get_last_state()
        res_tables = dict()
        last_indexes = dict()
        total_schemas = len(schemas)
        logging.info(f'{total_schemas} schemas found matching the filter/pattern.')
        for i, s in enumerate(schemas):
            logging.info(f'Dowloading all tables from schema {s}')
            if i % 10 == 0:
                logging.info(f'Processing {i}. schema out of {total_schemas}.')
            table_cols, downloaded_tables_indexes = self.download_tables(s, params, last_state, cl)
            last_indexes = {**last_indexes, **downloaded_tables_indexes}
            res_tables = {**res_tables, **table_cols}
            # get table counts if validation
            if validation_mode:
                table_cols = self.download_table_row_counts(s, params, downloaded_tables_indexes, cl)
                res_tables = {**res_tables, **table_cols}
            if self.is_timed_out():
                logging.warning(f'Max exection time of {self.max_runtime_sec}s has been reached. '
                                f'Terminating. Job will continue next run.')
                break

        # gzip and store state
        data = json.dumps(last_indexes)
        data_gzipped = gzip.compress(bytes(data, 'utf-8'))
        data_gzipped = str(base64.b64encode(data_gzipped), 'utf-8')
        self.write_state_file({'data': data_gzipped})

        # store manifest
        default_bucket = f'in.c-kds-team-ex-mysql-multi-schema-{os.getenv("KBC_CONFIGID")}'
        if params.get(KEY_DEST_BUCKET):
            default_bucket = params.get(KEY_DEST_BUCKET)
        for t in res_tables:
            self.configuration.write_table_manifest(os.path.join(self.tables_out_path, t),
                                                    destination=f'{default_bucket}.{t}',
                                                    columns=res_tables[t]['columns'],
                                                    incremental=True, primary_key=res_tables[t]['pk'])
        self._close_res_stream()

    def download_tables(self, schema, params, last_state, client):
        """
        Download tables using buffered cursor (in-mem full result)
        """
        cl = client
        downloaded_tables = {}
        downloaded_tables_indexes = dict()
        for t in params[KEY_TABLES]:
            incremental_fetch = t.get(KEY_INCREMENTAL_FETCH, True)
            name = t[KEY_NAME]
            columns = t[KEY_COLUMNS]
            pkey = t.get(KEY_PKEY)
            last_index = None
            row_limit = None
            sort_key = dict()
            if not isinstance(pkey, list):
                pkey = [pkey]
            if incremental_fetch:
                row_limit = params.get(KEY_ROW_LIMIT)
                sort_key = t.get(KEY_SORT_KEY, {KEY_SORTKEY_TYPE: 'numeric', KEY_SORT_KEY_COL: ','.join(pkey)})
                last_index = last_state.get(schema, {}).get(name)

            # get sort key
            # validate
            if incremental_fetch and len(pkey) > 1 and not t.get(KEY_SORT_KEY):
                raise Exception(
                    f'Table "{name}" containing a composite pkey is set to incremental fetch '
                    f'but no sort key is specified! ')

            logging.debug(f"Downloading table '{name}' from schema '{schema}''.")

            buffered_cursor = False if not incremental_fetch or (
                    incremental_fetch and int(row_limit) > 500000) else True
            if buffered_cursor:
                downloaded_tables, downloaded_tables_indexes = self.get_table_data(name, schema, columns, pkey,
                                                                                   row_limit, last_index,
                                                                                   sort_key,
                                                                                   downloaded_tables,
                                                                                   downloaded_tables_indexes, cl)
            else:
                downloaded_tables, downloaded_tables_indexes = self.get_table_data_chunks(name, schema, columns, pkey,
                                                                                          row_limit, last_index,
                                                                                          sort_key,
                                                                                          downloaded_tables,
                                                                                          downloaded_tables_indexes, cl)
            if self.is_timed_out():
                logging.warning(f'Max exection time of {self.max_runtime_sec}s has been reached. '
                                f'Terminating. Job will continue next run.')
                break

        return downloaded_tables, downloaded_tables_indexes

    def get_table_data_chunks(self, name, schema, columns, pkey, row_limit, last_index, sort_key, downloaded_tables,
                              downloaded_tables_indexes, client):
        """
        Download tables using sccursor (in chunks)
        :param name:
        :param schema:
        :param columns:
        :param pkey:
        :param row_limit:
        :param last_index:
        :param sort_key:
        :param downloaded_tables:
        :param downloaded_tables_indexes:
        :param client:
        :return:
        """
        has_data = False
        col_names = []
        for data, col_names, last_id in client.get_table_data_chunks(name, schema, columns=columns,
                                                                     row_limit=row_limit, since_index=last_index,
                                                                     sort_key_col=sort_key.get(KEY_SORT_KEY_COL),
                                                                     sort_key_type=sort_key.get(KEY_SORTKEY_TYPE)):

            if data:
                has_data = True
                col_names = col_names
                self.store_table_data(data, name, schema)

        if has_data:
            # append schema col
            col_names.append('schema_nm')
            pkey.append('schema_nm')
            downloaded_tables[name] = {'columns': col_names, 'pk': pkey}
            downloaded_tables_indexes[schema] = {**downloaded_tables_indexes.get(schema, dict()), **{name: last_id}}

        return downloaded_tables, downloaded_tables_indexes

    def get_table_data(self, name, schema, columns, pkey, row_limit, last_index, sort_key, downloaded_tables,
                       downloaded_tables_indexes, client):
        data, col_names, last_id = client.get_table_data_buffered(name, schema, columns=columns,
                                                                  row_limit=row_limit, since_index=last_index,
                                                                  sort_key_col=sort_key.get(KEY_SORT_KEY_COL),
                                                                  sort_key_type=sort_key.get(KEY_SORTKEY_TYPE))

        if data:
            # append schema col
            col_names.append('schema_nm')
            pkey.append('schema_nm')
            self.store_table_data(data, name, schema)
            downloaded_tables[name] = {'columns': col_names, 'pk': pkey}
            downloaded_tables_indexes[schema] = {**downloaded_tables_indexes.get(schema, dict()), **{name: last_id}}

        return downloaded_tables, downloaded_tables_indexes

    def download_table_row_counts(self, schema, params, table_indexes, client):
        """
        Get count of rows until provided last index
        :param schema:
        :param params:
        :param table_indexes:
        :param client: MySQL client instance
        :return:
        """
        cl = client
        downloaded_tables = {}
        # downloaded_tables_indexes = dict()
        for t in params[KEY_TABLES]:
            name = t[KEY_NAME]
            if name not in table_indexes.get(schema, {}).keys():
                continue
            last_index = table_indexes[schema][name]
            logging.debug(f"Downloading row count of table '{name}' from schema '{schema}''.")
            pkey = t.get(KEY_PKEY)
            if not isinstance(pkey, list):
                pkey = [pkey]
            sort_key = t.get(KEY_SORT_KEY, {KEY_SORTKEY_TYPE: 'numeric', KEY_SORT_KEY_COL: ','.join(pkey)})
            data, col_names = cl.get_table_row_count(name, schema, last_index,
                                                     sort_key.get(KEY_SORT_KEY_COL),
                                                     sort_key.get(KEY_SORTKEY_TYPE))

            if data:
                # append schema col
                col_names.append('table')
                self.store_table_count_data(data, name, schema)
                downloaded_tables['row_counts'] = {'columns': col_names, 'pk': ['table']}
            if self.is_timed_out():
                logging.warning(f'Max exection time of {self.max_runtime_sec}s has been reached. '
                                f'Terminating. Job will continue next run.')
                break

        return downloaded_tables

    def store_table_count_data(self, data, name, schema):
        folder_path = os.path.join(self.tables_out_path, 'row_counts')
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

        file_path = os.path.join(folder_path, 'row_counts' + '.csv')

        # append if exists
        if self._res_file_cache.get(file_path):
            out_file = self._res_file_cache.get(file_path)
        else:
            out_file = open(file_path, 'w+', encoding='utf-8', newline='')
            self._res_file_cache[file_path] = out_file

        writer = csv.writer(out_file)
        for r in data:
            # append schema name
            r = list(r)
            r.append(f'{schema}.{name}')
            writer.writerow(r)

    def store_table_data(self, data, name, schema):
        folder_path = os.path.join(self.tables_out_path, name)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

        file_path = os.path.join(folder_path, name + '.csv')

        # append if exists
        if self._res_file_cache.get(file_path):
            out_file = self._res_file_cache.get(file_path)
        else:
            out_file = open(file_path, 'w+', encoding='utf-8', newline='')
            self._res_file_cache[file_path] = out_file

        writer = csv.writer(out_file)
        for r in data:
            # append schema name
            r = list(r)
            r.append(schema)
            writer.writerow(r)

    def is_timed_out(self):
        elapsed = time.perf_counter() - self.start_time
        return elapsed >= self.max_runtime_sec

    def get_last_state(self):
        last_state = self.last_state
        if not last_state.get('data'):
            return dict()
        else:
            # unzip
            val = gzip.decompress(base64.b64decode(last_state['data'].encode('utf-8'))).decode()
            return json.loads(val)

    def _close_res_stream(self):
        """
        Close all output streams / files. Has to be called at end of extraction, before result processing.

        :return:
        """
        for res in self._res_file_cache:
            self._res_file_cache[res].close()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = False
    try:
        comp = Component(debug)
        comp.run()
    except Exception as e:
        logging.exception(e)
        exit(1)
