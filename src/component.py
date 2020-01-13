'''
Template Component main class.

'''

import csv
import logging
import os
import sys

from kbc.env_handler import KBCEnvHandler

from mysql_connect.client import Client

# configuration variables
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

# #### Keep for debug
KEY_DEBUG = 'debug'
MANDATORY_PARS = [KEY_USER, KEY_PASSWORD, KEY_HOST, KEY_PORT, KEY_TABLES, [KEY_SCHEMA_PATTERN, KEY_SCHEMA_LIST]]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.2'


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
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)
        state = self.get_state_file()
        self.last_state = state if state else dict()

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

        # iterate through schemas
        res_tables = dict()
        last_indexes = dict()
        for s in schemas:
            table_cols, downloaded_tables_indexes = self.download_tables(s, params)
            last_indexes = {**last_indexes, **downloaded_tables_indexes}
            res_tables = {**res_tables, **table_cols}

        self.write_state_file(last_indexes)

        # store manifest
        for t in res_tables:
            self.configuration.write_table_manifest(os.path.join(self.tables_out_path, t),
                                                    columns=res_tables[t]['columns'],
                                                    incremental=True, primary_key=res_tables[t]['pk'])
        logging.debug(res_tables)
        logging.info(last_indexes)

    def download_tables(self, schema, params):
        cl = Client(params[KEY_HOST], params[KEY_PORT], params[KEY_USER], params[KEY_PASSWORD])
        downloaded_tables = {}
        downloaded_tables_indexes = dict()
        for t in params[KEY_TABLES]:
            name = t[KEY_NAME]
            columns = t[KEY_COLUMNS]
            pkey = t.get(KEY_PKEY)
            # get sort key
            sort_key = t.get(KEY_SORT_KEY, {KEY_SORTKEY_TYPE: 'numeric', KEY_SORT_KEY_COL: pkey})
            last_index = None
            if params.get(KEY_INCREMENTAL_FETCH):
                last_index = self.last_state.get('.'.join([schema, name]))

            logging.info(f"Downloading table {name} from schema {schema}.")

            data, col_names, last_id = cl.get_table_data(name, schema, columns=columns,
                                                         row_limit=params.get(KEY_ROW_LIMIT), since_index=last_index,
                                                         sort_key_col=sort_key[KEY_SORT_KEY_COL],
                                                         sort_key_type=sort_key[KEY_SORTKEY_TYPE])

            if data:
                # append schema col
                col_names.append('schema_nm')
                self.store_table_data(data, name, schema)
                downloaded_tables[name] = {'columns': col_names, 'pk': [pkey]}
                downloaded_tables_indexes['.'.join([schema, name])] = last_id
        return downloaded_tables, downloaded_tables_indexes

    def store_table_data(self, data, name, schema):
        folder_path = os.path.join(self.tables_out_path, name)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

        file_path = os.path.join(folder_path, name + '_' + schema + '.csv')
        if os.path.exists(file_path):
            mode = 'a'
        else:
            mode = 'w+'

        with open(file_path, mode, newline='', encoding='utf-8') as out_file:
            writer = csv.writer(out_file)
            for r in data:
                # append schema name
                r = list(r)
                r.append(schema)
                writer.writerow(r)


# ####### EXAMPLE TO REMOVE END

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
