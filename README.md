# Simple multischema mysql extractor

## Configuration
    
**Connection parameters**

```
    "#password": "mypass",
    "user": "root",
    "host": "localhost",
    "port": 3308,
```

- **schema_pattern** - regex schema pattern, all schemas matching the pattern will be queried, ex "northwind*"
- **schema_list** - explicit schema list, overrides `schema_pattern`
- **row_limit** - optional limit of rows each run will retrieve - to limit db load, only used with incremental fetch set to true. 
- **tables** - List of tables that will be downloaded from each schema - must have same structure.
    - `name` - table name
    - `incremental_fetch` - true/false, if set to true the extractor will always continue from the last point defined 
by the index column value. Default is `true` if omitted.
  - `incremental_loading` - true,false. Default is `true`. If false the output table will be overwritten with current result, otherwise upserted based on the primary key.
    - `columns` - array of column names, if empty all available columns downloaded
    - `pkey` - array or single name of the primary key column to support incremental fetching
    - `sort_key` - only used with incremental fetch, parameters of a column that should be used for incremental fetching => each new record has larger or equal value 
     of that key than the previous one. If left empty 'pkey' with type 'numeric' is used. If pkey is composite and incremental fetch is true,
      this must be set, otherwise it fails.
        - `col_name` - name of the sort column, e.g. "order_date"
        - `sort_key_type` - type of the sort column: either `string` or `numeric`
    
    
```json
{
"tables": [
      {"name": "orders",
       "incremental_fetch": true,
      "columns": [],
      "pkey": ["id"],
      "sort_key" : {
        "col_name": "order_date",
        "sort_key_type":  "string"
      }}
    ]
}
```

### Example config

```json
{
    "#password": "test",
    "user": "root",
    "host": "localhost",
    "port": 3308
    "schema_pattern": "northwind*",
    "schema_list": [],
    "row_limit": 10,
    "tables": [
      {"name": "customers",
    "incremental_fetch": true,
      "columns": [],
      "pkey": "id"},
      {"name": "orders",,
    "incremental_fetch": true,
      "columns": [],
      "pkey": ["id","id2"],
      "sort_key" : {
            "col_name": "order_date",
            "sort_key_type":  "string"
      }}
    ]
  }
```
  

 
## Development
 
This example contains runnable container with simple unittest. For local testing it is useful to include `data` folder in the root
and use docker-compose commands to run the container or execute tests. 

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```
git clone https://bitbucket.org:kds_consulting_team/kbc-python-template.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 