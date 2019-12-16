# Simple multischema mysql extractor

## Configuration
    
- **Connection parameters**

    ```"#password": "mypass",
    "user": "root",
    "host": "localhost",
    "port": 3308,
  ```
- **`incremental_fetch`** - true/false, if set to true the extractor will always continue from the last point defined 
by the index column value.
- **`schema_pattern`** - regex schema pattern, all schemas matching the pattern will be queried, ex "northwind*"
- **`schema_list`** - explicit schema list, overrides `schema_pattern`
- **`row_limit`** - optional limit of rows each run will retrieve - to limit db load
- **`tables`** - List of tables that will be downloaded from each schema - must have same structure.
    - `name` - table name
    - `columns` - array of column names, if empty all available columns downloaded
    - 'pkey' - name of the primary key column to support incremental fetching
    
    ```"tables": [
      {"name": "customers",
      "columns": [],
      "pkey": "id"}
    ]
  ```

### Example config

```json
{
    "#password": "test",
    "user": "root",
    "host": "localhost",
    "port": 3308,
    "incremental_fetch": true,
    "schema_pattern": "northwind*",
    "schema_list": [],
    "row_limit": 10,
    "tables": [
      {"name": "customers",
      "columns": [],
      "pkey": "id"}
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