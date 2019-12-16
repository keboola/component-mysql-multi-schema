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