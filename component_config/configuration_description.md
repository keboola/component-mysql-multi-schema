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
    - `pkey` - name of the primary key column to support incremental fetching
    - `sort_key` - parameters of a column that should be used for incremental fetching => each new record has larger or equal value 
     of that key than the previous one. If left empty 'pkey' with type 'numeric' is used
        - `col_name` - name of the sort column, e.g. "order_date"
        - `sort_key_type` - type of the sort column: either `string` or `numeric`
    
```json
{
"tables": [
      {"name": "orders",
      "columns": [],
      "pkey": "id",
      "sort_key" : {
        "col_name": "order_date",
        "sort_key_type":  "string"
      }}
    ]
}
  ```