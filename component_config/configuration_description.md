## Configuration
    
- **Connection parameters**

    ```"#password": "mypass",
    "user": "root",
    "host": "localhost",
    "port": 3308,
  ```
- **`schema_pattern`** - regex schema pattern, all schemas matching the pattern will be queried, ex "northwind*"
- **`schema_list`** - explicit schema list, overrides `schema_pattern`
- **`dest_bucket`** - optional destination bucket ID
- **`row_limit`** - optional limit of rows each run will retrieve - to limit db load, only used with incremental fetch set to true. 
- **`tables`** - List of tables that will be downloaded from each schema - must have same structure.
  - `name` - table name
  - `incremental_fetch` - true/false, if set to true the extractor will always continue from the last point defined by the index column value. Default is `true` if omitted.
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