### Parquet
The `parquet/basic.parquet` was converted from `csv/basic.csv` via [dbt](https://github.com/andygrove/bdt).

Internal of  `parquet/basic.parquet`: 

Data: 
```
+-----+-------+
| num | str   |
+-----+-------+
| 5   | test  |
| 2   | hello |
| 4   | foo   |
+-----+-------+
```
Schema:
```
+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| num         | Int64     | YES         |
| str         | Utf8      | YES         |
+-------------+-----------+-------------+
```