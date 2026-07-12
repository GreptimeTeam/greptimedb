## Generate orc data

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install -U pyorc

./venv/bin/python write.py

cargo test
```

Schema:
```
+------------------------+-----------------------------+-------------+
| column_name            | data_type                   | is_nullable |
+------------------------+-----------------------------+-------------+
| double_a               | Float64                     | YES         |
| a                      | Float32                     | YES         |
| b                      | Boolean                     | YES         |
| str_direct             | Utf8                        | YES         |
| d                      | Utf8                        | YES         |
| e                      | Utf8                        | YES         |
| f                      | Utf8                        | YES         |
| int_short_repeated     | Int32                       | YES         |
| int_neg_short_repeated | Int32                       | YES         |
| int_delta              | Int32                       | YES         |
| int_neg_delta          | Int32                       | YES         |
| int_direct             | Int32                       | YES         |
| int_neg_direct         | Int32                       | YES         |
| bigint_direct          | Int64                       | YES         |
| bigint_neg_direct      | Int64                       | YES         |
| bigint_other           | Int64                       | YES         |
| utf8_increase          | Utf8                        | YES         |
| utf8_decrease          | Utf8                        | YES         |
| timestamp_simple       | Timestamp(Nanosecond, None) | YES         |
| date_simple            | Date32                      | YES         |
+------------------------+-----------------------------+-------------+
```


Data:
```
"+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+",
"| double_a | a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple |",
"+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+",
"| 1.0      | 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  |",
"| 2.0      | 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |",
"| 3.0      |     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  |",
"| 4.0      | 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  |",
"| 5.0      | 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  |",
"+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+",

```
