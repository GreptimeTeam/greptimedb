# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
import datetime
import pyorc

data = {
    "double_a": [1.0, 2.0, 3.0, 4.0, 5.0],
    "a": [1.0, 2.0, None, 4.0, 5.0],
    "b": [True, False, None, True, False],
    "str_direct": ["a", "cccccc", None, "ddd", "ee"],
    "d": ["a", "bb", None, "ccc", "ddd"],
    "e": ["ddd", "cc", None, "bb", "a"],
    "f": ["aaaaa", "bbbbb", None, "ccccc", "ddddd"],
    "int_short_repeated": [5, 5, None, 5, 5],
    "int_neg_short_repeated": [-5, -5, None, -5, -5],
    "int_delta": [1, 2, None, 4, 5],
    "int_neg_delta": [5, 4, None, 2, 1],
    "int_direct": [1, 6, None, 3, 2],
    "int_neg_direct": [-1, -6, None, -3, -2],
    "bigint_direct": [1, 6, None, 3, 2],
    "bigint_neg_direct": [-1, -6, None, -3, -2],
    "bigint_other": [5, -5, 1, 5, 5],
    "utf8_increase": ["a", "bb", "ccc", "dddd", "eeeee"],
    "utf8_decrease": ["eeeee", "dddd", "ccc", "bb", "a"],
    "timestamp_simple": [datetime.datetime(2023, 4, 1, 20, 15, 30, 2000), datetime.datetime.fromtimestamp(int('1629617204525777000')/1000000000), datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1), datetime.datetime(2023, 3, 1)],
    "date_simple": [datetime.date(2023, 4, 1), datetime.date(2023, 3, 1), datetime.date(2023, 1, 1), datetime.date(2023, 2, 1), datetime.date(2023, 3, 1)]
}

def infer_schema(data):
    schema = "struct<"
    for key, value in data.items():
        dt = type(value[0])
        if dt == float:
            dt = "float"
        elif dt == int:
            dt = "int"
        elif dt == bool:
            dt = "boolean"
        elif dt == str:
            dt = "string"
        elif key.startswith("timestamp"):
            dt = "timestamp"
        elif key.startswith("date"):
            dt = "date"
        else:
            print(key,value,dt)
            raise NotImplementedError
        if key.startswith("double"):
            dt = "double"
        if key.startswith("bigint"):
            dt = "bigint"
        schema += key + ":" + dt + ","

    schema = schema[:-1] + ">"
    return schema



def _write(
    schema: str,
    data,
    file_name: str,
    compression=pyorc.CompressionKind.NONE,
    dict_key_size_threshold=0.0,
):
    output = open(file_name, "wb")
    writer = pyorc.Writer(
        output,
        schema,
        dict_key_size_threshold=dict_key_size_threshold,
        # use a small number to ensure that compression crosses value boundaries
        compression_block_size=32,
        compression=compression,
    )
    num_rows = len(list(data.values())[0])
    for x in range(num_rows):
        row = tuple(values[x] for values in data.values())
        writer.write(row)
    writer.close()

    with open(file_name, "rb") as f:
        reader = pyorc.Reader(f)
        list(reader)


_write(
    infer_schema(data),
    data,
    "test.orc",
)
