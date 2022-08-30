"""
Be note that this is a mock library, if not connected to database, 
it can only run on mock data and mock function which is support by numpy
"""
import functools
import numpy as np
from urllib import request
import inspect
import requests

GREPTIME_DB_CONN_ADDRESS = "localhost:443"
"""The Global Variable for address for conntect to database"""

log = np.log
sum = np.nansum
sqrt = np.sqrt
pow = np.power
nan = np.nan


class TimeStamp(str):
    """
    TODO: impl date time
    """
    pass


class i32(int):
    """
    For Python Coprocessor Type Annotation ONLY
    A signed 32-bit integer.
    """

    def __repr__(self) -> str:
        return "i32"


class i64(int):
    """
    For Python Coprocessor Type Annotation ONLY
    A signed 64-bit integer.
    """

    def __repr__(self) -> str:
        return "i64"


class f32(float):
    """
    For Python Coprocessor Type Annotation ONLY
    A 32-bit floating point number.
    """

    def __repr__(self) -> str:
        return "f32"


class f64(float):
    """
    For Python Coprocessor Type Annotation ONLY
    A 64-bit floating point number.
    """

    def __repr__(self) -> str:
        return "f64"


class vector(np.ndarray):
    """
    A compact Vector with all elements of same Data type.
    """
    _datatype: str | None = None

    def __new__(
        cls,
        lst,
        dtype=None
    ) -> ...:
        self = np.asarray(lst).view(cls)
        self._datatype = dtype
        #print("In vector's __new__",lst, ty)
        return self

    def __str__(self) -> str:
        #print("In vector's __str__")
        return "vector({}, \"{}\")".format(super().__str__(), self.datatype())

    def datatype(self):
        return self._datatype

    def filter(self, lst_bool):
        return self[lst_bool]


def prev(lst):
    ret = np.zeros(len(lst))
    ret[1:] = lst[0:-1]
    ret[0] = nan
    return ret


def query(sql: str):
    pass


def interval(arr: list, duration: int, fill, step: None | int = None, explicitOffset=False):
    """
    Note that this is a mock function with same functionailty to the actual Python Coprocessor
    `arr` is a vector of integral or temporal type.

    `duration` is the length of sliding window

    `stpe` being the length when sliding window take a step

    `fill` indicate how to fill missing value:
    - "prev": use previous 
    - "post": next
    - "linear": linear interpolation, if not possible to interpolate certain types, fallback to prev
    - "null": use null
    - "none": do not interpolate
    """
    if step == None:
        step = duration

    tot_len = int(np.ceil(len(arr) // step))
    slices = np.zeros((tot_len, int(duration)))
    for idx, start in enumerate(range(0, len(arr), step)):
        slices[idx] = arr[start:(start + duration)]
    return slices


def factor(unit: str) -> int:
    if unit == "d":
        return 24 * 60 * 60
    elif unit == "h":
        return 60 * 60
    elif unit == "m":
        return 60
    elif unit == "s":
        return 1
    else:
        raise Exception("Only d,h,m,s, found{}".format(unit))


def datetime(input_time: str) -> int:
    """
    support `d`(day) `h`(hour) `m`(minute) `s`(second)

    support format:
    `12s` `7d` `12d2h7m`
    """

    prev = 0
    cur = 0
    state = "Num"
    parse_res = []
    for idx, ch in enumerate(input_time):
        if ch.isdigit():
            cur = idx

            if state != "Num":
                parse_res.append((state, input_time[prev:cur], (prev, cur)))
                prev = idx
                state = "Num"
        else:
            cur = idx
            if state != "Symbol":
                parse_res.append((state, input_time[prev:cur], (prev, cur)))
                prev = idx
                state = "Symbol"
    parse_res.append((state, input_time[prev:cur+1], (prev, cur+1)))

    cur_idx = 0
    res_time = 0
    while cur_idx < len(parse_res):
        pair = parse_res[cur_idx]
        if pair[0] == "Num":
            val = int(pair[1])
            nxt = parse_res[cur_idx+1]
            res_time += val * factor(nxt[1])
            cur_idx += 2
        else:
            raise Exception("Two symbol in a row is impossible")

    return res_time


def coprocessor(args, returns, sql=None):
    """
    The actual coprocessor, which will connect to database and update 
    whatever function decorated with `@coprocessor(args=[...], returns=[...], sql=...)`
    """
    def decorator_copr(func):
        @functools.wraps(func)
        def wrapper_do_actual(*args, **kwargs):
            # print("Mock Python Coprocessor post:")
            # print("args=", args,"kwargs=", kwargs)
            print(inspect.getsource(func))
            # print(func(*args, **kwargs))
            # insert actual communciation code here for real thing
            if len(args)!=0 or len(kwargs)!=0:
                raise Exception("Expect call with no arguements(for all args are given by coprocessor itself)")
            source = inspect.getsource(func)
            global GREPTIME_DB_CONN_ADDRESS
            res = requests.post(
                "{}/v1/scripts".format(GREPTIME_DB_CONN_ADDRESS),
                json={
                    "script": source
                })
            return res
        return wrapper_do_actual
    return decorator_copr


# make a alias for short
copr = coprocessor
