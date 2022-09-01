import sys
# for annoying releative import beyond top-level package
sys.path.insert(0, "../")
from greptime import mock_tester, coprocessor, greptime as gt_builtin
from greptime.greptime import interval, vector, log, prev, sqrt, datetime
import greptime.greptime as greptime
import json
import numpy as np


def data_sample(k_lines, symbol, density=5 * 30 * 86400):
    """
    Only return close data for simplicty for now
    """
    k_lines = k_lines["result"] if k_lines["ret_msg"] == "OK" else None
    if k_lines == None:
        raise Exception("Expect a `OK`ed message")
    close = [float(i["close"]) for i in k_lines]

    return interval(close, density, "prev")


def as_table(kline: list):
    col_len = len(kline)
    ret = {
        k: vector([fn(row[k]) for row in kline], str(ty))
        for k, fn, ty in
        [
            ("symbol", str, "str"),
            ("period", str, "str"),
            ("open_time", int, "int"),
            ("open", float, "float"),
            ("high", float, "float"),
            ("low", float, "float"),
            ("close", float, "float")
        ]
    }
    return ret

@coprocessor(args=["open_time", "close"], returns=[
    "rv_7d",
    "rv_15d",
    "rv_30d",
    "rv_60d",
    "rv_90d",
    "rv_180d"
],
sql="select open_time, close from k_line")
def calc_rvs(open_time, close):
    from greptime import vector, log, prev, sqrt, datetime, pow, sum
    def calc_rv(close, open_time, time, interval):
        mask = (open_time < time) & (open_time > time - interval)
        close = close[mask]

        avg_time_interval = (open_time[-1] - open_time[0])/(len(open_time)-1)
        ref = log(close/prev(close))
        var = sum(pow(ref, 2)/(len(ref)-1))
        return sqrt(var/avg_time_interval)

    # how to get env var, 
    # maybe through accessing scope and serde then send to remote?
    timepoint = open_time[-1]
    rv_7d = calc_rv(close, open_time, timepoint, datetime("7d"))
    rv_15d = calc_rv(close, open_time, timepoint, datetime("15d"))
    rv_30d = calc_rv(close, open_time, timepoint, datetime("30d"))
    rv_60d = calc_rv(close, open_time, timepoint, datetime("60d"))
    rv_90d = calc_rv(close, open_time, timepoint, datetime("90d"))
    rv_180d = calc_rv(close, open_time, timepoint, datetime("180d"))
    return rv_7d, rv_15d, rv_30d, rv_60d, rv_90d, rv_180d