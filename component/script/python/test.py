from example.calc_rv import as_table, calc_rvs
from greptime import coprocessor, set_conn_addr, mock_tester
import sys
import json

@coprocessor(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n):
    return n+2

if __name__ == "__main__":
    if len(sys.argv)!=2:
        raise Exception("Expect only one address as cmd's args")
    set_conn_addr(sys.argv[1])
    res = test()
    print(res.headers)
    print(res.text)
    with open("component/script/python/example/kline.json", "r") as kline_file:
        kline = json.load(kline_file)
        # vec = vector([1,2,3], int)
        # print(vec, vec.datatype())
        table = as_table(kline["result"])
        # print(table)
        close = table["close"]
        open_time = table["open_time"]
        # print(repr(close), repr(open_time))
        # print("calc_rv:", calc_rv(close, open_time, open_time[-1]+datetime("10m"), datetime("7d")))
        env = {"close":close, "open_time": open_time}
        # print("env:", env)
        print(mock_tester(calc_rvs, env=env))
