from example.calc_rv import as_table, calc_rvs
from greptime import coprocessor, set_conn_addr, get_conn_addr, mock_tester
import sys
import json
import requests

@coprocessor(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n):
    return n+2

def init_table(close, open_time):
    req_init = "/v1/sql?sql=create table k_line (close double, open_time bigint, TIME INDEX (open_time))"
    print(get_db(req_init).text)
    for c1, c2 in zip(close, open_time):
        req = "/v1/sql?sql=INSERT INTO k_line(close, open_time) VALUES ({}, {})".format(c1, c2)
        print(get_db(req).text)
    print(get_db("/v1/sql?sql=select * from k_line").text)

def get_db(req:str):
    return requests.get("http://{}{}".format(get_conn_addr(), req))

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
        init_table(close, open_time)
        exit()
        # print(repr(close), repr(open_time))
        # print("calc_rv:", calc_rv(close, open_time, open_time[-1]+datetime("10m"), datetime("7d")))
        env = {"close":close, "open_time": open_time}
        # print("env:", env)
        print("Mock result:", mock_tester(calc_rvs, env=env))
        real = calc_rvs()
        print(real)
        try:
            print(real.text["error"])
        except:
            print(real.text)
