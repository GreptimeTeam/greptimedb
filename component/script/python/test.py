import inspect
from turtle import goto
from greptime import coprocessor
import sys
import cfg

@coprocessor(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n):
    return n

if __name__ == "__main__":
    # print(sys.argv[1])
    if len(sys.argv)!=2:
        raise Exception("Expect only one address as cmd's args")
    cfg.set_conn_addr(sys.argv[1])
    # print(cfg.GREPTIME_DB_CONN_ADDRESS)
    print(test())