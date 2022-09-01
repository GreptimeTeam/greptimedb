GREPTIME_DB_CONN_ADDRESS = "localhost:3000"
"""The Global Variable for address for conntect to database"""

def set_conn_addr(addr: str):
    """set database address to given `addr`"""
    global GREPTIME_DB_CONN_ADDRESS
    GREPTIME_DB_CONN_ADDRESS = addr

def get_conn_addr()->str:
    global GREPTIME_DB_CONN_ADDRESS
    return GREPTIME_DB_CONN_ADDRESS
