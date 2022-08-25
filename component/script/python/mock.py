"""
Note this is a mock library, if not connected to database, 
it can only run on mock data and support by numpy
"""
from typing import Any
import numpy as np
from greptime import i32,i64,f32,f64, vector, interval, query, prev, datetime, log, sum, sqrt, pow, nan, copr, coprocessor

# TODO: write a python side coprocessor to passs all info to db
import inspect
import functools
import ast



def mock_tester(
    func,
    env:dict, 
    table=None
):
    # print("Mock Helper:")
    code = inspect.getsource(func)
    # print(code)
    tree = ast.parse(code)
    tree = HackyReplaceDecorator("env").visit(tree)
    new_func = tree.body[0]
    fn_name = new_func.name

    # print(ast.dump(tree, indent=4))
    code_obj = compile(tree, "<embedded>", "exec")
    exec(code_obj)

    ret = eval("{}()".format(fn_name))
    # print(ret)
    return ret

def mock_copr(args, returns, sql=None, env:None|dict=None, table=None):
    """
    This should not be used directly by user
    """
    def decorator_copr(func):
        @functools.wraps(func)
        def wrapper_do_actual(*fn_args, **fn_kwargs):
            # print("Mock Python Coprocessor:")
            # print("args=", fn_args,"kwargs=", fn_kwargs)

            real_args = [env[name] for name in args]
            ret = func(*real_args)
            # print(ret)
            return ret
        
        return wrapper_do_actual
    return decorator_copr

@coprocessor(args={"a":2, "b":3}, 
returns=[
    "rv_7d", 
    "rv_15d", 
    "rv_30d", 
    "rv_60d", 
    "rv_90d", 
    "rv_180d"
    ])
def abc(a, b):
    return a + b

@copr(args=["a", "b"], returns=["c"])
def test_mock(a, b):
    return a + b

@mock_copr(args=["a", "b"], returns=[], env={"a":2, "b":3})
def test_mock(a, b):
    return a + b

class HackyReplaceDecorator(ast.NodeTransformer):
    """
    This class accept a `env` dict for environment to extract args from,
    and put `env` dict in the param list of `mock_copr` decorator, i.e:

    a `@copr(args=["a", "b"], returns=["c"])` with call like mock_helper(abc, env={"a":2, "b":3})
    
    will be transform into `@mock_copr(args=["a", "b"], returns=["c"], env={"a":2, "b":3})`
    """
    def __init__(self, env: str) -> None:
        # make a better repr function
        """
        env = {
            k:("vector({},dtype={})".format([i for i in v], v.datatype())) 
            for k, v in env.items()
        }
        env = "{{ {} }}".format(
            ",\n".join(["{}:{}".format(k, v) for k, v in env.items()])
            )
        print("env(in ast) = ", env)
        env = ast.parse(env).body[0].value
        print("env:", env)
        """
        
        self.env = env

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        new_node = node
        decorator_list = new_node.decorator_list
        # new_node.decorator_list = []
        if len(decorator_list)!=1:
            return node
            """
            raise Exception(
                "Expect only one decorator for coprocessor function, found {} as following: \n{}"
                .format(len(decorator_list), decorator_list)
                )
            """
        deco = decorator_list[0]
        if deco.func.id!="coprocessor" and deco.func.id !="copr":
            raise Exception("Expect a @copr or @coprocessor, found {}.".format(deco.func.id))
        deco.func = ast.Name(id="mock_copr", ctx=ast.Load())
        new_kw = ast.keyword(arg="env", value=ast.Name(id=self.env, ctx=ast.Load()))
        deco.keywords.append(new_kw)

        # Tie up loose ends in the AST.
        ast.copy_location(new_node, node)
        ast.fix_missing_locations(new_node)
        self.generic_visit(node)
        return new_node

"""
if __name__ == "__main__":
    #abc(1, 2)
    #print(interval([1,2,3,4],2, "prev"))
    # print(test_mock())
    mock_helper(abc, env={"a":2, "b":3})
"""