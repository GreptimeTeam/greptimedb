"""
Note this is a mock library, if not connected to database, 
it can only run on mock data and support by numpy
"""
from typing import Any
import numpy as np
from .greptime import i32,i64,f32,f64, vector, interval, query, prev, datetime, log, sum, sqrt, pow, nan, copr, coprocessor

import inspect
import functools
import ast



def mock_tester(
    func,
    env:dict, 
    table=None
):
    """
    Mock tester helper function,
    What it does is replace `@coprocessor` with `@mock_cpor` and add a keyword `env=env`
    like `@mock_copr(args=...,returns=...,env=env)`
    """
    code = inspect.getsource(func)
    tree = ast.parse(code)
    tree = HackyReplaceDecorator("env").visit(tree)
    new_func = tree.body[0]
    fn_name = new_func.name

    code_obj = compile(tree, "<embedded>", "exec")
    exec(code_obj)

    ret = eval("{}()".format(fn_name))
    return ret

def mock_copr(args, returns, sql=None, env:None|dict=None):
    """
    This should not be used directly by user
    """
    def decorator_copr(func):
        @functools.wraps(func)
        def wrapper_do_actual(*fn_args, **fn_kwargs):

            real_args = [env[name] for name in args]
            ret = func(*real_args)
            return ret
        
        return wrapper_do_actual
    return decorator_copr

class HackyReplaceDecorator(ast.NodeTransformer):
    """
    This class accept a `env` dict for environment to extract args from,
    and put `env` dict in the param list of `mock_copr` decorator, i.e:

    a `@copr(args=["a", "b"], returns=["c"])` with call like mock_helper(abc, env={"a":2, "b":3})
    
    will be transform into `@mock_copr(args=["a", "b"], returns=["c"], env={"a":2, "b":3})`
    """
    def __init__(self, env: str) -> None:
        # just for add `env` keyword
        self.env = env

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        new_node = node
        decorator_list = new_node.decorator_list
        if len(decorator_list)!=1:
            return node

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
