import hashlib
from airflow.operators.python import get_current_context

import functools
import hashlib
import inspect
from inspect import signature
from typing import Callable, Dict, Optional, TypeVar, cast

from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import _PythonDecoratedOperator
from airflow.operators.python import get_current_context


class Edge:
    def __init__(self, source_node, cumulative_params, data):
        self.source_node = source_node
        self.cumulative_params = cumulative_params,
        self.data = data


class ExperimentStep:
    def __init__(self, checkpoint: bool = False):
        if checkpoint:
            self.root_path = lambda: get_current_context()['dag'].default_args['persistent_root']
        else:
            self.root_path = lambda: get_current_context()['dag'].default_args['tempfs_root']
        self.used_params = set()
        self.checkpoint = bool(checkpoint)

    def __call__(self, func):
        # @functools.wraps(func)
        def wrapper(*f_args, **f_kwargs):
            config = get_current_context()['params']
            sig = inspect.signature(func)
            func_params = sorted(sig.parameters.keys())
            func_param_string = ', '.join(func_params).encode('ascii')
            cache_dir = f'{func.__name__}_{hashlib.md5(func_param_string).hexdigest()}'
            print(f'cache_dir: {cache_dir}')

            for k, v in config:
                f_kwargs.update(k, v)
            result = func(*f_args, **f_kwargs)
            # pull_params = original_method  # switch back
            edge = Edge(source_node=func, cumulative_params=self.used_params, data=result)
            return edge

        return wrapper


T = TypeVar("T", bound=Callable)


def xtask(
        python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
) -> Callable[[T], T]:
    """
    Python operator decorator. Wraps a function into an Inviscid cached operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool

    """
    # try to infer from  type annotation
    if python_callable and multiple_outputs is None:
        sig = signature(python_callable).return_annotation
        ttype = getattr(sig, "__origin__", None)

        multiple_outputs = sig != inspect.Signature.empty and ttype in (dict, Dict)

    def wrapper(f: T):
        def no_current_context():
            raise AirflowException(
                "Current context cannot be requested inside of an xtask. "
                "Please ingest all input through function parameters."
            )

        """
        Python wrapper to generate PythonDecoratedOperator out of simple python functions.
        Used for Airflow Decorated interface
        """
        _PythonDecoratedOperator.validate_python_callable(f)
        kwargs.setdefault('task_id', f.__name__)

        @functools.wraps(f)
        def factory(*args, **f_kwargs):
            ff = ExperimentStep()(f)
            op = _PythonDecoratedOperator(
                python_callable=ff,
                op_args=args,
                op_kwargs=f_kwargs,
                multiple_outputs=multiple_outputs,
                **kwargs,
            )
            if f.__doc__:
                op.doc_md = f.__doc__
            return XComArg(op)

        return cast(T, factory)

    if callable(python_callable):
        return wrapper(python_callable)
    elif python_callable is not None:
        raise AirflowException('No args allowed while using @task, use kwargs instead')
    return wrapper
