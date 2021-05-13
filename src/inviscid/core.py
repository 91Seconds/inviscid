import functools
import hashlib
import inspect
import json
import os
import pickle
from collections import OrderedDict
from inspect import signature
from typing import Callable, Dict, Optional, TypeVar, cast

from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import _PythonDecoratedOperator
from airflow.operators.python import get_current_context


class Edge:
    @classmethod
    def union(cls, edges):
        cumulative_params = {}
        for edge in edges:
            cumulative_params.update(edge.cumulative_params)
        data = tuple(edge.data for edge in edges)
        ran_this_time = any(edge.ran_this_time for edge in edges)
        return Edge(cumulative_params=cumulative_params,
                    data=data,
                    ran_this_time=ran_this_time)

    @classmethod
    def already_ran(cls, path, config):
        if type(path) is not str:
            raise Exception('path must be a string')
        if not os.path.exists(path):
            return False
        manifest_path = os.path.join(path, 'manifest.json')
        if not os.path.exists(manifest_path):
            return False
        manifest = json.load(open(manifest_path, 'r'))
        params_path = manifest['params']
        if not os.path.exists(params_path):
            return False
        params = json.load(open(params_path, 'r'))
        return params == config

    @classmethod
    def decache(cls, path):
        if type(path) is not str:
            raise Exception('path must be a string')
        if not os.path.exists(path):
            raise Exception('path must exist')
        manifest_path = os.path.join(path, 'manifest.json')
        manifest = json.load(open(manifest_path, 'r'))
        values_path = manifest['values']
        params_path = manifest['params']
        params = json.load(open(params_path, 'r'))
        result = pickle.load(open(values_path, 'rb'))
        if path.startswith(os.path.abspath(ExperimentStep.tempfs_root) + os.sep):
            # should remove after deaching but for now will leave it.
            pass
        return Edge(cumulative_params=params, data=result, ran_this_time=False)

    def cache(self, path) -> None:
        params_path = os.path.join(path, 'params.json')
        values_path = os.path.join(path, 'values.pkl')
        manifest_path = os.path.join(path, 'manifest.json')
        json.dump(self.cumulative_params, open(params_path, 'w'))
        pickle.dump(self.data, open(values_path, 'wb'))
        manifest = {'params': params_path,
                    'values': values_path,
                    'manifest': manifest_path}
        json.dump(manifest, open(manifest_path, 'w'))

    def __init__(self, cumulative_params, data, ran_this_time):
        self.cumulative_params = cumulative_params
        self.data = data
        self.ran_this_time = ran_this_time

    def __repr__(self):
        return f'Edge(cumulative_params: {self.cumulative_params}, data: {self.data}, ran_this_time: {self.ran_this_time})'


class ExperimentStep:
    clean = False
    persistent_root = '/vol/grid-solar/sgeusers/kingciar1/pipeline_cache'
    tempfs_root = '/dev/shm'

    def __init__(self, checkpoint: bool = False, impure: bool = False):
        if checkpoint:
            self.root_path = ExperimentStep.persistent_root
        else:
            self.root_path = ExperimentStep.tempfs_root
        self.used_params = set()
        self.checkpoint = bool(checkpoint)
        self.impure = bool(impure)

    def __call__(self, func):
        # @functools.wraps(func)
        def wrapper(*f_args, **f_kwargs):
            print(f'f_args: {f_args}, f_kwargs: {f_kwargs}')
            # must make sure edges in f_args are upacked and their contents passed to the functions *args and kwargs go to kwargs
            skip = True
            if self.impure:
                skip = False
            incoming_edges = []
            f_args = list(f_args)
            for idx, arg in enumerate(f_args):
                if type(arg) is Edge:
                    incoming_edges.append(arg)
                    f_args[idx] = arg.data
            for key, kwarg in f_kwargs.items():
                if type(kwarg) is Edge:
                    incoming_edges.append(kwarg)
                    f_kwargs[key] = kwarg.data
            incoming_edges_ran = [edge.ran_this_time for edge in incoming_edges]
            if any(incoming_edges_ran):
                skip = False

            super_edge = Edge.union(incoming_edges)

            total_config = get_current_context()['params']

            sig = inspect.signature(func)
            func_params = sorted(sig.parameters.keys())

            cumulative_config = super_edge.cumulative_params
            for func_param in func_params:
                if func_param in total_config.keys():
                    cumulative_config[func_param] = total_config[func_param]

            # sort dict before stringing to make hashing permutatio invariant.
            if cumulative_config:
                cumulative_config = OrderedDict(sorted(cumulative_config.items(), key=lambda t: t[0]))

            func_param_string = str(cumulative_config).encode('ascii')
            cache_dir_name = f'{func.__name__}_{hashlib.md5(func_param_string).hexdigest()}'
            cache_dir = os.path.join(self.root_path, cache_dir_name)
            if not os.path.exists(cache_dir):
                skip = False
                os.mkdir(cache_dir)
            if skip:
                skip = Edge.already_ran(cache_dir, cumulative_config)
            if skip:
                return Edge.decache(cache_dir)
            else:
                print(f'f_args: {f_args}, cumulative_config: {cumulative_config}')
                result = func(*f_args, **cumulative_config)
                edge = Edge(cumulative_params=cumulative_config, data=result, ran_this_time=True)
                edge.cache(cache_dir)
                return edge

        return wrapper

    def cache(self, values, config, path):
        params_path = os.path.join(path, 'params.txt')
        values_path = os.path.join(path, 'values.pkl')
        manifest_path = os.path.join(path, 'manifest.txt')
        with open(params_path, 'w') as f:
            f.write(str(config))
        pickle.dump(values, open(values_path, 'wb'))
        manifest = {'params': params_path,
                    'values': values_path,
                    'manifest': manifest_path}
        json.dump(manifest, open(manifest_path, 'w'))

T = TypeVar("T", bound=Callable)


# TODO: make xtask rely only on the public task api to ensure 2.x compatibility
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
