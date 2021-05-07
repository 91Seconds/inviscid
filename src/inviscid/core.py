import numpy as np
import os
import inspect
import functools
import json
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

    # def pull_params(self, *args):
    #     self.used_params = self.used_params.union(set(args))
    #     all_params = get_current_context()['params']
    #     params = tuple(all_params[key] for key in args)
    #     return params

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*f_args, **f_kwargs):
            # global pull_params  # get reference to pull_params method
            # original_method = pull_params  # save a reference to the original function
            # pull_params = self.pull_params  # pull the old switcheroo

            result = func(*f_args, **f_kwargs)
            # pull_params = original_method  # switch back
            edge = Edge(source_node=func, cumulative_params=self.used_params, data=result)
            return edge

        return wrapper
