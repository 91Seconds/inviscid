import os
import pickle
import hashlib
from typing import Any
from airflow.models.xcom import BaseXCom
from src.inviscid.core import Edge
import json


class FileLikePointerXComBackend(BaseXCom):
    @staticmethod
    def serialize_value(value: Any):
        if type(value) is not Edge:
            return BaseXCom.serialize_value(value)

        location, funcname, src, params, result = value.values()
        funcname = f'{funcname}{hashlib.md5(src.encode("ascii")).hexdigest()}'
        path = os.path.join(location, funcname)
        if not os.path.exists(path):
            os.mkdir(path)
        path = os.path.join(path, hashlib.md5(str(params).encode("ascii")).hexdigest())
        if not os.path.exists(path):
            os.mkdir(path)
        params_path = os.path.join(path, 'params.txt')
        values_path = os.path.join(path, 'values.pkl')
        manifest_path = os.path.join(path, 'manifest.txt')
        with open(params_path, 'w') as f:
            f.write(str(params))
        pickle.dump(result, open(values_path, 'wb'))
        manifest = {'params': params_path,
                    'values': values_path,
                    'manifest': manifest_path}
        json.dump(manifest, open(manifest_path, 'w'))
        return BaseXCom.serialize_value(manifest_path)



    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if type(result) is not str:
            return result
        if not os.path.exists(result):
            return result
        manifest_path = result
        manifest = json.load(open(manifest_path, 'r'))
        values_path = manifest['values']
        result = pickle.load(open(values_path, 'rb'))
        return result