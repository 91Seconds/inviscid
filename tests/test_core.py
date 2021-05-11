from airflow.models.dag import DAG
from airflow.operators.python import _CURRENT_CONTEXT
from airflow.utils.dates import days_ago

from src.inviscid import ExperimentStep, xtask

PARAMS = {'param1': 'the first param'}


def test_xtask():
    daaag = DAG(dag_id='dag00', description='test dag created without the decorator', start_date=days_ago(1),
                schedule_interval=None, default_args=PARAMS)

    @xtask(dag=daaag)
    def step1(param1):
        print('happened')
        print(param1)
        return 6

    print(f'type(step1().operator): {type(step1().operator.run(start_date=days_ago(1), end_date=days_ago(0)))}')


def test_experiment_step():
    _CURRENT_CONTEXT.append({'params': PARAMS, 'dag': {'default_args': {'tempfs_root': '/dev/shm'}}})

    @ExperimentStep()
    def mock(param1):
        print('mock')
        print(f'param1: {param1}')

    mock()
    _CURRENT_CONTEXT.pop(-1)
