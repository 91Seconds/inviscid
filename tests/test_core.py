from airflow.models.dag import DAG
from airflow.operators.python import _CURRENT_CONTEXT
from airflow.utils.dates import days_ago

from src.inviscid import ExperimentStep, xtask

PARAMS = {'param1': 'the first param', 'param2': 'the second param'}


def test_xtask_runs():
    daaag = DAG(dag_id='dag00', description='test dag created without the decorator', start_date=days_ago(1),
                schedule_interval=None, default_args=PARAMS)

    @xtask(dag=daaag)
    def step1(param1):
        print('happened')
        print(param1)
        return 6


def test_xtask_can_be_composed():
    daaag = DAG(dag_id='dag000', description='test dag created without the decorator', start_date=days_ago(1),
                schedule_interval=None, default_args=PARAMS)

    @ExperimentStep()
    def step1(param1):
        if param1 == 'the first param':
            return 5
        else:
            return 7

    @ExperimentStep()
    def step2(step1_result, param2):
        if param2 == 'the second param':
            return step1_result * 3
        else:
            return step1_result * 11

    _CURRENT_CONTEXT.append({'params': PARAMS})
    stp1 = step1()
    stp2 = step2(stp1)
    _CURRENT_CONTEXT.pop(-1)


def test_xtask_can_be_composed_with_kwargs():
    daaag = DAG(dag_id='dag000', description='test dag created without the decorator', start_date=days_ago(1),
                schedule_interval=None, default_args=PARAMS)

    @ExperimentStep()
    def step1(param1):
        if param1 == 'the first param':
            return 5
        else:
            return 7

    @ExperimentStep()
    def step2(step1_result, param2):
        if param2 == 'the second param':
            return step1_result * 3
        else:
            return step1_result * 11

    _CURRENT_CONTEXT.append({'params': PARAMS})
    stp1 = step1()
    stp2 = step2(step1_result=stp1)
    _CURRENT_CONTEXT.pop(-1)


def test_experiment_step_runs():
    @ExperimentStep()
    def mock(param1):
        print(f'experiment_step_runs with param1: {param1}')

    _CURRENT_CONTEXT.append({'params': PARAMS})
    mock()
    _CURRENT_CONTEXT.pop(-1)
