import os
import shutil

from airflow.models.dag import DAG
from airflow.operators.python import _CURRENT_CONTEXT
from airflow.utils.dates import days_ago

from src.inviscid import ExperimentStep, xtask

PARAMS = {'param1': 'the first param',
          'param2': 'the second param'}


def test_xtask_runs():
    daaag = DAG(dag_id='dag00', description='test dag created without the decorator', start_date=days_ago(1),
                schedule_interval=None, default_args=PARAMS)

    @xtask(dag=daaag)
    def step1(param1):
        print('happened')
        print(param1)
        return 6


def test_xtask_can_be_composed():
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


def test_xtask_clean():
    # This is going to be more difficult than I thought.  What should be the behavior when the task is run in clean
    # mode?  What should the task return and feed to downstream tasks,  Should downstream tasks know that an upstream
    # task has been cleaned? probably not.  Then each cache should store the entire upstream subdag definition.  That's
    # a good thing to use gusty for.
    def dir_empty_or_non_existent(path):
        if not os.path.exists(path):
            return True
        if not os.path.isdir(path):
            raise Exception('path must point to a dir if it exists.')
        if not os.listdir(path):
            return True
        return False

    ExperimentStep.persistent_root = '/tmp/inviscid_test'
    shutil.rmtree(ExperimentStep.persistent_root)
    os.makedirs(ExperimentStep.persistent_root, exist_ok=False)

    @ExperimentStep(checkpoint=True)
    def messy_step():
        return ('this is very messy')

    _CURRENT_CONTEXT.append({'params': PARAMS})

    assert dir_empty_or_non_existent(ExperimentStep.persistent_root)
    stp1 = messy_step()
    assert not dir_empty_or_non_existent(ExperimentStep.persistent_root)
    ExperimentStep.clean = True
    stp1 = messy_step()
    assert dir_empty_or_non_existent(ExperimentStep.persistent_root)

    _CURRENT_CONTEXT.pop(-1)


def test_experiment_step_runs():
    @ExperimentStep()
    def mock(param1):
        print(f'experiment_step_runs with param1: {param1}')

    _CURRENT_CONTEXT.append({'params': PARAMS})
    mock()
    _CURRENT_CONTEXT.pop(-1)
