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
    _CURRENT_CONTEXT.append({'params': PARAMS})

    @ExperimentStep()
    def mock(param1):
        print('mock')
        print(f'param1: {param1}')

    mock()
    _CURRENT_CONTEXT.pop(-1)

# dagbag = DagBag(dag_folder='test_dags', include_examples=False)
# dagbag.bag_dag()
# print(dagbag.dag_folder)
# assert dagbag.size() > 0
# print(dagbag.dag_ids)
# dag = dag0()  # dagbag.get_dag('dag0')
# run = dag.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING, execution_date=DEFAULT_DATE)
# print(f'dag run: {run}')
