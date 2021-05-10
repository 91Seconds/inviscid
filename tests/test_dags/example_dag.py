from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from src.inviscid import ExperimentStep

PARAMS = {'param1', 'the first param'}


@dag(default_args=PARAMS, start_date=days_ago(1), schedule_interval=None)
def dag0():
    @task()
    @ExperimentStep()
    def step1(param1):
        print('happened')
        print(param1)

    stp1 = step1()
