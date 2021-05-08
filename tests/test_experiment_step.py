from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from src.inviscid import ExperimentStep, Edge

PARAMS = {'param1', 'the first param'}


@dag(default_args=PARAMS, start_date=days_ago(1), schedule_interval=None)
def test_dag0():
    @task()
    @ExperimentStep()
    def step1(param1):
        print('happened')
        print(param1)

    stp1 = step1()

#
if __name__ == '__main__':
  from airflow.utils.state import State
  daag = test_dag0()
  daag.clear(dag_run_state=State.NONE)
  daag.run()