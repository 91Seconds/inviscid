from airflow.decorators import dag
from src.inviscid import ExperimentStep, Edge

PARAMS = {'param1', 'the first param'}


@dag(default_args=PARAMS)
def test_dag0():
    @ExperimentStep()
    def step1(param1):
        print('happened')
        print(param1)

    stp1 = step1()

#
# if __name__ == '__main__':
#     daag = dag0()