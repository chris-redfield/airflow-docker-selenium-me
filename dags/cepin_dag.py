import os
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from selenium_scripts.cepim import get_url
from datetime import datetime, timedelta
import logging



class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')


args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

#processo: CEPIM
dag = DAG(dag_id='cepim', default_args=args, schedule_interval=timedelta(days=1))

# /usr/local/airflow/
local_downloads = os.path.join(os.getcwd(), 'downloads')

t1 = SeleniumOperator(
    task_id='get_url',
    script=get_url,
    script_args=[os.path.join(local_downloads,'cepim_url.txt'),
        'http://www.portaltransparencia.gov.br/download-de-dados/cepim/'],
    dag=dag)

# Ordem das operações
t1