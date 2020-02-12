import os
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from selenium_scripts.cepim import get_url
from datetime import datetime, timedelta, date
import logging
import pandas as pd

import pyodbc

import requests
import zipfile

import unicodedata

class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')


args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 12),
    #'start_date': datetime(date.today().year, date.today().month, date.today().day),
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
# UTILS
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

# Tarefas:
#  -> get url
#  -> get data
#  -> unzip
#  -> move to base
#  -> delete folder


# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 1 -----------------------------------------------------------------------
# Usada utilizando plugin do selenium: from selenium_scripts.cepim import get_url

# /usr/local/airflow/downloads
local_downloads = os.path.join(os.getcwd(), 'downloads')


# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 2 -----------------------------------------------------------------------

def get_data_from_url (target_url, target_file):
    with open(target_url, 'r', encoding="utf-8") as f:
        url = f.readlines()
        f.close()
    with requests.get(url[0], stream=True) as r:
        r.raise_for_status()
        with open(target_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                if chunk:               # filter out keep-alive new chunks
                    f.write(chunk)
            # f.flush()         # pode ser necessário para escrever o arquivo caso o sistema fique esperando juntar muita coisa


# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 3 -----------------------------------------------------------------------

def unzip_file (from_file, to_file):

    print("================================================================")
    print("================================================================")
    print("================================================================")

    

    with zipfile.ZipFile(from_file, 'r') as zip_ref:
            #zip_ref.extractall()
            zip_ref.extractall(path=local_downloads)
            with open(to_file, 'w') as f:
                f.write(zip_ref.namelist()[0])
                print("unzipped file:",zip_ref.namelist()[0], "to", local_downloads)
                #f.write("\n")


# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 4 -----------------------------------------------------------------------

def copy_to_sqlserver (nome_tabela, nome_arquivo):

    print("================================================================")
    print("================================================================")
    print("================================================================")

    #Parametros da conexão (conectando com o banco no latitude)
    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = os.environ['CEPIM_DATABASE']
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    print('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    #ESVAZIA A TABELA PARA SER ATUALIZADA
    deleta = "DELETE " + nome_tabela
    cursor.execute(deleta) 

    with open(nome_arquivo) as file_csv_name:
        csv_name = file_csv_name.readline()
    file_csv_name.close()

    csv_name = os.path.join(local_downloads,csv_name)

    print("CSV FILE TO OPEN:", csv_name)

    panda = pd.read_csv(csv_name,sep=';',encoding='latin')
    new_columns = []
    for value in panda.columns.values:
        new_columns.append(remove_accents(value.replace(' ','_')))
    panda.columns = new_columns

    for index,row in panda.iterrows():
        query = 'INSERT INTO {}(CNPJ_ENTIDADE,NOME_ENTIDADE,NUMERO_CONVENIO,ORGAO_CONCEDENTE,MOTIVO_IMPEDIMENTO) VALUES(?,?,?,?,?)'.format(nome_tabela)
        cursor.execute(query,row.CNPJ_ENTIDADE,row.NOME_ENTIDADE,row.NUMERO_CONVENIO,row.ORGAO_CONCEDENTE,row.MOTIVO_DO_IMPEDIMENTO)
        cnxn.commit()
    cursor.close()
    cnxn.close()


# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 5,6,7,8 -----------------------------------------------------------------

def delete_file (file_name):
   if os.path.exists(file_name):
       os.remove(file_name)

def delete_from_file (file_name):
   with open(file_name, 'r', encoding="utf-8") as f:
       file_delete = f.readlines()
       file_delete = os.path.join(local_downloads,file_delete[0])
       delete_file(file_delete)
       f.close()

#processo: CEPIM
dag = DAG(dag_id='cepim', default_args=args, schedule_interval=timedelta(days=1))

t1 = SeleniumOperator(
    task_id='get_url',
    script=get_url,
    script_args=[os.path.join(local_downloads,'cepim_url.txt'),
        'http://www.portaltransparencia.gov.br/download-de-dados/cepim/'],
    dag=dag)

t2 = PythonOperator(
    task_id='get_data_from_url',
    python_callable=get_data_from_url,
    #op_kwargs={'target_url':'cepim_url.txt', 'target_file':'cepim.zip'},
    op_kwargs={'target_url': os.path.join(local_downloads,'cepim_url.txt'), 'target_file':os.path.join(local_downloads,'cepim.zip')},
    dag=dag)

t3 = PythonOperator(
    task_id='unzip_file',
    python_callable=unzip_file,
    op_kwargs={'from_file':os.path.join(local_downloads,'cepim.zip'), 'to_file':os.path.join(local_downloads,'cepim_filename.txt')},
    dag=dag)

t4 = PythonOperator(
    task_id='copy_to_sqlserver',
    python_callable=copy_to_sqlserver,
    op_kwargs={'nome_tabela':'CEPIM.dbo.Dados_CEPIM', 'nome_arquivo': os.path.join(local_downloads,'cepim_filename.txt')},
    dag=dag)

t5 = PythonOperator(
   task_id='delete_url_txt',
   python_callable=delete_file,
   op_kwargs={'file_name': os.path.join(local_downloads,'cepim_url.txt')},
   dag=dag)

t6 = PythonOperator(
   task_id='delete_zip',
   python_callable=delete_file,
   op_kwargs={'file_name':os.path.join(local_downloads,'cepim.zip')},
   dag=dag)

t7 = PythonOperator(
   task_id='delete_csv',
   python_callable=delete_from_file,
   op_kwargs={'file_name':os.path.join(local_downloads,'cepim_filename.txt')},
   dag=dag)

t8 = PythonOperator(
   task_id='delete_filename_txt',
   python_callable=delete_file,
   op_kwargs={'file_name':os.path.join(local_downloads,'cepim_filename.txt')},
   dag=dag)

# Ordem das operações
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8