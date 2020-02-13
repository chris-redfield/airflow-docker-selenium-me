import os
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.python_operator import PythonOperator
from selenium_scripts.ceis import get_url
from datetime import datetime, timedelta
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
    'start_date': datetime(2020, 2, 14),
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

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 1 -----------------------------------------------------------------------

local_downloads = os.path.join(os.getcwd(), 'downloads')

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 2 -----------------------------------------------------------------------

def get_data_from_url (target_url, target_file):
    try:
        with open(target_file, 'rb') as f:
            f.close()
    except FileNotFoundError:
        with open(target_url, 'r', encoding="utf-8") as f:
            url = f.readline()
            f.close()
        url = url.splitlines()
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
    try:
        with open(to_file) as f:
            f.close()
    except FileNotFoundError:
        with zipfile.ZipFile(from_file, 'r') as zip_ref:
                zip_ref.extractall()
                with open(to_file, 'w') as f:
                    for string in zip_ref.namelist():
                        f.write(string)
                        f.write('\n')
                    f.close()

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 4 -----------------------------------------------------------------------

def copy_to_sqlserver (nome_tabela, nome_arquivo):

    #Parametros da conexão (conectando com o banco no latitude)
    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = os.environ['CEIS_DATABASE']
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    print('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    #
    #ESVAZIA A TABELA PARA SER ATUALIZADA
    deleta = "DELETE " + nome_tabela
    cursor.execute(deleta) 

    with open(nome_arquivo, 'r', encoding="utf-8") as f:
        file_name_csv = f.readline()
    file_name_csv = file_name_csv.splitlines()

    panda = pd.read_csv(file_name_csv[0],sep=';',encoding='latin')
    new_columns = []
    for value in panda.columns.values:
        new_columns.append(remove_accents(value.replace(' ','_')))
    panda.columns = new_columns

    for index,row in panda.iterrows():
        
        query = 'INSERT INTO {}(TIPO_PESSOA,CPF_CNPJ_SANCIONADO,NOME,RAZAO_SOCIAL,NOME_FANTASIA,NUMERO_PROCESSO,TIPO_SANCAO,DATA_INICIO_SANCAO,DATA_FINAL_SANCAO,ORGAO_SANCIONADOR,UF_ORGAO_SANCIONADOR,ORIGEM_INFORMACOES,DATA_ORIGEM_INFORMACOES,DATA_PUBLICACAO,PUBLICACAO,DETALHAMENTO,ABRAGENCIA_JUDICIAL,FUNDAMENTACAO_LEGAL,DESCRICAO_FUND_LEGAL,DATA_TRANSITO_JULGADO,COMPLEMENTO_ORGAO,OBSERVACOES) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(nome_tabela)
        cursor.execute(query,
        str(row['TIPO_DE_PESSOA']),
        str(row['CPF_OU_CNPJ_DO_SANCIONADO']),
        str(row['NOME_INFORMADO_PELO_ORGAO_SANCIONADOR']),
        str(row['RAZAO_SOCIAL_-_CADASTRO_RECEITA']),
        str(row['NOME_FANTASIA_-_CADASTRO_RECEITA']),
        str(row['NUMERO_DO_PROCESSO']),
        str(row['TIPO_SANCAO']),
        str(row['DATA_INICIO_SANCAO']),
        str(row['DATA_FINAL_SANCAO']),
        str(row['ORGAO_SANCIONADOR']),
        str(row['UF_ORGAO_SANCIONADOR']),
        str(row['ORIGEM_INFORMACOES']),
        str(row['DATA_ORIGEM_INFORMACOES']),
        str(row['DATA_PUBLICACAO']),
        str(row['PUBLICACAO']),
        str(row['DETALHAMENTO']),
        str(row['ABRAGENCIA_DEFINIDA_EM_DECISAO_JUDICIAL']),
        str(row['FUNDAMENTACAO_LEGAL']),
        str(row['DESCRICAO_DA_FUNDAMENTACAO_LEGAL']),
        str(row['DATA_DO_TRANSITO_EM_JULGADO']),
        str(row['COMPLEMENTO_DO_ORGAO']),
        str(row['OBSERVACOES']))
        cnxn.commit()

    cursor.close()
    cnxn.close()

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 5,6,7,8 -----------------------------------------------------------------

def delete_arquivo (nome_arquivo):
    if os.path.exists(nome_arquivo):
        os.remove(nome_arquivo)

def delete_from_file (file_name):
    with open(file_name, 'r', encoding="utf-8") as f:
        file_delete = f.readlines()
        delete_arquivo(file_delete[0])
        f.close()


# --------------------------------------------------------------------------------------------------------------
# ------------------------------- AIRFLOW -----------------------------------------------------------------------

# Processo: inidoneos
dag = DAG('ceis', default_args=args, schedule_interval=timedelta(days=1))
url_file: str = 'ceis_url.txt'
url_request: str = 'http://www.portaltransparencia.gov.br/download-de-dados/ceis/'
zip_file: str = 'ceis.zip'
filename_file: str = 'ceis_filename.txt'
nome_tabela: str = 'CEIS.dbo.Dados_CEIS'


# Tarefas:
#  -> get url
#  -> get data
#  -> unzip
#  -> move to base
#  -> delete zip
#  -> delete file
# 

t1 = SeleniumOperator(
    task_id='get_url',
    script=get_url,
    script_args=[os.path.join(local_downloads,url_file),url_request],
    dag=dag)

t2 = PythonOperator(
    task_id='get_data_from_url',
    python_callable=get_data_from_url,
    op_kwargs={'target_url':os.path.join(local_downloads,url_file), 'target_file':os.path.join(local_downloads,zip_file)},
    dag=dag)

t3 = PythonOperator(
    task_id='unzip_file',
    python_callable=unzip_file,
    op_kwargs={'from_file':os.path.join(local_downloads,zip_file), 'to_file':os.path.join(local_downloads,filename_file)},
    dag=dag)

t4 = PythonOperator(
    task_id='copy_to_sqlserver',
    python_callable=copy_to_sqlserver,
    op_kwargs={'nome_tabela':nome_tabela, 'nome_arquivo': os.path.join(local_downloads,filename_file)},
    dag=dag)

t5 = PythonOperator(
    task_id='delete_url_txt',
    python_callable=delete_arquivo,
    op_kwargs={'nome_arquivo': os.path.join(local_downloads,url_file)},
    dag=dag)

t6 = PythonOperator(
    task_id='delete_zip',
    python_callable=delete_arquivo,
    op_kwargs={'nome_arquivo': os.path.join(local_downloads,zip_file)},
    dag=dag)

t7 = PythonOperator(
    task_id='delete_csv',
    python_callable=delete_from_file,
    op_kwargs={'file_name': os.path.join(local_downloads,filename_file)},
    dag=dag)

t8 = PythonOperator(
    task_id='delete_filename_txt',
    python_callable=delete_arquivo,
    op_kwargs={'nome_arquivo': os.path.join(local_downloads,filename_file)},
    dag=dag)

# Ordem das operações
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
