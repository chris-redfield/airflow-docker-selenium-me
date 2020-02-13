import airflow 
from airflow.models import DAG                                             # airflow
from airflow.operators.python_operator import PythonOperator        # airflow
from datetime import datetime, timedelta                            # airflow

import requests
import pyodbc
import json

import os

args = {
    'owner': 'airflow',
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

class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')

local_downloads = os.path.join(os.getcwd(), 'downloads')

#---------------------------------------------------------------------------------
#-----------------  UNICA -------------------------------------------------------
#FUNÇÃO SECUNDÁRIA RESPONSAVEL PELO ETL
def copy_to_sqlserver(from_file,nome_tabela):

    with open(from_file, 'r') as f:
        data = json.load(f)
        #REALIZA CONEXÃO COM O BANCO DE DADOS
        #Parametros da conexão (conectando com o banco no latitude)

        server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
        database = os.environ['LENIENCIA_DATABASE']
        username = os.environ['LAKE_USER']
        password = os.environ['LAKE_PASS']
        
        
        #Fazendo a conexão
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()

        #ESVAZIA A TABELA PARA SER ATUALIZADA
        deleta = "DELETE " + nome_tabela 
        cursor.execute(deleta) 
        
        for temp in data:
            #BAIXA E INSERE OS DADOS NA TABELA 
            query ='INSERT INTO {}(CNPJ,DATA_FIM_ACORDO,DATA_INICIO_ACORDO,ID,NOME_EMPRESA,ORGAO_RESPONSAVEL,NOME_FANTASIA,QUANTIDADE,RAZAO_SOCIAL,SITUACAO_ACORDO,UF_EMPRESA) VALUES (?,?,?,?,?,?,?,?,?,?,?)'.format(nome_tabela)

            print(query,temp['cnpj'], temp['dataFimAcordo'],temp['dataInicioAcordo'],temp['id'],temp['nomeEmpresa'],temp['orgaoResponsavel'],temp['nomeFantasia'],temp['quantidade'],temp['razaoSocial'],temp['situacaoAcordo'],temp['ufEmpresa'])

            cursor.execute(query,temp['cnpj'], temp['dataFimAcordo'],temp['dataInicioAcordo'],temp['id'],temp['nomeEmpresa'],temp['orgaoResponsavel'],temp['nomeFantasia'],temp['quantidade'],temp['razaoSocial'],temp['situacaoAcordo'],temp['ufEmpresa'])

            cnxn.commit()
        cursor.close()    
        cnxn.close()

def get_url(url_request,to_file):
    #BUSCA LINK DA API LENIENCIA DO PORTAL DA TRASPARENCIA
    r = requests.get(url_request)
    with open(to_file, 'w') as f:
        json.dump(r.json(),f)

def delete_file (file_name):
    os.remove(file_name) 

# ---------------------------------------------------------------------------------------------------------------
# ------------------------------- AIRFLOW -----------------------------------------------------------------------

# Processo: inidoneos
dag = DAG('leniencia', default_args=args, schedule_interval=timedelta(days=1))
url_request = 'http://www.transparencia.gov.br/api-de-dados/acordos-leniencia'
json_file = 'leniencia_file.json'
tabela = 'Leniencia.dbo.Dados_Leniencia'

#TAREFAS
t0 = PythonOperator(
    task_id='get_url',
    python_callable=get_url,
    op_kwargs={'url_request':url_request,'to_file':os.path.join(local_downloads,json_file)},
    dag=dag)

t1 = PythonOperator(
    task_id='copy_to_sqlserver',
    python_callable=copy_to_sqlserver,
    op_kwargs={'from_file':os.path.join(local_downloads,json_file),'nome_tabela': tabela},
    dag=dag)

t2 = PythonOperator(
    task_id='delete_file',
    python_callable=delete_file,
    op_kwargs={'file_name':os.path.join(local_downloads,json_file)},
    dag=dag)

# Ordem das operações
t0 >> t1 >> t2