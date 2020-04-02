from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.python_operator import PythonOperator
from selenium_scripts.sancoesRJ import get_url
from datetime import datetime, timedelta
import logging

import os
import pyodbc
import pandas as pd

from airflow.utils.dates import days_ago

class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')

args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': days_ago(1),
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

local_downloads = os.path.join(os.getcwd(), 'downloads')

def tira_ponto_traco_e_barra (texto):
    return texto.replace(".", "").replace("-", "").replace("/", "")

def edita_base(nome_arquivo_out):

    # Comeca vazio
    dataset_vazio = True

    for file in os.listdir(local_downloads):

         if ("Sancoes" in file) and (".xls" in file):

            # Se for o primeiro arquivo, leia na variavel dataset. Se for do segundo ao quarto,
            # já leia em outra variável e concatene ao dataset
            if dataset_vazio == True:
                dataset = pd.read_excel(io=local_downloads + "/" + file, header=1)
                
                # Se for um conjunto de dados sem Prazo (tudo NaN), o pandas se confundo com o tipo
                # da coluna e depois dá problema para juntar (conflito de tipo). Assim, vou garantir que o
                # tipo é sempre object
                dataset['Prazo']=dataset['Prazo'].astype(object)
                dataset['Data Início']=dataset['Data Início'].astype(object)
                dataset['Data Final']=dataset['Data Final'].astype(object)


                dataset_vazio = False
            else:
                temp = pd.read_excel(io=local_downloads + "/" + file, header=1)

                temp['Prazo']=temp['Prazo'].astype(object)
                temp['Data Início']=temp['Data Início'].astype(object)
                temp['Data Final']=temp['Data Final'].astype(object)

                dataset = pd.merge(dataset, temp, how="outer")

    # Nessa base os cpfs tem o formato 123.456.789-01 com 14 digitos
    # Assim, se for 14, é CPF se não for 14 é CNPJ
    dataset['CPF']= [str(x).replace(".", "").replace("-","") if len(str(x)) == 14 else '' for x in dataset['CPF/CNPJ']]
    dataset['CNPJ']=[''                                      if len(str(x)) == 14 else str(x).replace(".", "").replace("-","").replace("/","")  for x in dataset['CPF/CNPJ']]

    #Jogando no arquivo final
    dataset[["CPF","CNPJ","Nome/Razão Social ","Enquadramento Legal", "Data de Efetivação", "Status"] ].to_csv(
        nome_arquivo_out, sep='\t', encoding='utf-8', index=False)

# ----------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 3 -------------------------------------------------------------------
# Importar as informações no banco
def truncate_e_reinsert_to_sqlserver (nome_tabela, nome_arquivo):

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = os.environ['INIDONEOS_DATABASE']
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    #
    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    #
    #ESVAZIA A TABELA PARA SER ATUALIZADA
    deleta = "DELETE " + nome_tabela
    cursor.execute(deleta) 

    # Lendo os dados
    with open(nome_arquivo) as arquivo:
        cab = arquivo.readline()        # primeira linha eh o cabecalho
        cab = cab.strip().split("\t")   # processando o cabecalho
        dados = arquivo.read()          # o resto das linhas são dados (o processamento fica para o iterador linha a linha)
    

    # Copiando para o banco
    query = 'INSERT INTO {}(CPF,CNPJ,NOME,ENQUADRAMENTO_LEGAL,DATA_SANCAO,STATUS) VALUES(?,?,?,?,?,?)'.format(nome_tabela)
    for observacao in dados.strip().split("\n"):
        row = observacao.split("\t")
        cursor.execute(query,row[cab.index('CPF')],row[cab.index('CNPJ')],row[cab.index('Nome/Razão Social ')],row[cab.index('Enquadramento Legal')],row[cab.index('Data de Efetivação')], row[cab.index('Status')])
        cnxn.commit()
    
    cursor.close()
    cnxn.close()


# ----------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 4 -------------------------------------------------------------------
# Apagar os arquivos intermediarios
def apagar_arquivos(lista_arquivos):
	for arquivo in lista_arquivos:
		os.remove(arquivo)

# se houver arquivos do excel aqui, apaga tudo
def apaga_resquicios():

    # Apagar arquivos do excel resquícios
    for file in os.listdir(local_downloads):

        # Não quero fazer regex não.
        # Seria algo como match com "Sancao(\([0-9]+\)){0,1}\.xls", mas prefiro simplificar
        if ("Sancoes" in file) and (".xls" in file):
            os.remove(local_downloads + "/" + file)


# ----------------------------------------------------------------------------------------------------------
# ------------------------------- AIRFLOW ------------------------------------------------------------------
dag = DAG(dag_id='sancoes_rj', default_args=args, schedule_interval=timedelta(days=1))
url_request = 'http://www.compras.rj.gov.br/Portal-Siga/Sancao/buscar.action'
csv_out_file = 'sancoesRJfinal.csv'
tabela = 'Inidoneos.dbo.Dados_SancaoRJ'

t1 = SeleniumOperator(
    task_id='get_url',
    script=get_url,
    script_args=[url_request],
    dag=dag)

t2 = PythonOperator(
    task_id='edita_base',
    python_callable=edita_base,
    op_kwargs={'nome_arquivo_out':os.path.join(local_downloads,csv_out_file)},
    dag=dag)

t3 = PythonOperator(
    task_id='truncate_e_reinsert_to_sqlserver',
    python_callable=truncate_e_reinsert_to_sqlserver,
    op_kwargs={'nome_tabela':tabela, 'nome_arquivo':os.path.join(local_downloads,csv_out_file)},
    dag=dag)

t4 = PythonOperator(
    task_id='apagar_resquicios',
    python_callable=apaga_resquicios,
    op_kwargs={},
    dag=dag)

t5 = PythonOperator(
    task_id='apagar_arquivos',
    python_callable=apagar_arquivos,
    op_kwargs={'lista_arquivos': [os.path.join(local_downloads,csv_out_file)]},
    dag=dag)

# Ordem das operações
t1 >> t2 >> t3 >> t4 >> t5