from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.python_operator import PythonOperator
from selenium_scripts.sancoesSP import get_url
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import logging

import os
import time
import pyodbc
import pandas as pd
import csv

import glob

class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')

args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 31),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

local_downloads = os.path.join(os.getcwd(), 'downloads')
local_csv = os.path.join(local_downloads,'csv')
local_xls = os.path.join(local_downloads,'xls')


# Aqui as informações pertinentes a cada uma das três bases para poder operá-las em sequencia
# Procuramos abstrair para aqui dentro o que difere as três bases:
#   -> Nome
#   -> ID da função javascript pra acessar as mesmas
#   -> Dados, representados aqui pelos cabeçalhos
dicionario_bases = {
    'sancoes' : {
        'tipo' : 'SançõesRestritivas',
        'id_tipo' : "__tab_ctl00_ContentPlaceHolder1_TabsSancaos_TabSancaoIncluidas",
        'cab' : ['TIPO_MEDIDA','PODER','SECRETARIA/ORGAO','UGE','NUMERO DO PROCESSO','TIPO DE PESSOA','RAZAO SOCIAL/NOME','CNPJ/CPF','TIPO DE SANCAO','PERIODO DE SANCAO', 'DATA INICIO', 'DATA TERMINO', 'ABRANGENCIA DA PENALIDADE']
    },
    'multas' : {
        'tipo' : 'Multas',
        'id_tipo' : "__tab_ctl00_ContentPlaceHolder1_TabsSancaos_TabMultas",
        'cab' : ['TIPO_MEDIDA','PODER','SECRETARIA/ORGAO','UGE','NUMERO DO PROCESSO','TIPO DE PESSOA','RAZAO SOCIAL/NOME','CNPJ/CPF','TIPO DE SANCAO','VALOR MULTA']
    },
    'advertencias' : {
        'tipo' : 'Advertencias',
        'id_tipo' : "__tab_ctl00_ContentPlaceHolder1_TabsSancaos_TabAdvertencias",
        'cab' : ['TIPO_MEDIDA','PODER','SECRETARIA/ORGAO','UGE','NUMERO DO PROCESSO','TIPO DE PESSOA','RAZAO SOCIAL/NOME','CNPJ/CPF','TIPO DE SANCAO']
    }
}

def remove_files():
    for filename in os.listdir(local_xls):
        os.remove(local_xls + "/" + filename)
    for filename in os.listdir(local_csv):
        os.remove(local_csv + "/" + filename)
    os.rmdir(local_xls)
    os.rmdir(local_csv)


def copy_to_sql_server ():
    nome_tabela = 'Inadimplentes.dbo.Dados_inadimplentes'

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = os.environ['INADIMPLENTES_DATABASE']
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    #Evazia tabela
    deleta = "DELETE " + nome_tabela
    cursor.execute(deleta) 

    for nome in ['sancoes','multas','advertencias']:
        nome_arquivo = local_csv+'/output'+dicionario_bases[nome]['tipo']+'_normalizado.csv'
        #
        # Preparando os comandos
        panda = pd.read_csv(nome_arquivo,encoding='utf-8')

        #TODO row['VALOR MULTA'] retornando nan e dando erro no banco

        for index,row in panda.iterrows():
            print('----> Tipo medida:',len(str(row['TIPO_MEDIDA'])))
            print('----> poder:',len(str(row['PODER'])))
            print('----> secretaria:',len(str(row['SECRETARIA/ORGAO'])))
            print('----> uge:',len(str(row['UGE'])))
            print('----> numero p:',len(str(row['NUMERO DO PROCESSO'])))
            print('----> tipo pessoa:',len(str(row['TIPO DE PESSOA'])))
            print('----> razao:',len(str(row['RAZAO SOCIAL/NOME'])))
            print('----> cpf:',len(str(row['CNPJ/CPF'])))
            print('----> tipo sancao:',len(str(row['TIPO DE SANCAO'])))
            print('----> periodo sancao:',len(str(row['PERIODO DE SANCAO'])))
            print('----> data inicio:',len(str(row['DATA INICIO'])))
            print('----> data termino:',len(str(row['DATA TERMINO'])))
            print('----> abrangencia:',len(str(row['ABRANGENCIA DA PENALIDADE'])))
            print('----> valor multa',len(str(row['VALOR MULTA'])))
            query = 'INSERT INTO {}(TIPO_MEDIDA,PODER,SECRETARIA_ORG,UGE,NUMERO_PROCESSO,TIPO_PESSOA,RAZAO_SOCIAL,CNPJ_CPF,TIPO_SANCAO,PERIOD_SANCAO,DATA_INICIO,DATA_TERMINO,ABRANGENCIA_PENALIDADE,VALOR_MULTA) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(nome_tabela)
            cursor.execute(query,
            str(row['TIPO_MEDIDA']),
            str(row['PODER']),
            str(row['SECRETARIA/ORGAO']),
            str(row['UGE']),
            str(row['NUMERO DO PROCESSO']),
            str(row['TIPO DE PESSOA']),
            str(row['RAZAO SOCIAL/NOME']),
            str(row['CNPJ/CPF']),
            str(row['TIPO DE SANCAO']),
            str(row['PERIODO DE SANCAO']),
            str(row['DATA INICIO']),
            str(row['DATA TERMINO']),
            str(row['ABRANGENCIA DA PENALIDADE']),
            str(row['VALOR MULTA']))
            cnxn.commit()

    #Fechando a conexão
    cursor.close()
    cnxn.close()
        

def normalize_data():
    for nome in ['sancoes','multas','advertencias']:
        cab_banco = ['TIPO_MEDIDA','PODER','SECRETARIA/ORGAO','UGE','NUMERO DO PROCESSO','TIPO DE PESSOA','RAZAO SOCIAL/NOME','CNPJ/CPF','TIPO DE SANCAO','PERIODO DE SANCAO','DATA INICIO', 'DATA TERMINO', 'ABRANGENCIA DA PENALIDADE','VALOR MULTA']

        filename = open(local_csv+'/output'+dicionario_bases[nome]['tipo']+'.csv', 'r')
        base_normalizada = [cab_banco]
        cab_arq = filename.readline().replace('\n','').split(',')
        for line in filename:
            line = line.replace('\n','').split(',')
            reg_final = []
            for campo in cab_banco:
                if campo in cab_arq:
                    reg_final.append(line[cab_arq.index(campo)])
                else:
                    reg_final.append('')
            base_normalizada.append(reg_final)

        with open(local_csv+'/output'+dicionario_bases[nome]['tipo']+'_normalizado.csv', 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(base_normalizada)

def html_to_csv():

    # Garante que a pasta de csv existe
    if not os.path.exists(local_csv):
        os.makedirs(local_csv)


    # Opera uma base de cada vez (sancoes, multas e advertencias)
    for (nome_base, base) in dicionario_bases.items():

        print (f'Inicio da leitura de {nome_base}')

        full_base = []
        full_base.append(base['cab'])

        # Para cada arquivo do tipo relevante, ou seja, <pasta xls>/<tipo base>_<pagina>_1<fornecedor>.xls
        for filename in glob.glob( f"{local_xls}/{base['tipo']}_*"):
            html = open(filename).read()
            soup = BeautifulSoup(html,"html.parser")
            table = soup.find("table")

            for table_row in table.findAll('tr'):
                columns = table_row.findAll('td')
                output_row = []
                for column in columns[1:]:
                    text = column.text
                    text = text.replace('Visualizar descrição do valor','').replace(',','.').replace('\n','').strip()
                    output_row.append(text)

            if output_row != [] and output_row != ['Nenhum registro encontrado']:
                full_base.append([base['tipo']] + output_row)

        print ('Final da leitura: ', time.ctime())

        with open(local_csv+'/output'+base['tipo']+'.csv', 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(full_base)

# Processo: DADOS_BEC_SP

dag = DAG(dag_id='sancoes_sp', default_args=args, schedule_interval=timedelta(days=30))

t1 = SeleniumOperator(
    task_id='get_url',
    script=get_url,
    script_args=[local_downloads, local_xls, dicionario_bases],
    dag=dag)

t2 = PythonOperator(
    task_id='html_to_csv',
    python_callable=html_to_csv,
    op_kwargs={},
    dag=dag)

t3 = PythonOperator(
    task_id='normalize_data',
    python_callable=normalize_data,
    op_kwargs={},
    dag=dag)

t4 = PythonOperator(
    task_id='copy_to_sql_server',
    python_callable=copy_to_sql_server,
    op_kwargs={},
    dag=dag)

t5 = PythonOperator(
    task_id='remove_files',
    python_callable=remove_files,
    op_kwargs={},
    dag=dag)

# # Ordem das operações
t1 >> t2 >> t3 >> t4 >> t5

