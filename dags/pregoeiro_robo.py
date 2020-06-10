from datetime import timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
import pymssql
import time
import petl as etl
import pyodbc
import sqlalchemy
from sqlalchemy.dialects import registry
registry.register("postgresql.teiid", "sqlalchemy_teiid", "TeiidDialect")

import os

### Tabela Fornecedor
import dags.pregoeiro_robo_entities.Pessoa as Pessoa


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

dag = DAG(
    'pregoeiro_robo',
    default_args=default_args,
    description='dag para cargas utilizadas pelo pregoeiro robo',
    schedule_interval=timedelta(days=30),
)

def execute_sql(comando, engine):
    connection = engine.connect()
    trans = connection.begin()
    try:
        connection.execute(comando)
        trans.commit()
    except:
        trans.rollback()
        raise

def get_data_lake_engine():
    # CONEXÂO NO BANCO DE DADOS DO PAINEL DO FORNECEDOR
    DB_SERVER = os.environ['LAKE_HOST']
    DB_PORT = os.environ['LAKE_PORT']
    DB_NAME = os.environ['PAINEL_DATABASE']
    DB_USER = os.environ['LAKE_USER']
    DB_PASS = os.environ['LAKE_PASS']

    # URL de configuração do SQL Alchemy
    dl_db_url = 'mssql+pymssql://{user}:{password}@{server}:{port}/{database}'.format(
        user=DB_USER,
        password=DB_PASS,
        server=DB_SERVER,
        port=DB_PORT,
        database=DB_NAME
    )

    # create the connection
    print(f"Conectando com banco SQL Server: {DB_SERVER}...")
    engine = sqlalchemy.create_engine(dl_db_url)
    
    return engine

# CONEXÂO NO BANCO DE DADOS DO QUARTZO
# Sicaf
def get_sicaf_dw_connection():
    user = os.environ['TEIID_USER']
    password = os.environ['TEIID_PASS']

    connSicaf = sqlalchemy.create_engine(
        f"postgresql+teiid://{user}:{password}@daas.serpro.gov.br:35432/Sicaf2018", 
        executemany_mode='batch', 
        server_side_cursors=False
    )
    return connSicaf

def reset_temp_tables():

    engine = get_data_lake_engine()

    print("Apagando tabela temporarias")

    # Deletar as tabelas
    execute_sql(Pessoa.DROP_TABLE, engine)
    
    print("Recriando tabelas temporarias")
    # Criar a estrutura de dados
    execute_sql(Pessoa._PESSOA, engine)

    return 0

def load_pessoa():
    
    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    print("Consultando DW SICAF")

    ## Pessoa
    tabela_sql = etl.fromdb(connSicaf, Pessoa.PESSOA)


    print("realizando carga na tabela _PESSOA")
    etl.todb(tabela_sql, engine, '_PESSOA')

    return 0

def commit_etl():

    engine = get_data_lake_engine()

    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - PESSOA")
    
    # _PESSOA
    execute_sql("""DROP TABLE IF EXISTS PESSOA_ANT;""", engine)
    execute_sql("""sp_rename 'PESSOA', 'PESSOA_ANT';""", engine)
    execute_sql("""sp_rename '_PESSOA', 'PESSOA';""", engine)

    return 0

t0 = PythonOperator(
    task_id='reset_temporary_tables',
    python_callable=reset_temp_tables,
    dag=dag
)

t1 = PythonOperator(
    task_id='load_pessoa',
    python_callable=load_pessoa,
    dag=dag)

t2 = PythonOperator(
    task_id='commit_etl',
    python_callable=commit_etl,
    dag=dag
)

commit_etl

t0 >> t1 >> t2