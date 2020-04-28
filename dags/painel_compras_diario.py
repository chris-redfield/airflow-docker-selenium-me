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
import painel_compras_entities.Estrutura_DDL_Tabelas_Sicaf as Estrutura
import painel_compras_entities.Sicaf as Sicaf
import painel_compras_entities.Painel_Fornecedor as Painel_Fornecedor

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    'execution_timeout': timedelta(hours=3),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'painel_compras_diario',
    default_args=default_args,
    description='dag diaria do painel de fornecedor',
    schedule_interval=timedelta(days=1),
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

    print("Apagando tabelas temporarias")

    # Deletar as tabelas
    execute_sql(Estrutura.DROP_TABLE, engine)
    
    print("Recriando tabelas temporarias")
    # Criar a estrutura de dados
    execute_sql(Estrutura._FORNECEDOR, engine)
    execute_sql(Estrutura._FORNECEDOR_COMPLEMENTO, engine)
    execute_sql(Estrutura._SERVIDOR, engine)
    execute_sql(Estrutura._FORNECEDOR_SOCIO, engine)
    execute_sql(Estrutura._SOCIO, engine)
    execute_sql(Estrutura._FORNECEDOR_PENALIDADE, engine)
    execute_sql(Estrutura._FORNECEDOR_INDICADORES, engine)
    execute_sql(Estrutura._FORNECEDOR_PENALIDADE_SOCIO, engine)
    execute_sql(Estrutura._FORNECEDOR_PENALIDADE_FORN, engine)

    return 0

def load_fornecedor():
    
    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    print("Consultando DW SICAF")

    ## FORNECEDOR
    tabela_sql = etl.fromdb(connSicaf, Sicaf.FORNECEDOR)

    print("realizando carga na tabela _FORNECEDOR")
    etl.todb(tabela_sql, engine, '_FORNECEDOR')

    return 0

def load_fornecedor_complemento():

    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    ## FORNECEDOR_COMPLEMENTO

    print("Consultando DW SICAF")

    df = pd.read_sql_query(sql=Sicaf.FORNECEDOR_COMPLEMENTO, con=connSicaf)
    df['DT_VALIDADE_BALANCO'] = pd.to_datetime(df['DT_VALIDADE_BALANCO'], format="%d/%m/%Y", errors='coerce')
    df['DT_CADASTRO'] = pd.to_datetime(df['DT_CADASTRO'], format="%d/%m/%Y", errors='coerce')
    df['DT_ULTIMA_ATUALIZACAO'] = pd.to_datetime(df['DT_ULTIMA_ATUALIZACAO'], format="%d/%m/%Y", errors='coerce')
    #df['DT_VENCIMENTO_CREDENCIAMENTO'] = pd.to_datetime(df['DT_VENCIMENTO_CREDENCIAMENTO'], format="%d/%m/%Y", errors='coerce')
    #df['DT_VALIDADE_CREDENCIAMENTO'] = pd.to_datetime(df['DT_VALIDADE_CREDENCIAMENTO'], format="%d/%m/%Y", errors='coerce')

    print("Realizando carga na tabela _FORNECEDOR_COMPLEMENTO")
    df.to_sql(name='_FORNECEDOR_COMPLEMENTO', con=engine, if_exists = 'append',index=False)

    return 0

def load_servidor():

    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()
    
    ## SERVIDOR
    print("Consultando DW SICAF")
    tabela_sql = etl.fromdb(connSicaf, Sicaf.SERVIDOR)

    print("Realizando carga na tabela _SERVIDOR")
    etl.todb(tabela_sql, engine, '_SERVIDOR')

    return 0

def load_fornecedor_socio():
    ## FORNECEDOR_SOCIO
    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    print("Consultando DW SICAF")
    tabela_sql = etl.fromdb(connSicaf, Sicaf.FORNECEDOR_SOCIO)
    
    print("Realizando carga na tabela _FORNECEDOR_SOCIO")
    etl.todb(tabela_sql, engine, '_FORNECEDOR_SOCIO')

    return 0

def load_socio():

    ## SOCIO
    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    print("Consultando DW SICAF")
    tabela_sql = etl.fromdb(connSicaf, Sicaf.SOCIO)

    print("Realizando carga na tabela _SOCIO")
    etl.todb(tabela_sql, engine, '_SOCIO')

    return 0

def load_fornecedor_penalidade():

    ## FORNECEDOR_PENALIDADE
    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    print("Consultando DW SICAF")
    df_penalidade = pd.read_sql_query(sql=Sicaf.FORNECEDOR_PENALIDADE, con=connSicaf)
    df_penalidade['DT_INICIO'] = pd.to_datetime(df_penalidade['DT_INICIO'], format="%d/%m/%Y", errors='coerce')
    df_penalidade['DT_FINAL'] = pd.to_datetime(df_penalidade['DT_FINAL'], format="%d/%m/%Y", errors='coerce')

    print("Realizando carga na tabela _FORNECEDOR_PENALIDADE")
    df_penalidade.to_sql(name='_FORNECEDOR_PENALIDADE', con=engine, if_exists = 'append',index=False)

    return 0


def load_fornecedor_indicadores():

    connSicaf = get_sicaf_dw_connection()
    engine = get_data_lake_engine()

    ## CREDENCIAMENTO_INDICADORES -> FORNECEDOR_INDICADORES
    print("Realizando carga CREDENCIAMENTO_INDICADORES -> FORNECEDOR_INDICADORES")
    execute_sql(Painel_Fornecedor.CREDENCIAMENTO_INDICADORES, engine)

    ## REGULARIDADE_TRABALHISTA_FEDERAL -> FORNECEDOR_INDICADORES
    print("Realizando carga REGULARIDADE_TRABALHISTA_FEDERAL -> FORNECEDOR_INDICADORES")
    tabela_sql = etl.fromdb(connSicaf, Sicaf.REGULARIDADE_TRABALHISTA_FEDERAL)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_INDICADORES')

    ## REGULARIDADE_TRABALHISTA_MUNICIPAL -> FORNECEDOR_INDICADORES
    print("Realizando carga REGULARIDADE_TRABALHISTA_MUNICIPAL -> FORNECEDOR_INDICADORES")
    tabela_sql = etl.fromdb(connSicaf, Sicaf.REGULARIDADE_TRABALHISTA_MUNICIPAL)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_INDICADORES')

    ## IMPEDITIVO_PENALIDADE -> FORNECEDOR_INDICADORES
    print("Realizando carga IMPEDITIVO_PENALIDADE -> FORNECEDOR_INDICADORES")
    tabela_sql = etl.fromdb(connSicaf, Sicaf.IMPEDITIVO_PENALIDADE)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_INDICADORES')

    ## PENALIDADES_VIGENTES -> FORNECEDOR_INDICADORES
    print("Realizando carga PENALIDADES_VIGENTES -> FORNECEDOR_INDICADORES")
    execute_sql(Painel_Fornecedor.PENALIDADES_VIGENTES, engine)

    ## VINCULO_SERVIDOR_SOCIO -> FORNECEDOR_INDICADORES
    print("Realizando carga VINCULO_SERVIDOR_SOCIO -> FORNECEDOR_INDICADORES")
    execute_sql(Painel_Fornecedor.VINCULO_SERVIDOR_SOCIO, engine)

    return 0


def load_fornecedor_penalidade_socio():

    engine = get_data_lake_engine()

    ## FORNECEDOR_PENALIDADE_SOCIO
    print("Realizando carga na tabela _FORNECEDOR_PENALIDADE_SOCIO")
    execute_sql(Painel_Fornecedor.FORNECEDOR_PENALIDADE_SOCIO, engine)

    return 0

def load_fornecedor_penalidade_forn():

    engine = get_data_lake_engine()

    ### FORNECEDOR_PENALIDADE_FORN
    print("Realizando carga na tabela _FORNECEDOR_PENALIDADE_FORN")
    execute_sql(Painel_Fornecedor.FORNECEDOR_PENALIDADE_FORN, engine)

    return 0

def commit_etl():

    engine = get_data_lake_engine()

    # _FORNECEDOR
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR', 'FORNECEDOR_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR', 'FORNECEDOR';""", engine)

    # _FORNECEDOR_COMPLEMENTO
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR_COMPLEMENTO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_COMPLEMENTO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_COMPLEMENTO', 'FORNECEDOR_COMPLEMENTO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_COMPLEMENTO', 'FORNECEDOR_COMPLEMENTO';""", engine)
    
    # _SERVIDOR
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - SERVIDOR")
    execute_sql("""DROP TABLE IF EXISTS SERVIDOR_ANT;""", engine)
    execute_sql("""sp_rename 'SERVIDOR', 'SERVIDOR_ANT';""", engine)
    execute_sql("""sp_rename '_SERVIDOR', 'SERVIDOR';""", engine)

    # _FORNECEDOR_SOCIO
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR_SOCIO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_SOCIO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_SOCIO', 'FORNECEDOR_SOCIO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_SOCIO', 'FORNECEDOR_SOCIO';""", engine)

    # _SOCIO
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - SOCIO")
    execute_sql("""DROP TABLE IF EXISTS SOCIO_ANT;""", engine)
    execute_sql("""sp_rename 'SOCIO', 'SOCIO_ANT';""", engine)
    execute_sql("""sp_rename '_SOCIO', 'SOCIO';""", engine)

    # _FORNECEDOR_PENALIDADE
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR_PENALIDADE")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_PENALIDADE_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_PENALIDADE', 'FORNECEDOR_PENALIDADE_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_PENALIDADE', 'FORNECEDOR_PENALIDADE';""", engine)

    # _FORNECEDOR_INDICADORES
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR_INDICADORES")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_INDICADORES_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_INDICADORES', 'FORNECEDOR_INDICADORES_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_INDICADORES', 'FORNECEDOR_INDICADORES';""", engine)

    # _FORNECEDOR_PENALIDADE_SOCIO
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR_PENALIDADE_SOCIO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_PENALIDADE_SOCIO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_PENALIDADE_SOCIO', 'FORNECEDOR_PENALIDADE_SOCIO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_PENALIDADE_SOCIO', 'FORNECEDOR_PENALIDADE_SOCIO';""", engine)

    # _FORNECEDOR_PENALIDADE_FORN
    print("Apagando tabelas antigas de backup - criando novas - renomeando tabela temporária - FORNECEDOR_PENALIDADE_FORN")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_PENALIDADE_FORN_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_PENALIDADE_FORN', 'FORNECEDOR_PENALIDADE_FORN_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_PENALIDADE_FORN', 'FORNECEDOR_PENALIDADE_FORN';""", engine)

    execute_sql("""
    Update CARGA_ATUALIZADA set
    DT_ATUALIZACAO_CARGA_ANTERIOR = DT_ATUALIZACAO_CARGA,
    DT_ATUALIZACAO_CARGA = getdate()
    WHERE FONTE = 'SICAF';
    """, engine)

    return 0

t0 = PythonOperator(
    task_id='reset_temporary_tables',
    python_callable=reset_temp_tables,
    dag=dag
)

t1 = PythonOperator(
    task_id='load_fornecedor',
    python_callable=load_fornecedor,
    dag=dag)

t2 = PythonOperator(
    task_id='load_fornecedor_complemento',
    python_callable=load_fornecedor_complemento,
    dag=dag)

t3 = PythonOperator(
    task_id='load_servidor',
    python_callable=load_servidor,
    dag=dag)

t4 = PythonOperator(
    task_id='load_fornecedor_socio',
    python_callable=load_fornecedor_socio,
    dag=dag)

t5 = PythonOperator(
    task_id='load_socio',
    python_callable=load_socio,
    dag=dag)

t6 = PythonOperator(
    task_id='load_fornecedor_penalidade',
    python_callable=load_fornecedor_penalidade,
    dag=dag
)

t7 = PythonOperator(
    task_id='load_fornecedor_indicadores',
    python_callable=load_fornecedor_indicadores,
    dag=dag
)

t8 = PythonOperator(
    task_id='load_fornecedor_penalidade_socio',
    python_callable=load_fornecedor_penalidade_socio,
    dag=dag
)

t9 = PythonOperator(
    task_id='load_fornecedor_penalidade_forn',
    python_callable=load_fornecedor_penalidade_forn,
    dag=dag
)

t10 = PythonOperator(
    task_id='commit_etl',
    python_callable=commit_etl,
    dag=dag
)

commit_etl

t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10