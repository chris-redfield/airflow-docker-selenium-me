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
import painel_compras_entities.Estrutura_DDL_Tabelas_Compras as Estrutura
import painel_compras_entities.Siasg_DW as Siasg_DW
import painel_compras_entities.Painel_Fornecedor as Painel_Fornecedor
import painel_compras_entities.Comprasnet as Comprasnet
import painel_compras_entities.Siasgnet as Siasgnet

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
    'painel_compras_mensal',
    default_args=default_args,
    description='dag mensal do painel de fornecedor',
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

# CONEXÂO NO BANCO DE DADOS DO QUATZO
# ESTA CONEXÃO SERA TEMPORARIA ATE A ATUALIZACAO DO DATALAKE NO SQL SERVER
# Siasgnet
def get_siasg_dw_connection():
    user = os.environ['TEIID_USER']
    password = os.environ['TEIID_PASS']
    connSiasg_DW = sqlalchemy.create_engine(
        f"postgresql+teiid://{user}:{password}@daas.serpro.gov.br:35432/Siasg_DW", 
        executemany_mode='batch', 
        server_side_cursors=False
    )

    return connSiasg_DW

def get_comprasnet_connection():
    user = os.environ['TEIID_USER']
    password = os.environ['TEIID_PASS']
    connComprasnet = sqlalchemy.create_engine(
        f"postgresql+teiid://{user}:{password}@daas.serpro.gov.br:35432/Comprasnet", 
        executemany_mode='batch', 
        server_side_cursors=False
    )

    return connComprasnet

def get_siasgnet_connection():
    user = os.environ['TEIID_USER']
    password = os.environ['TEIID_PASS']
    connQSiasgnet = sqlalchemy.create_engine(
        f"postgresql+teiid://{user}:{password}@daas.serpro.gov.br:35432/Siasgnet", 
        executemany_mode='batch', 
        server_side_cursors=False
    )

    return connQSiasgnet

def drop_temporary_tables():

    engine = get_data_lake_engine()
    # Creates temp tables
    print("Apagando tabelas temporárias antigas...")
    execute_sql(Estrutura.DROP_TABLE, engine)
    print("Criando novas tabelas temporárias")
    execute_sql(Estrutura._FORNECEDOR_COMPORTAMENTO, engine)
    execute_sql(Estrutura._FORNECEDOR_CONTRATO, engine)
    execute_sql(Estrutura._FORNECEDOR_CONTRATO_ITEM, engine)
    execute_sql(Estrutura._FORNECEDOR_HISTORICO, engine)
    execute_sql(Estrutura._FORNECEDOR_HISTORICO_CONTRATO, engine)
    execute_sql(Estrutura._FORNECEDOR_SUMARIO_COMPRA, engine)
    print("Tabelas temporárias criadas com sucesso")

    return 0


def load_fornecedor_contrato():

    connSiasg_DW = get_siasg_dw_connection()

    engine = get_data_lake_engine()

    # Fornecedor_Contrato
    print("Realizando carga fornecedor - contrato")
    print("Fazendo select SIASG DW")
    df_contrato = pd.read_sql_query(sql=Siasg_DW.FORNECEDOR_CONTRATO, con=connSiasg_DW)
    print("Select feito com sucesso:")
    print(df_contrato.shape)
    print(df_contrato.head())
    df_contrato['DS_CNTR_OBJ_CONT'] = df_contrato['DS_CNTR_OBJ_CONT'].replace('\0','', regex=True)
    df_contrato['DT_CNTR_INICIO_VIGENCIA'] = pd.to_datetime(df_contrato['DT_CNTR_INICIO_VIGENCIA'], format="%Y-%m-%d", errors='coerce')
    df_contrato['DT_CNTR_FIM_VIGENCIA'] = pd.to_datetime(df_contrato['DT_CNTR_FIM_VIGENCIA'], format="%Y-%m-%d", errors='coerce')
    df_contrato['DT_CNTR_ASS_CONT'] = pd.to_datetime(df_contrato['DT_CNTR_ASS_CONT'], format="%Y-%m-%d", errors='coerce')
    print("Fazendo carga no SQL Server")
    df_contrato.to_sql(name='_FORNECEDOR_CONTRATO', con=engine, if_exists = 'append',index=False)
    print("Concluída carga fornecedor - contrato")
    return 0


def load_fornecedor_contrato_item():

    connSiasg_DW = get_siasg_dw_connection()

    engine = get_data_lake_engine()

    print("Selecionando faixas de itens contrato")

    faixa = pd.read_sql_query(con=connSiasg_DW, sql="""
    SELECT cast((D.ID_ITCT_ITEM_CONTRATO  / 500000) as integer) as faixa 
    FROM D_ITCT_ITEM_CONTRATO D
    group by cast((D.ID_ITCT_ITEM_CONTRATO  / 500000) as integer)
    order by cast((D.ID_ITCT_ITEM_CONTRATO  / 500000) as integer)
    """); 
    for index, row in faixa.iterrows():
        print("faixa", index+1)
        print(f"Selecionando faixa de {int(index * 500000)} ate {int(index * 500000 + 499999 )}...")
        n = row['faixa']
        sql = """
            SELECT 
                I.ID_ITCT_ITEM_CONTRATO,    
                I.ID_CNTR_CONTRATO,
                I.ID_ITCP_ITEM_COMPRA,
                CH_ITCT_ITEM_CONTRATO_EDIT,
                DS_ITCP_ITEM_COMPRA as NO_ITCP_ITEM_CONTRATO,
                I.ID_ITCP_TP_COD_MAT_SERV,
                CASE WHEN TI.ID_ITCP_TP_MATERIAL_SERVICO = '1' THEN 'Material' WHEN TI.ID_ITCP_TP_MATERIAL_SERVICO = '2' THEN 'Serviço' else 'Não informado' end as NO_ITCP_TP_COD_MAT_SERV,
                I.QT_ITCT_ITENS_CONTRATO,
                I.QT_ITCT_CONTRATADA,
                I.VL_ITCT_CONTRATADO
            FROM D_CNTR_CONTRATO CO 
            INNER JOIN F_ITEM_CONTRATO I ON CO.ID_CNTR_CONTRATO = I.ID_CNTR_CONTRATO 
            AND I.ID_ITCT_ITEM_CONTRATO between {0} * 500000 and {0}* 500000 + 499999
            inner join D_ITCT_ITEM_CONTRATO D ON D.ID_ITCT_ITEM_CONTRATO = I.ID_ITCT_ITEM_CONTRATO
            INNER JOIN D_ITCP_ITEM_COMPRA C ON C.ID_ITCP_ITEM_COMPRA = I.ID_ITCP_ITEM_COMPRA
            INNER JOIN D_ITCP_MATERIAL_SERVICO TI ON I.ID_ITCP_TP_COD_MAT_SERV = TI.ID_ITCP_TP_COD_MAT_SERV;
        """.format(n);
        tabela_sql = etl.fromdb(connSiasg_DW, sql);
        print("Inserindo faixa na base de dados...")
        etl.todb(tabela_sql, engine, '_FORNECEDOR_CONTRATO_ITEM')
    print("Carga realizada com sucesso")
    return 0

def load_fornecedor_comportamento():
    
    connComprasnet = get_comprasnet_connection()

    connSiasg_DW = get_siasg_dw_connection()

    engine = get_data_lake_engine()

    print("Iniciando carga desclassificação ...")
    ## DESCLASSIFICACAO_FORNECEDORES -> FORNECEDOR_COMPORTAMENTO
    tabela_sql = etl.fromdb(connComprasnet, Comprasnet.DESCLASSIFICACAO_FORNECEDORES)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_COMPORTAMENTO')
    print("Carga desclassificação executada com sucesso")

    print("Iniciando carga \"contrato continuado\"")
    ## CONTRATO_CONTINUADO -> FORNECEDOR_COMPORTAMENTO
    tabela_sql = etl.fromdb(connSiasg_DW, Siasg_DW.CONTRATO_CONTINUADO)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_COMPORTAMENTO')
    print("Carga \"contrato continuado\" executada com sucesso")

    print("Iniciando carga recursos ...")
    ## RECURSOS -> FORNECEDOR_COMPORTAMENTO
    # Esta consulta terá que ser ajustada, quando for implentado as tabelas faltante no datalake
    # Hoje ela está no Quartzo (Postgres) e será migrada para o Datalake(SQL SERVER)
    tabela_sql = etl.fromdb(connComprasnet, Comprasnet.RECURSOS)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_COMPORTAMENTO')
    print("Carga recursos executada com sucesso")
    return 0

def load_fornecedor_sumario_compra():

    connSiasg_DW = get_siasg_dw_connection()
    
    engine = get_data_lake_engine()

    # FORNECEDOR_SUMARIO_COMPRA
    #tabela_sql = etl.fromdb(connSiasg_DW, Siasg_DW.FORNECEDOR_SUMARIO_COMPRA)
    #etl.todb(tabela_sql, engine, '_FORNECEDOR_SUMARIO_COMPRA')
    print("Selecionando faixas de itens contrato")
    faixa = pd.read_sql_query(con=connSiasg_DW, sql="""
    SELECT cast((D.ID_ITFN_ITEM_FORNECEDOR  / 10000000) as integer) as faixa 
    FROM F_ITEM_FORNECEDOR D
    group by cast((D.ID_ITFN_ITEM_FORNECEDOR  / 10000000) as integer)
    order by cast((D.ID_ITFN_ITEM_FORNECEDOR  / 10000000) as integer)
    """); 
    for index, row in faixa.iterrows():
        print("faixa", index+1)
        print(f"Selecionando faixa de {int(index * 10000000)} ate {int(index * 10000000 + 9999999 )}...")
        n = row['faixa']
        sql = """ SELECT F.ID_FRND_FORNECEDOR_COMPRA as ID_FORNECEDOR,
                    count(Distinct F.ID_CMPR_COMPRA) as QTD_COMPRA,
                    count(Distinct F.ID_ITCP_ITEM_COMPRA) as QTD_ITEM_COMPRA,
                    count(Distinct case when F.VL_PRECO_TOTAL_HOMOLOG > 0 then F.ID_CMPR_COMPRA end) as QTD_COMPRA_HOMOLOG,
                    count(Distinct case when F.VL_PRECO_TOTAL_HOMOLOG > 0 then F.ID_ITCP_ITEM_COMPRA end) as QTD_ITEM_COMPRA_HOMOLOG,
                    sum(F.VL_PRECO_TOTAL_HOMOLOG) as VALOR_HOMOLOGADO
                    from F_ITEM_FORNECEDOR F
                    WHERE F.ID_ITFN_ITEM_FORNECEDOR between {0} * 10000000 and {0}* 10000000 + 9999999
                    group by F.ID_FRND_FORNECEDOR_COMPRA;
        """.format(n);
        tabela_sql = etl.fromdb(connSiasg_DW, sql);
        print("Inserindo faixa na base de dados...")
        etl.todb(tabela_sql, engine, '_FORNECEDOR_SUMARIO_COMPRA')

    print("Carga realizada com sucesso")
    return 0

def load_fornecedor_historico():

    # Siasgnet
    connQSiasgnet = get_siasgnet_connection()
    engine = get_data_lake_engine()

    # CONTRATO_ATIVOS -> FORNECEDOR_HISTORICO
    print("Iniciando carga contratos ativos")
    execute_sql(Painel_Fornecedor.CONTRATO_ATIVOS, engine)
    print("Carga contratos ativos realizada com sucesso")

    # CONTRATO_VENCIDOS -> FORNECEDOR_HISTORICO
    print("Iniciando carga contratos vencidos")
    execute_sql(Painel_Fornecedor.CONTRATO_VENCIDOS, engine)
    print("Carga contratos vencidos realizada com sucesso")

    # LICITACOES -> FORNECEDOR_HISTORICO
    print("Iniciando carga licitações")
    execute_sql(Painel_Fornecedor.LICITACOES, engine)
    print("Carga licitações realizada com sucesso")

    ## ATA_VIGENTES -> FORNECEDOR_HISTORICO
    # Esta consulta terá que ser ajustada, quando for implentado as tabelas faltante no datalake
    # Hoje ela está no Quartzo (Postgres) e será migrada para o Datalake(SQL SERVER)
    print("Iniciando carga atas vigentes")
    tabela_sql = etl.fromdb(connQSiasgnet, Siasgnet.ATA_VIGENTES)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_HISTORICO_CONTRATO')
    print("Carga atas vigentes realizada com sucesso")

    ## ATA_VENCIDAS -> FORNECEDOR_HISTORICO
    # Esta consulta terá que ser ajustada, quando for implentado as tabelas faltante no datalake
    # Hoje ela está no Quartzo (Postgres) e será migrada para o Datalake(SQL SERVER)
    print("Iniciando carga atas vencidas")
    tabela_sql = etl.fromdb(connQSiasgnet, Siasgnet.ATA_VENCIDAS)
    etl.appenddb(tabela_sql, engine, '_FORNECEDOR_HISTORICO_CONTRATO')
    print("Carga atas vigentes realizada com sucesso")

    return 0

def commit_etl():

    engine = get_data_lake_engine()

    
    # _FORNECEDOR_COMPORTAMENTO
    print("Apagando tabelas antigas de backup - criando novas - criando nova tabela - FORNECEDOR_COMPORTAMENTO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_COMPORTAMENTO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_COMPORTAMENTO', 'FORNECEDOR_COMPORTAMENTO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_COMPORTAMENTO', 'FORNECEDOR_COMPORTAMENTO';""", engine)

    # _FORNECEDOR_CONTRATO
    print("Apagando tabelas antigas de backup - criando novas - criando nova tabela - FORNECEDOR_CONTRATO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_CONTRATO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_CONTRATO', 'FORNECEDOR_CONTRATO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_CONTRATO', 'FORNECEDOR_CONTRATO';""", engine)

    # _FORNECEDOR_CONTRATO_ITEM
    print("Apagando tabelas antigas de backup - criando novas - criando nova tabela - FORNECEDOR_CONTRATO_ITEM")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_CONTRATO_ITEM_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_CONTRATO_ITEM', 'FORNECEDOR_CONTRATO_ITEM_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_CONTRATO_ITEM', 'FORNECEDOR_CONTRATO_ITEM';""", engine)

    # _FORNECEDOR_HISTORICO
    print("Apagando tabelas antigas de backup - criando novas - criando nova tabela - FORNECEDOR_HISTORICO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_HISTORICO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_HISTORICO', 'FORNECEDOR_HISTORICO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_HISTORICO', 'FORNECEDOR_HISTORICO';""", engine)

    # _FORNECEDOR_HISTORICO_CONTRATO
    print("Apagando tabelas antigas de backup - criando novas - criando nova tabela - FORNECEDOR_HISTORICO_CONTRATO")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_HISTORICO_CONTRATO_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_HISTORICO_CONTRATO', 'FORNECEDOR_HISTORICO_CONTRATO_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_HISTORICO_CONTRATO', 'FORNECEDOR_HISTORICO_CONTRATO';""", engine)

    # _FORNECEDOR_SUMARIO_COMPRA
    print("Apagando tabelas antigas de backup - criando novas - criando nova tabela - FORNECEDOR_SUMARIO_COMPRA")
    execute_sql("""DROP TABLE IF EXISTS FORNECEDOR_SUMARIO_COMPRA_ANT;""", engine)
    execute_sql("""sp_rename 'FORNECEDOR_SUMARIO_COMPRA', 'FORNECEDOR_SUMARIO_COMPRA_ANT';""", engine)
    execute_sql("""sp_rename '_FORNECEDOR_SUMARIO_COMPRA', 'FORNECEDOR_SUMARIO_COMPRA';""", engine)

    print("Atualizando tabela de metadados da carga")
    execute_sql("""
    Update CARGA_ATUALIZADA set
    DT_ATUALIZACAO_CARGA_ANTERIOR = DT_ATUALIZACAO_CARGA,
    DT_ATUALIZACAO_CARGA = getdate()
    WHERE FONTE = 'COMPRAS';
    """, engine)

    print("Pipeline executado com sucesso")

    return 0


t0 = PythonOperator(
    task_id='drop_temporary_tables',
    python_callable=drop_temporary_tables,
    dag=dag
)

t1 = PythonOperator(
    task_id='load_fornecedor_contrato',
    python_callable=load_fornecedor_contrato,
    dag=dag)

# Para rodar a t2 (maior) em paralelo, basta deixá-la fora da 
# fila de execução e descomentar abaixo
t2 = PythonOperator(
    task_id='load_fornecedor_contrato_item',
    python_callable=load_fornecedor_contrato_item,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_fornecedor_comportamento',
    python_callable=load_fornecedor_comportamento,
    dag=dag
)

t4 = PythonOperator(
    task_id='load_fornecedor_sumario_compra',
    python_callable=load_fornecedor_sumario_compra,
    dag=dag
)

t5 = PythonOperator(
    task_id='load_fornecedor_historico',
    python_callable=load_fornecedor_historico,
    dag=dag
)

t6 = PythonOperator(
    task_id='commit_etl',
    python_callable=commit_etl,
    dag=dag
)

t0 >> t1 >> t3 >> t4
t0 >> t2
t4 >> t5
t2 >> t5
t5 >> t6