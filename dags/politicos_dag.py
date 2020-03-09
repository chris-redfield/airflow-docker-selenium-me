from airflow import DAG                                             # airflow
from airflow.operators.python_operator import PythonOperator        # airflow
from datetime import datetime, timedelta                            # airflow

import requests
import pyodbc
import zipfile
import os
import csv
import json
import time

args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 17),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=60),
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

local_downloads = os.path.join(os.getcwd(), 'downloads/politicos_dag')
deputados_json_array = []


#Conexão ao banco de dados
def conexao_sql():

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = os.environ['POLITICOS_DATABASE']
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    #
    #Fazendo a conexão
    con = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return con

#Busca o arquivo Json
def request_json(url_request):
    r = requests.get(url_request, headers={'Accept': 'application/json'})
    return r.json()

#Limpa Tabelas para inserção
def limpaTabelas():

    #Chama a conexão ao banco de dados
    con = conexao_sql()

    cursor1 = con.cursor()
    deletaDeputado = "TRUNCATE TABLE Politicos..Dados_Deputado;"
    cursor1.execute(deletaDeputado)

    cursor2 = con.cursor()
    deletaSenador = "TRUNCATE TABLE Politicos..Dados_Senador;"
    cursor2.execute(deletaSenador)

    cursor3 = con.cursor()
    deletaCongressistas = "TRUNCATE TABLE Politicos..Dados_Congressistas;"
    cursor3.execute(deletaCongressistas)

    con.commit()
    con.close()

#Baixa arquivo CSV
def baixaArquivo(url):
    
    #Baixa Arquivo de Politicos
    anos = [2014,2018]
    links = []
    
    for ano in anos:
        links.append(url+'consulta_cand_'+str(ano))
   
        for link in links:
            link = link + '.zip'
            r = requests.get(link)
            if(r.status_code!= 404):
                open(str(link.split('/')[-1]), 'wb').write(r.content)
                arquivo = str(link.split('/')[-1])
                descompactaArquivo(arquivo)

#Descompacta Arquivo CSV
def descompactaArquivo(arquivo):
    #Descompacta Arquivo
    if not zipfile.is_zipfile(arquivo):
        return     

    # carrega o zip
    zfobj = zipfile.ZipFile(arquivo)
    zfobj.extractall()

#Insere dados de Deputado
def insereDeputados(url,nomeTabela1,nomeTabela2,deputados_file): 

    try:
        for filename in os.listdir(local_downloads):
            os.remove(filename)
    except:
        os.mkdir(local_downloads)

    try:
        with open(deputados_file,'r') as f:
            deputados_json = f.readlines()
            for deputado_json in deputados_json:
                deputados_json_array.append(deputado_json)
            f.close()
    except FileNotFoundError:
        print('criando arquivo de controle de fluxo')

    #Limpar tabelas
    limpaTabelas()

    #Seleciona json principal da API
    req_dados = request_json(url)

    #Deputado
    for dado in (req_dados['dados']):

        run_bool = True
        for deputado_json in deputados_json_array:
            if dado['id'] == deputado_json['id']:
                run_bool = False

        if run_bool:

            print('=========> requisição',dado['id'])

            #Seleciona dados secundarios
            req = request_json(str(dado['uri']).lstrip())
            req = req['dados']

            print('=========> reposta',req)

            insereDeputado(nomeTabela1,nomeTabela2,req)

            deputados_json_array.append(req)
            with open(deputados_file,'a') as f:
                f.write(json.dumps(req))

    for deputado_json in deputados_json_array:
        insereDeputado(nomeTabela1,nomeTabela2,deputado_json)

def insereDeputado(nomeTabela1,nomeTabela2,req):
    #Chama a conexão ao banco de dados
    con = conexao_sql() 

    #Insere dados deputado na tabela
    cursor = con.cursor()
    query = """INSERT INTO """+nomeTabela1+""" (NUMERO_DEPUTADO,NOME_DEPUTADO,CPF_DEPUTADO) 
    VALUES ('%s','%s','%s');""" % (req['id'],req['nomeCivil'].replace("'",""),req['cpf'])
    cursor.execute(query)
    

    #Insere dados daputado na tabela congressistas
    cursor1 = con.cursor()
    query = """INSERT INTO """+nomeTabela2+""" (NUMERO_PARLAMENTAR,NOME_PARLAMENTAR,CPF_PARLAMENTAR,SITUACAO_PARLAMENTAR,CARGO_PARLAMENTAR) 
    VALUES ('%s','%s','%s','%s','%s');""" % (req['id'],req['nomeCivil'].replace("'",""),req['cpf'],req['ultimoStatus']['situacao'],'Deputado')
    cursor1.execute(query)

    con.commit()
    con.close()


#Insere Dados de Senador    
def insereSenador(nomeTabela1,nomeTabela2):

    #Baixar arquivo
    baixaArquivo('http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/')

    anos = [2014,2018]

    for ano in anos: 

        #Abre o Arquivo
        with open("""consulta_cand_"""+str(ano)+"""_BRASIL.csv""",encoding='latin') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            for row in csv_reader:

                if row[14] in ('SENADOR'):

                    if row[53] not in ('NÃO ELEITO'):

                        #Chama a conexão ao banco de dados
                        con = conexao_sql()

                        cursor1 = con.cursor()
                        #Verifica se o registro ja foi inserido
                        select = """SELECT CPF_SENADOR FROM """+nomeTabela1+""" where CPF_SENADOR ='"""+(str(row[20]))+"'"
                        cursor1.execute(select)
                        linha = cursor1.fetchall()
                        
                        #Permita inserir dados se não hoverem registros repetidos de outros anos
                        if linha == []:

                            cursor2 = con.cursor()
                            #Insere dados Senador na tabela
                            query = """INSERT INTO """+nomeTabela1+""" (NUMERO_SENADOR,NOME_SENADOR,CPF_SENADOR) 
                            VALUES ('%s','%s','%s');""" % (row[18],row[17],row[20])                     
                            cursor2.execute(query)

                            #Insere dados daputado na tabela congressistas
                            cursor3 = con.cursor()
                            query = """INSERT INTO """+nomeTabela2+""" (NUMERO_PARLAMENTAR,NOME_PARLAMENTAR,CPF_PARLAMENTAR,SITUACAO_PARLAMENTAR,CARGO_PARLAMENTAR) 
                            VALUES ('%s','%s','%s','%s','%s');""" % (row[18],row[17],row[20],row[53],'Senador')
                            cursor3.execute(query)

                        con.commit()
                        con.close()
    
#Deleta arquivos
def deletaArquivo():  
    for filename in os.listdir(local_downloads):
        os.remove(os.path.join(local_downloads,filename))  
    os.rmdir(local_downloads)


# ---------------------------------------------------------------------------------------------------------------
# ------------------------------- AIRFLOW -----------------------------------------------------------------------

# Processo: Politicos
dag = DAG('politicos', default_args=args, schedule_interval=timedelta(days=1))
url_request = 'https://dadosabertos.camara.leg.br/api/v2/deputados'#?idLegislatura=56&ordem=ASC&ordenarPor=nome'
tabela_deputados = 'Politicos.dbo.Dados_Deputado'
tabela_congressistas = 'Politicos.dbo.Dados_Congressistas'
tabela_senador = 'Politicos.dbo.Dados_Senador'
deputados_file = 'deputados_politicos.json'

t0 = PythonOperator(
    task_id='insereDeputados',
    python_callable=insereDeputados,
    op_kwargs={'url':url_request,'nomeTabela1':tabela_deputados,'nomeTabela2':tabela_congressistas, 'deputados_file': os.path.join(local_downloads,deputados_file)},
    dag=dag)

t1 = PythonOperator(
    task_id='insereDadosSenador',
    python_callable=insereSenador,
    op_kwargs={'nomeTabela1':tabela_senador,'nomeTabela2':tabela_congressistas},
    dag=dag)

t2 = PythonOperator(
    task_id='deletaArquivo',
    python_callable=deletaArquivo,
    op_kwargs={},
    dag=dag)

# Ordem das operações
t0 >> t1 >> t2