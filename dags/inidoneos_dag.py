import airflow 
from airflow.models import DAG                                      # airflow
from airflow.operators.python_operator import PythonOperator        # airflow
from datetime import datetime, timedelta                            # airflow

# 
import pyodbc
import requests                                                     # Tarefa 1 e 2
from bs4 import BeautifulSoup                                       # Tarefa 1
import math                                                         # Tarefa 2
from tqdm import tqdm                                               # Tarefa 2
import pandas                                                       # Tarefa 3
import os                                                           # Tarefa 5

args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 17),
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

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 1 -----------------------------------------------------------------------
# Buscar dentro da url dos dados abertos o link para o conteudo em csv e joga em um arquivo txt

def get_url (url_request,url_file):
    pagina = requests.get(url_request)
    soup = BeautifulSoup(pagina.content,'html.parser')
    listarecursos = soup.findAll("li", {"class": "resource-item"}) # Todos os recursos para baixar
    for recurso in listarecursos:
        if "em CSV" in recurso.text:            # Quero o recurso que diz csv e não o que é PDF e nem o que diz que é HTML
            for child in recurso.descendants:   
                if "Ir para recurso" in child:  # Dentro dos descendentes, quero aquele que diz "Ir para recurso"
                    url = child.parent["href"]  # Quero o link

    #Jogando o dado para um arquivo txt (para a tarefa ser finalizada)
    with open(url_file, 'w') as arquivo_saida:
        arquivo_saida.write(url)

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 2 -----------------------------------------------------------------------
# Busca a url de um arquivo e pega os dados hospedados nessa url para um arquivo target

def get_data_from_url ( url_file, target_file ):

    # Buscando os dados d arquivo da tarefa anterior
    with open(url_file) as arquivo_com_url:
        target_url = arquivo_com_url.read()

    # Buscando o arquivo csv 
    with open(target_file,"wb") as f:
        response = requests.get(target_url, stream=True)
        total_length = response.headers.get('content-length',0)

        if total_length is None: # no content length header
            f.write(response.content)
        else:
            dl = 0
            total_length = int(total_length)
            block_size = 1024

            for data in tqdm(response.iter_content(block_size), total=math.ceil(total_length//block_size) , unit='KB', unit_scale=True):
                dl = dl  + len(data)
                f.write(data)
            if total_length != 0 and dl != total_length:
                print("ERROR, something went wrong")

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 3 -----------------------------------------------------------------------
# Edita algumas coisas do arquivo
#  -> Na coluna CPF tira ponto e traço, para deixar soh os numeros
def tira_ponto_e_traco (cpf):
    return cpf.replace(".", "").replace("-", "")

def edita_base( nome_arquivo_in, nome_arquivo_out ):

    #Já que essa base é pequena, dá para ler ela toda
    base = pandas.read_csv( nome_arquivo_in, sep=";", encoding="latin-1" )

    #Arrumando o CPF
    base.CPF = base.CPF.apply(tira_ponto_e_traco)

    #Jogando no arquivo final
    base.to_csv(nome_arquivo_out, sep='\t', encoding='utf-8', index=False)

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 4 -----------------------------------------------------------------------
# Parte do banco: limpando a base e reinserindo 
def truncate_e_reinsert_to_sqlserver (nome_tabela, nome_arquivo):

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = os.environ['INIDONEOS_DATABASE']
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    #ESVAZIA A TABELA PARA SER ATUALIZADA
    deleta = "DELETE " + nome_tabela
    cursor.execute(deleta) 

    dados = pandas.read_csv(nome_arquivo,sep='\t',encoding='utf-8')

    for index,row in dados.iterrows():
        query = 'INSERT INTO {}(CPF, NOME, PROCESSO, DELIBERACAO, DATA_TRANSINT, DATA_FINAL) VALUES(?,?,?,?,?,?)'.format(nome_tabela)

        #print(query, f"CPF: {row['CPF']}, NOME: {row['Nome do Responsável']}, PROCESSO: {row['Processo ']}")
        #print(f"DELIBERACAO: {row['Deliberação']}, DATA_TRANSINT: {row['Trânsito em Julgado']}, DATA_FINAL: {row['Data Final']}")

        cursor.execute(query,row['CPF'],row['Nome do Responsável'],row['Processo '],row['Deliberação'],row['Trânsito em Julgado'], row['Data Final'])
        cnxn.commit()
    cursor.close()
    cnxn.close()

# --------------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 5 -----------------------------------------------------------------------

def delete_arquivos (lista_de_arquivos):
    for nome_arquivo in lista_de_arquivos:
        os.remove(nome_arquivo)

# --------------------------------------------------------------------------------------------------------------
# ------------------------------- AIRFLOW -----------------------------------------------------------------------

# get_url ()
# get_data_from_url ( "url_inidoneos.txt", "inidoneos.csv" )
# edita_base( "inidoneos.csv", "inidoneos_final.csv" )
# truncate_e_reinsert_to_sqlserver("Dados_Inidoneos", "inidoneos_final.csv")
# delete_arquivos( ["url_inidoneos.txt", "inidoneos.csv", "inidoneos_final.csv"] )

dag = DAG(dag_id='inidoneos', default_args=args, schedule_interval=timedelta(days=1))
url_request = 'http://dados.gov.br/dataset/inabilitados-para-funcao-publica-segundo-tcu'
url_file = 'url_inidoneos.txt'
target_file = 'inidoneos.csv'
final_file = 'inidoneos_final.csv'
tabela = 'Inidoneos.dbo.Dados_Inidoneos'

t1 = PythonOperator(
    task_id='get_url',
    python_callable=get_url,
    op_kwargs={'url_request':url_request,'url_file':os.path.join(local_downloads,url_file)},
    dag=dag)

t2 = PythonOperator(
    task_id='get_data_from_url',
    python_callable=get_data_from_url,
    op_kwargs={'url_file':os.path.join(local_downloads,url_file), 'target_file':os.path.join(local_downloads,target_file)},
    dag=dag)

t3 = PythonOperator(
    task_id='edita_base',
    python_callable=edita_base,
    op_kwargs={'nome_arquivo_in':os.path.join(local_downloads,target_file), 'nome_arquivo_out': os.path.join(local_downloads,final_file)},
    dag=dag)

t4 = PythonOperator(
    task_id='truncate_e_reinsert_to_sqlserver',
    python_callable=truncate_e_reinsert_to_sqlserver,
    op_kwargs={'nome_tabela': tabela,'nome_arquivo':os.path.join(local_downloads,final_file)},
    dag=dag)

t5 = PythonOperator(
    task_id='delete_arquivos',
    python_callable=delete_arquivos,
    op_kwargs={'lista_de_arquivos': [os.path.join(local_downloads,url_file), os.path.join(local_downloads,target_file), os.path.join(local_downloads,final_file)]},
    dag=dag)

# Ordem das operações
t1 >> t2 >> t3 >> t4 >> t5
