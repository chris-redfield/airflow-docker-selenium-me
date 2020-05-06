# Essa trava é para não apagar os arquivos baixados. Utilizamos para testes
# para evitar que os dados sejam apagados e necessitar esperar dias baixando os dados do site da receita
trava_de_seguranca = True

from airflow import DAG                                             # airflow
from airflow.operators.python_operator import PythonOperator        # airflow
from datetime import datetime, timedelta                            # airflow
from airflow.utils.dates import days_ago
import time

# Para tratamento de arquivos
import glob
import os

# Tarefa 1 - Download de arquivos
from tqdm import tqdm
from math import ceil
import requests

# Tarefa 2 - Extração dos arquivos
import zipfile

# Tarefa 4 - Inserção dos arquivos no banco
import pyodbc
import multiprocessing # talvez dẽ para estender o paralelismo à outras etapas, mas no momento só a 4 tem isso

args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_downloads = os.path.join(os.getcwd(), 'downloads')

def copy_to_sql_server_cnpj (nome_arquivo):

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = "CNPJ_RFB"
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    nome_tabela = 'CNPJ_RFB.dbo.DADOS_RECEITA_CNPJ'

    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()


    query = f'INSERT INTO {nome_tabela} (CNPJ,IDENTIFICADOR_MATRIZ_FILIAL,RAZAO_SOCIAL,NOME_FANTASIA,SITUACAO_CADASTRAL,DATA_SITUACAO_CADASTRAL,MOTIVO_SITUACAO_CADASTRAL,NOME_CIDADE_EXTERIOR,COD_PAIS,NOME_PAIS,COD_NATUREZA_JURIDICA,DATA_INICIO_ATIVIDADE,CNAE_FISCAL,DESCRICAO_LOGRADOURO,LOGRADOURO,NUMERO,COMPLEMENTO,BAIRRO,CEP,UF,COD_MUNICIPIO,MUNICIPIO,TELEFONE1,TELEFONE2,FAX,EMAIL,QUALIFICACAO_RESPONSAVEL,CAPITAL_SOCIAL,PORTE_EMPRESA,OPCAO_SIMPLES,DATA_OPCAO_SIMPLES,DATA_EXCLUSAO_SIMPLES,OPCAO_MEI,SITUACAO_ESPECIAL,DATA_SITUACAO_ESPECIAL) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'.format(nome_tabela)
    
    print (f" ** Comecando o arquivo {nome_arquivo}")
    with open(nome_arquivo) as arq:
        contador = 0
        contador_problemas = 0
        for registro in arq: 
            row = registro.replace("\n", "").split("\t")

            contador += 1
            if contador % 100000 == 0: print (f" ** [{nome_arquivo}] Processados {contador/1000000}M de registros com {contador_problemas} registros falhados")

            try:
                cursor.execute(query,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                    row[15],
                    row[16],
                    row[17],
                    row[18],
                    row[19],
                    row[20],
                    row[21],
                    row[22],
                    row[23],
                    row[24],
                    row[25],
                    row[26],
                    row[27],
                    row[28],
                    row[29],
                    row[30],
                    row[31],
                    row[32],
                    row[33],
                    row[34])
                cnxn.commit()
            except:
                contador_problemas += 1
                with open('dados_problematicos_cnpj.txt', 'a') as dados_erro:
                    dados_erro.write(registro)

    print (f" *** [FIM] [{nome_arquivo}] Processados {contador/1000000}M de registros com {contador_problemas} registros falhados")

    #Fechando a conexão
    cursor.close()
    cnxn.close()

def copy_to_sql_server_socios (nome_arquivo):

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = "CNPJ_RFB"
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']

    nome_tabela = 'CNPJ_RFB.dbo.DADOS_RECEITA_SOCIOS'
    
    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    query = 'INSERT INTO {}(CNPJ,ID_SOCIO,NOME_SOCIO,CNPJ_CPF_SOCIO,COD_QUALIFICACAO_SOCIO,PERCENTUAL_CAP_SOCIAL,DATA_ENTRADA_SOCIEDADE,COD_PAIS,NOME_PAIS_SOCIO,CPF_REPRESENTANTE_LEGAL,NOME_REPRESENTANTE_LEGAL,COD_QUALIFICACAO_REP_LEGAL) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'.format(nome_tabela)

    print (f" ** Comecando o arquivo {nome_arquivo}")
    with open(nome_arquivo) as arq:
        contador = 0
        contador_problemas = 0
        for registro in arq: 
            row = registro.replace("\n", "").split("\t")

            contador += 1
            if contador % 100000 == 0: print (f" ** [{nome_arquivo}] Processados {contador/1000000}M de registros com {contador_problemas} registros falhados")

            try:
                cursor.execute(query,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11])
                cnxn.commit()
            except:
                contador_problemas += 1
                with open(os.path.join(local_downloads,'dados_problematicos_socios.txt'), 'a') as dados_erro:
                    dados_erro.write(registro)

    print (f" *** [FIM] [{nome_arquivo}] Processados {contador/1000000}M de registros com {contador_problemas} registros falhados")

    #Fechando a conexão
    cnxn.commit()
    cursor.close()
    cnxn.close()

def limpa_tabela (nome_tabela):

    server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
    database = "CNPJ_RFB"
    username = os.environ['LAKE_USER']
    password = os.environ['LAKE_PASS']
    
    #Fazendo a conexão
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    #ESVAZIA A TABELA PARA SER ATUALIZADA
    print (f'executa a query: TRUNCATE TABLE {nome_tabela}') 
    deleta = "TRUNCATE TABLE " + nome_tabela
    cursor.execute(deleta) 

    #Fechando a conexão
    cnxn.commit()
    cursor.close()
    cnxn.close()

def deleteAll():
    # Para agilizar os testes, evito apagar os dados
    # Assim, esse return deve ser removido ao rodar o serviço do começo ao fim
    if trava_de_seguranca:
        return

    for filename in glob.glob(os.path.join(local_downloads,'K*')):
        os.remove(filename)
    for filename in glob.glob(os.path.join(local_downloads,'DADOS_ABERTOS_CNPJ_*')):
        os.remove(filename)
    os.remove(os.path.join(local_downloads,'zip_files.tmp'))
    os.remove(os.path.join(local_downloads,'number_files.tmp'))


def uploader():

    # Limpando os arquivos de dados problematicos
    # Abro para escrita (não apend), não salvo nada e fecho
    with open(os.path.join(local_downloads,'dados_problematicos_cnpj.txt'), 'w') as a1 , open(os.path.join(local_downloads,'dados_problematicos_socios.txt'), 'w') as a2:
        print("Limpando os arquivos de dados com problema")

    limpa_tabela ('CNPJ_RFB.dbo.DADOS_RECEITA_CNPJ')
    limpa_tabela ('CNPJ_RFB.dbo.DADOS_RECEITA_SOCIOS')

    # Paralelizando os arquivos distintos de sócios
    pool = multiprocessing.Pool()
    pool.map(copy_to_sql_server_socios, glob.glob(os.path.join(local_downloads,'K*_socios.txt')))
    pool.close()

    # Paralelizando os arquivos distintos de cnpjs
    pool = multiprocessing.Pool()
    pool.map(copy_to_sql_server_cnpj, glob.glob(os.path.join(local_downloads,'K*_cnpj.txt')))
    pool.close()


def analyze_data():

    try:
        with open(os.path.join(local_downloads,'last_extract_file.tmp'),'r') as f:
            last_extract_file = f.readline()
            print('last_extraxt_file',last_extract_file)
            f.close()
        filename_last_file_extract = last_extract_file + '_cnpj.txt'
        with open(os.path.join(local_downloads,filename_last_file_extract),'r') as f:
            print('found',last_extract_file+'_cnpj.txt')
            f.close()
            return
    except:
        print('not found',filename_last_file_extract)
        for filename in glob.glob(os.path.join(local_downloads,'K*')):
            file_out_cnpj = open(os.path.join(local_downloads,filename+'_cnpj.txt'),'w')
            file_out_socios = open(os.path.join(local_downloads,filename+'_socios.txt'),'w')
            with open(filename,'r',encoding='ISO-8859-1') as file_in:
                for line in file_in:
                    vet_reg = []
                    if line[0] == '1':
                        vet_reg.append((''.join (c for c in line[3:17]).strip())) #cnpj
                        vet_reg.append((''.join (c for c in line[17:18]).strip())) #matriz filial
                        vet_reg.append((''.join (c for c in line[18:168]).strip())) #razao social
                        vet_reg.append((''.join (c for c in line[168:223]).strip())) #nome fantasia
                        vet_reg.append((''.join (c for c in line[223:225]).strip())) #sit cadastral
                        vet_reg.append((''.join (c for c in line[225:233]).strip())) #data sit cadastral
                        vet_reg.append((''.join (c for c in line[233:235]).strip())) #motivo sit cadastral
                        vet_reg.append((''.join (c for c in line[235:290]).strip())) #nome cidade exterior
                        vet_reg.append((''.join (c for c in line[290:293]).strip())) #cod pais
                        vet_reg.append((''.join (c for c in line[293:363]).strip())) #nome pais
                        vet_reg.append((''.join (c for c in line[363:367]).strip())) #cod nat jur
                        vet_reg.append((''.join (c for c in line[367:375]).strip())) #data inicio atividade
                        vet_reg.append((''.join (c for c in line[375:382]).strip())) #cnae fiscal
                        vet_reg.append((''.join (c for c in line[382:402]).strip())) #desc logradouro
                        vet_reg.append((''.join (c for c in line[402:462]).strip())) #logradouro
                        if (''.join (c for c in line[462:468]).strip()) == "S/N":
                            vet_reg.append('') #numero
                        else:
                            vet_reg.append((''.join (c for c in line[462:468]).strip()))
                        vet_reg.append((''.join (c for c in line[468:624]).strip())) #complemento
                        vet_reg.append((''.join (c for c in line[624:674]).strip())) #bairro
                        vet_reg.append((''.join (c for c in line[674:682]).strip())) #cep
                        vet_reg.append((''.join (c for c in line[682:684]).strip())) #uf
                        vet_reg.append((''.join (c for c in line[684:688]).strip())) #cod municipio
                        vet_reg.append((''.join (c for c in line[688:732]).strip())) #municipio
                        vet_reg.append((''.join (c for c in line[738:750]).strip())) #telefone 1
                        vet_reg.append((''.join (c for c in line[750:762]).strip())) #telefone 2
                        vet_reg.append((''.join (c for c in line[762:774]).strip())) #fax
                        vet_reg.append((''.join (c for c in line[774:889]).strip())) #email
                        vet_reg.append((''.join (c for c in line[889:891]).strip())) #qualificacao responsavel
                        vet_reg.append((''.join (c for c in line[891:905]).strip())) #capital social
                        vet_reg.append((''.join (c for c in line[905:907]).strip())) #porte empresa
                        vet_reg.append((''.join (c for c in line[907:908]).strip())) #opcao simples
                        vet_reg.append((''.join (c for c in line[908:916]).strip())) #data opcao simples
                        vet_reg.append((''.join (c for c in line[916:924]).strip())) #data exclusao simples
                        vet_reg.append((''.join (c for c in line[924:925]).strip())) #opcao mei
                        vet_reg.append((''.join (c for c in line[925:948]).strip())) #sit especial
                        vet_reg.append((''.join (c for c in line[948:956]).strip())) #data sit especial
                        file_out_cnpj.write('\t'.join(campo for campo in vet_reg)+'\n')
                    elif line[0] == '2':
                        vet_reg.append((''.join (c for c in line[3:17]).strip())) #cnpj
                        vet_reg.append((''.join (c for c in line[17:18]).strip())) #id socio
                        vet_reg.append((''.join (c for c in line[18:168]).strip())) #nome socio
                        vet_reg.append((''.join (c for c in line[168:182]).strip())) #cnpj cpf socio
                        vet_reg.append((''.join (c for c in line[182:184]).strip())) #cod qualificacao socio
                        vet_reg.append((''.join (c for c in line[184:189]).strip())) #perc capital social
                        vet_reg.append((''.join (c for c in line[189:197]).strip())) #data entr sociedade
                        vet_reg.append((''.join (c for c in line[197:200]).strip())) #cod pais
                        vet_reg.append((''.join (c for c in line[200:270]).strip())) #nome pais socio
                        vet_reg.append((''.join (c for c in line[270:281]).strip())) #cpf representante legal
                        vet_reg.append((''.join (c for c in line[281:341]).strip())) #nome representante
                        vet_reg.append((''.join (c for c in line[341:343]).strip())) #cod qualificacao representante legal
                        file_out_socios.write('\t'.join(campo for campo in vet_reg)+'\n')
                    else:
                        None
            file_in.close()
            file_out_cnpj.close()
            file_out_socios.close()
            #os.remove(filename)

def extract_data():
    with open(os.path.join(local_downloads,'number_files.tmp'),'r') as final_number:
        cod = int(final_number.readline())
    final_number.close()
    try:
        with open(os.path.join(local_downloads,'zip_files.tmp'),'r') as zip_final_number:
            init = int(zip_final_number.readline())
        zip_final_number.close()
    except FileNotFoundError:
        init = 1
        
    for i in range(init,cod):
        filename = 'DADOS_ABERTOS_CNPJ_'+str(i).zfill(2)+'.zip'
        file_in = zipfile.ZipFile(os.path.join(local_downloads,filename), mode='r')
        file_txt = file_in.namelist()[0] 
        file_in.extract(file_txt,local_downloads)
        #os.remove(os.path.join(local_downloads,filename))
        with open(os.path.join(local_downloads,'last_extract_file.tmp'),'w') as f:
            f.write(file_txt)
        with open(os.path.join(local_downloads,'zip_files.tmp'),'w+') as final_number:
            final_number.write(str(i))
        final_number.close()

def get_data_from_url ( target_url, target_file ):
    # Buscando o arquivo csv
    with open(os.path.join(local_downloads,target_file),"wb") as f:
        response = requests.get(target_url, stream=True)
        total_length = response.headers.get('content-length',0)
        if int(total_length) <= 1024 : return 1

        if total_length is None: # no content length header
            f.write(response.content)
        else:
            dl = 0
            total_length = int(total_length)
            block_size = 1024

            for data in tqdm(response.iter_content(block_size), total=ceil(total_length//block_size) , unit='KB', unit_scale=True, mininterval=20):
                dl = dl  + len(data)
                f.write(data)
            if total_length != 0 and dl != total_length:
                return 2
        return 0

def downloader():
    print ('INICIO download')
    try:
        with open(os.path.join(local_downloads,'number_files.tmp'),'r') as final_number:
            cod = int(final_number.readline())
        final_number.close()
    except FileNotFoundError:
        cod = 20
    retry = [False,0] #retry flag, contador
    sleep_time = 600
    while True:
        print("Download arquivo nº", cod)
        mascara_link = 'http://200.152.38.155/CNPJ/DADOS_ABERTOS_CNPJ_'+str(cod).zfill(2)+'.zip'
        mascara_file = 'DADOS_ABERTOS_CNPJ_'+str(cod).zfill(2)+'.zip'
        print (mascara_link,'...')
        status = get_data_from_url(mascara_link, mascara_file)
        if status == 0:
            retry = [False,0]
        elif status == 1:
                if cod == 1:
                    print ('Portal de Dados Abertos indisponível.')
                    raise ConnectionRefusedError
                    return
                else:
                    print ('Download concluido com sucesso.')
                    #os.remove(os.path.join(local_downloads,'DADOS_ABERTOS_CNPJ_'+str(cod).zfill(2)+'.zip'))
                    return
                    
        else: #status = 2
            retry = [True,retry[1]+1]

        if retry[0] == False:
            cod += 1
            with open(os.path.join(local_downloads,'number_files.tmp'),'w+') as final_number:
                final_number.write(str(cod))
            final_number.close()
        else:
            if retry[1] <= 3:
                print('Atenção: problema no download do arquivo: ', mascara_link)
                print('Tentando novamente em ',str(sleep_time),'s.')
                time.sleep(sleep_time)
            else:
                print('Atenção: problema no download do arquivo: ', mascara_link)
                raise ConnectionRefusedError
                return
    print ('FIM download')
    return

# Processo: DADOS_RECEITA
dag = DAG('cnpj', default_args=args, schedule_interval=timedelta(days=90))

t0 = PythonOperator(
    task_id='downloader',
    python_callable=downloader,
    op_kwargs={},
    dag=dag)

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={},
    dag=dag)

t2 = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    op_kwargs={},
    dag=dag)

t3 = PythonOperator(
    task_id='uploader',
    python_callable=uploader,
    op_kwargs={},
    dag=dag)

t4 = PythonOperator(
    task_id='deleteAll',
    python_callable=deleteAll,
    op_kwargs={},
    dag=dag)

# Ordem das operações
t0 >> t1 >> t2 >> t3 >> t4
