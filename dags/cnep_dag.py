import os
import airflow 
from airflow.models import DAG                                      # airflow
from airflow.operators.python_operator import PythonOperator        # airflow
from datetime import datetime, timedelta                            # airflow

import logging
import pandas as pd
import unicodedata
import pyodbc
import json
import requests

class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')

args = {
    'owner': 'COGIT-ME',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 7),
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

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_data(url_request,to_file):
    try:
        with open(to_file,'r') as f:
            f.close()
    except FileNotFoundError:
        r = requests.get(url_request)
        with open(to_file, 'w') as f:
            json.dump(r.json(),f)

def copy_to_sqlserver(from_file):

    with open(from_file, 'r') as f:
        data = json.load(f)
        #REALIZA CONEXÃO COM O BANCO DE DADOS
        #Parametros da conexão (conectando com o banco no latitude)
        server = f"{os.environ['LAKE_HOST']},{os.environ['LAKE_PORT']}"
        database = os.environ['CNEP_DATABASE']
        username = os.environ['LAKE_USER']
        password = os.environ['LAKE_PASS']
        
        #
        #Fazendo a conexão
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()

        #ESVAZIA AS TABELAS PARA SEREM ATUALIZADAS
        deleta = "DELETE FONTE_SANCAO"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE LEGISLACAO"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE ORGAO_SANCIONADOR"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE PESSOA"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE PESSOA_CNAE"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE PESSOA_MUNICIPIO"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE PESSOA_NATUREZA_JURIDICA"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE SANCIONADO"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE TIPO_SANCAO"
        cursor.execute(deleta)
        cnxn.commit()
        deleta = "DELETE DADOS_CNEP"
        cursor.execute(deleta)
        cnxn.commit()

        for obj in data:

            #DADOS_CNEP
            query0 ='INSERT INTO DADOS_CNEP(ID,DATA_REFERENCIA,DATA_INICIO_SANCAO,DATA_FIM_SANCAO,DATA_TRAN_JULGADO,DATA_ORIGEM_INFO,VALOR_MULTA,TEXTO_PUBLICACAO,LINK_PUBLICACAO,DETALHAMENTO_PUBLICACAO,NUMERO_PROCESSO,ABRANG_DEF_DEC_JUDICIAL,INFO_ORG_SANCIONADOR) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
            cursor.execute(query0,
            int(obj['id']),
            str(obj['dataReferencia']),
            str(obj['dataInicioSancao']),
            str(obj['dataFimSancao']),
            str(obj['dataTransitadoJulgado']),
            str(obj['dataOrigemInformacao']),
            str(obj['valorMulta']),
            str(obj['textoPublicacao']),
            str(obj['linkPublicacao']),
            str(obj['detalhamentoPublicacao']),
            str(obj['numeroProcesso']),
            str(obj['abrangenciaDefinidaDecisaoJudicial']),
            str(obj['informacoesAdicionaisDoOrgaoSancionador']))
            cnxn.commit()

            #TIPO_SANCAO
            tipo_sancao = obj['tipoSancao']
            query9 ='INSERT INTO TIPO_SANCAO(ID,DESC_RESUMIDA,DESC_PORTAL) VALUES (?,?,?)'
            cursor.execute(query9,
            int(obj['id']),
            str(tipo_sancao['descricaoResumida']),
            str(tipo_sancao['descricaoPortal']))
            cnxn.commit()

            #FONTE_SANCAO
            fonteSansao = obj['fonteSancao']
            query1 ='INSERT INTO FONTE_SANCAO(ID,NOME_EXIBICAO,TELEFONE_CONTATO,ENDERECO_CONTATO) VALUES (?,?,?,?)'
            cursor.execute(query1,
            int(obj['id']),
            str(fonteSansao['nomeExibicao']),
            str(fonteSansao['telefoneContato']),
            str(fonteSansao['enderecoContato']))
            cnxn.commit()
            
            #LEGISLACAO
            legislacao = obj['legislacao']
            query2 ='INSERT INTO LEGISLACAO(ID,FUND_LEGAL,DESC_FUND_LEGAL) VALUES (?,?,?)'
            cursor.execute(query2,
            int(obj['id']),
            str(legislacao['fundamentacaoLegal']),
            str(legislacao['descricaoFundamentacaoLegal']))
            cnxn.commit()
            
            #ORGAO_SANCIONADOR
            orgaoSancionador = obj['orgaoSancionador']
            query3 ='INSERT INTO ORGAO_SANCIONADOR(ID,NOME,SIGLA_UF,PODER) VALUES (?,?,?,?)'
            cursor.execute(query3,
            int(obj['id']),
            str(orgaoSancionador['nome']),
            str(orgaoSancionador['siglaUf']),
            str(orgaoSancionador['poder']))
            cnxn.commit()
            
            #PESSOA
            pessoa = obj['pessoa']
            pessoa_cnae = pessoa['cnae']

            print(len(str(pessoa['numeroInscricaoSocial'])))
            print(len(str(pessoa['nome'])))
            print(len(str(pessoa['razaoSocialReceita'])))
            print(len(str(pessoa['nomeFantasiaReceita'])))
            print(len(str(pessoa_cnae['codigoSecao'])))
            print(len(str(pessoa_cnae['secao'])))
            print(len(str(pessoa_cnae['codigoSubclasse'])))
            print(len(str(pessoa_cnae['subclasse'])))
            print(len(str(pessoa_cnae['codigoDivisao'])))
            print(len(str(pessoa['localidadePessoa'])))
            print(len(str(pessoa['dataAbertura'])))
            print(len(str(pessoa['enderecoEletronico'])))
            print(len(str(pessoa['numeroTelefone'])))
            print(len(str(pessoa['descricaoLogradouro'])))
            print(len(str(pessoa['numeroEndereco'])))
            print(len(str(pessoa['complementoEndereco'])))
            print(len(str(pessoa['numeroCEP'])))
            print(len(str(pessoa['nomeBairro'])))
            print(len(str(pessoa['codigoFormatado'])))
            print(len(str(pessoa['tipoCodigo'])))
            print(len(str(pessoa['tipoPessoa'])))

            query4 ='INSERT INTO PESSOA(ID,NUMERO_INSC_SOCIAL,NOME,RAZAO_SOCIAL_RECEITA,NOME_FANTASIA,CODIGO_SECAO,SECAO,COD_SUB_CLASSE,SUB_CLASSE,COD_DIVISAO,LOCALIDADE,DATA_ABERTURA,END_ELETRONICO,NUMERO_TELEFONE,DESC_LOGRADOURO,NUMERO_ENDERECO,COMPLEMENTO_ENDERECO,NUMERO_CEP,NOME_BAIRRO,COD_FOMATADO,TIPO_CODIGO,TIPO_PESSOA) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            cursor.execute(query4,
            int(obj['id']),
            str(pessoa['numeroInscricaoSocial']),
            str(pessoa['nome']),
            str(pessoa['razaoSocialReceita']),
            str(pessoa['nomeFantasiaReceita']),
            str(pessoa_cnae['codigoSecao']),
            str(pessoa_cnae['secao']),
            str(pessoa_cnae['codigoSubclasse']),
            str(pessoa_cnae['subclasse']),
            str(pessoa_cnae['codigoDivisao']),
            str(pessoa['localidadePessoa']),
            str(pessoa['dataAbertura']),
            str(pessoa['enderecoEletronico']),
            str(pessoa['numeroTelefone']),
            str(pessoa['descricaoLogradouro']),
            str(pessoa['numeroEndereco']),
            str(pessoa['complementoEndereco']),
            str(pessoa['numeroCEP']),
            str(pessoa['nomeBairro']),
            str(pessoa['codigoFormatado']),
            str(pessoa['tipoCodigo']),
            str(pessoa['tipoPessoa']))
            cnxn.commit()

            #PESSOA_CNAE
            query5 ='INSERT INTO PESSOA_CNAE(ID,NUMERO_INSC_SOCIAL,DIVISAO,COD_GRUPO,GRUPO,COD_CLASSE,CLASSE) VALUES (?,?,?,?,?,?,?)'
            cursor.execute(query5,
            int(obj['id']),
            str(pessoa['numeroInscricaoSocial']),
            str(pessoa_cnae['divisao']),
            str(pessoa_cnae['codigoGrupo']),
            str(pessoa_cnae['grupo']),
            str(pessoa_cnae['codigoClasse']),
            str(pessoa_cnae['classe']))
            cnxn.commit()
            
            #PESSOA_MUNICIPIO
            pessoa_municipio = pessoa['municipio']
            pessoa_municipio_uf = pessoa_municipio['uf']
            query6 ='INSERT INTO PESSOA_MUNICIPIO(ID,NUMERO_INSC_SOCIAL,COD_IBGE,NOME_IBGE,PAIS,SIGLA_UF,NOME_UF) VALUES (?,?,?,?,?,?,?)'
            cursor.execute(query6,
            int(obj['id']),
            str(pessoa['numeroInscricaoSocial']),
            int(pessoa_municipio['codigoIBGE']),
            str(pessoa_municipio['nomeIBGE']),
            str(pessoa_municipio['pais']),
            str(pessoa_municipio_uf['sigla']),
            str(pessoa_municipio_uf['nome']))
            cnxn.commit()
            
            #PESSOA_NATUREZA_JURIDICA
            pessoa_natureza_juridica = pessoa['naturezaJuridica']
            query7 ='INSERT INTO PESSOA_NATUREZA_JURIDICA(ID,NUMERO_INSC_SOCIAL,CODIGO,DESCRICAO,COD_TIPO,DESC_TIPO) VALUES (?,?,?,?,?,?)'
            cursor.execute(query7,
            int(obj['id']),
            str(pessoa['numeroInscricaoSocial']),
            str(pessoa_natureza_juridica['codigo']),
            str(pessoa_natureza_juridica['descricao']),
            str(pessoa_natureza_juridica['codigoTipo']),
            str(pessoa_natureza_juridica['descricaoTipo']))
            cnxn.commit()
            
            #SANCIONADO
            #ID|NOME|COD_FORMATADO|
            sancionado = obj['sancionado']
            query8 ='INSERT INTO SANCIONADO(ID,NOME,COD_FORMATADO) VALUES (?,?,?)'
            cursor.execute(query8,
            int(obj['id']),
            str(sancionado['nome']),
            str(sancionado['codigoFormatado']))
            cnxn.commit()

        cursor.close()    
        cnxn.close()

def delete_file (file_name):
    os.remove(file_name) 

dag = DAG('cnep', default_args=args, schedule_interval=timedelta(days=1))
url_request: str = 'http://www.transparencia.gov.br/api-de-dados/cnep'
json_file: str = 'cnep.json'

t1 = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    op_kwargs={'url_request':url_request,'to_file':os.path.join(local_downloads,json_file)},
    dag=dag)

t2 = PythonOperator(
    task_id='copy_to_sqlserver',
    python_callable=copy_to_sqlserver,
    op_kwargs={'from_file':os.path.join(local_downloads,json_file)},
    dag=dag)

t3 = PythonOperator(
    task_id='delete_file',
    python_callable=delete_file,
    op_kwargs={'file_name': os.path.join(local_downloads,json_file)},
    dag=dag)

t1 >> t2 >> t3
