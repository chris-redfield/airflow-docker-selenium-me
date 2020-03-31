import os 										# Para avisar para o selenium aonde é para jogar os arquivos
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.python_operator import PythonOperator
from selenium_scripts.sancoesMG import get_url
from datetime import datetime, timedelta
import logging

import PyPDF2 														# Para abrir o pdf
import re
import pyodbc
import pandas															# Para processamento de texto

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

local_downloads = os.path.join(os.getcwd(), 'downloads')

# ----------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 2 -------------------------------------------------------------------
# Transformar pdf em um csv

# Essas três funções auxiliares são coisas que eu arrumo no texto para transformar em csv, pois o texto saido do pdf
# não tem marcas de separação de registro ou de campo.
#   * O primeiro vai ser usado com o cpf identificado e bota um "__endline__" antes (marca de fim de registro) e um "__split__"
#  (fim de campo) depois
#   * O segundo vai ser usado com a penalidade e vai colocar separações de campo antes e depois da mesma
#   * O terceiro vai ser usado com a data e coloca separação só depois da mesma
# PS: A razão pela qual eu não uso \t e \n é para evitar a possibilidade de o conteúdo da mensagem incluir \t e \n.
# Se já é improvavel \t e \n então __split__ e __newline__ são quase impossíveis
def fix_first_member ( match_object ):
	return "__endline__" + match_object.group(1) + "__split__"

def fix_penalidade ( match_object ):
	return "__split__" + match_object.group(1) + "__split__"

def fix_data ( match_object ):
	return match_object.group(1) + "__split__"

def pdf_to_csv(pdf_file,to_file):

	# Criando o leitor de pdf
	pdfReader = PyPDF2.PdfFileReader(open(os.path.join(local_downloads,pdf_file), 'rb')) 

	# Tipos de penalidade conhecidas para poder testar uma a uma se o registro contem uma dessas
	tipos_penalidade = ["Inidoneidade",
						"Suspensão por até 2 anos",
						"Suspensão por até 5 anos",
						"Suspensão por DecisãoJudicial",
						"Suspensão por Decisão"]

	# Estrutura para guardar os registros prontos para depois jogar tudo em um arquivo csv
	registros_finalizados = []

	# Para cada página
	for i in range(0,pdfReader.numPages):
		texto_pagina = pdfReader.getPage(i).extractText()

		# Em todas as páginas tem que tirar o footer de emissão
		texto_pagina = re.sub("www.cagef.*", "", texto_pagina)

		# Se aparecer o header da página tira tambem
		texto_pagina = texto_pagina.replace("GOVERNO DO ESTADO DE MINAS GERAISSECRETARIA DE ESTADO DE PLANEJAMENTO DE GESTÃOSistema Integrado de Administração de Materiais e Serviços - SIADRelatórios de Fornecedores Impedidos", "")
		
		# Colocando as marcações para quebrar as linhas pelos CPFs e CNPJs
		temp = re.sub("([0-9]{2}\.[0-9]{3}\.[0-9]{3}\/[0-9]{4}\-[0-9]{2})", fix_first_member, texto_pagina)
		texto_marcado = re.sub("([0-9]{3}\.[0-9]{3}\.[0-9]{3}\-[0-9]{2})", fix_first_member, temp)

		# Quebrando as linhas
		registros_pagina = texto_marcado.split("__endline__")

		# Removendo o primeiro elemento (tudo que veio antes do primeiro CPF/CNPJ)
		# Na primeira página é o cabeçalho e em páginas subsequentes é algum resto da linha de cima
		#     -> Pode ser parte do nome da empresa
		#     -> Pode ser parte do tipo de penalidade
		#     -> Pode ser parte do nome do órgão que aplicou a multa
		# 
		# Caso seja informação importante:
		# 	-> Nome da empresa (vou deixar sem o final mesmo, depois que se acerte com a base de CNPJ da receita)
		# 	-> Tipo de penalidade: soh fazer se "Suspensão por Decisão" -> Suspensão por Decisão Judicial
		# 	-> Nome do órgão: fazer um dicionario de correções para órgãos com nome comprido
		registros_pagina.pop(0)

		for indice_registro in range(0, len(registros_pagina)):

			# Copio uma versão temporaria aqui para poder editar sem medo
			registro_temp = registros_pagina[indice_registro]
			
			# Tento para cada penalidade encontrar ela no registro e adicionar um split antes dela, caso encontre
			for penalidade in tipos_penalidade:
				if penalidade in registro_temp:
					registro_temp = re.sub("(" + penalidade + ")", fix_penalidade, registro_temp)
					break
			
			# Coloco um separador de campos entre datas
			registro_temp = re.sub( "([0-9]{2}\/[0-9]{2}\/[0-9]{4})", fix_data, registro_temp )

			#######
			# Agora que já separei todos os campos, posso de fato quebrar a linha em seus campos
			dados = registro_temp.split("__split__")

			#######
			# Arrumando os casos de inforação faltante
			
			# Se tem 6 dados, é por que a data de fim de vigência não existe. Nesse caso vou adicionar um elemento vazio
			if len(dados) == 6:
				dados.insert(4, "")

			# Se tem 5 dados é por que não tem fim de vigencia e nem orgao
			elif len(dados) == 5:
				dados.insert(4, "")
				dados.insert(6, "")

			#######
			# Editando as variáveis
			
			# Separando CPF e CNPJ em colunas diferentes
			# Assim vou procurar se a primeira coisa é CPF. Se for eu adicino um espaço vazio depois (CNPJ vazio)
			# e se não for adiciono antes (CPF vazio e CNPJ preenchido)
			if re.match("([0-9]{2}\.[0-9]{3}\.[0-9]{3}\/[0-9]{4}\-[0-9]{2})", dados[0]):
				dados.insert(0, "")
			else:
				dados.insert(1, "")

			# Ajustando o cpf para ser só números:
			dados[0] = dados[0].replace("-", "").replace(".", "")

			# Ajustando cnpj para ser só números 
			dados[1] = dados[1].replace(".", "").replace("/", "").replace("-", "")

			# Ajustando os nomes das punições
			if dados[3] in ["Suspensão por DecisãoJudicial", "Suspensão por Decisão"]:
				dados[3] =  "Suspensão por Decisão Judicial"

			# Pronto, joga na pilha de registros prontos
			registros_finalizados.append(dados)
			
	# Escrevendo os dados em um arquivo de saida
	with open(to_file, 'w') as arq:
		arq.write('"cpf"\t"cnpj"\t"Nome"\t"TipoPenalidade"\t"DataInicioVigenciaPenalidade"\t"FimVigenciaPenalidade"\t"DataDespacho"\t"OrgaoAplicador"\n')
		for registro in registros_finalizados:
			arq.write( "\t".join('"' + str(x) + '"' for x in registro) + "\n")
	  
# ----------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 3 -------------------------------------------------------------------
# Importar dados no banco de dados
def truncate_e_reinsert_to_sqlserver (nome_tabela, nome_arquivo):

    #Parametros da conexão (conectando com o banco no latitude)
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
        cab = arquivo.readline().replace('"',"") # primeira linha eh o cabecalho
        cab = cab.strip().split("\t")            # processando o cabecalho
        dados = arquivo.read()                   # o resto das linhas são dados (o processamento fica para o iterador linha a linha)

    # Copiando para o banco
    query = 'INSERT INTO {}(CPF, CNPJ, NOME, TIPO_PENALIDADE, DATA_INICIO_PENALIDADE, DATA_FIM_PENALIDADE, DATA_DESPACHO, ORGAO_APLICADOR) VALUES(?,?,?,?,?,?,?,?)'.format(nome_tabela)
    for observacao in dados.strip().split("\n"):
        row = observacao.replace('"', "").split("\t")
        print(query,row[cab.index('cpf')],row[cab.index('cnpj')],row[cab.index('Nome')],row[cab.index('TipoPenalidade')],row[cab.index('DataInicioVigenciaPenalidade')], row[cab.index('FimVigenciaPenalidade')], row[cab.index('DataDespacho')],row[cab.index('OrgaoAplicador')])
        cursor.execute(query,row[cab.index('cpf')],row[cab.index('cnpj')],row[cab.index('Nome')],row[cab.index('TipoPenalidade')],row[cab.index('DataInicioVigenciaPenalidade')], row[cab.index('FimVigenciaPenalidade')], row[cab.index('DataDespacho')],row[cab.index('OrgaoAplicador')])
        cnxn.commit()

# ----------------------------------------------------------------------------------------------------------
# ----------------------------- TAREFA 4 -------------------------------------------------------------------
# Apagar os arquivos intermediarios
def apagar_arquivos(lista_arquivos):
	for arquivo in lista_arquivos:
		os.remove(arquivo)

# ----------------------------------------------------------------------------------------------------------
# ------------------------------- AIRFLOW ------------------------------------------------------------------
dag = DAG(dag_id='sancoes_mg', default_args=args, schedule_interval=timedelta(days=1))
url_request = "https://www.cagef.mg.gov.br/fornecedor-web/br/gov/prodemge/seplag/fornecedor/publico/index.zul"
tabela = 'Inidoneos.dbo.Dados_SancaoMG'
file_name = 'sancoes_mg_dados.txt'
pdf_file = 'Relatorio_Fornecedores_Impedidos.pdf'

t1 = SeleniumOperator(
    task_id='get_url',
    script=get_url,
    script_args=[url_request],
    dag=dag)

t2 = PythonOperator(
    task_id='pdf_to_csv',
    python_callable=pdf_to_csv,
    op_kwargs={'pdf_file':os.path.join(local_downloads,pdf_file),'to_file':os.path.join(local_downloads,file_name)},
    dag=dag)

t3 = PythonOperator(
    task_id='truncate_e_reinsert_to_sqlserver',
    python_callable=truncate_e_reinsert_to_sqlserver,
    op_kwargs={'nome_tabela':tabela, 'nome_arquivo':os.path.join(local_downloads,file_name)},
    dag=dag)

t4 = PythonOperator(
    task_id='apagar_arquivos',
    python_callable=apagar_arquivos,
    op_kwargs={'lista_arquivos':[os.path.join(local_downloads,pdf_file),os.path.join(local_downloads,file_name)]},
    dag=dag)

t1 >> t2 >> t3 >> t4
