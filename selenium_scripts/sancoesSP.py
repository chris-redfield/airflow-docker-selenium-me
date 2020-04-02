from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from time import sleep
import re

# Para tratar os arquivos baixados
import os
import glob

def get_url(driver, pasta_download, pasta_destino, dicionario_bases):

    # Opera uma base de cada vez (sancoes, multas e advertencias)
    for (nome_base, base) in dicionario_bases.items():

        # Garante que a pasta destino existe
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)

        # Verifica se tem downloads prontos. Se tiver, comecaremos a partir do ultimo download concluido
        # Se não tiver, vamos comecar do comeco, da 1a pagina e 1o forcenedor
        lista_arquivos_baixados = glob.glob( f"{pasta_destino}/{base['tipo']}_*")
        if len(lista_arquivos_baixados) > 1:
            
            # Garantindo que não vai ter problema de ordenação com dados baixados em uma versão passada do código
            lista_arquivos_baixados = [ re.sub('_([0-9])_', '_00\g<1>_', k ) for k in lista_arquivos_baixados]    # troca _1_ por _001_
            lista_arquivos_baixados = [ re.sub('_([0-9][0-9])_', '_0\g<1>_', k ) for k in lista_arquivos_baixados] # troca _10_ por _010_
            lista_arquivos_baixados = [ re.sub('_([0-9].xls)',  '_0\g<1>', k ) for k in lista_arquivos_baixados]  # troca _1.xls por _01.xls
            
            # Pega o ultimo arquivo da lista
            ultimo = sorted( lista_arquivos_baixados )[-1]

            # aproveita que o nome eh do tipo <<"./SançõesRestritivas_51_01.xls">> e tira a terminacao e separa pelos _
            print ("Ultimo download: " + ultimo)
            pagina     = int(ultimo.replace(".xls", "").split("_")[1])
            fornecedor = int(ultimo.replace(".xls", "").split("_")[2])
        else:
            pagina     = 1
            fornecedor = 1

        # Descobre a ultima pagina, para saber a condicao de saida
        pagina_final = descobre_pagina_final(driver)
        
        # Indo na pagina base
        driver.get('https://www.bec.sp.gov.br/Sancoes_ui/aspx/ConsultaAdministrativaFornecedor.aspx')

        # Clicando em "Exibir todos". O botao nao tem o nome nas propriedades do FirefoxWebElement entao estou usando o indice do mesmo em opcoes_navegacao[]
        sleep(4)
        opcoes_navegacao = driver.find_elements_by_class_name('button')
        opcoes_navegacao[1].click()
        esperaCarregar(driver)

        print (f"Retomando a base {nome_base}, da pagina {pagina}/{pagina_final}, fornecedor {fornecedor}")

        # Inicio do loop de download
        while pagina <= pagina_final:

            # Indo para a pagina de interesse
            if pagina > 1:
                print(f"Carregando pagina {pagina}")
                driver.execute_script(f"javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm','Page${pagina}')")
                esperaCarregar(driver)

            # A ultima pagina tem numero variavel de fornecedores (pode não completar 15)
            # então precisa saber certinho para parar no momento certo
            qtd_fornecedores = 15 if pagina < pagina_final else driver.execute_script('return $(".gridview > tbody > tr").length -2')

            for index in range(fornecedor, qtd_fornecedores+1):

                # Tem uma pegadinha, o primeiro link é o n=2 e o 15º é o n=16 
                n = str(index+1).zfill(2)

                driver.execute_script(f"javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm$ctl{n}$linkSelecionar','')")
                esperaCarregar(driver)

                # Entrando na aba das multas/sancoes/advertencias
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, base['id_tipo'])))
                botao_pagina = driver.find_element_by_id(base['id_tipo'])
                botao_pagina.click()
                esperaCarregar(driver)

                # Baixando os arquivos excel
                opcoes_navegacao = driver.find_elements_by_class_name('button')
                actions = ActionChains(driver)
                actions.move_to_element(opcoes_navegacao[3]).click().perform() # opcoes_navegacao[3] é "Exportar para Excel"
                esperaCarregar(driver)

                # Renomeando o arquivo baixado
                nome_original = os.path.join( pasta_download, base['tipo'] +                                                          ".xls")
                nome_novo     = os.path.join( pasta_destino,  base['tipo'] + "_" + str(pagina).zfill(3) + "_" + str(index).zfill(2) + ".xls")
                os.rename(nome_original, nome_novo)

                # Atualiza a informação do último que teve sucesso
                fornecedor = index

                # Retornando a tela anterior para ir pro proximo fornecedor
                # opcoes_navegacao[1] é "Exibir Todos"
                opcoes_navegacao = driver.find_elements_by_class_name('button')
                opcoes_navegacao[1].click()

            # Atualiza os contadores
            pagina += 1
            fornecedor = 1
        print (f'Download de {nome_base} concluido com sucesso.')

# Bom jeito de esperar a página carregar sem excesso de espera
def esperaCarregar(driver):
    sleep(2)
    loading = driver.find_element_by_id('ctl00_ContentPlaceHolder1_UpdateProgress1')
    while "block" in loading.get_attribute("style"):
        sleep(1)
        loading = driver.find_element_by_id('ctl00_ContentPlaceHolder1_UpdateProgress1')

# Vai na página de SancoesSP, vai até a última página, descobre o número e informa para o programa
def descobre_pagina_final (driver):

    # Indo na pagina
    driver.get('https://www.bec.sp.gov.br/Sancoes_ui/aspx/ConsultaAdministrativaFornecedor.aspx')

    # Clicando em "Exibir todos". O botao nao tem o nome nas propriedades do FirefoxWebElement entao estou usando o indice do mesmo em opcoes_navegacao[]
    sleep(4)
    opcoes_navegacao = driver.find_elements_by_class_name('button')
    opcoes_navegacao[1].click()
    esperaCarregar(driver)

    # Vai para a ultima pagina
    driver.execute_script("javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm','Page$Last')")
    esperaCarregar(driver)

    # Descobre o indice
    main_table = driver.find_element_by_id('ctl00_ContentPlaceHolder1_gdvConsultaAdm')
    return int(main_table.find_element_by_tag_name('span').text)

