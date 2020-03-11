import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from time import sleep
from time import ctime
from pathlib import Path

def get_url(driver, local_downloads,dict_BEC,url_request):

    print(f"Operation get_url started, retrieving: {url_request}")
    print(f"Current working dir: {local_downloads}")

    try:
        for filename in os.listdir(local_downloads+"/csv"):
            return
    except:

        for nome in ['sancoes','multas','advertencias']:
            # O objetivo dessa funcao eh garantir o download das 254 paginas mesmo no cenario de queda de conexao ou temperamentalidade do navegador/página.
            # signal eh uma resposta da get_dados com [0,0] se o download foi terminado com sucesso ou com [pagina,fornecedor] se tiver sido interrompido.
            # flow_control garante o reinicio do codigo em situacao de finalizacao abrupta do mesmo
            if Path(os.path.join(local_downloads,'sancoesSP_flow_control_'+dict_BEC[nome]['tipo']+'.tmp')).is_file() :
                flow = open(os.path.join(local_downloads,'sancoesSP_flow_control_'+dict_BEC[nome]['tipo']+'.tmp'),'r')
                signal = flow.readline().split(',')
                flow.close()
                signal = runSelenium(driver,local_downloads,int(signal[0]), int(signal[1]), int(signal[3]), nome,dict_BEC,url_request)
            else:
                # se nao houver arquivo de controle de fluxo, comece do inicio
                signal = runSelenium(driver,local_downloads,None, None, None, nome,dict_BEC,url_request)

            #broken_counter eh um contador pra o while (signal) nao durar eternamente.
            broken_counter = 0

            # esse vetor eh pra entender em que ponto que a pagina da BEC esta dando mais erros, ja que as excecoes estao em toda a extensao do codigo pra garantir a continuidade do mesmo
            vet_err = [0,0,0,0,0]

            while signal != [0,0,0,0]:
                #print ('signal : ', str(signal))
                vet_err[signal[2]] += 1 # incrementa-se a posicao relativa ao tipo de erro encontrado, devolvida pela get_dados_controller
                broken_counter +=1 #incrementa-se o contador de erro, tambem.

                print ('Tentando novamente. Pagina ', str(signal[0]), ', fornecedor ', str(signal[1]), '. Tentativa ', str(broken_counter), '. Erro no local ', str(signal[2]))
                # se caso o codigo for interrompido com erro, flow_control tem que saber onde está o erro, e que erro eh, tambem.
                with open(os.path.join(local_downloads,'sancoesSP_flow_control_'+dict_BEC[nome]['tipo']+'.tmp'),'w') as flow:
                    flow.write(','.join(str(c) for c in signal))
                sleep(15)
                signal = runSelenium(driver,local_downloads,signal[0], signal[1], signal[3], nome,dict_BEC,url_request)
                if broken_counter > 50:
                    print ('Os dados de'+dict_BEC[nome]['tipo']+' nao estao conseguindo ser importados apos a pagina ', str(signal[0]), ', fornecedor ', str(signal[1]-1), '. Tente novamente mais tarde.')
                    exit()

            print ('Download de {} concluido com sucesso. '.format(str(dict_BEC[nome]['tipo'])), ctime())
            #print ('Estatisticas de tipo de erro na execucao da pagina: ', str(vet_err[1:]))
            #os.remove(os.path.join(local_downloads,'sancoesSP_flow_control_'+dict_BEC[nome]['tipo']+'.tmp'))
        print('Todos os downloads concluídos! ', ctime())


def esperaCarregar(driver):
    sleep(2)
    loading = driver.find_element_by_id('ctl00_ContentPlaceHolder1_UpdateProgress1')
    while "block" in loading.get_attribute("style"):
        sleep(1)
        loading = driver.find_element_by_id('ctl00_ContentPlaceHolder1_UpdateProgress1')

def runSelenium(driver,local_downloads,pagina, fornecedor, final_index, nome,dict_BEC,url_request):

    # Indo na pagina
    driver.get(url_request)

    # Clicando em "Exibir todos". O botao nao tem o nome nas propriedades do FirefoxWebElement entao estou usando o indice do mesmo em opcoes_navegacao[]
    sleep(4)
    opcoes_navegacao = driver.find_elements_by_class_name('button')
    opcoes_navegacao[1].click()
    esperaCarregar(driver)

    #Trecho para ir ate a ultima pagina, pegar o indice e retornar a pagina inicial
    #Ultima pagina
    if final_index == None:
        esperaCarregar(driver)
        driver.execute_script("javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm','Page$Last')")
        esperaCarregar(driver)
        #Pegando o indice
        main_table = driver.find_element_by_id('ctl00_ContentPlaceHolder1_gdvConsultaAdm')
        final_index = main_table.find_element_by_tag_name('span')
        print ('Pagina final : ', final_index.text)
        final_index = int(final_index.text)
        #Retornando pra primeira
        driver.execute_script("javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm','Page$First')")
        esperaCarregar(driver)

    # Trecho para avancar as paginas no cenario em que o download foi interrompido e precisa ser recuperado
    flag_bypass = False # inicializando como false
    if pagina != None:
        # range comecando em 2 por que a pesquisa ja entra em pagina = 1, e uma pagina em um determinado indice nao pode chamar a si mesma
        # flag_bypass é pra bypassar o primeiro bloco try/except depois do for (1, final_index), pq, novamente, uma pagina em um determinado indice nao pode chamar a si mesma
        flag_bypass = True
        # vai para pagina 171
        if pagina < final_index:
            print(f"Carregando pagina {pagina}")
            driver.execute_script(f"javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm','Page${pagina}')")
            esperaCarregar(driver)
            print(f"Retomando a partir da pagina {pagina}")
        else:
            return [0,0,0,0]
                


    # Inicio do loop de download
    print ('Inicio do download: ', ctime())
    if pagina == None : pagina_bkp = 1
    else: pagina_bkp = pagina # CUIDADO NESSE TRECHO, PQ O USO DE 'PAGINA_BKP' EH APENAS PRA INICIAR O LACO FOR! (construi o codigo com um while, depois mudei pra um for, e rolou uma dificuldade no uso do contador pagina)

    for pagina in range(pagina_bkp, final_index+1):
        if pagina > 1 and flag_bypass == False:
            try: # Tente passar para o proximo indice da pagina
                
                driver.execute_script(f"javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm','Page${pagina}')")
                esperaCarregar(driver)
                print ('Download da Pagina {}'.format(str(dict_BEC[nome]['tipo'])), str(pagina), ctime())
            except Exception as ex2 : # se a pagina nao colaborar, mata e comeca denovo do ponto onde parou
                template = "Fase1 : Uma excecao do tipo {0} aconteceu. Argumentos:\n{1!r}"
                message = template.format(type(ex2).__name__, ex2.args)
                print (message)
                return [pagina, 2, 2, final_index] # ponto de saida com erro (2). Se a pagina nao conseguir ser carregada, inicie do primeiro fornecedor (indice = 2)

        if fornecedor == None : fornecedor = 2
        for index in range(fornecedor,17):
            # Entrando nos detalhes dos fornecedores listados na pagina
            try:
                
                driver.execute_script(f"javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdvConsultaAdm$ctl{str(index).zfill(2)}$linkSelecionar','')")
                # botao_pagina = driver.find_element_by_xpath('//a[@href=\"javascript:__doPostBack(\'ctl00$ContentPlaceHolder1$gdvConsultaAdm$ctl'+str(index).zfill(2)+'$linkSelecionar\',\'\')\"]')
                # botao_pagina.click()
                esperaCarregar(driver)
            except NoSuchElementException as ex3: # Aqui, senhores, temos a UNICA CONDICAO DE SAIDA DA ROTINA DE DOWNLOAD.
                if pagina == final_index: # SE O PROXIMO ELEMENTO NAO EXISTIR, E A PAGINA FOR A ULTIMA, EH POR QUE O DOWNLOAD ACABOU! VIVA!
                    print ('Download concluido (1).')
                    return [0, 0, 0, 0] # ponto de saida SEM erro (0)
                else :  # se nao, mata e volta de onde parou.
                    template = "Fase2 : Uma excecao do tipo NoSuchElementException aconteceu. Argumentos:\n{1!r}"
                    message = template.format(type(ex3).__name__, ex3.args)
                    print (message)
                    return [pagina, index, 3, final_index]
            except Exception as ex4 : # timeout da pagina, pagina nao colaborando, erro aleatorio em qualquer indice da pagina entra aqui. Mata e comeca denovo.
                template = "Fase2 : Uma excecao do tipo {0} aconteceu. Argumentos:\n{1!r}"
                message = template.format(type(ex4).__name__, ex4.args)
                print (message)
                return [pagina, index, 3, final_index]
            #PARA MAIS TARDE, PROCURAR O ULTIMO <tr> DO ULTIMO <TD> da ultima pagina atraves do LAST_PAGE e coloca-lo AQUI!

            #Pra usar se necessario
            #WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "")))

            # Entrando na aba das multas/sancoes/advertencias
            #WebDriverWait(driver, 5) ; sleep(1)
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, dict_BEC[nome]['id_tipo'])))
            botao_pagina = driver.find_element_by_id(dict_BEC[nome]['id_tipo'])
            botao_pagina.click()
            esperaCarregar(driver)

            # Baixando os arquivos excel
            opcoes_navegacao = driver.find_elements_by_class_name('button')
            actions = ActionChains(driver)
            actions.move_to_element(opcoes_navegacao[3]).click().perform() # opcoes_navegacao[3] é "Exportar para Excel"
            esperaCarregar(driver)

            rename_file = str(dict_BEC[nome]['tipo']+'.xls')
            renamed_file = str(dict_BEC[nome]['tipo']+'_'+str(pagina)+'_'+str(index)+'.xls')

            os.rename(os.path.join(local_downloads,rename_file),os.path.join(local_downloads,renamed_file))

            # arquivo de controle de fluxo, atualizado em get_dados mas usado por get_dados_controller para memoria de estado em caso de finalizacao abrupta do codigo
            # ps: benchmark feito, essa escrita custa 0.0018s por execucao
            # por que index+1? Pq no resto do codigo, signal[1] se refere a algum fornecedor que deu erro e nao conseguiu ser baixado, e aqui a referencia eh feita ao ultimo download feito com sucesso.
            # sendo assim, pra que signal[1] tenha o mesmo significado que o resto do codigo, flow_control vai receber o endereco do proximo arquivo a ser baixado.
            with open(os.path.join(local_downloads,'sancoesSP_flow_control_'+dict_BEC[nome]['tipo']+'.tmp'),'w') as flow:
                flow.write(','.join(c for c in [str(pagina), str(index+1), '0', str(final_index)]))

            # Retornando a tela anterior pxlsra ir pro proximo fornecedor
            # opcoes_navegacao[1] é "Exibir Todos"
            opcoes_navegacao = driver.find_elements_by_class_name('button')
            try : #tente voltar pro proximo fornecedor
                opcoes_navegacao[1].click()
            except :
                try: #se nao conseguir, espere ate o 'exibir todos' aparecer
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "ctl00$ContentPlaceHolder1$btnTodos"))) ; sleep (5)
                    opcoes_navegacao[1].click()
                except : #se ele nao aparecer, mata e comeca denovo.
                    return [pagina, index, 4, final_index]

        # Se precisava bypassar o bloco anterior uma vez, agora nao precisa mais.
        if flag_bypass == True :
            fornecedor = None # pra que na proxima pagina todos os fornecedores sejam baixados, e nao apenas os restantes da execucao anterior.
            flag_bypass = False

    # se o loop FOR for concluido com sucesso, e a pagina estiver com o numero total de fornecedores.
    print ('Download concluido (2).')
    return [0,0,0,0]