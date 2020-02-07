import os
import time
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def get_url(driver,url_request):

    print(f"Operation get_url started, retrieving: {url_request}")
    print(f"Current working dir: {os.getcwd()}")

    # Indo na pagina
    
    driver.get(url_request)

    # A ideia eh fazer uma pesquisa para cada status. Assim, preciso da quantidade de opções e filtrar cada opção 
    quantidade_status = len(Select(driver.find_element_by_id('status')).options)


    for i in range(1, quantidade_status):

        # Na página de filtros
        Select(driver.find_element_by_id('status')).select_by_index(i)       # Seleciono o status
        driver.find_element_by_id('pesquisar').click()                       # Click no botao pesquisar

        # Indo para a pagina de resultados
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "dataTable_next")))

        # Procuro e aprto o botão de export
        driver.find_element_by_id('exportExcel').click()                    # aperto o botao do export
        time.sleep(5) # tem que aprender a detectar o termino do download ao inves de esperar x segundos e torcer para ter dado certo
                # provavelmente alguma coisa como um while que olha de x em x segundos se na pasta o arquivo já foi baixado

        # Volta para a página de filtro
        driver.get("http://www.compras.rj.gov.br/Portal-Siga/Sancao/buscar.action")

    # fecha a sessão e fecha a janela
    driver.close()
    driver.quit()