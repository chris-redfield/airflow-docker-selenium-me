import re
import os
import time
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup

def get_url(driver, url_request):

    print(f"Operation get_url started, retrieving: {url_request}")
    print(f"Current working dir: {os.getcwd()}")

    driver.get(url_request)

    # Clicando no menu de opções
    menu = driver.find_element_by_class_name('menuicon')
    menu.click()

    #################
    # Existe mais de uma opção com essa classe então acho melhor pesquisar todas e depois selecionar aquela que está escrito Consultas
    # Alternativamente pode-se ir direto no id, mas esse id tem cara de variar com versão. Assim me parece mais future-proof
    # 
    # A ideia eh pegar todas as opções em uma lista e depois selecionar aquela que está escrito "Cnsultas"
    WebDriverWait(driver, 5)
    time.sleep(5)
    opcoes_navegacao = driver.find_elements_by_class_name('z-nav-text')
    for opcao in opcoes_navegacao:
        if opcao.text == "Consultas":
            botao_consulta = opcao
    botao_consulta.click()

    ################
    # Mesmo raciocinio da particao de cima, selecionando dentre as opcoes aquela que fala sobre os impossibilitados de usar a base
    WebDriverWait(driver, 5)
    time.sleep(1)
    opcoes_navegacao = driver.find_elements_by_class_name('z-navitem-text')
    for opcao in opcoes_navegacao:
        if "Impedidos" in opcao.text:
            botao_fornecedores = opcao
    botao_fornecedores.click()

    # Procurando os botões 
    WebDriverWait(driver, 5)
    time.sleep(1)
    botoes = driver.find_elements_by_tag_name('button')
    for index, botao in enumerate(botoes):
        if "LISTAR" in botao.text:
            botao_exportar = botao

    botao_exportar.click() 
    time.sleep(10)