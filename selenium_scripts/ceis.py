import re
import os
from bs4 import BeautifulSoup

def get_url(driver, to_file,url_request):

    try:
        with open(to_file,'r') as f:
            f.close()
    except FileNotFoundError:

        print(f"Operation get_url started, retrieving: {url_request}")
        print(f"Current working dir: {os.getcwd()}")

        driver.get(url_request)

        #Usa Selenium para abrir a pagina dinamicamente
        page_source = driver.page_source

        #fecha a sessão e fecha a janela

        #Usa BeautifulSoup para recuperar o HTML da pagina
        soup = BeautifulSoup(page_source,'html.parser')

        #procura todas as tags HREF
        hrefs = soup.find_all('a')

        index = -1
        i = 0
        for link in (hrefs):
            if len(re.findall(r'\/download-de-dados\/ceis\/',link.get('href'))) != 0:
                index = i
            i += 1
        if index != -1:
            #Concatena a URL do link que faz o download do arquivo
            with open(to_file, 'w') as f:
                f.write('http://www.portaltransparencia.gov.br' + (hrefs[index].get('href')))
                f.write('\n')
            
            print(f'SUCCESS: file {to_file} saved')