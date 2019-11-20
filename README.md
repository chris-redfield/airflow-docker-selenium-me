# airflow-docker-selenium-me

### Repositório com infra experimental para executar DAGs com Selenium no Airflow.

Principais referência: 
* https://github.com/HDaniels1991/airflow_selenium
* https://towardsdatascience.com/selenium-on-airflow-automate-a-daily-online-task-60afc05afaae

### Instalação ambiente DEV

1. Criar mais uma rede compartilhada entre os containers:

```console
    docker network create container_bridge
```

2. Criar volume compartilhado para download de arquivo do script exemplo

```console
    docker volume create downloads
```

3. Criar o container abaixo (será usado para executar os comandos do Selenium)

Navegar até pasta do projeto para poder referenciar o Dockerfile correto.

```console
    docker build -t docker_selenium -f Dockerfile-selenium .
```

### Execução do serviço

Navegar até a pasta do projeto e subir com docker-compose

```console
    docker-compose up
```