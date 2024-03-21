# Fluxo de extração de dados do CNES do portal datasus

##Introdução

O DATASUS (Departamento de Informática do Sistema Único de Saúde) é uma plataforma do Ministério da Saúde do Brasil responsável pela coleta, armazenamento e disponibilização de dados relacionados à saúde pública no país. Ele fornece acesso a uma ampla gama de informações, incluindo estatísticas de saúde, registros de pacientes, indicadores epidemiológicos e muito mais.

O CNES (Cadastro Nacional de Estabelecimentos de Saúde) é um dos principais bancos de dados mantidos pelo DATASUS. Ele contém informações detalhadas sobre os estabelecimentos de saúde em todo o território brasileiro, incluindo hospitais, clínicas, postos de saúde, laboratórios e outros serviços de saúde. O CNES é utilizado para registro e gestão dos estabelecimentos de saúde, sendo uma fonte fundamental de dados para análises e planejamento no setor da saúde pública.

As informações do CNES são de fundamentala importancia para negócios voltados a área da saúde, pois é possivel identificar possíveis clientes cadastrados dentro do portal. O processo descrito a seguir é uma reformulção de um projeto que fiz utilizando apenas python e um servidor linux. Com o Airflow tenho mais possibilidades de aplicar novos atributos além de poder visualizar cada etapa do processo.

## Objetivo

Este artigo tem como foco detalhar um método robusto para extrair dados do portal DATASUS e integrá-los em um banco de dados Postgres. Para isso, serão empregadas ferramentas de ponta como Apache Airflow, Python e Postgres, proporcionando uma solução eficiente e escalável para gerenciamento e processamento de dados.

Ao longo do artigo, serão apresentados os passos necessários para configurar e executar o fluxo de dados, desde a extração dos dados do portal DATASUS até a inserção e atualização dos mesmos no banco de dados Postgres. Serão abordadas técnicas de programação em Python para manipulação e transformação dos dados, além de práticas recomendadas para garantir a integridade e a segurança dos processos.

Com o uso do Apache Airflow, será possível automatizar e agendar tarefas de extração e carga de dados de forma confiável e flexível. A integração com o banco de dados Postgres permitirá armazenar e consultar os dados de maneira eficiente, proporcionando uma base sólida para análises e tomadas de decisão.

Este artigo destina-se a profissionais e entusiastas de dados que buscam uma solução completa e escalável para integrar dados do DATASUS em seus projetos, aproveitando o poder e a flexibilidade das ferramentas modernas de ciência de dados e engenharia de dados.

##Materiais e Bibliotecas

Materiais
- Computador com configuração minima para rodar o Apache airflow e docker pelo menos 16gb de ram
- Python 3.8
- Docker 4.1.7
- AirFlow 2.5.1
- Postgres 13

Bibliotecas
- airflow
- datetime
- timedelta
- json
- os
- urllib.request
- zipfile
- pandas
- pyodbc
- re
- create_engine
- text
- shutil
- configparser

# Descrição do processo

O processo básicamente consiste em acessar o site o servidor FTP do DataSus, fazer o download dos arquivos, descompactar, renomear e inserir os dados dos arquivos em um banco de dados postGres. Um processo simples, que futuramente pode ser implementado com outras ferramentas mais robustas de processamento de dados.

![Gráfico](https://github.com/Jezandre/CNES_Fluxo_AirFlow/blob/main/Jornada%20do%20usu%C3%A1rio%20(3).jpg)

## Importar bibliotecas

O primeiro passo é bastante simples apenas importamos todas as bibliotecas que iremos utilizar durante todo o processo:

``` python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import json
import os
import urllib.request
from datetime import datetime
import zipfile
import pandas as pd
import pyodbc
import os
import re
from sqlalchemy import create_engine, text 
import shutil
from urllib.error import URLError
import configparser
```

## Configurar e criar a dag

Nesse passo simplesmente definimos os padrões que serão aplicados nas tasks de todo o processo

default_args: Este é um dicionário que define os argumentos padrão para as tarefas da DAG. Aqui está o que cada chave faz:

depends_on_past: Define se a execução de uma tarefa depende do sucesso da execução anterior.
email: Define o endereço de e-mail para notificações.
email_on_failure: Define se deve enviar um e-mail em caso de falha.
email_on_retry: Define se deve enviar um e-mail ao tentar novamente.
retries: Define o número de tentativas em caso de falha.
retry_delay: Define o atraso entre as tentativas.
owner: Define o proprietário da DAG.
start_date: Define a data de início da DAG.
catchup: Define se a DAG deve executar tarefas para datas passadas ao ser ativada pela primeira vez.
dag: Aqui é onde a DAG é definida. Cada argumento neste objeto tem o seguinte significado:

'cnes_files': O nome da DAG.
description: Uma descrição da DAG.
schedule_interval: Define com que frequência a DAG deve ser executada. Neste caso, é definido como None, o que significa que a DAG não será agendada automaticamente e precisará ser acionada manualmente ou por outra DAG.
start_date: A data de início da DAG.
catchup: Define se a DAG deve ou não pegar tarefas perdidas de datas anteriores.
default_args: Os argumentos padrão para as tarefas da DAG.
default_view: Define a visualização padrão quando a DAG é acessada no Airflow UI.
doc_md: Documentação em formato Markdown para a DAG.

``` python
default_args={
    'depends_on_past': False,
    'email': ['<insira aqui o email>>'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'catchup': False
}

dag = DAG(
    'cnes_files',
    description='Dados do dataSUS para download',
    schedule_interval=None,
    start_date=datetime(2023,3,5),
    catchup=False,
    default_args=default_args,
    default_view='graph',
    doc_md='DAG para registrar dados de unidades de saúde'
    )
```

## Task - pega_url_task

Este código define uma função chamada testarURL() que verifica a disponibilidade de um arquivo ZIP em um servidor FTP do DataSUS para um mês e ano específicos. Aqui está uma descrição de cada parte:

1. Função testarURL():

- Obtém a data atual.
- Extrai o mês e o ano da data atual.
- Constrói uma URL usando o ano e o mês para tentar acessar o arquivo ZIP no servidor FTP do DataSUS.
- Tenta abrir a URL. Se a URL não estiver disponível, diminui o mês atual em 1 e tenta novamente. Se ainda não estiver disponível, diminui o ano atual em 1 e tenta com o mês de dezembro do ano anterior.

2. PythonOperator:

- task_id: Um identificador único para esta tarefa.
- python_callable: A função Python que será chamada para executar a tarefa. Neste caso, é a função testarURL().
- provide_context: Se definido como True, fornece o contexto da execução (como a data de execução) para a função chamada.
- dag: A DAG à qual esta tarefa pertence.

Essencialmente, esta DAG tentará obter a URL para um arquivo ZIP contendo dados do CNES (Cadastro Nacional de Estabelecimentos de Saúde) do DataSUS para o mês e ano mais recentes possíveis. Se a URL não estiver disponível para o mês atual, ele tentará o mês anterior, e assim por diante, até encontrar um arquivo ZIP válido.

``` python
def testarURL():
    data_atual = datetime.now()
    # definir datas para inserir no link
    mes_atual = (data_atual.month)
    ano_atual = (data_atual.year)

    url = 'ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'

    # Avaliando se o link referente ao mês existe
    try:
        resposta = urllib.request.urlopen(url)

    except URLError as e:
        mes_atual = mes_atual - 1
        url = 'ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'
        # Caminho para o arquivo ZIP
        caminho_zip = 'BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'
        local_filename = 'BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'

        try:
            resposta = urllib.request.urlopen(url)

        except URLError as e:
            ano_atual = ano_atual - 1
            mes_atual = 12
            url = 'ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'
            # Caminho para o arquivo ZIP
            caminho_zip = 'BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'
            local_filename = 'BASE_DE_DADOS_CNES_' + str(ano_atual) + str(mes_atual) + '.ZIP'

    return url, caminho_zip, local_filename

pegar_url_task = PythonOperator(
    task_id = 'pegar_url',
    python_callable = testarURL,
    provide_context = True,
    dag=dag
)
```

  ## Task - baixar_arquivos_task

Este código representa outra tarefa dentro da sua DAG do Apache Airflow. Vamos entender o que cada parte faz:

1. Recuperação de dados da tarefa anterior (pegar_url):

- ti = kwargs['ti']: Recupera a instância de TaskInstance.
- url, caminho_zip, local_filename = ti.xcom_pull(task_ids='pegar_url'): Usa xcom_pull para obter os dados de saída da tarefa pegar_url. Esses dados são a URL do arquivo ZIP, o caminho para o arquivo ZIP e o nome do arquivo local.

2. Configuração do diretório de destino:

- pasta_destino = Variable.get("path_file_cnes"): Obtém o caminho do diretório de destino dos arquivos do CNES do Airflow Variables.
- Se o diretório de destino existir, ele é excluído e recriado para garantir que esteja vazio e pronto para receber os novos arquivos.

3. Download do arquivo ZIP:

- O arquivo ZIP é baixado da URL especificada para o diretório local.
- with urllib.request.urlopen(url) as response, open(local_filename, 'wb') as out_file:: Abre a URL e cria o arquivo local.
- data = response.read(): Lê o conteúdo da resposta.
- out_file.write(data): Escreve o conteúdo no arquivo local.

4. Extração dos arquivos do ZIP:

- A pasta de destino é criada se ainda não existir.
- O conteúdo do arquivo ZIP é extraído para a pasta de destino.

5. Mensagens de registro:

- Mensagens são impressas no console para indicar o início e o término do download e extração de arquivos.
- print("Diretório atual após a extração:", os.getcwd()): Imprime o diretório atual após a extração, para fins de registro.
- print(f'Arquivos extraídos para: {pasta_destino} - {datetime.now()}'): Imprime a mensagem indicando onde os arquivos foram extraídos.

Essa tarefa é responsável por baixar o arquivo ZIP do CNES do URL fornecido, extrair seu conteúdo para um diretório específico e fornecer mensagens de registro durante o processo.

``` python
def baixarArquivosCSV(**kwargs):

    ti = kwargs['ti']
    url, caminho_zip, local_filename = ti.xcom_pull(task_ids='pegar_url')
    pasta_destino = Variable.get("path_file_cnes")
    print("Diretório atual antes do download:", os.getcwd())

    # avaliar se a pasta destino existe, se existir ela será excluída para ser atualizada
    if os.path.exists(pasta_destino):
        shutil.rmtree(pasta_destino)

    print(f'Iniciando download: {datetime.now()}')
    # Baixar o arquivo
    with urllib.request.urlopen(url) as response, open(local_filename, 'wb') as out_file:
        data = response.read()
        out_file.write(data)

    # Criar a pasta de destino se não existir
    print(f'Download finalizado: {datetime.now()}')
    print(f'Iniciando extração: {datetime.now()}')
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    # Extrair o conteúdo do arquivo ZIP
    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
        zip_ref.extractall(pasta_destino)
        
    print("Diretório atual após a extração:", os.getcwd())

    print(f'Arquivos extraídos para: {pasta_destino} - {datetime.now()}')

baixar_arquivos_task = PythonOperator(
    task_id='baixar_arquivos',
    python_callable=baixarArquivosCSV,    
    provide_context=True,
    dag=dag
)
```

## Task - renomear_arquivos_task

1. Definição da função renomearArquivos():

- pasta = Variable.get("path_file_cnes"): Obtém o caminho do diretório de destino dos arquivos do CNES do Airflow Variables.
- Imprime uma mensagem indicando o início do processo de renomeação.
- Um padrão de expressão regular é compilado para encontrar números no final dos nomes de arquivo.
- Itera sobre os arquivos no diretório.
- Para cada arquivo no diretório:
  - Obtém o caminho completo do arquivo.
  - Verifica se o item é um arquivo.
  - Extrai o nome do arquivo sem a extensão e a extensão do arquivo.
  - Remove números do final do nome do arquivo usando a expressão regular.
  - Remove espaços em branco extras do novo nome do arquivo.
  - Concatena o novo nome do arquivo com a extensão.
  - Renomeia o arquivo com o novo nome.
  - Imprime uma mensagem indicando o término do processo de renomeação.
  
2. PythonOperator:

- task_id='renomear_arquivos': Um identificador único para esta tarefa.
- python_callable=renomearArquivos: A função Python que será chamada para executar a tarefa.
- provide_context=True: Se definido como True, fornece o contexto da execução (como a data de execução) para a função chamada.
- dag=dag: A DAG à qual esta tarefa pertence.

Essa tarefa é responsável por renomear os arquivos no diretório de destino dos arquivos do CNES, removendo números do final dos nomes de arquivo, garantindo assim que os nomes dos arquivos estejam formatados adequadamente após a extração.

``` python
def renomearArquivos(**kwargs):
    pasta = Variable.get("path_file_cnes")
    print(f'Inciando renomeamento de arquivos: {datetime.now()}')
    padrao_numeros = re.compile(r'\d+$')
    for nome_arquivo in os.listdir(pasta):
        caminho_completo = os.path.join(pasta, nome_arquivo)
        if os.path.isfile(caminho_completo):
            # Extrair o nome do arquivo sem a extensão
            nome_sem_extensao, extensao = os.path.splitext(nome_arquivo)
            # Remover números do final do nome do arquivo
            novo_nome = re.sub(padrao_numeros, '', nome_sem_extensao)
            novo_nome = novo_nome.strip()  # Remover espaços em branco extras
            novo_nome_com_extensao = f"{novo_nome}{extensao}"
            
            # Renomear o arquivo
            novo_caminho = os.path.join(pasta, novo_nome_com_extensao)
            os.rename(caminho_completo, novo_caminho)
            # print(f"Arquivo renomeado: {nome_arquivo} -> {novo_nome_com_extensao}")
    print(f'Finalizando renomeamento de arquivos: {datetime.now()}')

renomear_arquivos_task = PythonOperator(
    task_id='renomear_arquivos',
    python_callable=renomearArquivos,
    provide_context=True,
    dag=dag
)
```
## Task - selecionar_arquivos_task

Nesta parte do código, uma função chamada selecionarArquivosCSVutilizados() é definida para determinar quais arquivos CSV serão utilizados no processo. Aqui está uma explicação detalhada:

1. Definição da função selecionarArquivosCSVutilizados():

- pasta_destino = Variable.get("path_file_cnes"): Obtém o caminho do diretório de destino dos arquivos do CNES do Airflow Variables.
- csv_files: É um dicionário que contém os caminhos para os arquivos CSV relevantes. Cada chave no dicionário representa um nome de arquivo CSV e seu valor é o caminho completo para o arquivo CSV correspondente no diretório de destino.

2. PythonOperator:

- task_id='obter_arquivos_csv': Um identificador único para esta tarefa.
- python_callable=selecionarArquivosCSVutilizados: A função Python que será chamada para executar a tarefa.
- provide_context=True: Se definido como True, fornece o contexto da execução (como a data de execução) para a função chamada.
- dag=dag: A DAG à qual esta tarefa pertence.

Essa tarefa é responsável por determinar quais arquivos CSV serão utilizados no processo de extração de dados. No momento, está configurada para retornar os caminhos de dois arquivos CSV ('tb_estado.csv' e 'tb_servico_especializado.csv'), mas há outros arquivos CSV comentados que podem ser incluídos posteriormente, se necessário.

``` python
def selecionarArquivosCSVutilizados(**kwargs):
    pasta_destino = Variable.get("path_file_cnes")
    #Dicionário de Variáveis
    csv_files = {
        # 'tb_estabelecimento': str(pasta_destino) + 'tbEstabelecimento.csv',
        # 'rl_estab_complementar': str(pasta_destino) + 'rlEstabComplementar.csv',
        # 'cness_rl_estab_serv_calss': str(pasta_destino) + 'rlEstabServClass.csv',
        # 'tb_tipo_unidade': str(pasta_destino) + 'tbTipoUnidade.csv',
        # 'tb_municipio': str(pasta_destino) + 'tbMunicipio.csv',
        # 'rl_estab_atend_prest_conv': str(pasta_destino) + 'rlEstabAtendPrestConv.csv',
        'tb_estado': str(pasta_destino) + 'tbEstado.csv',
        'tb_servico_especializado': str(pasta_destino) + 'tbServicoEspecializado.csv'
    }

    return csv_files

selecionar_arquivos_task = PythonOperator(
    task_id='obter_arquivos_csv',
    python_callable=selecionarArquivosCSVutilizados,
    provide_context=True,
    dag=dag,
)
```

## Task - criar_tabelas_task

Este trecho de código define uma tarefa que cria tabelas em um banco de dados PostgreSQL a partir de arquivos CSV. Vamos entender o que cada parte faz:

1. Recuperação dos arquivos CSV:

- csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv'): Utiliza xcom_pull para obter os caminhos dos arquivos CSV retornados pela tarefa anterior.

2. Criação das tabelas:

- Itera sobre os pares de nome da tabela e caminho do arquivo CSV.
- Para cada par:
  - Abre o arquivo CSV e lê a primeira linha para obter os nomes das colunas.
  - Formata os nomes das colunas para criar uma string representando a definição das colunas no SQL.
  - Cria uma instrução SQL para criar a tabela com os nomes das colunas formatados.
  - Cria uma tarefa PostgresOperator para executar a instrução SQL de criação da tabela no banco de dados PostgreSQL.

3. PythonOperator:

- task_id='criar_tabelas_from_csv': Um identificador único para esta tarefa.
- python_callable=criarTabelasAPartirDoCSV: A função Python que será chamada para executar a tarefa.
- dag=dag: A DAG à qual esta tarefa pertence.

Essa tarefa é responsável por criar tabelas no banco de dados PostgreSQL a partir dos arquivos CSV fornecidos. Cada arquivo CSV é utilizado para determinar a estrutura da tabela correspondente.


``` python
def inserirDadosNasTabelas(**kwargs):

    csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv')

    for table_name, file_path in csv_files.items():
        with open(file_path, 'r', encoding='latin-1') as file:  # Especifique o encoding adequado aqui
            columns = file.readline().strip().split(';')
        columns = [col.replace('"', '').strip() for col in columns]  # Remover as aspas e espaços em branco desnecessários
        columns_str = ', '.join([f'"{col}" VARCHAR' for col in columns])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_str});'
        create_table_task = PostgresOperator(
            task_id=f'create_table_{table_name}',
            postgres_conn_id='postgres',
            sql=create_table_sql,
            dag=kwargs['dag']
        )
        create_table_task.execute(kwargs)

criar_tabelas_task = PythonOperator(
    task_id='criar_tabelas_from_csv',
    python_callable=criarTabelasAPartirDoCSV,
    dag=dag
)
```
## Task - inserir_dados_task

Este trecho de código define uma tarefa que insere dados nas tabelas do banco de dados PostgreSQL a partir dos arquivos CSV. Aqui está uma explicação detalhada:

1. Recuperação dos arquivos CSV:

- csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv'): Utiliza xcom_pull para obter os caminhos dos arquivos CSV retornados pela tarefa anterior.

2. Inserção de dados nas tabelas:

- Itera sobre os pares de nome da tabela e caminho do arquivo CSV.
- Para cada par:
  - Executa uma instrução SQL TRUNCATE TABLE para limpar os dados existentes da tabela.
  - Abre o arquivo CSV e itera sobre as linhas, excluindo a primeira linha (cabeçalho).
  - Para cada linha no arquivo CSV, divide-a em valores e formata uma instrução SQL INSERT INTO para inserir esses valores na tabela correspondente.
  - Executa a instrução SQL INSERT INTO utilizando um PostgresHook para inserir os dados no banco de dados PostgreSQL.

3. PythonOperator:

- task_id='inserir_dados': Um identificador único para esta tarefa.
- python_callable=inserirDadosNasTabelas: A função Python que será chamada para executar a tarefa.
- provide_context=True: Se definido como True, fornece o contexto da execução (como a data de execução) para a função chamada.
- dag=dag: A DAG à qual esta tarefa pertence.

Essa tarefa é responsável por limpar as tabelas existentes e inserir dados atualizados nas tabelas do banco de dados PostgreSQL a partir dos arquivos CSV fornecidos. Cada arquivo CSV é utilizado para inserir dados na tabela correspondente.

``` python
def inserirDadosNasTabelas(**kwargs):
    csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv')
    for table_name, file_path in csv_files.items():
        tuncate_sql = f"TRUNCATE TABLE {table_name}"
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.run(tuncate_sql)
        with open(file_path, 'r', encoding='latin-1') as file:
            # Pular o cabeçalho
            next(file)
            for line in file:
                values = [value.replace('"', '').strip() for value in line.strip().split(';')]                
                insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(values))})"
                hook = PostgresHook(postgres_conn_id='postgres')
                hook.run(insert_sql, parameters=values)

inserir_dados_task = PythonOperator(
    task_id='inserir_dados',
    python_callable=inserirDadosNasTabelas,
    provide_context=True,
    dag=dag
)
```
## Fluxo da aplicação

Este trecho de código define a relação entre as tarefas na DAG (Directed Acyclic Graph) do Apache Airflow. Aqui está uma explicação de como as tarefas estão conectadas:

- pegar_url_task: Esta tarefa obtém a URL para o arquivo ZIP contendo os dados do CNES.

- baixar_arquivos_task: Depende da tarefa pegar_url_task e baixa o arquivo ZIP contendo os dados do CNES.

- renomear_arquivos_task: Depende da tarefa baixar_arquivos_task e renomeia os arquivos extraídos do arquivo ZIP, removendo números dos nomes dos arquivos.

- selecionar_arquivos_task: Depende da tarefa renomear_arquivos_task e seleciona os arquivos CSV relevantes para o processo de extração de dados.

- criar_tabelas_task: Depende da tarefa selecionar_arquivos_task e cria as tabelas no banco de dados PostgreSQL com base nos arquivos CSV selecionados.

- inserir_dados_task: Depende da tarefa criar_tabelas_task e insere os dados dos arquivos CSV nas tabelas correspondentes no banco de dados PostgreSQL.

Essa DAG define uma sequência de tarefas que começam com a obtenção da URL para o arquivo ZIP, passam pela extração, renomeação e seleção de arquivos, e finalmente, criam tabelas e inserem dados no banco de dados PostgreSQL. Cada tarefa depende do sucesso da tarefa anterior para ser executada.

``` python
pegar_url_task >> baixar_arquivos_task >> renomear_arquivos_task >> selecionar_arquivos_task

selecionar_arquivos_task >> criar_tabelas_task >> inserir_dados_task
```

# Conclusão

Esse processo de DAG no Apache Airflow representa uma pipeline de extração, transformação e carga (ETL) de dados do CNES (Cadastro Nacional de Estabelecimentos de Saúde) fornecidos pelo DataSUS. Aqui está uma conclusão sobre o processo como um todo:

1. Obtenção dos dados: A DAG começa obtendo a URL para o arquivo ZIP contendo os dados do CNES.

2. Download e preparação dos dados: Os dados são baixados, extraídos e renomeados conforme necessário. Isso garante que os arquivos estejam prontos para serem processados.

3. Seleção e preparação dos arquivos: A DAG seleciona os arquivos CSV relevantes para o processo de extração de dados e prepara a estrutura das tabelas no banco de dados PostgreSQL com base nesses arquivos.

4. Criação das tabelas: As tabelas são criadas no banco de dados PostgreSQL de acordo com a estrutura definida pelos arquivos CSV selecionados.

5. Inserção dos dados: Os dados dos arquivos CSV são inseridos nas tabelas correspondentes no banco de dados PostgreSQL, garantindo que estejam disponíveis para consulta e análise.

Essa DAG automatiza o processo completo de obtenção, preparação e carregamento dos dados do CNES, garantindo consistência e confiabilidade nos dados armazenados no banco de dados. Isso facilita a análise e o uso desses dados para tomada de decisões em análises de saúde pública, pesquisa acadêmica e outros contextos.

A Aplicação do processo também me permitiu demonstrar um pouco do conhecimento que obtive nos cursos de Apache Airflow permitindo me evidenciar o conhcimento obtido.
