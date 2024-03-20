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

default_args={
    'depends_on_past': False,
    'email': ['<<Coloque o email de notificação aqui>>'],
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

def baixarArquivosCSV(**kwargs):
    import os
    import shutil
    import urllib.request
    import zipfile

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

def selecionarArquivosCSVutilizados(**kwargs):
    pasta_destino = Variable.get("path_file_cnes")
    #Dicionário de Variáveis
    csv_files = {
        'tb_estabelecimento': str(pasta_destino) + 'tbEstabelecimento.csv',
        'rl_estab_complementar': str(pasta_destino) + 'rlEstabComplementar.csv',
        'cness_rl_estab_serv_calss': str(pasta_destino) + 'rlEstabServClass.csv',
        'tb_tipo_unidade': str(pasta_destino) + 'tbTipoUnidade.csv',
        'tb_municipio': str(pasta_destino) + 'tbMunicipio.csv',
        'rl_estab_atend_prest_conv': str(pasta_destino) + 'rlEstabAtendPrestConv.csv',
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

def criarTabelasAPartirDoCSV(**kwargs):

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

pegar_url_task >> baixar_arquivos_task >> renomear_arquivos_task >> selecionar_arquivos_task

selecionar_arquivos_task >> criar_tabelas_task >> inserir_dados_task