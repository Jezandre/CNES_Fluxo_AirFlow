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

![Gráfico](https://github.com/Jezandre/CNES_Fluxo_AirFlow/blob/main/Fluxo%20AirflowCnes.jpg)
