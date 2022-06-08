from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataProcPySparkOperator, DataprocClusterDeleteOperator, DataProcJobBaseOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators import BashOperator, PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import os
import sys
import pandas as pd
import numpy as np
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import collect_list, col
# from pyspark.sql import functions as F
# from pyspark.sql.functions import *
# from pyspark.ml.fpm import FPGrowth
import pyarrow.parquet as pq
from google.cloud import bigquery


#Parametros do Airflow
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',  #O Owner da TASK
    'depends_on_past': False, #Sempre Falso
    'start_date': datetime(2020, 1, 1), #Inserir a data quando deve iniciar o pipeline.
    'email_on_failure': Variable.get('email_on_failure'), #Variavel que vai definir o parametro se recebe e-mail ou não em caso de falha.
    'email_on_retry': Variable.get('email_on_retry'), #Variavel que vai definir o parametro se recebe e-mail ou não em caso de retry.
    'project_id': Variable.get('gcp_project')  # Variavel que irá trazer o ID do Projeto onde fica o Cloud Composer.

}

#Linha para definir as Libraries que devem ser instaladas no Dataproc para rodar os scripts.
LIBS = {'CONDA_PACKAGES':'pandas-gbq google-cloud-core=1.3.0 six fsspec gcsfs joblib matplotlib tensorflow=2.2.0 statsmodels numpy protobuf scikit-learn mkl pytorch seaborn',
		'CONDA_CHANNELS':'conda-forge'}

 

# Create Directed Acyclic Graph for Airflow
with DAG('DEFINA_NOME_DA_DAG_QUE_IRÁ_APARECER_NO_AIRFLOW', #Defina o nome da DAG que irá aparecer dentro do Airflow, o espaço deve ser substituído por underline "_"
		default_args=DEFAULT_DAG_ARGS, #Passa os parametros que foram definidos de forma padrão ao Airflow dentro de DEFAULT_DAG_ARGS na linha 26.
		schedule_interval='SCHEDULE') as dag: #Definir o intervalor de schedule. Este deve ser inserido em CRON, neste link você tem um CRON generator para auxilia-lo: https://www.freeformatter.com/cron-expression-generator-quartz.html
	
	#Operador para criar um cluster spark no Dataproc para processar os dados necessários.
	create_cluster = DataprocClusterCreateOperator(
					task_id='DEFINA_O_ID_DA_TASK', #O ID deve ser separado por underline "_"
					cluster_name='defina-o-nome-do-cluster-a-ser-criado', #O Nome do Cluster deve ser separado por hifen "-" e em minusculo
					image_version=Variable.get('image_version'), #Variavel para Versão da Imagem do Cluster
					num_workers=Variable.get('job_spark'), #Variavel para número de workers. (#Tipos de Variaveis: job_python, job_spark)
					master_machine_type=Variable.get('machine_type'), #Variavel para tipo da máquina
					subnetwork_uri=Variable.get('subnetwork_uri'), #Variavel que trás a sub-rede a ser utilizada para o Cluster
					region=Variable.get('region'), #Variavel para regiao
					metadata=LIBS, #Definir as LIBS a serem instaladas no cluster na linha 37 desta DAG.
					init_actions_uris=['gs://goog-dataproc-initialization-actions-us-central1/python/conda-install.sh'], #Variavel que irá trazer as ações de inicialiação para instalação das LIBS.
					service_account_scopes=Variable.get('account_scope') #Variavel para escopo da service account a ser utilizada pelo cluster
    
	#Operador para mandar um JOB ao cluster do Dataproc criado na primeira etapa.
	cluster_task = DataProcPySparkOperator(
					task_id='DEFINA_O_ID_DA_TASK', 
					main='INSIRER O URI DO SCRIPT', #Exemplo de caminho para o seu script gs://analytics-seara/scripts-data-science/cluster_app.py
					cluster_name='INSERIR O MESMO NOME DO CLUSTER CRIADO NA ETAPA ACIMA',
					region=Variable.get('region'), #Variavel para regiao
					files='gs://analytics-proa-lmbr/scripts-data-science/dependencies/') #Caso precise enviar alguns arquivos para o cluster, por favor, inserir o URI destes arquivos no GCS. Exemplo: gs://analytics-proa-lmbr/scripts-data-science/dependencies/
                                                                                         #Caso Não precise, deletar a linha 67 e retirar a virgula ao final da linha 66.
	#Operador para mandar os dados do Google Cloud Storage para o BigQuery.																					 
	cloud_storage_to_bigquery = GoogleCloudStorageToBigQueryOperator(
					task_id='DEFINA_O_ID_DA_TASK', #O ID deve ser separado por underline "_"
					bucket='DEFINIR O NOME DO BUCKET', #Exemplo do nome de bucket 'seara-analytics
					source_objects= ['DEFINIR O CAMINHO/PATH'], #Definir o caminho abaixo do tronco principal, neste caso, abaixo de seara-analytics. Exemplo: Data/Cluster_read_data/*.parquet 
					destination_project_dataset_table='DEFINIR PROJETO.DATASET.TABELA', #Definir o projeto, Dataset e Tabela a ser inserido os dados. Exemplo: 	seara-analytics-prod.erp_seara.canal_venda
					autodetect= True, #Definir se o script deve detectar automaticamente o schema do arquivo onde, True é para sim, False não.
					skip_leading_rows =Variable.get('skip_leading_rows'), #Variavel que vai definir o Skip rows. o Padrão é 1.
					create_disposition='DEFINIR A CONDIÇÃO PARA O JOB CRIAR OU NÃO A TABELA', #Definir qual será a condição da criação da tabela no BigQuery. CREATE_NEVER - O Job Nunca criará uma tabela | CREATE_IF_NEEDED - O Job vai criar se ela não existir, em caso de existir irá ignorar.
					write_disposition='DEFINIR A CONDIÇÃO PARA O JOB DE COMO GRAVAR OS DADOS',  #Definir qual será a condição da forma para gravar os dados, são eles:
																							  #WRITE_EMPTY - O Job só irá gravar se a tabela destino estiver vazia 
																							  #WRITE_TRUNCATE - O Job irá apagar os dados da tabela destino e inserir os novos (Trunca a tabela e carrega). 
																							  #WRITE_APPEND - O Job irá apenas adicionar os dados, parametro o qual é normalmente utilizado para inserções de forma incremental.
					source_format='DEFINIR FORMATO DA FONTE') #Definir o formato da fonte, são elas: avro, parquet, orc, csv e json.
	
	
	#Deleta o cluster criado no Dataproc ao final do processamento.
	delete_cluster = DataprocClusterDeleteOperator(
					task_id='DEFINA_O_ID_DA_TASK', #O ID deve ser separado por underline "_"
					subnetwork_uri=Variable.get('subnetwork_uri'), #Variavel que trás a sub-rede a ser utilizada para o Cluster
					region=Variable.get('region'), #Variavel para regiao
					cluster_name='INSERIR O MESMO NOME DO CLUSTER CRIADO NA PRIMEIRA ETAPA',
					trigger_rule=TriggerRule.ALL_DONE) 

#Abaixo você deve definir como será o pipeline das tarefas, colocando pelo nomes das tarefas que foram criadas, por exemplo. delete_cluster é a última tarefa a ser executada. 
create_cluster >> cluster_task >> cloud_storage_to_bigquery >> delete_cluster