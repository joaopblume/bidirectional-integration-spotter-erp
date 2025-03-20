#!/usr/bin/env python
# coding: utf-8

# # Create Spark Session

# In[1]:


#!sudo pip3 install python-dotenv


# In[2]:


import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, concat_ws, md5
from datetime import datetime
import shutil
from dotenv import load_dotenv
from pyspark.sql.types import StringType
import hashlib




spark = SparkSession.builder\
    .appName('insert_clientes_app')\
    .master("spark://localhost:7077")\
    .getOrCreate()


# # Load environment variables

# In[3]:


# Limpa variáveis de ambiente que já foram carregadas
keys_to_clear = ['DB_USER', 'DB_PASSWD', 'DB_HOST', 'DB_PORT', 'DB_SERVICE_NAME']
for key in keys_to_clear:
    if key in os.environ:
        del os.environ[key]

load_dotenv(dotenv_path='/path/to/app/.env')

USER = os.getenv('DB_USER')
PASSWD = os.getenv('DB_PASSWD')
HOST = os.getenv('DB_HOST')
PORT = os.getenv('DB_PORT')
SERVICE_NAME = os.getenv('DB_SERVICE_NAME')

env_vars = [USER, PASSWD, HOST, PORT, SERVICE_NAME]

if None in env_vars:
    raise ValueError('Nem todas as variaveis foram carregadas corretamente!')
else:
    print('Todas as variáveis foram carregadas com sucesso')
        


# # Read "Clientes" View

# In[4]:


query = '''
SELECT * FROM view_insert_lead_in_app
'''

df_insert_app = spark.read.format("jdbc")\
     .option("url",f"jdbc:oracle:thin:{USER}/{PASSWD}@{HOST}:{PORT}/{SERVICE_NAME}")\
     .option("driver", "oracle.jdbc.driver.OracleDriver")\
     .option("query", query)\
     .load()


# In[5]:


df_insert_app.show()


# In[6]:


def calculate_dataframe_hash(df):
    # Concatena todos os valores do DataFrame em uma string
    df_str = df.select(concat_ws('|', *df.columns).alias('concat')).rdd.map(lambda row: row[0]).collect()
    # Concatena todas as strings resultantes
    concatenated_str = '|'.join(df_str)
    # Calcula o hash MD5 da string concatenada
    return hashlib.md5(concatenated_str.encode('utf-8')).hexdigest()


# # Save dataframe as json

# In[7]:


linhas_dataframe = df_insert_app.count()
print(linhas_dataframe)

if linhas_dataframe > 0:
    PROCESSADOS = '/data/app/sync/leads/processar/'
    
    dataframe_hash = calculate_dataframe_hash(df_insert_app)
    files = os.listdir(PROCESSADOS)
    hash_found = any(dataframe_hash in f for f in files)

    if not hash_found:
        HOJE = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        TEMP = '/data/app/sync/tmp'
        
        df_insert_app.write.mode(saveMode='overwrite').json(TEMP)
        
        part_filename = next(entry for entry in os.listdir(TEMP) if entry.startswith('part-'))
        
        temporary_json = os.path.join(TEMP, part_filename)
        shutil.copyfile(temporary_json, PROCESSADOS + f'insert_leads_in_app_{dataframe_hash}_{HOJE}.json')
    else:
        print('Arquivo ja existe')


# # Finish execution and stop Spark

# In[8]:


spark.stop()
