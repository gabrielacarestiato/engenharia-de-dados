# Databricks notebook source
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA silver")

# COMMAND ----------

#Seleção de 9 colunas da tabela premierleague usando Spark DataFrame
df = spark.table("mvp.bronze.premierleague").select(
    "Name", "Team_Name", "Position", "Age", "Season", "FIFA_rating", "Injury", "Date_of_Injury", "Date_of_return"
)

#Conversão explícita dos tipos das colunas conforme desejado
from pyspark.sql.functions import col

df = df.select(
    col("Name").alias("nome"),
    col("Team_Name").alias("time"),
    col("Position").alias("posição"),
    col("Age").cast("int").alias("idade_anos"),
    col("Season").alias("temporada"),
    col("FIFA_rating").cast("int").alias("fifa"),
    col("Injury").alias("lesão"),
    col("Date_of_Injury").alias("dt_lesão"),
    col("Date_of_return").alias("dt_retorno")
)

# COMMAND ----------

#Criação de um DataFrame apenas com as colunas que serão usadas no modelo
#10 primeiras linhas do dataset
display(df.limit(10))

# COMMAND ----------

#10 últimas linhas do dataset
display(df.orderBy("nome", ascending=False).limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

#Verificações iniciais
display(df.sample(False, 0.01))
print("\nFormato:", (df.count(), len(df.columns)))
print("\nValores ausentes por coluna:")
display(df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]))

# COMMAND ----------

#DataFrame ordenado de distribuição dos nomes
df_nomes = df.groupBy('nome').count().orderBy('count', ascending=False)
display(df_nomes)

# COMMAND ----------

#DataFrame ordenado de distribuição dos times
df_times = df.groupBy('time').count().orderBy('count', ascending=False)
display(df_times)

# COMMAND ----------

#DataFrame ordenado de distribuição das posições
df_posicoes = df.groupBy('posição').count().orderBy('count', ascending=False)
display(df_posicoes)

# COMMAND ----------

#DataFrame ordenado de distribuição das temporadas
df_temporadas = df.groupby('temporada').count().orderBy('count', ascending=False)
display(df_temporadas)

# COMMAND ----------

#DataFrame ordenado de distribuição das lesões
df_lesões = df.groupby('lesão').count().orderBy('count', ascending=False)
display(df_lesões)

# COMMAND ----------

#Criação de uma cópia do dataset original para preservar os dados
df_copia = df.toPandas().copy()

# COMMAND ----------

import pandas as pd

# Conversão das colunas de data
df_copia['dt_lesão'] = pd.to_datetime(df_copia['dt_lesão'].astype(str), errors='coerce')
df_copia['dt_retorno'] = pd.to_datetime(df_copia['dt_retorno'].astype(str), errors='coerce')

# Verificação de missing values do dataset que será tratado
df_copia.isnull().sum()

# COMMAND ----------

#Criando novamente a cópia do dataset original para desfazer a conversão das datas e tratar os problemas
df_tratado = df.toPandas().copy()

#Remoção das linhas com 'present' na coluna de retorno
df_tratado = df_tratado[~df_tratado['dt_retorno'].astype(str).str.lower().str.contains('present', na=False)]

#Correção das datas com vírgula colada no ano
df_tratado['dt_lesão'] = df_tratado['dt_lesão'].astype(str).str.replace(r',(?=\d)', ', ', regex=True)
df_tratado['dt_retorno'] = df_tratado['dt_retorno'].astype(str).str.replace(r',(?=\d)', ', ', regex=True)

#Correção de erro no ano
df_tratado['dt_lesão'] = df_tratado['dt_lesão'].str.replace('0202', '2020')
df_tratado['dt_retorno'] = df_tratado['dt_retorno'].str.replace('0202', '2020')

#Padronização dos formatos de datas (meses completos e abreviados misturados)
from dateutil import parser

def padronizar_data(data_str):
    try:
        return parser.parse(data_str).strftime('%b %d, %Y')
    except:
        return None  #Retorna None se a conversão falhar

df_tratado['dt_lesão'] = df_tratado['dt_lesão'].apply(padronizar_data)
df_tratado['dt_retorno'] = df_tratado['dt_retorno'].apply(padronizar_data)

#Conversão das colunas de data
df_tratado['dt_lesão'] = pd.to_datetime(df_tratado['dt_lesão'], errors='coerce')
df_tratado['dt_retorno'] = pd.to_datetime(df_tratado['dt_retorno'], errors='coerce')

#Tratamento para casos em que dt_retorno < dt_lesão: soma 1 ano à dt_retorno
mask_negativa = df_tratado['dt_retorno'] < df_tratado['dt_lesão']
df_tratado.loc[mask_negativa, 'dt_retorno'] = df_tratado.loc[mask_negativa, 'dt_retorno'] + pd.DateOffset(years=1)

#Criação da nova coluna 'duração (dias)'
df_tratado['duração_dias'] = (df_tratado['dt_retorno'] - df_tratado['dt_lesão']).dt.days

#Seleção das 7 primeiras colunas + da nova coluna
df_tratado = df_tratado[['nome', 'time', 'posição', 'idade_anos', 'temporada', 'fifa', 'lesão', 'duração_dias']].copy()
df_tratado.display(10)

# COMMAND ----------

#Verificação de missing values do dataset que foi tratado
df_tratado.isnull().sum()

# COMMAND ----------

#Padronização da coluna 'posição' removendo espaços extras nas extremidades e espaços múltiplos internos
df_tratado['posição'] = df_tratado['posição'].astype(str).str.strip()
df_tratado['posição'] = df_tratado['posição'].str.replace(r'\s+', ' ', regex=True)

#Verificação da distribuição após padronização
df_tratado['posição'].value_counts(normalize=True)

# COMMAND ----------

#Padronização os nomes dos tipos de lesão: tudo em minúsculo e sem espaços extras
df_tratado['lesão'] = df_tratado['lesão'].str.strip().str.lower()

#Verificação da distribuição após padronização
df_tratado['lesão'].value_counts(normalize=True)

# COMMAND ----------

#Salvando o DataFrame tratado como uma tabela Delta no schema atual
spark.createDataFrame(df_tratado).write.format("delta").mode("overwrite").saveAsTable("premierleague")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from premierleague limit 10
