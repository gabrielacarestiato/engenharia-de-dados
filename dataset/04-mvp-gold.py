# Databricks notebook source
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

df_tratado = spark.table("mvp.silver.premierleague")

# COMMAND ----------

#Frequência de lesões por temporada e time
frequencia_lesoes = df_tratado.groupBy("temporada", "time").count().orderBy("temporada", "time")

# COMMAND ----------

frequencia_lesoes.write.format("delta").mode("overwrite").saveAsTable("pergunta1")

# COMMAND ----------

#Incidência de lesões por posição
from pyspark.sql import functions as F

incidencia_por_posicao = (
    df_tratado.groupBy("posição")
    .agg(F.count("*").alias("num_lesoes"))
    .orderBy(F.desc("num_lesoes"))
)

# COMMAND ----------

incidencia_por_posicao.write.format("delta").mode("overwrite").saveAsTable("pergunta2")

# COMMAND ----------

#Média de duração da lesão por faixas de idade
from pyspark.sql import functions as F

df_tratado = df_tratado.withColumn(
    "faixa_etaria",
    F.when((F.col("idade_anos") >= 18) & (F.col("idade_anos") <= 24), "18–24")
     .when((F.col("idade_anos") > 24) & (F.col("idade_anos") <= 28), "25–28")
     .when((F.col("idade_anos") > 28) & (F.col("idade_anos") <= 32), "29–32")
     .when((F.col("idade_anos") > 32) & (F.col("idade_anos") <= 40), "33–40")
     .otherwise(None)
)

df_media_duracao = (
    df_tratado.groupBy("faixa_etaria")
    .agg(F.mean("duração_dias").alias("duração_média"))
    .orderBy("faixa_etaria")
)

# COMMAND ----------

df_media_duracao.write.format("delta").mode("overwrite").saveAsTable("pergunta3")

# COMMAND ----------

#Faixas de pontuação FIFA
from pyspark.sql import functions as F

bins = [65, 75, 80, 85, 90]
labels = ['<=75', '76-80', '81-85', '>85']

df_tratado = df_tratado.withColumn(
    "faixa_fifa",
    F.when((F.col("fifa") <= 75), "<=75")
     .when((F.col("fifa") > 75) & (F.col("fifa") <= 80), "76-80")
     .when((F.col("fifa") > 80) & (F.col("fifa") <= 85), "81-85")
     .when((F.col("fifa") > 85), ">85")
     .otherwise(None)
)

frequencia_por_faixa = df_tratado.groupBy("faixa_fifa").count().orderBy("faixa_fifa")

# COMMAND ----------

frequencia_por_faixa.write.format("delta").mode("overwrite").saveAsTable("pergunta4")

# COMMAND ----------

#Tempo médio de recuperação
tempo_medio_recuperacao = df_tratado.agg(F.mean("duração_dias").alias("tempo_médio_recuperação"))

# COMMAND ----------

tempo_medio_recuperacao.write.format("delta").mode("overwrite").saveAsTable("pergunta5")

# COMMAND ----------

#Times com maior número de jogadores lesionados
jogadores_lesionados_por_time = (
    df_tratado.groupBy("time")
    .agg(F.countDistinct("nome").alias("num_jogadores_lesionados"))
    .orderBy(F.desc("num_jogadores_lesionados"))
)

# COMMAND ----------

jogadores_lesionados_por_time.write.format("delta").mode("overwrite").saveAsTable("pergunta6")

# COMMAND ----------

#Relação entre tipo de lesão e tempo de retorno -- Top 10 somente
relacao_lesao_tempo = (
    df_tratado.groupBy("lesão")
    .agg(F.mean("duração_dias").alias("tempo_médio_retorno"))
    .orderBy(F.desc("tempo_médio_retorno"))
    .limit(10)
)

# COMMAND ----------

relacao_lesao_tempo.write.format("delta").mode("overwrite").saveAsTable("pergunta7")