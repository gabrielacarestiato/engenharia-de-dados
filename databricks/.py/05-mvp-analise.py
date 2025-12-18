# Databricks notebook source
#Frequência de lesões por temporada e time
df1 = spark.table("mvp.gold.pergunta1")
display(df1)

# COMMAND ----------

#Incidência de lesões por posição
df2 = spark.table("mvp.gold.pergunta2")
display(df2)

# COMMAND ----------

#Média de duração da lesão por faixas de idade 
df3 = spark.table("mvp.gold.pergunta3")
display(df3)

# COMMAND ----------

#Frequência de lesões por faixa
df4 = spark.table("mvp.gold.pergunta4")
display(df4)

# COMMAND ----------

#Média de tempo de recuperação
df5 = spark.table("mvp.gold.pergunta5")
display(df5)

# COMMAND ----------

#Times com maior número de jogadores lesionados
df6 = spark.table("mvp.gold.pergunta6")
display(df6)

# COMMAND ----------

#Relação entre tipo de lesão e tempo de retorno
df7 = spark.table("mvp.gold.pergunta7")
display(df7)
