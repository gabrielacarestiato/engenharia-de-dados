# Databricks notebook source
mvp.bronze.premierleaguespark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

df = spark.read.option("header", True).csv("/Volumes/mvp/staging/players/player_injuries_impact.csv")
display(df.limit(10))

# COMMAND ----------

df = df.withColumnRenamed(
    "Team Name", "Team_Name"
).withColumnRenamed(
    "FIFA rating", "FIFA_rating"
).withColumnRenamed(
    "Date of Injury", "Date_of_Injury"
).withColumnRenamed(
    "Date of return", "Date_of_return"
)

df.write.format("delta").mode("overwrite").saveAsTable("premierleague")

# COMMAND ----------

spark.sql("COMMENT ON COLUMN premierleague.Name IS 'Player name'")
spark.sql("COMMENT ON COLUMN premierleague.Team_Name IS 'Team name'")
spark.sql("COMMENT ON COLUMN premierleague.Position IS 'Player position'")
spark.sql("COMMENT ON COLUMN premierleague.Age IS 'Player age'")
spark.sql("COMMENT ON COLUMN premierleague.Season IS 'Season of injury'")
spark.sql("COMMENT ON COLUMN premierleague.FIFA_rating IS 'Player FIFA rating at time of injury'")
spark.sql("COMMENT ON COLUMN premierleague.Injury IS 'Type of injury'")
spark.sql("COMMENT ON COLUMN premierleague.Date_of_Injury IS 'Date when injury occurred'")
spark.sql("COMMENT ON COLUMN premierleague.Date_of_return IS 'Date when player returned from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_before_injury_Result IS 'Result of match 1 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_before_injury_Opposition IS 'Opposition in match 1 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_before_injury_GD IS 'Goal difference in match 1 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_before_injury_Player_rating IS 'Player rating in match 1 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_before_injury_Result IS 'Result of match 2 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_before_injury_Opposition IS 'Opposition in match 2 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_before_injury_GD IS 'Goal difference in match 2 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_before_injury_Player_rating IS 'Player rating in match 2 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_before_injury_Result IS 'Result of match 3 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_before_injury_Opposition IS 'Opposition in match 3 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_before_injury_GD IS 'Goal difference in match 3 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_before_injury_Player_rating IS 'Player rating in match 3 before injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_missed_match_Result IS 'Result of match 1 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_missed_match_Opposition IS 'Opposition in match 1 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_missed_match_GD IS 'Goal difference in match 1 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_missed_match_Result IS 'Result of match 2 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_missed_match_Opposition IS 'Opposition in match 2 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_missed_match_GD IS 'Goal difference in match 2 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_missed_match_Result IS 'Result of match 3 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_missed_match_Opposition IS 'Opposition in match 3 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_missed_match_GD IS 'Goal difference in match 3 missed due to injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_after_injury_Result IS 'Result of match 1 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_after_injury_Opposition IS 'Opposition in match 1 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_after_injury_GD IS 'Goal difference in match 1 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match1_after_injury_Player_rating IS 'Player rating in match 1 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_after_injury_Result IS 'Result of match 2 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_after_injury_Opposition IS 'Opposition in match 2 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_after_injury_GD IS 'Goal difference in match 2 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match2_after_injury_Player_rating IS 'Player rating in match 2 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_after_injury_Result IS 'Result of match 3 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_after_injury_Opposition IS 'Opposition in match 3 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_after_injury_GD IS 'Goal difference in match 3 after return from injury'")
spark.sql("COMMENT ON COLUMN premierleague.Match3_after_injury_Player_rating IS 'Player rating in match 3 after return from injury'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from premierleague limit 10
