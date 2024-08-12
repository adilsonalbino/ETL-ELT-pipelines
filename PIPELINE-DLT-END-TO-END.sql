-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Criando um Pipeline com Delta Live Tables(DLT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Criando a tabela da camada bronze

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE clickStream_raw
COMMENT "Dados brutos de clicks da wikipedia, obtido do /databrics-datasets"
TBLPROPERTIES ("Quality" = "Bronze")
AS 
SELECT * FROM JSON.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Criando a tabela da camada silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O Delta Live Tables tem uma caractística importante que é o controle da qualidade dos dados. Usei o exemplo abaixo com o comando "CONSTRAINT" que nos permite adicionar algumas regras de validação da qualidade dos dados antes de adicionar dentro da nossa tabela, essa validação é executada linha por linha. 
-- MAGIC
-- MAGIC Existe algumas ações que podem ser executadas caso a informação esteja dentro da nossa regra de validação:
-- MAGIC
-- MAGIC **1- ON VIOLATION FAIL UPDATE** - Caso queiramos interromper o pipeline
-- MAGIC
-- MAGIC **2- ON VIOLATION DROP UPDATE** - Caso queiramos remover 
-- MAGIC
-- MAGIC **3- NÃO FAZER NADA**(Mas o DLT irá documentar que aquele registro não atendeu os requistos de qualidade)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE clickstream_prepared(
  CONSTRAINT valid_current_page EXPECT(current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Dados de clicks da wikipedia limpos preparados para análise"
TBLPROPERTIES ("Quality" = "Silver")
AS SELECT 
  curr_title AS current_page_title,
  CAST (n AS INT) AS click_count,
  prev_title AS previous_page_title
FROM LIVE.clickStream_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Criando a tabela da camada gold

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE top_spark_referes
COMMENT "Tabela com as principais páginas com links para página do apache spark!"
TBLPROPERTIES ("Quality" = "Gold")
AS SELECT
  previous_page_title as referrer,
  click_count
FROM live.clickstream_prepared
WHERE current_page_title = "Apache_Spark"
ORDER BY click_count DESC
LIMIT 7
