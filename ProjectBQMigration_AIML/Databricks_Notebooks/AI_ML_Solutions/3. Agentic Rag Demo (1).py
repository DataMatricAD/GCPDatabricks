# Databricks notebook source
# MAGIC %md
# MAGIC # Agentic RAG SQL Assistant in Databricks – Step-by-Step Summary
# MAGIC
# MAGIC This notebook builds a lightweight Retrieval-Augmented Generation (RAG) assistant using **LangChain**, **GPT-4**, and **Databricks SQL** to answer business questions with context-aware SQL generation and execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1: Install Required Libraries
# MAGIC Install essential libraries:
# MAGIC - `openai`, `langchain`, `tiktoken`, `chromadb`, `databricks-sql-connector`
# MAGIC
# MAGIC These enable interaction with LLMs, vector search, and Databricks SQL.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2: Restart Python Kernel
# MAGIC Restart the Python environment to ensure proper loading of newly installed packages.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3: Import Databricks Config
# MAGIC Run a utility notebook (`ai_util_mod`) to import `DATABRICKS_CONFIG` used for SQL connections.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4: Create SQL Execution Function
# MAGIC Defines a utility function `run_sql(sql)` to:
# MAGIC - Connect to Databricks SQL
# MAGIC - Run the generated SQL query
# MAGIC - Return results as a Pandas DataFrame
# MAGIC - Handle any execution errors
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Embed Table Schema as Knowledge Base
# MAGIC Schema for four tables from `gcp_dbsbq_migration_demo.gold_db` is:
# MAGIC - Loaded as a single document
# MAGIC - Split into chunks using `RecursiveCharacterTextSplitter`
# MAGIC - Embedded using `OpenAIEmbeddings`
# MAGIC - Stored in a local `Chroma` vector database
# MAGIC - Converted into a retriever
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 6: Create Retrieval-Augmented QA Chain
# MAGIC - Loads `ChatOpenAI` with `gpt-4` model
# MAGIC - Wraps the vectorstore retriever into a `RetrievalQA` chain
# MAGIC - Enables schema-aware context retrieval based on input queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 7: Prompt Template for SQL Generation
# MAGIC - Custom LangChain prompt template defines how to generate SQL from:
# MAGIC   - Retrieved context
# MAGIC   - User question
# MAGIC - The `LLMChain` is created using this prompt + LLM
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 8: Agent Function – `agentic_rag_sql()`
# MAGIC This core function:
# MAGIC 1. Retrieves schema context related to the input question
# MAGIC 2. Generates SQL using the LLM and context
# MAGIC 3. Executes SQL on Databricks using `run_sql()`
# MAGIC 4. Returns both the generated SQL and result DataFrame
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 9: Test the Agent
# MAGIC - Run a sample query like `"Share top 10 products"`
# MAGIC - Print the generated SQL
# MAGIC - Display the results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Outcome
# MAGIC This notebook successfully implements an **RAG + SQL generation assistant**, capable of:
# MAGIC - Understanding user questions
# MAGIC - Retrieving relevant schema knowledge
# MAGIC - Generating and executing accurate SQL on Databricks
# MAGIC - Delivering results in a structured format
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

#Install Required Libraries
%pip install openai langchain tiktoken chromadb databricks-sql-connector

# COMMAND ----------

# MAGIC %pip install --upgrade langchain langchain-openai langchain-community

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run /Workspace/Users/datamatricwithabhijit@gmail.com/ai_util_mod

# COMMAND ----------

#Create SQL Query Runner
import databricks.sql
import pandas as pd

def run_sql(sql: str) -> pd.DataFrame:
    try:
        conn = databricks.sql.connect(**DATABRICKS_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        return pd.DataFrame([[f"Error: {str(e)}"]], columns=["message"])

# COMMAND ----------

# Load and Embed Schema into Vector Store

from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document

schema_knowledge = """
Table: gcp_dbsbq_migration_demo.gold_db.gold_customer_features
Columns: customer_id, first_name, last_name, total_orders, unique_stores_visited,
         lifetime_value, avg_order_value, first_order_date, last_order_date, 
         days_since_last_order, median_order_value

Table: gcp_dbsbq_migration_demo.gold_db.gold_daily_store_sales
Columns: order_date, store_id, store_name, total_orders, revenue

Table: gcp_dbsbq_migration_demo.gold_db.gold_inventory_health
Columns: store_id, store_name, product_id, product_name, stock, stock_status

Table: gcp_dbsbq_migration_demo.gold_db.gold_top_products
Columns: product_id, product_name, category, total_quantity_sold, total_revenue
"""

docs = [Document(page_content=schema_knowledge)]
splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
split_docs = splitter.split_documents(docs)

vectorstore = Chroma.from_documents(split_docs, embedding=OpenAIEmbeddings(), persist_directory="rag_chroma")

# COMMAND ----------

#Create Retrieval-Augmented Chain
from langchain.chat_models import ChatOpenAI
from langchain.chains import RetrievalQA

llm = ChatOpenAI(temperature=0, model="gpt-4")
retriever = vectorstore.as_retriever()
rag_chain = RetrievalQA.from_chain_type(llm=llm, retriever=retriever)

# COMMAND ----------

#Generate SQL and Execute It
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

sql_prompt = PromptTemplate(
    input_variables=["question"],
    template="""
You are a SQL expert. Based on the following table schema:

{context}

Generate a valid SQL query using tables in `gcp_dbsbq_migration_demo.gold_db`.
Do not use markdown or backticks. Only return valid SQL.

User Question: {question}
SQL:
"""
)

sql_chain = LLMChain(llm=llm, prompt=sql_prompt)

def agentic_rag_sql(query: str):
    # Retrieve relevant context
    context_docs = retriever.get_relevant_documents(query)
    context = "\n".join([doc.page_content for doc in context_docs])

    # Generate SQL
    sql = sql_chain.run({"question": query, "context": context})
    df_result = run_sql(sql)

    return sql, df_result



# COMMAND ----------

#Test the Agentic RAG

question = "Share top 10 products"
sql, result_df = agentic_rag_sql(question)

print("Generated SQL:\n", sql)
display(result_df)