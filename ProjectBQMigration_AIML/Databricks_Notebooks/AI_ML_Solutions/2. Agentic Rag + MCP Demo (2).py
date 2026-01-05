# Databricks notebook source
# MAGIC %md
# MAGIC # Agentic AI Assistant with RAG + Databricks SQL
# MAGIC
# MAGIC This notebook builds a simple yet powerful **Agentic AI** assistant capable of:
# MAGIC
# MAGIC - Understanding natural language queries
# MAGIC - Retrieving relevant schema context using RAG
# MAGIC - Generating SQL queries via GPT-4
# MAGIC - Executing the SQL on Databricks
# MAGIC - Returning clean, readable results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1: Install Required Libraries
# MAGIC
# MAGIC Install all necessary Python libraries such as:
# MAGIC - LangChain
# MAGIC - OpenAI
# MAGIC - ChromaDB
# MAGIC - Databricks SQL Connector
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2: Restart Python Kernel
# MAGIC
# MAGIC Restart the Python environment to ensure all installed packages are correctly loaded.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3: Import Utility Configuration
# MAGIC
# MAGIC Run a helper notebook (`ai_util_mod`) to load any Databricks-specific configs like `DATABRICKS_CONFIG`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4: Load Schema and Create Vector Store
# MAGIC
# MAGIC - Load a schema description of four Delta tables.
# MAGIC - Split the schema text into retrievable chunks.
# MAGIC - Generate vector embeddings using OpenAI.
# MAGIC - Store embeddings in a persistent local ChromaDB directory.
# MAGIC - Create a retriever to perform similarity searches over the schema text.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Define LangChain Tools
# MAGIC
# MAGIC Create three custom tools:
# MAGIC
# MAGIC 1. **`retrieve_context`** – retrieves relevant schema chunks from ChromaDB based on the user’s query.
# MAGIC 2. **`generate_sql`** – uses GPT-4 to translate schema context and query into a valid SQL statement.
# MAGIC 3. **`execute_sql`** – runs the generated SQL query on Databricks SQL and returns the result as a markdown table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 6: Build the AI Agent
# MAGIC
# MAGIC - Define a system prompt that explains the role of each tool to the LLM.
# MAGIC - Configure LangChain to use `OpenAI Function Calling` with the defined tools.
# MAGIC - Assemble the agent using the tools and prompt.
# MAGIC - Wrap it in an `AgentExecutor` to handle step-by-step reasoning and return intermediate steps.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 7: Ask a Question to the Agent
# MAGIC
# MAGIC - Provide an input query such as "Share the top 10 products."
# MAGIC - The agent:
# MAGIC   - Retrieves the schema context
# MAGIC   - Generates a SQL query
# MAGIC   - Executes it on Databricks
# MAGIC   - Returns a nicely formatted markdown response with 3 parts:
# MAGIC     - **[1] Retrieved Context**
# MAGIC     - **[2] Generated SQL**
# MAGIC     - **[3] SQL Result**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Outcome
# MAGIC
# MAGIC You now have an **end-to-end, RAG-powered, SQL-generating AI assistant** in Databricks that can interpret business questions and return actionable insights—all using natural language.
# MAGIC
# MAGIC
# MAGIC

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

#Load Schema & Create Vector Store
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document

schema_text = """
Table: gcp_dbsbq_migration_demo.gold_db.gold_customer_features
Columns: customer_id, first_name, last_name, total_orders, unique_stores_visited, lifetime_value, avg_order_value, first_order_date, last_order_date, days_since_last_order, median_order_value

Table: gcp_dbsbq_migration_demo.gold_db.gold_daily_store_sales
Columns: order_date, store_id, store_name, total_orders, revenue

Table: gcp_dbsbq_migration_demo.gold_db.gold_inventory_health
Columns: store_id, store_name, product_id, product_name, stock, stock_status

Table: gcp_dbsbq_migration_demo.gold_db.gold_top_products
Columns: product_id, product_name, category, total_quantity_sold, total_revenue
"""

splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
documents = splitter.split_documents([Document(page_content=schema_text)])
vectorstore = Chroma.from_documents(documents, embedding=OpenAIEmbeddings(), persist_directory="rag_mcp_demo")
retriever = vectorstore.as_retriever()

# COMMAND ----------

#Define MCP Functions for Tool Use
from langchain.chat_models import ChatOpenAI
from langchain.agents import tool, AgentExecutor, create_openai_functions_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.schema import SystemMessage
import databricks.sql
import pandas as pd

llm = ChatOpenAI(model="gpt-4", temperature=0)

@tool
def retrieve_context(question: str) -> str:
    """Retrieve relevant schema context from vectorstore."""
    docs = retriever.get_relevant_documents(question)
    return "\n".join([doc.page_content for doc in docs])

@tool
def generate_sql(context_and_question: str) -> str:
    """Generate SQL from schema context and user question."""
    prompt = f"""
You are a SQL expert. Based on the schema below, generate a correct SQL query.

{context_and_question}

Only return valid SQL syntax.
"""
    response = llm.predict(prompt)
    return response.strip()

@tool
def execute_sql(query: str) -> str:
    """Execute SQL on Databricks and return result as markdown."""
    try:
        conn = databricks.sql.connect(**DATABRICKS_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        conn.close()
        return df.to_markdown(index=False)
    except Exception as e:
        return f"Execution Error: {str(e)}"


# COMMAND ----------

#Create Agent with OpenAI Function Calling

from langchain.agents import Tool

tools = [retrieve_context, generate_sql, execute_sql]

system_message = SystemMessage(content="""
You are an AI assistant that uses three tools:
1. `retrieve_context` - for schema lookup
2. `generate_sql` - to write SQL using schema + question
3. `execute_sql` - to run SQL and return results

For every user query, show the output of each tool step-by-step:
- First, show the retrieved schema context.
- Second, show the generated SQL.
- Third, show the SQL execution result.

Return your answer as a markdown section with three clearly labeled parts: 
**[1] Retrieved Context**, **[2] Generated SQL**, **[3] SQL Result**.
""")

prompt = ChatPromptTemplate.from_messages([
    system_message,
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

agent = create_openai_functions_agent(llm=llm, tools=tools, prompt=prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, return_intermediate_steps=True)

# COMMAND ----------

# Ask Agentic RAG Question

response = agent_executor.invoke({
    "input": "Share the top 10 products",
    "chat_history": [],  # Add an empty list or the appropriate chat history
    "agent_scratchpad": ""  # Add the appropriate value for agent_scratchpad if needed
})
print(response["output"])