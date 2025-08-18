# Databricks notebook source
# ---------------------------------------------
# Install Required Libraries
# ---------------------------------------------
%pip install llama-index openai tiktoken

# COMMAND ----------

# MAGIC %run /Workspace/Users/datamatricwithabhijit@gmail.com/ai_util_mod

# COMMAND ----------

# ---------------------------------------------
# Load Sample Text Data
# ---------------------------------------------
from llama_index.core import SimpleDirectoryReader

# You can create a file or simulate loading text
sample_text = """
LlamaIndex is a powerful framework for building LLM-powered applications.
It provides support for document ingestion, indexing, and retrieval.
It works seamlessly with OpenAI, LangChain, and various vector stores.
"""

with open("sample_doc.txt", "w") as f:
    f.write(sample_text)

documents = SimpleDirectoryReader(input_files=["sample_doc.txt"]).load_data()


# COMMAND ----------

# ---------------------------------------------
# Ask Questions Using RAG
# ---------------------------------------------
response = query_engine.query("What is LlamaIndex used for?")
print("Answer:", response)

response2 = query_engine.query("Does it support OpenAI?")
print("Answer:", response2)


# COMMAND ----------

# ---------------------------------------------
# Summary
# ---------------------------------------------
# Uses OpenAI under the hood for embeddings + answers
# Indexing with LlamaIndex's VectorStoreIndex
# Simple and fast RAG workflow
# You can now extend this with PDF/Text loaders, FAISS/Chroma integration, or advanced tools.