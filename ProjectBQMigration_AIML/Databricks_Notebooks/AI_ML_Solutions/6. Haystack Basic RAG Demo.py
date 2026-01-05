# Databricks notebook source
# MAGIC %pip install farm-haystack==1.26.4

# COMMAND ----------

# MAGIC %run /Workspace/Users/datamatricwithabhijit@gmail.com/ai_util_mod

# COMMAND ----------

# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install farm-haystack pandas --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ---------------------------------------------
# Step 2: Load Sample Document
# ---------------------------------------------
from haystack.nodes import EmbeddingRetriever, PromptNode
from haystack.document_stores import InMemoryDocumentStore
from haystack.pipelines import Pipeline
from haystack import Document

# Sample text
doc = Document(content="""
Haystack is an open-source framework for building search and RAG-based applications.
It supports pipelines for document retrieval and generation using LLMs.
Haystack integrates with OpenAI, Cohere, Hugging Face, and more.
""")


# COMMAND ----------

# ---------------------------------------------
# Step 3: Set Up DocumentStore and Retriever
# ---------------------------------------------
document_store = InMemoryDocumentStore(use_bm25=True)
document_store.write_documents([doc])

retriever = EmbeddingRetriever(
    document_store=document_store,
    embedding_model="sentence-transformers/all-MiniLM-L6-v2"
)
document_store.update_embeddings(retriever)

# COMMAND ----------

# ---------------------------------------------
# Step 4: Use PromptNode with OpenAI
# ---------------------------------------------
import os
os.environ["OPENAI_API_KEY"] = "sk-..."  # Set your API key here

prompt_node = PromptNode(
    model_name_or_path="gpt-3.5-turbo",
    api_key=os.environ["OPENAI_API_KEY"],
    default_prompt_template="question-answering",
    use_openai=True
)


# COMMAND ----------

# ---------------------------------------------
# Step 5: Build Pipeline with Text Output
# ---------------------------------------------
rag_pipeline = Pipeline()
rag_pipeline.add_node(component=retriever, name="Retriever", inputs=["Query"])
rag_pipeline.add_node(component=prompt_node, name="PromptNode", inputs=["Retriever"])


# COMMAND ----------

# ---------------------------------------------
# Step 6: Ask Questions and Return Plain Text
# ---------------------------------------------
def ask_question(query: str) -> str:
    try:
        result = rag_pipeline.run(query=query, params={})
        return result["answers"][0].answer if result["answers"] else "No answer found."
    except Exception as e:
        return f"Error: {str(e)}"

# Example questions
print("Q1:", ask_question("What is Haystack used for?"))
print("Q2:", ask_question("Does it support Hugging Face models?"))

# COMMAND ----------

# ---------------------------------------------
# Step 7: Summary
# ---------------------------------------------
# ✅ Uses latest working version of Haystack
# ✅ LLM via OpenAI GPT-3.5 PromptNode
# ✅ Pydantic-safe, returns string outputs only
