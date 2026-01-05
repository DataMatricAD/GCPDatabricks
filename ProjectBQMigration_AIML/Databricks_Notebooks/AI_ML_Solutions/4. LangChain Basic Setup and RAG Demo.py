# Databricks notebook source
# MAGIC %pip install langchain openai chromadb tiktoken

# COMMAND ----------

# MAGIC %run /Workspace/Users/datamatricwithabhijit@gmail.com/ai_util_mod

# COMMAND ----------


# ---------------------------------------------
# Create a Simple Document Vector Store
# ---------------------------------------------
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document

# Sample knowledge base
text = """
LangChain is a framework for building applications with LLMs through composable components.
It supports chains, agents, memory, and retrieval-augmented generation (RAG).
You can integrate it with various tools like OpenAI, Hugging Face, Databricks SQL, and vector databases.
LangChain is highly modular and production ready.
"""

# Split the text into smaller chunks
documents = [Document(page_content=text)]
splitter = RecursiveCharacterTextSplitter(chunk_size=300, chunk_overlap=50)
split_docs = splitter.split_documents(documents)

# Create vector store using Chroma
vectorstore = Chroma.from_documents(split_docs, embedding=OpenAIEmbeddings(), persist_directory="lc_demo")
retriever = vectorstore.as_retriever()

# COMMAND ----------

# ---------------------------------------------
# Build a Retrieval-Augmented Chain
# ---------------------------------------------
from langchain.chains import RetrievalQA
from langchain.chat_models import ChatOpenAI

llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
qa_chain = RetrievalQA.from_chain_type(llm=llm, retriever=retriever)

# COMMAND ----------

# ---------------------------------------------
# Ask Questions with RAG
# ---------------------------------------------
query = "What is Hungging Face used for?"
response = qa_chain.run(query)
print(response)

# Try another query
print(qa_chain.run("Does Hungging Face support vector databases?"))