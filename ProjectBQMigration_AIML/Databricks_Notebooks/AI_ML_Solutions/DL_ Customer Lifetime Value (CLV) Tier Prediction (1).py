# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Value Tier Classification using PyTorch in Databricks
# MAGIC
# MAGIC ## Objective
# MAGIC Build and evaluate a deep learning model to classify retail customers into **value tiers** — `low`, `medium`, or `high` — based on behavioral and transactional metrics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dataset Used
# MAGIC Table: `gcp_dbsbq_migration_demo.gold_db.gold_customer_features`
# MAGIC
# MAGIC ### Selected Features:
# MAGIC - `total_orders`
# MAGIC - `unique_stores_visited`
# MAGIC - `avg_order_value`
# MAGIC - `days_since_last_order`
# MAGIC - `median_order_value`
# MAGIC - `value_tier` (derived label based on `lifetime_value`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Feature Engineering
# MAGIC - Added `value_tier` column using conditional logic on `lifetime_value`:
# MAGIC   - `< 500` → `low`
# MAGIC   - `500 to <1500` → `medium`
# MAGIC   - `>=1500` → `high`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Preprocessing
# MAGIC - Converted Spark DataFrame to Pandas.
# MAGIC - Applied **Label Encoding** to `value_tier`.
# MAGIC - Standardized numerical features using **StandardScaler**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Model Architecture (PyTorch)
# MAGIC ### `CLVNet` Neural Network:
# MAGIC - Input Layer → 64 → ReLU → Dropout(0.3)  
# MAGIC - Hidden Layer → 32 → ReLU  
# MAGIC - Output Layer → 3 units (for 3 classes)
# MAGIC
# MAGIC ### Training Details:
# MAGIC - Loss Function: `CrossEntropyLoss`
# MAGIC - Optimizer: `Adam`
# MAGIC - Epochs: `20`
# MAGIC - Batch Size: `32`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Model Training
# MAGIC Tracked and printed training loss per epoch.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Model Evaluation
# MAGIC ### Metrics:
# MAGIC - **Precision**, **Recall**, **F1-score**, and **Support** for each class (`low`, `medium`, `high`)
# MAGIC - **Accuracy**, **Macro avg**, **Weighted avg**
# MAGIC
# MAGIC Used `classification_report` and `confusion_matrix` from `sklearn`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Visualizations
# MAGIC 1. **Training Loss Curve** over 20 epochs.
# MAGIC 2. **Confusion Matrix** Heatmap for true vs predicted labels.
# MAGIC 3. Classification Report as a DataFrame for in-depth analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC This notebook demonstrates an end-to-end ML pipeline using PyTorch in Databricks to classify customers based on value tiers. It includes data extraction, feature engineering, model building, evaluation, and visualization.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import when

df = spark.table("gcp_dbsbq_migration_demo.gold_db.gold_customer_features")

df = df.withColumn(
    "value_tier",
    when(df.lifetime_value < 500, "low")
    .when((df.lifetime_value >= 500) & (df.lifetime_value < 1500), "medium")
    .otherwise("high")
)

selected_df = df.select(
    "total_orders",
    "unique_stores_visited",
    "avg_order_value",
    "days_since_last_order",
    "median_order_value",
    "value_tier"
)


# COMMAND ----------

pdf = selected_df.toPandas()

from sklearn.preprocessing import LabelEncoder, StandardScaler

X = pdf.drop("value_tier", axis=1)
y = LabelEncoder().fit_transform(pdf["value_tier"])  # 0 = low, 1 = medium, 2 = high

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


# COMMAND ----------

import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader

X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
y_tensor = torch.tensor(y, dtype=torch.long)

dataset = TensorDataset(X_tensor, y_tensor)
loader = DataLoader(dataset, batch_size=32, shuffle=True)

class CLVNet(nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(X.shape[1], 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 3)  # 3 classes: low, medium, high
        )

    def forward(self, x):
        return self.layers(x)

model = CLVNet()
loss_fn = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)


# COMMAND ----------

for epoch in range(20):
    total_loss = 0
    for xb, yb in loader:
        pred = model(xb)
        loss = loss_fn(pred, yb)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    print(f"Epoch {epoch+1}: Loss = {total_loss:.4f}")


# COMMAND ----------

from sklearn.metrics import classification_report

with torch.no_grad():
    y_pred = model(X_tensor).argmax(dim=1).numpy()

print(classification_report(y, y_pred, target_names=["low", "medium", "high"]))


# COMMAND ----------

from IPython.display import display, Markdown

markdown_text = """
### Classification Report Explanation

The table below summarizes the performance of the classification model for each class ("low", "medium", "high"):

- **precision**: The ratio of correctly predicted positive observations to the total predicted positives for each class.
- **recall**: The ratio of correctly predicted positive observations to all actual positives for each class.
- **f1-score**: The weighted average of precision and recall for each class.
- **support**: The number of actual occurrences of each class in the dataset.

The report also includes macro, weighted, and accuracy metrics to provide an overall assessment of the model's performance.

"""

display(Markdown(markdown_text))
display(report_df)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Visualize training loss over epochs
losses = []
for epoch in range(20):
    total_loss = 0
    for xb, yb in loader:
        pred = model(xb)
        loss = loss_fn(pred, yb)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    losses.append(total_loss)

plt.figure(figsize=(8, 4))
plt.plot(range(1, 21), losses, marker='o')
plt.title("Training Loss over Epochs")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.grid(True)
plt.show()

# Visualize classification report as a heatmap
from sklearn.metrics import confusion_matrix

with torch.no_grad():
    y_pred = model(X_tensor).argmax(dim=1).numpy()

cm = confusion_matrix(y, y_pred)
labels = ["low", "medium", "high"]

plt.figure(figsize=(6, 5))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=labels, yticklabels=labels)
plt.title("Confusion Matrix")
plt.xlabel("Predicted")
plt.ylabel("Actual")
plt.show()

# Display classification report as a DataFrame
report = classification_report(y, y_pred, target_names=labels, output_dict=True)
report_df = pd.DataFrame(report).transpose()
display(report_df)