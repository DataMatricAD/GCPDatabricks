# Databricks notebook source
# Load from Gold Layer
df = spark.table("gcp_dbsbq_migration_demo.gold_db.gold_customer_features")

# Select relevant features
df = df.select("total_orders", "unique_stores_visited", "avg_order_value", "days_since_last_order", "median_order_value")

# Convert to Pandas and normalize
pdf = df.toPandas()

from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(pdf)


# COMMAND ----------

import torch
import torch.nn as nn

input_dim = 5
latent_dim = 16

# Generator
class Generator(nn.Module):
    def __init__(self):
        super().__init__()
        self.gen = nn.Sequential(
            nn.Linear(latent_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.Linear(64, input_dim),
        )

    def forward(self, z):
        return self.gen(z)

# Discriminator
class Discriminator(nn.Module):
    def __init__(self):
        super().__init__()
        self.disc = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.LeakyReLU(0.2),
            nn.Linear(64, 32),
            nn.LeakyReLU(0.2),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        return self.disc(x)

generator = Generator()
discriminator = Discriminator()


# COMMAND ----------

from torch.utils.data import DataLoader, TensorDataset

real_data = torch.tensor(scaled_data, dtype=torch.float32)
dataset = DataLoader(real_data, batch_size=64, shuffle=True)

loss_fn = nn.BCELoss()
g_optimizer = torch.optim.Adam(generator.parameters(), lr=0.0002)
d_optimizer = torch.optim.Adam(discriminator.parameters(), lr=0.0002)

for epoch in range(100):
    for real_batch in dataset:
        # Train Discriminator
        z = torch.randn(real_batch.size(0), latent_dim)
        fake_batch = generator(z)
        
        d_real = discriminator(real_batch)
        d_fake = discriminator(fake_batch.detach())
        
        loss_real = loss_fn(d_real, torch.ones_like(d_real))
        loss_fake = loss_fn(d_fake, torch.zeros_like(d_fake))
        d_loss = loss_real + loss_fake

        d_optimizer.zero_grad()
        d_loss.backward()
        d_optimizer.step()

        # Train Generator
        z = torch.randn(real_batch.size(0), latent_dim)
        fake_batch = generator(z)
        d_fake = discriminator(fake_batch)
        g_loss = loss_fn(d_fake, torch.ones_like(d_fake))

        g_optimizer.zero_grad()
        g_loss.backward()
        g_optimizer.step()
    
    if epoch % 10 == 0:
        print(f"Epoch {epoch}: D_loss={d_loss.item():.4f}, G_loss={g_loss.item():.4f}")


# COMMAND ----------

# Generate synthetic customer features
z = torch.randn(500, latent_dim)
generated = generator(z).detach().numpy()

# Inverse scale to original values
synthetic_customers = scaler.inverse_transform(generated)

import pandas as pd
cols = ["total_orders", "unique_stores_visited", "avg_order_value", "days_since_last_order", "median_order_value"]
synthetic_df = pd.DataFrame(synthetic_customers, columns=cols)

display(synthetic_df.head())


# COMMAND ----------

spark_df = spark.createDataFrame(synthetic_df)
spark_df.write.mode("overwrite").format("delta").saveAsTable("gcp_dbsbq_migration_demo.gold_db.synthetic_customers")

# COMMAND ----------

import mlflow
import mlflow.pytorch

mlflow.set_experiment("/Users/datamatricwithabhijit@gmail.com/gan-retail-synthetic")  # Change to your path


# COMMAND ----------

from datetime import datetime

with mlflow.start_run(run_name=f"GAN-{datetime.now().strftime('%Y%m%d-%H%M%S')}"):
    # Log hyperparams
    mlflow.log_param("latent_dim", latent_dim)
    mlflow.log_param("generator_lr", 0.0002)
    mlflow.log_param("discriminator_lr", 0.0002)
    mlflow.log_param("epochs", 100)
    mlflow.log_param("batch_size", 64)

    # Training loop
    for epoch in range(100):
        for real_batch in dataset:
            # Discriminator training
            z = torch.randn(real_batch.size(0), latent_dim)
            fake_batch = generator(z)
            
            d_real = discriminator(real_batch)
            d_fake = discriminator(fake_batch.detach())
            
            loss_real = loss_fn(d_real, torch.ones_like(d_real))
            loss_fake = loss_fn(d_fake, torch.zeros_like(d_fake))
            d_loss = loss_real + loss_fake

            d_optimizer.zero_grad()
            d_loss.backward()
            d_optimizer.step()

            # Generator training
            z = torch.randn(real_batch.size(0), latent_dim)
            fake_batch = generator(z)
            d_fake = discriminator(fake_batch)
            g_loss = loss_fn(d_fake, torch.ones_like(d_fake))

            g_optimizer.zero_grad()
            g_loss.backward()
            g_optimizer.step()

        # Log loss at each 10th epoch
        if epoch % 10 == 0:
            mlflow.log_metric("generator_loss", g_loss.item(), step=epoch)
            mlflow.log_metric("discriminator_loss", d_loss.item(), step=epoch)
            print(f"Epoch {epoch}: D_loss={d_loss.item():.4f}, G_loss={g_loss.item():.4f}")


# COMMAND ----------

# Save trained Generator to MLflow
mlflow.pytorch.log_model(generator, "model_generator")

# Save model output samples (optional)
sample_df = pd.DataFrame(synthetic_customers[:10], columns=cols)
sample_path = "/dbfs/tmp/synthetic_sample.csv"
sample_df.to_csv(sample_path, index=False)
mlflow.log_artifact(sample_path)
