# GCPDatabricks
GCP Databricks Project
![image](https://github.com/user-attachments/assets/ea4f6544-f98f-49c6-a3d2-b5c672660f33)

Create Google Cloud Account.
![image](https://github.com/user-attachments/assets/4011b8a0-3b50-4b3d-b26b-dc351c7bb129)

Login with your Gmail account credentials.
![image](https://github.com/user-attachments/assets/79510ab9-9911-481c-9746-dd097d31c1fb)

Google Cloud Account (Billing tab)
![image](https://github.com/user-attachments/assets/052c58e3-5487-4460-8dc3-7eeb454354ed)

Create a separate project for implementing Databricks Project.
![image](https://github.com/user-attachments/assets/2956ccee-3cbe-4a2d-af41-851bf761a3d3)
![image](https://github.com/user-attachments/assets/d4597e7a-9b4c-4b76-9582-e91d2237d2a4)

To create GCP Databricks account, we need to subscribe to Databricks.
Find Databricks under Marketplace (Partner Solutions)
![image](https://github.com/user-attachments/assets/d9d9d834-5c9a-40e7-8aba-5bf3d2721bdf)
![image](https://github.com/user-attachments/assets/1b8edc4e-b9fd-420f-9f12-a6a76812ad79)
![image](https://github.com/user-attachments/assets/ec42efbf-0ee3-467f-bb3f-06731c086609)
![image](https://github.com/user-attachments/assets/a5d70402-981d-4d4a-b5e6-c2774687a52a)

Click on Activate
![image](https://github.com/user-attachments/assets/6f43cfcc-c019-4d1a-9677-63fa1dc1570f)

No need to activate the full account, unless you want to use any specific services. (experienced a few issues with CPU quota, which we can adjust by using different series)
![image](https://github.com/user-attachments/assets/8264b67e-ae62-490d-a8f3-0bb45356063f)

Now you are ready and click on Manage On Provider to navigate to Databricks Account page.
![image](https://github.com/user-attachments/assets/0c8a7b59-e5d0-490f-a7d5-d0b2fb3e02c0)
![image](https://github.com/user-attachments/assets/198d4168-f96b-4699-9a7b-1faf9e18ae88)

Click on “Continue With Google” account that we used for Databricks account registration and select the plan
![image](https://github.com/user-attachments/assets/fb47c6bf-e35f-4162-b9bd-69fc0657754e)

Now we can create Databricks Workspace.
![image](https://github.com/user-attachments/assets/bcf60a6c-7202-4bb8-826f-401329744ac0)

To create workspace, we need to fetch/copy GCP project-id.
![image](https://github.com/user-attachments/assets/83e98aa8-e4ea-4e0e-8737-2a11571d160b)

Enter the details to create Databricks – Workspace. 
![image](https://github.com/user-attachments/assets/a2f15f6d-9380-407f-aef2-082a25a307c5)
We will keep other details unchanged for the demo. Click on “Save”
![image](https://github.com/user-attachments/assets/755d9bc4-ca4d-4bea-8e80-a0d498f55ad2)

Note: Understanding quota limits is essential for running jobs and clusters in GCP Databricks. link.
When you create a Databricks workspace, two Google Cloud Storage (GCS) buckets are automatically provisioned in your GCP project. These buckets are used to store data for external and internal DBFS (Databricks File System) storage. To learn how to manage access permissions for these buckets in the GCP Console, link.

It will open a window to authenticate your credentials. Select appropriate gmail-account.
![image](https://github.com/user-attachments/assets/de70f284-92d5-4a0c-8653-14530b1cde77)

Now your Databricks workspace will be provisioned and run.
![image](https://github.com/user-attachments/assets/371cc66a-1851-4976-a1e6-1a231b65892c)
![image](https://github.com/user-attachments/assets/930233ef-3335-4ba6-a8ed-9443553a2063)

Unity-Catalog will be created as well.
![image](https://github.com/user-attachments/assets/cccf818e-fdad-460f-90a6-a3da6575d274)

Open workspace.
![image](https://github.com/user-attachments/assets/dfd5f333-9a7c-466d-99f6-8f88168a3dcc)
![image](https://github.com/user-attachments/assets/f881e370-c7d1-481d-b457-1ae65784a788)

Here is your workspace.
![image](https://github.com/user-attachments/assets/b772ba6e-bd50-43d2-84b5-85e01a9aabab)

Now that Databricks workspace has been created. We can go ahead and create cluster. Next we will connect to Google Cloud Storage.
To create cluster, we need to navigate to Compute and “Create Compute”. 
![image](https://github.com/user-attachments/assets/dcafff6c-81a9-44c3-9f44-ccff01863bbb)

If you are having trouble to create cluster due to quota. Then we need to look into available quotas under IAM & Admin / Quotas. I was able to see A2 and C2 family CPUs with Value – 8. If you are looking for any specific family, then you can request for a quota increase.

![image](https://github.com/user-attachments/assets/746e3c23-65b6-4f22-aa6e-7a694d946598)

Connect to Google Cloud Storage: https://docs.databricks.com/gcp/en/connect/storage/gcs
Set up Google Cloud service account using Google Cloud Console

![image](https://github.com/user-attachments/assets/6687c9bb-f773-42ed-b4d6-d7ecd16597cd)
![image](https://github.com/user-attachments/assets/54fcce79-c082-414d-8b64-42d178222b56)
![image](https://github.com/user-attachments/assets/322ec33a-c721-4bae-9c10-02470e8a7e34)

Service Account has been created. Click on this service account to get the email-id for Databricks Compute Cluster
![image](https://github.com/user-attachments/assets/9570cd35-4b97-40a5-aa05-8dc070419c62)
![image](https://github.com/user-attachments/assets/22e6c6e0-2198-4f53-b388-f8d36768818e)

Next Step - Configure your GCS bucket 
Note: To work with DBFS mounts, your bucket name must not contain an underscore.
![image](https://github.com/user-attachments/assets/559aae7a-893b-4558-9d69-8c6de57cd9cc)
![image](https://github.com/user-attachments/assets/4971d3a1-b42c-4e28-8ec2-731999954415)

Configure the bucket:
![image](https://github.com/user-attachments/assets/b1da8021-1593-4f44-920b-ac029208131b)

Provide the desired permission to the service account on the bucket from the Cloud Storage roles - > Storage Admin: Grants full privileges on this bucket.
![image](https://github.com/user-attachments/assets/b6630263-f1fc-4b14-95db-6a84d950d5a1)

Optional: Add keys for global configuration (Click Keys. The Google Cloud console displays a list of keys for the service account, including metadata for each key)
Note: We can mount GCS bucket into Databricks without using private-keys. 

Configure a Databricks cluster - > When you configure your cluster, expand Advanced and set the Google Service Account field to your service account email address.
![image](https://github.com/user-attachments/assets/7aff8084-07d5-4e99-a0f5-41e740f1b44b)

After starting the cluster, we should be able to mount GCS Bucket and read files.
![image](https://github.com/user-attachments/assets/0cb7a560-51dc-403b-b913-bfd823964ba3)
Now we should be able to read files from GCS by using Databricks Mount.
![image](https://github.com/user-attachments/assets/8ebb6369-a058-446f-8f60-91d6eca6a34a)

Integrate Databricks With BigQuery
Connect to BigQuery Storage API - Enable the BigQuery Storage API.
![image](https://github.com/user-attachments/assets/d53d2af1-6f8b-443d-adab-6b88362987ad)

Now we need to allow a service account to access to BigQuery however we are not allowed to attach multiple service accounts to a single Databricks cluster. GCP IAM doesn’t support “multiple active identities” on the same compute resource. 
Workarounds:
•	Use a Service Account with Access to Multiple Resources: Grant one service account access to all required resources (e.g., BigQuery, GCS buckets, Pub/Sub). This is the recommended pattern in most use cases.
•	Assign Different Service Accounts to Each Job Cluster: If your workflow requires different identities for data isolation, break it into separate job clusters using different service accounts.
To run the pipeline end to end, we need to have two active Databricks clusters, however, I am able to keep only one cluster due to limited resource for this demo.
![image](https://github.com/user-attachments/assets/1637dff6-0149-4a9c-abb4-6724065486f1)
![image](https://github.com/user-attachments/assets/37e2be13-db72-4d56-9543-25f93a19eea2)

As a workaround, I will be granting one service account access to all required resources – GCS and BigQuery.
![image](https://github.com/user-attachments/assets/59292c70-d653-4ec3-839a-6123fbc5629a)

Now it's time to build the data pipeline. I'll be using the following dataset to construct and demonstrate the pipeline.
![image](https://github.com/user-attachments/assets/76c68045-f0e9-4b66-af67-621d826e390d)

Environment Setup and common utility notebooks for bronze, silver and gold data processing and load.
![image](https://github.com/user-attachments/assets/1a0a9ca3-40d3-4153-bf2e-e9337c84a80e)
![image](https://github.com/user-attachments/assets/fd05a106-51ad-4b63-aea1-b82b87fc0b1c)

Utility Module has functions required for all three layers. Those functions will be used as a library, if we need to add more new datasets in future.
util_mod
![image](https://github.com/user-attachments/assets/d8a13de2-8f13-4486-8818-6fb8fa4a7887)
![image](https://github.com/user-attachments/assets/6aa9beab-068d-4f12-a25e-d1ddfe774a4f)

Bronze Layer loads data from GCS (bucket – using Databricks mount) to Databricks dronze_db as delta table.
![image](https://github.com/user-attachments/assets/a460cb25-09b2-41bf-a60c-91528b7434c1)

Silver Layer loads data from bronze_db to silver_db as delta table. It performs upsert with data cleaning, dedup and other processing. It will read only latest partition from bronze_db table.
![image](https://github.com/user-attachments/assets/377334da-467f-4335-a031-6584bd9e70a8)

Gold layer will perform aggregation on silver_db tables and prepare final order_details table in gold_db. This order_details table (in gold layer) will be used to create / load different aggregated tables in BigQuery (consumption / serving). Power-BI reports will be generated from those tables in BigQuery.
![image](https://github.com/user-attachments/assets/14c6e950-906e-40c6-9553-ed0b8f9c1326)

Now we have prepared a workflow (data-pipeline) to run those steps in sequence. This can be schedules or run on demand.
![image](https://github.com/user-attachments/assets/b57a2f2e-8578-4f5f-aa5b-38ba1ef843fd)
![image](https://github.com/user-attachments/assets/5c2235ce-b71c-4e72-93a6-6329fd972558)
![image](https://github.com/user-attachments/assets/df9073ad-aad4-4c7f-b020-f18fa5522694)

Create dataset in your region for your project-id
![image](https://github.com/user-attachments/assets/9f27c5ef-5de5-4f5b-9e14-e7d9caf31828)

Connect to BigQuery from Power BI
![image](https://github.com/user-attachments/assets/b5d3dc75-3d5b-41bf-bf31-3c283f8ead20)



















































