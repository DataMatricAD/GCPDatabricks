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























