# Databricks notebook source
dbutils.widgets.text("sourceWorskpaceUrl", "https://adb-0123456789.azuredatabricks.net/", "Source Workspace URL")
dbutils.widgets.text("sourceWorkspacePat", "dapi1234567890", "Source Workspace PAT")
dbutils.widgets.text("targetWorskpaceUrl", "https://adb-0123456789.azuredatabricks.net/", "Target Workspace URL")
dbutils.widgets.text("targetWorkspacePat", "dapi1234567890", "Target Workspace PAT")

# COMMAND ----------

sourceWorskpaceUrl = dbutils.widgets.get("sourceWorskpaceUrl")
sourceWorkspacePat = dbutils.widgets.get("sourceWorkspacePat")
targetWorskpaceUrl = dbutils.widgets.get("targetWorskpaceUrl")
targetWorkspacePat = dbutils.widgets.get("targetWorkspacePat")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Resources Validator
# MAGIC 1. Users/Groups
# MAGIC 2. Notebooks
# MAGIC 3. Jobs
# MAGIC 4. Clusters
# MAGIC 5. Instance Pools
# MAGIC 6. Mounts
# MAGIC 7. Init Scripts
# MAGIC 8. Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secrets Validation

# COMMAND ----------

import requests

def get_secret_scopes(workspace, token):
  response = requests.get(
    f'{workspace}api/2.0/secrets/scopes/list',
    headers={
      'Authorization': f'Bearer {token}'
    }
  )
  scopes = response.json()['scopes']
  xformed_scopes = [{
    'workspace': workspace,
    'name': scope['name'],
    'databricks_scope': True if scope['backend_type'] == 'DATABRICKS' else False,
    'azure_keyvault_scope': True if scope['backend_type'] == 'AZURE_KEYVAULT' else False,
    'keyvault_resource_id': scope['keyvault_metadata']['resource_id'] if scope['backend_type'] == 'AZURE_KEYVAULT' else None,
    'keyvault_dns_name': scope['keyvault_metadata']['dns_name'] if scope['backend_type'] == 'AZURE_KEYVAULT' else None
  } for scope in scopes]
  return spark.createDataFrame(xformed_scopes,
                             'workspace string, name string, databricks_scope string, azure_keyvault_scope string, keyvault_resource_id string, keyvault_dns_name string')

source_scopes = get_secret_scopes(sourceWorskpaceUrl, sourceWorkspacePat)
target_scopes = get_secret_scopes(targetWorskpaceUrl, targetWorkspacePat)

validation_df = source_scopes.drop("workspace").exceptAll(target_scopes.drop("workspace"))
if validation_df.count() == 0:
  print('Secret scopes match between workspaces!')
else:
  print('Secret scopes don\'t match between workspaces!')
  validation_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Untility for Re-adding Key Vault Scopes

# COMMAND ----------

def recreate_keyvault_scope(workspace, token, scope_name, resource_id, dns_name, initial_manage_principal):
  response = requests.post(
    f'{workspace}api/2.0/secrets/scopes/create',
    headers={
      'Authorization': f'Bearer {token}'
    }
    json={
      "scope": scope_name,
      "scope_backend_type": "AZURE_KEYVAULT",
      "backend_azure_keyvault": {
        "resource_id": resource_id,
        "dns_name": dns_name
      },
      "initial_manage_principal": initial_manage_principal
    }
  )


# COMMAND ----------

def get_interactive_cluster(workspace, token):
  response = requests.get(
    f'{workspace}api/2.0/clusters/list',
    headers={
        'Authorization': f'Bearer {token}'
    }
  )
  clusters = response.json()['clusters']
  return [{
    'cluster_id': cluster['cluster_id']
    'cluter_came: cluster['cluster_']
          } for cluster in clusters]

missing_interactive_clusters = get_interactive_cluster(sourceWorskpaceUrl, sourceWorkspacePat)

# COMMAND ----------
