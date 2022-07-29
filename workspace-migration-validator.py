# Databricks notebook source
dbutils.widgets.text("sourceWorkspaceUrl", "https://adb-01234567890.azuredatabricks.net/", "Source Workspace URL")
dbutils.widgets.text("sourceWorkspacePat", "dapi01234567890", "Source Workspace PAT")
dbutils.widgets.text("targetWorkspaceUrl", "https://adb-01234567890.azuredatabricks.net/", "Target Workspace URL")
dbutils.widgets.text("targetWorkspacePat", "dapi01234567890", "Target Workspace PAT")

# COMMAND ----------

import requests

sourceWorkspaceUrl = dbutils.widgets.get("sourceWorkspaceUrl")
sourceWorkspacePat = dbutils.widgets.get("sourceWorkspacePat")
targetWorkspaceUrl = dbutils.widgets.get("targetWorkspaceUrl")
targetWorkspacePat = dbutils.widgets.get("targetWorkspacePat")

# Ensure that the workspace URLs end with a "/"
if sourceWorkspaceUrl[-1] != '/':
  sourceWorkspaceUrl = sourceWorkspaceUrl + '/'
if targetWorkspaceUrl[-1] != '/':
  targetWorkspaceUrl = targetWorkspaceUrl + '/'

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

def get_secret_scopes(workspace, token):
  response = requests.get(
    f'{workspace}api/2.0/secrets/scopes/list',
    headers={
      'Authorization': f'Bearer {token}'
    }
  )
  response.raise_for_status()
  schema = 'workspace string, name string, databricks_scope string, azure_keyvault_scope string, keyvault_resource_id string, keyvault_dns_name string'
  if 'scopes' in response.json():
    scopes = response.json()['scopes']
    xformed_scopes = [{
      'workspace': workspace,
      'name': scope['name'],
      'databricks_scope': True if scope['backend_type'] == 'DATABRICKS' else False,
      'azure_keyvault_scope': True if scope['backend_type'] == 'AZURE_KEYVAULT' else False,
      'keyvault_resource_id': scope['keyvault_metadata']['resource_id'] if scope['backend_type'] == 'AZURE_KEYVAULT' else None,
      'keyvault_dns_name': scope['keyvault_metadata']['dns_name'] if scope['backend_type'] == 'AZURE_KEYVAULT' else None
    } for scope in scopes]
    return spark.createDataFrame(xformed_scopes, schema)
  else:
    print('There are no Secrets!')
    return spark.createDataFrame([], schema)

source_scopes = get_secret_scopes(sourceWorkspaceUrl, sourceWorkspacePat)
target_scopes = get_secret_scopes(targetWorkspaceUrl, targetWorkspacePat)

validation_df = source_scopes.drop("workspace").exceptAll(target_scopes.drop("workspace"))
if validation_df.count() == 0:
  print('Secret scopes match between workspaces!')
else:
  print('Secret scopes don\'t match between workspaces!')
  validation_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instance Pools Validation

# COMMAND ----------

def get_instance_pools(workspace, token):
  response = requests.get(
    f'{workspace}api/2.0/instance-pools/list',
    headers={
      'Authorization': f'Bearer {token}'
    }
  )
  response.raise_for_status()
  schema = 'workspace string, name string, min_idle_instances long, node_type_id string, autotermination_minutes long, instance_availability string, elastic_disk_enabled boolean'
  if 'instance_pools' in response.json():
    pools = response.json()['instance_pools']
    xformed_pools = [{
      'workspace': workspace,
      'name': pool['instance_pool_name'],
      'min_idle_instances': pool['min_idle_instances'],
      'node_type_id': pool['node_type_id'],
      'autotermination_minutes': pool['idle_instance_autotermination_minutes'],
      'instance_availability': pool['azure_attributes']['availability'],
      'elastic_disk_enabled': pool['enable_elastic_disk']
    } for pool in pools]
    return spark.createDataFrame(xformed_pools, schema)
  else:
    print('There are no instance pools.')
    return spark.createDataFrame([], schema)
  
source_pools = get_instance_pools(sourceWorkspaceUrl, sourceWorkspacePat)
target_pools = get_instance_pools(targetWorkspaceUrl, targetWorkspacePat)

validation_df = source_pools.drop("workspace").exceptAll(target_pools.drop("workspace"))
if validation_df.count() == 0:
  print('Instance pools match between workspaces!')
else:
  print('Instance pools don\'t match between workspaces!')
  validation_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interactive Clusters Validation

# COMMAND ----------

def get_interactive_cluster(workspace, token):
  response = requests.get(
    f'{workspace}api/2.0/clusters/list',
    headers={
        'Authorization': f'Bearer {token}'
    }
  )
  response.raise_for_status()
  schema = 'workspace string, cluster_name string, dbr_version string, first_on_demand long, availability string, autotermination_minutes long, instance_type string'
  if 'clusters' in response.json():
    clusters = response.json()['clusters']
    xformed_clusters = [{
      'workspace': workspace,
      'cluster_name': cluster['cluster_name'],
      'dbr_version': cluster['spark_version'],
      'first_on_demand': cluster['azure_attributes']['first_on_demand'],
      'availability': cluster['azure_attributes']['availability'],
      'autotermination_minutes': cluster['autotermination_minutes'],
      'instance_type': cluster['node_type_id']
    } for cluster in clusters]
    return spark.createDataFrame(xformed_clusters, schema)
  else:
    print('There are no interactive clusters!')
    return spark.createDataFrame([], schema)
  
source_interactive_clusters = get_interactive_cluster(sourceWorkspaceUrl, sourceWorkspacePat)
target_interactive_clusters = get_interactive_cluster(targetWorkspaceUrl, targetWorkspacePat)

validation_df = source_interactive_clusters.drop("workspace").exceptAll(target_interactive_clusters.drop("workspace"))
if validation_df.count() == 0:
  print('Interactive clusters match between workspaces!')
else:
  print('Interactive clusters don\'t match between workspaces!')
  validation_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jobs Validation

# COMMAND ----------

def get_jobs(workspace, token):
  response = requests.get(
    f'{workspace}api/2.0/jobs/list',
    headers={
        'Authorization': f'Bearer {token}'
    }
  )
  response.raise_for_status()
  schema = 'workspace string, job_name string, job_type string, job_format string, cron_schedule string, timezone string, timeout_seconds long'
  if 'jobs' in response.json():
    jobs = response.json()['jobs']
    xformed_jobs = [{
      'workspace': workspace,
      'job_name': job['settings']['name'],
      'job_type': job['job_type'] if 'job_type' in job else None,
      'job_format': job['settings']['format'],
      'cron_schedule': job['settings']['schedule']['quartz_cron_expression'] if 'schedule' in job['settings'] else None,
      'timezone': job['settings']['schedule']['timezone_id'] if 'schedule' in job['settings'] else None,
      'timeout_seconds': job['settings']['timeout_seconds'] if 'timeout_seconds' in job['settings'] else None
    } for job in jobs]
    return spark.createDataFrame(xformed_jobs, schema)
  else:
    print('There are no Jobs!')
    return spark.createDataFrame([], schema)
  
source_jobs = get_jobs(sourceWorkspaceUrl, sourceWorkspacePat)
target_jobs = get_jobs(targetWorkspaceUrl, targetWorkspacePat)

validation_df = source_jobs.drop("workspace").exceptAll(target_jobs.drop("workspace"))
if validation_df.count() == 0:
  print('Jobs match between workspaces!')
else:
  print('Jobs don\'t match between workspaces!')
  validation_df.display()  

# COMMAND ----------


