# Databricks notebook source
dbutils.widgets.text("sourceWorkspaceUrl", "https://<REPLACE_ME>.azuredatabricks.net/", "Source Workspace URL")
dbutils.widgets.text("sourceWorkspacePat", "dapi<REPLACE_ME>", "Source Workspace token")
dbutils.widgets.text("targetWorkspaceUrl", "https://<REPLACE_ME>.azuredatabricks.net/", "Target Workspace URL")
dbutils.widgets.text("targetWorkspacePat", "dapi<REPLACE_ME>", "Target Workspace token")

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
# MAGIC 6. Init Scripts
# MAGIC 7. Secrets

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

# MAGIC %md
# MAGIC ### Groups Validation

# COMMAND ----------

def get_groups(workspace, token):
  response = requests.get(
      f'{workspace}api/2.0/preview/scim/v2/Groups',
      headers={
          'Authorization': f'Bearer {token}'
      }
  )
  response.raise_for_status()
  if 'Resources' in response.json():
    groups = response.json()['Resources']
    return list(map(lambda group: group['displayName'], groups))
  else:
    print("No Groups to process!")
    return []

missing_groups = []
source_groups = get_groups(sourceWorkspaceUrl, sourceWorkspacePat)
target_groups = get_groups(targetWorkspaceUrl, targetWorkspacePat)
for group in source_groups:
  if group not in target_groups:
    missing_groups.append(group)

if len(missing_groups) == 0:
  print("Groups match!")
else:
  spark.createDataFrame(list(map(lambda g: {'missing_group': g}, missing_groups)), 'missing_group string').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Users Validation

# COMMAND ----------

def get_users(workspace, token):
  response = requests.get(
      f'{workspace}api/2.0/preview/scim/v2/Users',
      headers={
          'Authorization': f'Bearer {token}'
      }
  )
  response.raise_for_status()
  schema = 'workspace string, displayName string, userName string, active boolean'
  if 'Resources' in response.json():
    users = response.json()['Resources']
    xformed_users = [{
      'workspace': workspace,
      'displayName': user['displayName'] if 'displayName' in user else None,
      'userName': user['userName'],
      'active': user['active']
    } for user in users]
    return spark.createDataFrame(xformed_users, schema)
  else:
    print("No Users to process!")
    return spark.createDataFrame([], schema)

source_users = get_users(sourceWorkspaceUrl, sourceWorkspacePat)
target_users = get_users(targetWorkspaceUrl, targetWorkspacePat)

validation_df = source_users.drop("workspace").exceptAll(target_users.drop("workspace"))
if validation_df.count() == 0:
  print('Users match between workspaces!')
else:
  print('Users don\'t match between workspaces!')
  validation_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init Scripts Validation

# COMMAND ----------

def get_workspace_objects(workspace, token, path):
  response = requests.get(
    f'{workspace}api/2.0/workspace/list',
    headers={
      'Authorization': f'Bearer {token}'
    },
    json={
      'path': path
    }
  )
  schema = 'workspace string, file_name string, file_path string, file_type string'
  if 'objects' in response.json():
    files = response.json()['objects']
    for file in files:
      xformed_workspace_objs = [{
        'workspace': workspace,
        'file_name': file['path'].split("/")[-1],
        'file_path': file['path'],
        'file_type': file['object_type']
      } for file in files]
      return spark.createDataFrame(xformed_workspace_objs, schema)
  else:
    print("No workspace objects to process!")
    return spark.createDataFrame([], schema)

source_init_scripts = get_workspace_objects(sourceWorkspaceUrl, sourceWorkspacePat, '/FileStore/scripts')
target_init_scripts = get_workspace_objects(targetWorkspaceUrl, targetWorkspacePat, '/FileStore/scripts')
missing_init_scripts = source_init_scripts.drop("workspace").exceptAll(target_init_scripts.drop("workspace"))
if missing_init_scripts.count() > 0:
  print("Init scripts don't match!")
  missing_init_scripts.display()
else:
  print("Init scripts match!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Root workspace validator

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

# User home directories
user_home_dir = '/Users'
source_user_home_dfs = []
source_user_homes = get_workspace_objects(sourceWorkspaceUrl, sourceWorkspacePat, user_home_dir).where("file_type='DIRECTORY'").select("file_path")
user_homes = [home['file_path'] for home in source_user_homes.collect()]
for user_home in user_homes:
  source_user_home_dfs.append(get_workspace_objects(sourceWorkspaceUrl, sourceWorkspacePat, user_home).where("file_type!='DIRECTORY'"))
source_user_notebooks = reduce(DataFrame.unionAll, source_user_home_dfs)

target_user_home_dfs = []
target_user_homes = get_workspace_objects(targetWorkspaceUrl, targetWorkspacePat, user_home_dir).where("file_type='DIRECTORY'").select("file_path")
user_homes = [home['file_path'] for home in target_user_homes.collect()]
for user_home in user_homes:
  target_user_home_dfs.append(get_workspace_objects(targetWorkspaceUrl, targetWorkspacePat, user_home).where("file_type!='DIRECTORY'"))
target_user_notebooks = reduce(DataFrame.unionAll, target_user_home_dfs)

missing_user_notebooks = source_user_notebooks.drop("workspace").exceptAll(target_user_notebooks.drop("workspace"))
if missing_user_notebooks.count() > 0:
  print("User notebooks don't match!")
  missing_user_notebooks.display()
else:
  print("User notebooks match!")
