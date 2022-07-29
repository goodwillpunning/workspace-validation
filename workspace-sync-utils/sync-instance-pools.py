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
# MAGIC ### Instance Pools Validation

# COMMAND ----------

def create_instance_pool(workspace, token, pool_config):
  response = requests.post(
    f'{workspace}api/2.0/instance-pools/create',
    headers={
      'Authorization': f'Bearer {token}'
    },
    json=pool_config
  )
  response.raise_for_status()
  return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print Instance Pools JSON Config

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
    for pool in pools:
      print(pool['instance_pool_name'] + '.json')
      print("===============================================")
      pool_config = {
        "instance_pool_name": pool['instance_pool_name'],
        "node_type_id": pool['node_type_id'],
        "min_idle_instances": pool['min_idle_instances'],
        "idle_instance_autotermination_minutes": pool['idle_instance_autotermination_minutes'],
        "azure_attributes": {
          "availability": pool['azure_attributes']['availability']
        },
        "enable_elastic_disk": pool['enable_elastic_disk']
      }
      print(pool_config)

  else:
    print('There are no instance pools.')
  
source_pools = get_instance_pools(sourceWorkspaceUrl, sourceWorkspacePat)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Re-Create Instance Pools

# COMMAND ----------

def recreate_instance_pools(workspace, token):
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
    for pool in pools:
      print('Re-creating isntance pool: ' + pool['instance_pool_name'])
      pool_config = {
        "instance_pool_name": pool['instance_pool_name'],
        "node_type_id": pool['node_type_id'],
        "min_idle_instances": pool['min_idle_instances'],
        "idle_instance_autotermination_minutes": pool['idle_instance_autotermination_minutes'],
        "azure_attributes": {
          "availability": pool['azure_attributes']['availability']
        },
        "enable_elastic_disk": pool['enable_elastic_disk']
      }
      print('Created instance pool id: ' + create_instance_pool(targetWorkspaceUrl, targetWorkspacePat, pool_config))

  else:
    print('There are no instance pools.')
  
recreate_instance_pools(sourceWorkspaceUrl, sourceWorkspacePat)
