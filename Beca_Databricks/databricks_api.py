# Databricks notebook source
def job_data(notebook_path,
            cluster_id,
            all_purpose_cluster,
            timeout_seconds=0,
            base_parameters={}):
  
    """Função responsavel por iniciar a execução de um notebook utilizando a API do databricks

      Args:
        notebook_path         - Caminho do notebook + nome do notebook que deverá ser executado
        cluster_id            - ID do cluster caso seja uma execução em um cluster All-purpose
        all_purpose_cluster   - True caso se uma execução em um cluster All-purpose
        timeout_seconds       - Timeout em segundo que o job deve executar (quando é informado zero não há limite de tempo) 
        base_parameters       - Parametros que serão enviados ao notebook

      Returns:
        O retorno é json com todas as configurações para a criação do job cluster.


      Call Example:
        job_data(notebook_path, cluster_id, timeout_seconds, base_parameters, all_purpose_cluster) """
  
  
    import json

    if all_purpose_cluster:
        job_data= json.dumps(
            {
                "run_name": f"Notebook {notebook_path}",
                "timeout_seconds": timeout_seconds,
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": base_parameters
                }
            }
        )
    else:
        table_name = base_parameters["table_name"]
        job_name = base_parameters["job_name"]


        system = 'beca'
        vm_work_name = 'Standard_DS3_v2'
        vm_driver_name = 'Standard_DS3_v2'
        qty_max_workers = '1'
        runtime_cluster_version = '10.4.x-scala2.12'



        job_data= json.dumps(
                {
                    "run_name": f"Notebook {notebook_path}",
                    "timeout_seconds": timeout_seconds,
                    "new_cluster": {
                                    "num_workers": qty_max_workers,
                                    "spark_version": runtime_cluster_version,
                                    "node_type_id": vm_work_name,
                                    "driver_node_type_id": vm_driver_name,
                                    "num_workers": qty_max_workers,
                                    "spark_conf": {
                                                  "spark.databricks.delta.preview.enabled": "true"
                                                  },
                                    "custom_tags": {
                                                    "jc_table": table_name,
                                                    "jc_business_unit": "stix",
                                                    "jc_project": "lake team",
                                                    "jc_provisioner": "databricks job",
                                                    "jc_purpose": "workflow",
                                                    "jc_system": system,
                                                    "jc_process_name": job_name
                                                   }
                       },
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": base_parameters
                    }
                }
            )

    return job_data

# COMMAND ----------

def submit_job(notebook_path,
                cluster_id,
                base_parameters,
                all_purpose_cluster,
                timeout_seconds=0):
        
        """Função responsavel por iniciar a execução de um notebook utilizando a API do databricks

          Args:
            notebook_path         - Caminho do notebook + nome do notebook que deverá ser executado
            cluster_id            - ID do cluster caso seja uma execução em um cluster All-purpose
            base_parameters       - Parametros que serão enviados ao notebook
            all_purpose_cluster   - True caso se uma execução em um cluster All-purpose
            timeout_seconds       - Timeout em segundo que o job deve executar (quando é informado zero não há limite de tempo) 

          Returns:
            O retorno é json com o run_id do job em execução. Ex: {'run_id': 2090834}


          Call Example:
            submit_job(notebook_path,cluster_id,timeout_seconds,base_parameters,all_purpose_cluster) """
    
        from time import sleep
        import requests
        import json

        
        #databricks_token = dbutils.secrets.get(scope="key-vault", key="databricks-linked-service-token")
        databricks_token = 'dapi30fc0320d0a3a7fa6c8c4e506ba76667-3'
        databricks_url = 'https://adb-457214277598602.2.azuredatabricks.net'
        end_point = f'{databricks_url}/api/2.0/'
        headers = {'Authorization': f'Bearer {databricks_token}'}
        
        job_data_ret = job_data(notebook_path, cluster_id, timeout_seconds, base_parameters, all_purpose_cluster)
        resp = requests.post(end_point+'jobs/runs/submit', data=job_data_ret, headers=headers)
        print(resp.text)
        if resp.status_code == 200:
            response_json = resp.json()
            return response_json

        else:
            print(resp.text)
            print(resp.content)
            print('an erro ocurred')

# COMMAND ----------

cluster_id = None
#cluster_id = dbutils.secrets.get(scope="key-vault", key="databricks-cluster-ti-id")
table_name = 'bronze.crimes'
job_name = 'databricks_api'
notebook_path = '/Users/vsousaol@emeal.nttdata.com/Beca_Databricks/select_crimes'
timeout_seconds=0
base_parameters ={"table_name": table_name,"job_name":job_name}
all_purpose_cluster=False
print(submit_job(notebook_path,cluster_id,timeout_seconds,base_parameters,all_purpose_cluster))
