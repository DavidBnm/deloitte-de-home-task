import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

PROJECT_ID = "deloitte-de-home-ty"
REGION = "us-central1"
REPOSITORY_ID = "deloitte-de-pipeline"
DATAPROC_CLUSTER = "deloitte-spark-cluster"
PYSPARK_SCRIPT_GCS = "gs://deloitte-code-bucket/spark_skew_analysis.py"

default_args = {
    "owner": "David Ben Ami",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 7, 1),
    "retries": 1,
}

with DAG(
    dag_id="deloitte_end_to_end_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["deloitte", "dataform", "dataproc", "spark"],
) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="dataform_create_compilation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
        },
    )

    execute_dataform_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="dataform_execute_models",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull(task_ids='dataform_create_compilation')['name'] }}",
            "invocation_config": {
                "service_account": "366162149287-compute@developer.gserviceaccount.com",
            },
        },
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=DATAPROC_CLUSTER,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 30},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 30},
            },
        },
    )

    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_GCS,
        },
    }

    submit_pyspark_aggregation = DataprocSubmitJobOperator(
        task_id="dataproc_submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=pyspark_job,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=DATAPROC_CLUSTER,
        trigger_rule="all_done",
    )

    (
        create_compilation_result
        >> execute_dataform_workflow
        >> create_dataproc_cluster
        >> submit_pyspark_aggregation
        >> delete_dataproc_cluster
    )