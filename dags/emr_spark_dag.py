from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import emr_utils as dagutil

dag_id = "emr_spark_job"

#dag = DAG(dag_id = dag_id,default_args=dgutil.default_args,schedule=CronTriggerTimetable("10 13 * * *", timezone="UTC"),catchup=False,max_active_runs=1)
dag = DAG(
    dag_id=dag_id,
    default_args=dagutil.default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1
)

# Spark command
try:
    bucket_name = dagutil.find_s3_bucket()
    common_s3_path = f"s3://{bucket_name}/ariflow_emr_spark_test/spark_snowflake_ingestion"
    spark_submit_command = [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--jars", "s3://dev-bucket/ariflow_emr_spark_test/jars/spark-snowflake_2.11-2.9.3-spark_2.4.jar,"
        "s3://dev-bucket/ariflow_emr_spark_test/jars/snowflake-jdbc-3.13.14.jar",
        "--py-files", f"{common_s3_path}/modules/snowflake_utility.py",
        f"{common_s3_path}/snowflake_to_spark_ingestion.py",
        "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER",
        "DEMO.DEMO_SCHEMA.CUSTOMER_TEST_INGESTION",
    ]
except Exception as e:
    print(f"Issue in Configuring Spark Job: {e}")
    raise

with dag:
    check_cluster_task = PythonOperator(
        task_id="check_cluster",
        python_callable=dagutil.check_and_manage_clusters,
        provide_context=True,
    )

    branch_check_cluster_task = BranchPythonOperator(
        task_id='branch_check_cluster',
        python_callable=dagutil.branch_create_cluster,
        provide_context=True,
    )
    
    # Task to create a new cluster
    create_cluster_task = dagutil.create_emr_cluster(
        task_id="create_emr_cluster"
    )

    wait_cluster_task = dagutil.wait_for_cluster(
        job_flow_id="{{ task_instance.xcom_pull(task_ids='check_cluster') or "
                    "task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        task_id='wait_for_cluster_ready'
    )

    add_spark_task = dagutil.add_spark_step(
        job_flow_id="{{ task_instance.xcom_pull(task_ids='check_cluster') or "
                    "task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        spark_submit_command=spark_submit_command,
        task_id="add_spark_step"
    )

    monitor_step_task = dagutil.monitor_spark_step(
        job_flow_id="{{ task_instance.xcom_pull(task_ids='check_cluster') or "
                    "task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
    )

    terminate_cluster_task = dagutil.terminate_emr_cluster(
        job_flow_id="{{ task_instance.xcom_pull(task_ids='check_cluster') or "
                    "task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        current_dag_id="emr_spark_job",
    )

    check_cluster_task >> branch_check_cluster_task
    branch_check_cluster_task >> [create_cluster_task, wait_cluster_task]
    create_cluster_task >> wait_cluster_task

    add_spark_task.trigger_rule = TriggerRule.ALL_DONE
    wait_cluster_task >> add_spark_task
    add_spark_task >> monitor_step_task
    monitor_step_task >> terminate_cluster_task

    terminate_cluster_task.trigger_rule = TriggerRule.ALL_DONE
