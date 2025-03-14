from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
import boto3
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import TaskInstance
from botocore.exceptions import ClientError

region = "eu-west-1"
sts = boto3.client("sts",region_name=region)
account_id = sts.get_caller_identity()["Account"]

def find_s3_bucket():
    if account_id == str(1234):
        return 'dev-bucket'
    elif account_id == str(12345):
        return 'prod-bucket'
    else:
        return 'xxx'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


DEFAULT_JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-EMR-Cluster",
    "ReleaseLabel": "emr-5.32.0",
    "LogUri": "s3://airflow-emr-logs-poc/emr-logs/",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "r5.4xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "r5.4xlarge",
                "InstanceCount": 2,  # Initial node count
                "AutoScalingPolicy": {
                    "Constraints": {
                        "MinCapacity": 2,
                        "MaxCapacity": 10,
                    },
                    "Rules": [
                        {
                            "Name": "ScaleOut",
                            "Description": "Scale out if YARNMemoryAvailablePercentage is low.",
                            "Action": {
                                "SimpleScalingPolicyConfiguration": {
                                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                                    "ScalingAdjustment": 1,  # Add one node
                                    "CoolDown": 300,  # Wait 5 minutes before further adjustments
                                },
                            },
                            "Trigger": {
                                "CloudWatchAlarmDefinition": {
                                    "ComparisonOperator": "LESS_THAN",
                                    "EvaluationPeriods": 1,
                                    "MetricName": "YARNMemoryAvailablePercentage",
                                    "Namespace": "AWS/ElasticMapReduce",
                                    "Period": 300,
                                    "Statistic": "AVERAGE",
                                    "Threshold": 15.0,
                                    "Dimensions": [
                                        {"Key": "JobFlowId", "Value": "${emr.clusterId}"},
                                    ],
                                },
                            },
                        },
                        {
                            "Name": "ScaleIn",
                            "Description": "Scale in if YARNMemoryAvailablePercentage is high.",
                            "Action": {
                                "SimpleScalingPolicyConfiguration": {
                                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                                    "ScalingAdjustment": -1,  # Remove one node
                                    "CoolDown": 300,  # Wait 5 minutes before further adjustments
                                },
                            },
                            "Trigger": {
                                "CloudWatchAlarmDefinition": {
                                    "ComparisonOperator": "GREATER_THAN",
                                    "EvaluationPeriods": 1,
                                    "MetricName": "YARNMemoryAvailablePercentage",
                                    "Namespace": "AWS/ElasticMapReduce",
                                    "Period": 300,
                                    "Statistic": "AVERAGE",
                                    "Threshold": 75.0,
                                    "Dimensions": [
                                        {"Key": "JobFlowId", "Value": "${emr.clusterId}"},
                                    ],
                                },
                            },
                        },
                    ],
                },
            },
        ],
        "Ec2KeyName": "emrclusterdev",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": "subnet-0a8797ac3c0e3c0dc",
    },
    "BootstrapActions": [
        {
            "Name": "Install Python Packages",
            "ScriptBootstrapAction": {
                "Path": "s3://airflowemrtest/DAGS/bootstrap_script.sh",
            },
        },
    ],
    "Applications": [{"Name": "Spark"}],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "AutoScalingRole": "EMR_AutoScaling_DefaultRole",
}



@provide_session
def is_cluster_in_use(current_dag_id, session=None):
    """
    Check if any DAGs other than the current one are actively running.

    Args:
        current_dag_id (str): The DAG ID of the current DAG.
        session (Session): Airflow's database session.

    Returns:
        bool: True if other DAGs are running, False otherwise.
    """
    active_dags = (
        session.query(DagRun)
        .filter(DagRun.state == State.RUNNING)
        .filter(DagRun.dag_id != current_dag_id)
        .filter(DagRun.dag_id != 'testairflow_dag')
        .count()
    )
    return active_dags > 0


def check_and_manage_clusters(protected_clusters=None):
    """
    Checks for active EMR clusters in valid states (WAITING or STARTING).
    """
    if protected_clusters is None:
        protected_clusters = [
            {"Name": "CLUSTER1", "Id": "j-1234"},
            {"Name": "CLUSTER2", "Id": "j-12345"},
            {"Name": "CLUSTER3", "Id": "j-123456"},
        ]

    protected_ids = {cluster["Id"] for cluster in protected_clusters}

    emr = boto3.client("emr")
    clusters = emr.list_clusters(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
    )["Clusters"]

    # Look for a cluster in WAITING or STARTING state
    for cluster in clusters:
        if cluster["Id"] not in protected_ids and cluster["Status"]["State"] in ["WAITING", "STARTING"]:
            print(f"Found existing cluster: {cluster['Name']} (ID: {cluster['Id']}, State: {cluster['Status']['State']})")
            return cluster["Id"]

    print("No suitable cluster found, proceeding to create a new one.")
    return None



def create_emr_cluster(task_id="create_emr_cluster",job_flow_overrides=None, aws_conn_id="aws_default"):
    """
    Creates a new EMR cluster.
    """
    if job_flow_overrides is None:
        job_flow_overrides = DEFAULT_JOB_FLOW_OVERRIDES

    return EmrCreateJobFlowOperator(
        task_id=task_id,
        job_flow_overrides=job_flow_overrides,
        aws_conn_id=aws_conn_id,
    )

# Branch task to decide if we need to create a new cluster or continue
def branch_create_cluster(task_instance: TaskInstance):
    cluster_id = task_instance.xcom_pull(task_ids="check_cluster")
    if cluster_id is None:
        return ["create_emr_cluster", "wait_for_cluster_ready"]
    else:
        return ["wait_for_cluster_ready"]


def wait_for_cluster(job_flow_id, task_id="wait_for_cluster_ready", aws_conn_id="aws_default"):
    """
    Wait for the cluster to be in WAITING or RUNNING state.
    """
    return EmrJobFlowSensor(
        task_id=task_id,
        job_flow_id=job_flow_id,
        target_states=["WAITING", "RUNNING"],
        aws_conn_id=aws_conn_id,
    )

def add_spark_step(job_flow_id, spark_submit_command, task_id, aws_conn_id="aws_default"):
    """
    Adds a Spark step to an EMR cluster.
    """
    spark_step = [
        {
            "Name": "Spark job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": spark_submit_command,
            },
        }
    ]
    return EmrAddStepsOperator(
        task_id=task_id,
        job_flow_id=job_flow_id,
        steps=spark_step,
        aws_conn_id=aws_conn_id,
    )


def monitor_spark_step(job_flow_id, step_id, aws_conn_id="aws_default"):
    """
    Monitors a Spark step in an EMR cluster.
    """
    return EmrStepSensor(
        task_id="monitor_spark_step",
        job_flow_id=job_flow_id,
        step_id=step_id,
        aws_conn_id=aws_conn_id,
    )


def terminate_emr_cluster(job_flow_id, current_dag_id, aws_conn_id="aws_default"):
    """
    Terminates an EMR cluster if no other DAGs are actively running.
    """
    def terminate_cluster_logic(job_flow_id, current_dag_id):
        if not job_flow_id:
            print("No valid cluster found to terminate.")
            raise AirflowSkipException("No valid cluster found to terminate.")

        if is_cluster_in_use(current_dag_id=current_dag_id):
            print("Skipping termination as other DAGs are actively running.")
            raise AirflowSkipException("Skipping termination as other DAGs are actively running.")

        print(f"Terminating EMR Cluster with ID: {job_flow_id}")

        try:
            # Initialize the EMR client
            emr_client = boto3.client('emr')
            
            # Call the terminate_job_flows API
            response = emr_client.terminate_job_flows(JobFlowIds=[job_flow_id])
            print(f"Cluster termination response: {response}")
        except ClientError as e:
            print(f"Error terminating EMR cluster: {e}")
            raise

    return PythonOperator(
        task_id="terminate_emr_cluster",
        python_callable=terminate_cluster_logic,
        op_kwargs={
            "job_flow_id": job_flow_id,
            "current_dag_id": current_dag_id,
        },
    )
