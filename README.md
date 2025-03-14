# EMR Transient Cluster with Snowflake Integration Using AWS MWAA and Airflow

## Overview
This project is designed to leverage **Amazon EMR Transient Clusters** for scalable data processing and integrates with **Snowflake** for data ingestion, transformation, and storage. The architecture ensures cost-efficiency by spinning up EMR clusters only when required and terminating them once the job is complete. Additionally, it utilizes **AWS MWAA (Managed Workflows for Apache Airflow)** for seamless orchestration and automation of data pipelines.

---

## Key Components
### 1. **Amazon EMR Transient Cluster**
- **Transient Mode**: EMR clusters are created dynamically, used for the job duration, and terminated immediately after processing to reduce costs.
- **Excluding Persistent Clusters**: The logic ensures that existing persistent EMR clusters are not disturbed during cluster creation or termination.
- **Active Cluster Job Submission**: Jobs are submitted to **active EMR clusters** instead of creating multiple clusters during parallel processing, improving resource efficiency.
- **Bootstrap Script**: Ensures proper environment setup by installing necessary dependencies like PySpark, Snowflake Connector, and AWS SDK.
- **Spark**: For large-scale data processing and efficient ETL operations.

### 2. **Snowflake Integration**
- Utilizes Snowflake's connector for seamless data exchange.
- Ensures secure credential management using **AWS Secrets Manager**.

### 3. **Airflow Orchestration (AWS MWAA)**
- Defines DAGs to manage cluster creation, data processing, and termination automatically using **AWS Managed Workflows for Apache Airflow (MWAA)**.

---

## Project Structure
```
📂 EMR_Snowflake_Pipeline
│
├── 📂 dags
│   └── emr_spark_dag.py             # Airflow DAG for orchestrating EMR-Snowflake ingestion
│
├── 📂 modules
│   ├── emr_utils.py                 # Utility functions for EMR cluster management
│   ├── snowflake_utility.py         # Utility functions for Snowflake connections
│
├── 📂 scripts
│   └── bootstrap_script.sh          # Bootstrap script for EMR setup
│
├── 📂 jobs
│   └── snowflake_to_spark_ingestion.py  # PySpark job for Snowflake data ingestion
│
├── 📂 config
│   ├── emr_config.json              # EMR cluster configuration details
│   ├── snowflake_config.json        # Snowflake credentials securely managed
│
├── 📂 requirements
│   └── requirements.txt             # Python dependencies
│
├── .gitignore                       # Ignore unnecessary files (logs, compiled code)
├── README.md                        # Project documentation with setup instructions
└── Dockerfile                       # For containerized deployments (if required)
```

---

## Installation and Setup
### 1. **Create a Virtual Environment**
```bash
python3 -m venv venv
source venv/bin/activate    # For Linux/Mac
venv\Scripts\activate       # For Windows
```

### 2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

### 3. **Configure AWS and Snowflake**
- Store your Snowflake credentials securely in **AWS Secrets Manager**.
- Add necessary IAM roles and permissions to your EMR cluster.

### 4. **Execute Airflow DAG**
```bash
airflow dags trigger emr_spark_dag
```

---

## How It Works
1. **Airflow DAG** triggers the EMR cluster creation.
2. The **bootstrap_script.sh** installs necessary dependencies on the cluster nodes.
3. The PySpark job in **`snowflake_to_spark_ingestion.py`** reads data from Snowflake, performs transformations, and writes back.
4. Upon successful job completion, the EMR cluster is automatically terminated.
5. Jobs are submitted to **active EMR clusters** when available to avoid creating multiple clusters during parallel processing.

---

## Best Practices for EMR Transient Clusters
✅ Use **Spot Instances** for cost optimization.  
✅ Minimize idle cluster time by ensuring proper termination logic in Airflow DAG.  
✅ Store intermediate data in **S3** to improve resilience and reduce dependency on active clusters.  
✅ Use **AWS Secrets Manager** for enhanced security with credential management.  
✅ Ensure logic to **exclude persistent clusters** from automated start/stop routines to avoid disruption.

---

## Troubleshooting
- **Cluster Fails to Start:** Verify the bootstrap script path and IAM role permissions.  
- **Snowflake Connection Issues:** Check the Snowflake secret keys and role configurations.  
- **Data Load Errors:** Ensure correct table mappings and data formats are defined in the Spark job.  

For further assistance, refer to the official [AWS EMR Documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview.html) and [Snowflake Documentation](https://docs.snowflake.com/).

---

## Contributors
- **[Vamshi Mahadev]** - Data Engineer

---

## License
This project is licensed under the [MIT License](LICENSE).

