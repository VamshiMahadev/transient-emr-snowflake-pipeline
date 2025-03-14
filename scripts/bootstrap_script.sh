#!/bin/bash

# Update package manager and install pip
sudo yum update -y
sudo yum install -y python3-pip

# Ensure the correct Python and pip versions are used
PYSPARK_PYTHON=$(which python3)
PYSPARK_DRIVER_PYTHON=$(which python3)
export PYSPARK_PYTHON
export PYSPARK_DRIVER_PYTHON

# Install required Python packages using pip for Python3
sudo pip3 install --upgrade boto3 pandas numpy snowflake-connector-python

# Add required Spark JARs for Snowflake
SPARK_CONF_DIR="/etc/spark/conf"
SPARK_DEFAULTS_FILE="${SPARK_CONF_DIR}/spark-defaults.conf"

if [ ! -d "$SPARK_CONF_DIR" ]; then
  sudo mkdir -p $SPARK_CONF_DIR
fi

# Write Spark JAR paths to the Spark defaults file
sudo tee -a $SPARK_DEFAULTS_FILE <<EOF
spark.jars s3://dev-bucket/ariflow_emr_spark_test/jars/spark-snowflake_2.11-2.9.3-spark_2.4.jar,s3://dev-bucket/ariflow_emr_spark_test/jars/snowflake-jdbc-3.13.14.jar
EOF

# Ensure Spark Python environment variables are properly set
echo "export PYSPARK_PYTHON=${PYSPARK_PYTHON}" | sudo tee -a /etc/environment
echo "export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON}" | sudo tee -a /etc/environment

# Restart Spark to apply changes
sudo stop spark-history-server
sudo start spark-history-server

echo "Bootstrap script completed successfully."