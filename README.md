# airflow_dags

Using Apache Airflow in order to read data from Cassandra table to an Avro file in HDFS.

> sudo apt install python3-venv

> python3 -m venv airflow

> source airflow/bin/activate

> AIRFLOW_VERSION=2.0.1

> PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

> CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

> pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

> pip install apache-airflow-providers-apache-cassandra # 1.0.1

> pip install apache-airflow-providers-apache-hdfs # 1.0.1

> pip install apache-airflow-providers-apache-spark # 1.0.2

> pip install avro

> pip install hdfs

~~pip install fastavro~~

> airflow db init

> airflow users create --username admin --firstname first --lastname last --role Admin --email first.last@example.com

