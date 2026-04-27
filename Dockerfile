FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    pkg-config \
    default-jdk \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Set Airflow Home
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# Download Postgres JDBC Driver (directory must exist first)
RUN mkdir -p /opt/airflow && \
    curl -o /opt/airflow/postgresql-42.2.18.jar https://jdbc.postgresql.org/download/postgresql-42.2.18.jar

# Install Python libraries
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.9.txt" && \
    pip install --no-cache-dir -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.9.txt"

# Initialize directory structure
RUN mkdir -p /opt/airflow/dags /opt/airflow/scripts /opt/airflow/data
