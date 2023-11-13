FROM apache/airflow:latest

ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

USER root

# RUN apt-get update -qq && apt-get install vim -qqq && apt-get install -y python3-pip

COPY ./requirements.txt .
USER $AIRFLOW_UID

RUN python -m pip install --upgrade pip

RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt

