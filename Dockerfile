FROM apache/airflow:2.3.0-python3.9

WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt .

COPY ./dags dags/

WORKDIR ${AIRFLOW_HOME}

COPY dbt/ ${AIRFLOW_HOME}/dbt/
# echo python version

RUN python -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

# export requirements 