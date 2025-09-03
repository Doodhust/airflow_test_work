FROM apache/airflow:2.10.4-python3.10

# Права администратора
USER root

# Обновление и установка пакетных менеджеров
RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y wget



# Возвращение к пользователю по умолчанию
USER airflow


# Установка остальных пакетов через pip

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /requirements.txt

COPY plugins/init_connections.py /opt/airflow/
# Указываем рабочую директорию
WORKDIR /opt/airflow

# Команда по умолчанию для выполнения веб-сервера Airflow
CMD ["/bin/bash", "-c", "python /opt/airflow/init_connections.py && airflow webserver"]