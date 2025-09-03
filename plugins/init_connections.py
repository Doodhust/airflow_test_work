from airflow import settings
from airflow.models import Connection
import logging


def create_connection(conn_id, conn_type, host, schema, login, password, port):
    session = settings.Session()
    try:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

        if existing_conn:
            logging.debug(f"Connection '{conn_id}' already exists. Skipping creation.")
        else:
            conn = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                schema=schema,
                login=login,
                password=password,
                port=port
            )
            session.add(conn)
            session.commit()
            logging.info(f"Connection '{conn_id}' has been created successfully.")

    except Exception as e:
        logging.error(f"An error occurred while creating the connection: {e}")
        session.rollback()
    finally:
        session.close()


create_connection(
    conn_id='postgres',
    conn_type='postgres',
    host='postgres',  # имя сервиса в docker-compose
    schema='airflow',
    login='airflow',
    password='airflow',
    port=5432
)

create_connection(
    conn_id='clickhouse',
    conn_type='generic',
    host='clickhouse',
    schema='default',
    login='clickhouse_user',
    password='clickhouse_password',
    port=9000
)

logging.info("Connections have been processed!")