import logging
import csv
import os
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

from transactional_activity.libs.pg_connector import PostgresManager
from transactional_activity.libs.vertica_connector import VerticaManager


def load_staging_transaction(configs, dt_load_inteval):
    """
    Функция:
    Загружает транзакционные данные из PostgreSQL в промежуточную таблицу Vertica.
    """
    TRANSACTION_SRC_TABLE = f"{configs.pg_table_transactions_schema_name}.{configs.pg_table_transactions_table_name}"
    TRANSACTION_RECEIVER_TABLE = f"{configs.vertica_table_transactions_schema_name}.{configs.vertica_table_transactions_table_name}"
    OFFSET = 0

    logger.info(f"Хост: {configs.pg_host}")
    logger.info(f"База данных: {configs.pg_database}")
    logger.info(f"Таблица источник: {TRANSACTION_SRC_TABLE}")
    logger.info(f"Таблица приёмник: {TRANSACTION_RECEIVER_TABLE}")
    logger.info(f"Данные за {dt_load_inteval}")

    total_inserted = 0
    while True:   
        with PostgresManager(configs) as db:
            load_query = f"""
            SELECT * 
            FROM {TRANSACTION_SRC_TABLE}
            WHERE {configs.pg_table_transactions_dt_load_column}::DATE = '{dt_load_inteval}'
            ORDER BY {configs.pg_table_transactions_pk_column}, {configs.pg_table_transactions_dt_load_column}
            LIMIT {configs.batch_size}
            OFFSET {OFFSET};
            """
            transactions_data  = db.execute_query(load_query)
            if not transactions_data:
                logger.info(f"Нет данных для вставки.")
                break
            batch_size_current = len(transactions_data)

        script_dir = os.path.dirname(os.path.abspath(__file__))
        tmp_dir = os.path.join(script_dir, 'tmp_data')
        CSV_FILE_PATH = os.path.join(tmp_dir, 'transactions.csv')
        os.makedirs(tmp_dir, exist_ok=True) 
        with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(transactions_data)

        with VerticaManager(configs) as vertica_db:
            copy_command = f"""
            COPY {TRANSACTION_RECEIVER_TABLE} 
            FROM STDIN 
            DELIMITER ',' 
            ENCLOSED BY '"' 
            NULL AS '' 
            """

            with vertica_db.get_cursor() as cursor:
                with open(CSV_FILE_PATH, 'rb') as f:
                    cursor.copy(copy_command, f)
                    vertica_db.connection.commit()
                    logger.info(f"Успешно загружено {batch_size_current} строк в {TRANSACTION_RECEIVER_TABLE} из {CSV_FILE_PATH}")
                    total_inserted += batch_size_current

        os.remove(CSV_FILE_PATH)
        OFFSET += batch_size_current

    logger.info(f"Цикл загрузки завершён. Всего вставлено строк: {total_inserted}")


def load_staging_currencies(configs, dt_load_inteval):
    """
    Функция:
    Загружает данные о курсах валют из PostgreSQL в промежуточную таблицу Vertica.
    """
    TRANSACTION_SRC_TABLE = f"{configs.pg_table_currencies_schema_name}.{configs.pg_table_currencies_table_name}"
    TRANSACTION_RECEIVER_TABLE = f"{configs.vertica_table_currencies_schema_name}.{configs.vertica_table_currencies_table_name}"

    logger.info(f"Хост: {configs.pg_host}")
    logger.info(f"База данных: {configs.pg_database}")
    logger.info(f"Таблица источник: {TRANSACTION_SRC_TABLE}")
    logger.info(f"Таблица приёмник: {TRANSACTION_RECEIVER_TABLE}")
    logger.info(f"Данные за {dt_load_inteval}")

    total_inserted = 0

    with PostgresManager(configs) as db:
        load_query = f"""
        SELECT * 
        FROM {TRANSACTION_SRC_TABLE}
        WHERE {configs.pg_table_currencies_dt_load_column}::DATE = '{dt_load_inteval}'
        ORDER BY {configs.pg_table_currencies_dt_load_column};
        """
        transactions_data  = db.execute_query(load_query)
        if not transactions_data:
            logger.info(f"Нет данных для вставки.")
        batch_size_current = len(transactions_data)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    tmp_dir = os.path.join(script_dir, 'tmp_data')
    CSV_FILE_PATH = os.path.join(tmp_dir, 'currencies.csv')
    os.makedirs(tmp_dir, exist_ok=True) 
    with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(transactions_data)

    with VerticaManager(configs) as vertica_db:
        copy_command = f"""
        COPY {TRANSACTION_RECEIVER_TABLE} 
        FROM STDIN 
        DELIMITER ',' 
        ENCLOSED BY '"' 
        NULL AS '' 
        """

        with vertica_db.get_cursor() as cursor:
            with open(CSV_FILE_PATH, 'rb') as f:
                cursor.copy(copy_command, f)
                vertica_db.connection.commit()
                logger.info(f"Успешно загружено {batch_size_current} строк в {TRANSACTION_RECEIVER_TABLE} из {CSV_FILE_PATH}")
                total_inserted += batch_size_current

    os.remove(CSV_FILE_PATH)
    logger.info(f"Цикл загрузки завершён. Всего вставлено строк: {total_inserted}")
