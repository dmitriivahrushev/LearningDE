import logging
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

from transactional_activity.libs.vertica_connector import VerticaManager


def load_global_metricts(configs, dt_load_inteval):
    """
    Функция:
    Загружает агрегированные данные в витрину global_metrics.
    """
    TRANSACTION_SRC_TABLE = f"{configs.vertica_table_transactions_schema_name}.{configs.vertica_table_transactions_table_name}"
    CURRENCIES_SRC_TABLE = f"{configs.vertica_table_currencies_schema_name}.{configs.pg_table_currencies_table_name}"
    GLOBAL_METRICS_RECEIVER_TABLE = f"{configs.vertica_table_global_metrics_schema_name}.{configs.vertica_table_global_metrics_table_name}"

    logger.info(f"Хост: {configs.vertica_host}")
    logger.info(f"База данных: {configs.vertica_database}")
    logger.info(f"Таблицы источники: {TRANSACTION_SRC_TABLE}, {CURRENCIES_SRC_TABLE}")
    logger.info(f"Таблица приёмник: {GLOBAL_METRICS_RECEIVER_TABLE}")
    logger.info(f"Данные за {dt_load_inteval}")

    query = f"""
    INSERT INTO VT2511048ACE98__DWH.global_metrics (
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
    )
    SELECT
        tr_date AS date_update,
        currency_code AS currency_from,
        SUM(amount_usd) AS amount_total,
        COUNT(*) AS cnt_transactions,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT account_number_from), 2) AS avg_transactions_per_account,
        COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
    FROM (
        SELECT
            t.transaction_dt::DATE AS tr_date,
            t.currency_code,
            t.account_number_from,
            t.amount,
            t.amount * c.currency_with_div / 100.0 AS amount_usd
        FROM
            VT2511048ACE98__STAGING.transactions t
        JOIN
            VT2511048ACE98__STAGING.currencies c
            ON t.currency_code = c.currency_code
            AND t.transaction_dt::DATE = c.date_update
        WHERE
            t.status = 'done'
            AND t.account_number_from > 0
            AND t.transaction_dt::DATE = '{dt_load_inteval}'
    ) t_with_usd
    GROUP BY
        tr_date,
        currency_code;
    """

    with VerticaManager(configs) as vertica_db:
        vertica_db.execute_non_query(query)

    logger.info(f"Вставка завершена.")
