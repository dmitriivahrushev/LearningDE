from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_upsert(
        self,
        user_id: str,
        category_id: str,
        category_name: str,
        order_cnt: int
    ) -> None:
        """
        Безопасный upsert для таблицы cdm.user_category_counters.
        При конфликте (user_id, category_id) обновляет category_name и суммирует order_cnt.
        """
        upsert_statement = """
            INSERT INTO cdm.user_category_counters
                (user_id, category_id, category_name, order_cnt)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, category_id) DO UPDATE
            SET
                category_name = EXCLUDED.category_name,
                order_cnt = cdm.user_category_counters.order_cnt + EXCLUDED.order_cnt;
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement, (user_id, category_id, category_name, order_cnt))

    def user_product_counters_upsert(
        self,
        user_id: str,
        product_id: str,
        product_name: str,
        order_cnt: int
    ) -> None:
        """
        Безопасный upsert для таблицы cdm.user_product_counters.
        При конфликте (user_id, product_id) обновляет product_name и суммирует order_cnt.
        """
        upsert_statement = """
            INSERT INTO cdm.user_product_counters
                (user_id, product_id, product_name, order_cnt)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, product_id) DO UPDATE
            SET
                product_name = EXCLUDED.product_name,
                order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt;
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement, (user_id, product_id, product_name, order_cnt))
