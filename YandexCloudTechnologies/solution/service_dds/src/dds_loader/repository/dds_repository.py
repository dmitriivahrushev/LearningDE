from datetime import datetime
from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_upsert(self, h_order_pk: str, order_id: int, order_dt: datetime,
                     order_cost: float, order_payment: float, order_status: str,
                     load_dt: datetime, load_src: str) -> None:
        """Upsert заказа в h_order, s_order_cost, s_order_status"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_order_pk, order_id, order_dt, load_dt, load_src))

                cur.execute("""
                    INSERT INTO dds.s_order_cost
                        (hk_order_cost_hashdiff, h_order_pk, cost, payment, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_cost_hashdiff) DO UPDATE
                    SET cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_order_pk, h_order_pk, order_cost, order_payment, load_dt, load_src))

                cur.execute("""
                    INSERT INTO dds.s_order_status
                        (hk_order_status_hashdiff, h_order_pk, status, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_status_hashdiff) DO UPDATE
                    SET status = EXCLUDED.status,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_order_pk, h_order_pk, order_status, load_dt, load_src))

    def user_upsert(self, h_user_pk: str, user_id: str, username: str, userlogin: str,
                    load_dt: datetime, load_src: str) -> None:
        """Upsert пользователя и его сателлита с именами"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_user_pk, user_id, load_dt, load_src))

                cur.execute("""
                    INSERT INTO dds.s_user_names
                        (hk_user_names_hashdiff, h_user_pk, username, userlogin, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hk_user_names_hashdiff) DO UPDATE
                    SET username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_user_pk, h_user_pk, username, userlogin, load_dt, load_src))

    def restaurant_upsert(self, h_restaurant_pk: str, restaurant_id: str, restaurant_name: str,
                          load_dt: datetime, load_src: str) -> None:
        """Upsert ресторана и сателлита с именем"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_restaurant_pk, restaurant_id, load_dt, load_src))

                cur.execute("""
                    INSERT INTO dds.s_restaurant_names
                        (hk_restaurant_names_hashdiff, h_restaurant_pk, name, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_restaurant_names_hashdiff) DO UPDATE
                    SET name = EXCLUDED.name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_restaurant_pk, h_restaurant_pk, restaurant_name, load_dt, load_src))

    def category_upsert(self, h_category_pk: str, category_name: str,
                        load_dt: datetime, load_src: str) -> None:
        """Upsert категории"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_category_pk) DO UPDATE
                    SET category_name = EXCLUDED.category_name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_category_pk, category_name, load_dt, load_src))

    def product_upsert(self, h_product_pk: str, product_id: str, product_name: str,
                       load_dt: datetime, load_src: str) -> None:
        """Upsert продукта и сателлита с именем"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_product_pk) DO UPDATE
                    SET product_id = EXCLUDED.product_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_product_pk, product_id, load_dt, load_src))

                cur.execute("""
                    INSERT INTO dds.s_product_names
                        (hk_product_names_hashdiff, h_product_pk, name, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_names_hashdiff) DO UPDATE
                    SET name = EXCLUDED.name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (h_product_pk, h_product_pk, product_name, load_dt, load_src))

    def l_product_category_upsert(self, hk_product_category_pk: str, h_product_pk: str, h_category_pk: str,
                                  load_dt: datetime, load_src: str) -> None:
        """Upsert связь продукт -> категория"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_category_pk) DO UPDATE
                    SET h_product_pk = EXCLUDED.h_product_pk,
                        h_category_pk = EXCLUDED.h_category_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src))

    def l_product_restaurant_upsert(self, hk_product_restaurant_pk: str, h_product_pk: str, h_restaurant_pk: str,
                                    load_dt: datetime, load_src: str) -> None:
        """Upsert связь продукт -> ресторан"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                    SET h_product_pk = EXCLUDED.h_product_pk,
                        h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src))

    def l_order_product_upsert(self, hk_order_product_pk: str, h_order_pk: str, h_product_pk: str,
                               load_dt: datetime, load_src: str) -> None:
        """Upsert связь заказ -> продукт"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET h_order_pk = EXCLUDED.h_order_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src))

    def l_order_user_upsert(self, hk_order_user_pk: str, h_order_pk: str, h_user_pk: str,
                             load_dt: datetime, load_src: str) -> None:
        """Upsert связь заказ -> пользователь"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_user_pk) DO UPDATE
                    SET h_order_pk = EXCLUDED.h_order_pk,
                        h_user_pk = EXCLUDED.h_user_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                """, (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src))
