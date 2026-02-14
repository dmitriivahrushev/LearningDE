import json
from lib import ConnectionBuilder
from stg.delivery_system.load_api import ApiClient


def load_couriers_to_stg():
    api = ApiClient()
    all_data = []

    limit = 50
    offset = 0

    while True:
        result = api.get_data("couriers", {
            "limit": limit,
            "offset": offset,
            "sort_field": "_id",
            "sort_direction": "asc"
        })

        if not result:
            break

        rows = [
            (item['_id'], json.dumps(item, ensure_ascii=False))
            for item in result
        ]
        all_data.extend(rows)

        if len(result) < limit:
            break

        offset += limit


    pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with pg_connect.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO stg.couriers (object_id, object_value)
                VALUES (%s, %s)
                ON CONFLICT (object_id) DO NOTHING;
            """, all_data)
            conn.commit()
 

def load_delivery_to_stg():
    api = ApiClient()
    all_data = []

    limit = 50
    offset = 0

    while True:
        result = api.get_data("deliveries", {
            "limit": limit,
            "offset": offset,
            "sort_field": "order_id",
            "sort_direction": "asc"
        })

        if not result:
            break

        rows = [
            (item['order_id'], json.dumps(item, ensure_ascii=False))
            for item in result
        ]
        all_data.extend(rows)

        if len(result) < limit:
            break

        offset += limit


    pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with pg_connect.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO stg.delivery (object_id, object_value)
                VALUES (%s, %s)
                ON CONFLICT (object_id) DO NOTHING;
            """, all_data)
            conn.commit()

