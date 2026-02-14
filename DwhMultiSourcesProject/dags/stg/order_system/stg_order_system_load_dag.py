import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.order_system.pg_saver import PgSaver
from stg.order_system.order_system_loader import RestaurantLoader, UserLoader, OrderLoader 
from stg.order_system.order_system_reader import RestaurantReader, UserReader, OrderReader
from lib import ConnectionBuilder, MongoConnect


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), 
    catchup=False,  
    tags=['stg', 'stg_order_system_load'], 
    is_paused_upon_creation=True  
)
def stg_order_system_load():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()
    restaurant_loader = load_restaurants()

    @task()
    def load_users():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()
    users_loader = load_users()

    @task()
    def load_orders():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = OrderReader(mongo_connect)
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()
    orders_loader = load_orders()


    restaurant_loader >> users_loader >> orders_loader 


order_stg_dag = stg_order_system_load()
