import logging
import pendulum
from airflow.decorators import dag, task
from stg.delivery_system.insert_stage_delivery import load_couriers_to_stg, load_delivery_to_stg


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['stg', 'delivery'],  
    is_paused_upon_creation=True 
)
def stg_delivery_load():
    @task(task_id="couriers_load")
    def load_couriers():
        load_couriers_to_stg()  
    stg_couriers = load_couriers()

    @task(task_id="delivery_load")
    def load_delivery():
        load_delivery_to_stg()
    stg_delivery = load_delivery()
    

    stg_couriers >> stg_delivery 


stg_load_couriers = stg_delivery_load()
