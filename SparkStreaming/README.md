### Перед запуском PySpark добавьте переменные окружения в .env  
TOPIC_NAME  
KAFKA_BOOTSTRAP  
KAFKA_USER  
KAFKA_PASSWORD  
KAFKA_INPUT_TOPIC  
KAFKA_OUTPUT_TOPIC  
PG_SUBSCRIBERS_URL  
PG_SUBSCRIBERS_USER  
PG_SUBSCRIBERS_PASS  
PG_SUBSCRIBERS_TABLE  
PG_LOCAL_URL  
PG_LOCAL_USER  
PG_LOCAL_PASS  
PG_FEEDBACK_TABLE  
SPARK_JARS_PACKAGES  
CHECKPOINT_LOCATION  

### Перед запуском PySpark приложения загрузите переменные окружения   
export $(cat .env | xargs) 
