# DwhMultiSourcesProject
![avatar](/img/dwh.jpg)

Цель проекта:
Разработка ETL-пайплайна, который интегрирует данные из трёх источников: PostgreSQL, MongoDB и API. Пайплайн обеспечивает загрузку и трансформацию данных, заполняя различные слои данных, такие как **STG**, **DDS** и **CDM** для дальнейшего анализа и принятия решений.
 
## Доступные сервисы
#### **БД локальная**
- **Postgres**
  - **Host**: localhost
  - **Port**: 15432
  - **DB**: de
  - **User**: jovyan
  - **Pass**: jovyan
- **Airflow**
  - **Login**: AirflowAdmin
  - **Pass**: airflow_pass
  - **URL**: [http://localhost:3000/airflow](http://localhost:3000/airflow)
#### **Источник**
 - **Host**: rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net 
 - **Port**: 6432 
 - **Database**: de-public 
 - **Username**: student 
 - **Password**: student1 
 - **SSL**: Use SSL 
 - **CA Certificate**: Выберите файл с сертификатом 
 - **SSL Mode**: verify-full 

#### **URI для MongoDB**
~~~
mongodb://student:student1@rc1a-ba83ae33hvt4pokq.mdb.yandexcloud.net:27018/db-mongo?authMechanism=DEFAULT&authSource=db-mongo&tls=true&replicaSet=rs01
~~~

#### Запуск проекта 🚀
Для сборки контейнеров выполните следующую команду:
~~~
docker compose up -d
~~~
#### Настройка Airflow 🛠️  
#### Добавляем коннекторы:     
**Коннектор источника**
 - Connection Id:`PG_ORIGIN_BONUS_SYSTEM_CONNECTION`
 - Connection Type:`Postgres`
 - Description:`Не заполняем`
 - Host:`rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net`
 - Schema:`de-public`
 - Login:`student`
 - Password:`student1`
 - Port:`6432`
 - Extra:`{"sslmode": "require"}`

**Коннектор Локальной БД**
 - Connection Id:`PG_WAREHOUSE_CONNECTION`
 - Connection Type:`Postgres`
 - Description:`Не заполняем`
 - Host:`localhost`
 - Schema:`de`
 - Login:`jovya`
 - Password:`jovyan`
 - Port:`5432`
 - Extra:`{"sslmode": "disable"}`

**Коннектор MongoDB**  
Перейдите в раздел Admin -> Variables. Через кнопку + добавьте следующие параметры:    
- MONGO_DB_CERTIFICATE_PATH:`/opt/airflow/certificates/PracticumSp5MongoDb.crt`  
- MONGO_DB_USER:`student`  
- MONGO_DB_PASSWORD:`student1`  
- MONGO_DB_REPLICA_SET:`rs01`  
- MONGO_DB_DATABASE_NAME:`db-mongo`  

**Настройка файла .env 🌿**
В файле .env заполните следующие параметры:  
 - X-Nickname:`ваш никнейм`
 - X-Cohort:`когорта`
 - X-API-KEY:`ваш API ключ`

**Запуск DAG`ов в Airflow 🔄**  
Заходим в Airflow и запускаем init_tables — это создаст все схемы и таблицы в DWH.  
После этого проект готов к запуску, можно приступать к запуску DAGов.  
![dags](/img/dags.png)
~~~
stg — Наполнение таблиц stg слоя
dds — Наполнение dds слоя
cdm — Наполнение витрин
~~~
