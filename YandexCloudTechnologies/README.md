# YandexCloudTechnologies
![avatar](./image/services.png)

---
Цель проекта:
Разработать и развернуть в Yandex Cloud архитектуру хранилища данных, построенную по принципам Data Vault. Архитектура включает в себя создание и настройку следующих компонентов: PostgreSQL, Valkey, Apache Kafka, Yandex Container Registry и Kubernetes. Проект предполагает реализацию трёх микросервисов, каждый из которых отвечает за автоматическое наполнение одного из слоёв хранилища: STG, DDS и CDM.  

---
### 📁 Структура проекта 
``` 
.
├── images/                  # Изображения.  
├── solution/service_        # Каталог сервисов.  
├            ── app          # Шаблоны настроек для деплоя в Kubernetes.
├            ── src          # Python модули.
├            ── dockerfile   # Образ Docker.
├── .gitignore/              # Файлы, игнорируемые Git-ом.
└──  README/                 # Информация о проекте.
```

---
### Релиз сервиса в Kubernetes  
export KUBECONFIG=<Путь до /.kube/config>  
docker build . -t cr.yandex/crpaf19gsd0s6onatoae/<Название сервиса>:v2025-11-03-r1  
docker push cr.yandex/crpaf19gsd0s6onatoae/<Название сервиса>:v2025-11-03-r1  
```
Создать secret  
kubectl create secret docker-registry yc-registry-key \
  --docker-server=cr.yandex \
  --docker-username=iam \
  --docker-password=$(yc iam create-token) \
  --docker-email=none@example.com \
  -n <NameSpace>
```
helm upgrade --install --atomic <Имя сервиса> app -n <NameSpace>
