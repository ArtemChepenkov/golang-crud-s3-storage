![Go](https://img.shields.io/badge/Golang-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-4A154B?style=for-the-badge&logo=grpc&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)

### О приложении
Данный проект представляет из себя хранилище для файлов, для которого реализованы функции добавления/получения/удаления файла, а также вывода списка хранящихся файлов.
#### Особенности реализации
- Язык программирования - Golang
- Микросервисная архитектура с использованием gRPC
- Использование kafka (sarama)
- Используется Minio для работы с файлами и PostgreSQL для хранения метаданных
- Для мониторинга используется prometheus + grafana
- Все микросервисы работают как отдельные Docker контейнеры

### Запуск приложения
#### Склонируйте репозиторий
```bash
git clone https://github.com/ArtemChepenkov/golang-crud-s3-storage.git
```
#### Запустите в корне проекта
```bash
docker compose up -d
```
### Пример работы
#### Отправка файла
```bash
curl -X POST \
  -H "Content-Type: application/octet-stream"   -H "filename: test.c" \
  -H "userId: 123"   --data-binary @test.c \
  "http://localhost:8080/api/v1/files/upload/123?filename=test.c&userId=123"
```
#### Скачать файл с хранилища
```bash
curl -X GET \
  "http://localhost:8080/api/v1/files/download/123?filename=123-test.c-123&userId=123" \
  -o downloaded.c
```
#### Показать список файлов для пользователя
```bash
curl -X GET \
  "http://localhost:8080/api/v1/files/list/123?userId=123"
```
#### Удалить файл
```bash
curl -X DELETE \
  "http://localhost:8080/api/v1/files/delete/123?filename=test.c&userId=123"
```

