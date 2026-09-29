# Инструкция для демонстрации проекта

Эта инструкция рассчитана на macOS и shell zsh. Демонстрация показывает три API-узла, отдельную базу данных у каждого узла, доставку событий через RabbitMQ и восстановление после временного отключения узла B.

## 1. Запуск проекта

Откройте Terminal в папке проекта. Проверьте, что файл `.env` заполнен значениями для MySQL, RabbitMQ и `API_KEY`. Не показывайте содержимое `.env` на демонстрации: в нём находятся секреты.

Запустите все сервисы:

```sh
docker compose up --build --force-recreate -d
```

Compose сам запускает базы и RabbitMQ, дожидается их готовности, выполняет миграции, а затем запускает API, workers и receivers. Данные сохраняются в Docker volumes.

Проверьте состояние контейнеров:

```sh
docker compose ps -a
```

Базы, RabbitMQ, API, workers и receivers должны иметь состояние `healthy` или `Up`. Сервисы `migrate_a`, `migrate_b` и `migrate_c` — одноразовые: после успешной миграции они обычно показывают `Exited (0)`. Код `Exited (1)` означает ошибку; подробности можно посмотреть так:

```sh
docker compose logs --no-color migrate_a migrate_b migrate_c
```

Проверьте доступность всех API:

```sh
for port in 8000 8001 8002; do
  curl -fsS "http://localhost:$port/health"
  printf '\n'
done
```

Ожидаемый ответ каждого узла содержит `"status":"healthy"` и его `node_id`.

## 2. Открытие Swagger и авторизация

Откройте документацию API:

- Узел A: [http://localhost:8000/docs](http://localhost:8000/docs)
- Узел B: [http://localhost:8001/docs](http://localhost:8001/docs)
- Узел C: [http://localhost:8002/docs](http://localhost:8002/docs)

Нажмите **Try it out** у нужного метода. Для всех запросов, кроме `GET /health`, укажите заголовок `X-API-Key`. Введите значение `API_KEY` из `.env` в поле `x-api-key`, которое Swagger показывает для запроса. Не отправляйте этот ключ преподавателю и не включайте его в скриншоты.

## 3. Создание и репликация заказа

Сначала на узле A откройте `POST /orders/`, нажмите **Try it out** и отправьте:

```json
{
  "item_name": "initial-A",
  "price": 100
}
```

В ответе появятся `record_id` и `event_id`. Сохраните `record_id`: это общий UUID записи для всех узлов. Числовой `order_id` локален для базы и на разных узлах может различаться.

Затем откройте `GET /manifest` на узлах B и C. Подождите несколько секунд и выполните запрос повторно: worker отправляет событие через RabbitMQ, поэтому репликация происходит асинхронно. На всех трёх узлах запись должна появиться с одинаковым `record_id`; после сходимости совпадут также `count` и `checksum`.

Для просмотра обычного списка используйте `GET /orders/`. Для проверки очередей и обработки событий используйте `GET /status`: `pending_outbox` — события, ожидающие отправки, `processed_events` — события, принятые узлом.

## 4. Обновление и удаление

Чтобы проверить обновление, на узле A выполните `PUT /orders/{record_id}`. Подставьте UUID из ответа создания и отправьте новое тело:

```json
{
  "item_name": "updated-A",
  "price": 150
}
```

Проверьте изменение через `GET /manifest` на B и C.

Удаление выполните последним: `DELETE /orders/{record_id}`. Оно создаёт tombstone (`is_deleted: true`), а не физически стирает запись. Поэтому удалённая запись скрыта в `GET /orders/`, но остаётся видна в `GET /manifest` или в `GET /orders/?include_deleted=true`. Это не позволяет старому событию восстановить удалённый заказ.

## 5. Демонстрация

Выполняйте пункты по порядку. Под «отключить B» здесь понимается отключить API B, worker B и receiver B от сети RabbitMQ; база B остаётся включённой, чтобы B мог принимать локальные записи.

### 1. Запустить узлы A, B и C

Выполните запуск и проверки из раздела 1. Перед демонстрацией убедитесь, что API, базы и RabbitMQ работают, а все три `GET /health` возвращают `healthy`.

### 2. Создать данные на A и показать репликацию

В Swagger A (`http://localhost:8000/docs`) выполните `POST /orders/`:

```json
{
  "item_name": "initial-A",
  "price": 100
}
```

Сохраните `record_id` из ответа. Выполните `GET /manifest` на B и C. Если запись не появилась сразу, подождите несколько секунд и повторите запрос: доставка асинхронная. Покажите, что один и тот же `record_id` появился на трёх узлах.

### 3. Отключить узел B от RabbitMQ

В терминале из папки проекта выполните:

```sh
for service in api_b worker_b receiver_b; do
  container_id=$(docker compose ps -q "$service")
  docker network disconnect event-system-broker "$container_id"
done
```

Контейнеры и база B продолжают работать, но сервисы B не могут обмениваться сообщениями через RabbitMQ. Docker Desktop может временно перестать перенаправлять порт Swagger B; для запросов к B используйте приведённые ниже команды `docker compose exec`.

### 4. Создать разные данные на A и B

Сначала на A создайте отдельный заказ через Swagger `POST /orders/`:

```json
{
  "item_name": "partition-A",
  "price": 110
}
```

Затем создайте другой заказ локально на B. Если Swagger B доступен, выполните там `POST /orders/` с телом:

```json
{
  "item_name": "partition-B",
  "price": 220
}
```

Если Swagger B недоступен, выполните запрос из контейнера B; ключ берётся из окружения контейнера и не печатается:

```sh
docker compose exec -T api_b python -c 'import json,os,urllib.request; data=json.dumps({"item_name":"partition-B","price":220}).encode(); req=urllib.request.Request("http://127.0.0.1:8000/orders/", data=data, headers={"X-API-Key":os.environ["API_KEY"],"Content-Type":"application/json"}, method="POST"); print(urllib.request.urlopen(req).read().decode())'
```

### 5. Запустить синхронизацию во время сбоя и показать безопасную ошибку

На B вызовите `POST /sync` без `replay_all`. Ожидаемый результат — HTTP `503`: брокер недоступен. Это намеренная проверка, а не поломка: событие B должно остаться в `Outbox` и не потеряться.

Если Swagger B недоступен, команда ниже вызовет sync из контейнера, покажет код ответа и завершится успешно только при ожидаемом `503`:

```sh
docker compose exec -T api_b python -c '
import os, urllib.request, urllib.error
req = urllib.request.Request("http://127.0.0.1:8000/sync", data=b"", headers={"X-API-Key": os.environ["API_KEY"]}, method="POST")
try:
    urllib.request.urlopen(req)
except urllib.error.HTTPError as error:
    print(error.code, error.read().decode())
    assert error.code == 503
else:
    raise AssertionError("Ожидался HTTP 503 при отключённом RabbitMQ")
'
```

Теперь проверьте `GET /status` на B: `pending_outbox` должен быть больше нуля. Если Swagger B недоступен:

```sh
docker compose exec -T api_b python -c 'import os,urllib.request; req=urllib.request.Request("http://127.0.0.1:8000/status", headers={"X-API-Key":os.environ["API_KEY"]}); print(urllib.request.urlopen(req).read().decode())'
```

### 6. Восстановить соединение B

Подключите API B, worker B и receiver B обратно к RabbitMQ:

```sh
for service in api_b worker_b receiver_b; do
  container_id=$(docker compose ps -q "$service")
  docker network connect event-system-broker "$container_id"
done
```

Если Docker сообщает `endpoint ... already exists in network event-system-broker`, этот контейнер уже подключён; повторно выполнять `docker network connect` не нужно. Проверьте список подключённых контейнеров:

```sh
docker network inspect event-system-broker
```

Убедитесь, что в `Containers` есть `api_b`, `worker_b` и `receiver_b`, затем переходите к шагу 7. Если приложение B отвечает внутри контейнера, но `http://localhost:8001/docs` не открывается, временно проверяйте B командами `docker compose exec -T api_b ...` из шагов 4 и 5; Docker Desktop может потерять перенаправление порта после изменения сети. Перезапуск Docker Desktop обычно восстанавливает его.

### 7. Показать завершение отложенной синхронизации

Подождите несколько секунд: worker B автоматически отправит событие из своего `Outbox`, а receiver B обработает сообщения, накопившиеся в очереди. Откройте `GET /manifest` на A, B и C. Оба заказа `partition-A` и `partition-B` должны присутствовать на всех узлах.

Откройте `GET /status` на B и покажите, что `pending_outbox` снова равен нулю. После сходимости `count` и `checksum` в `/manifest` должны совпасть на A, B и C.

### 8. Повторить успешную синхронизацию и показать отсутствие дублей

На A, B и C по очереди выполните `POST /sync?replay_all=true`. Это повторно публикует сохранённые события. Затем снова вызовите `GET /manifest` и `GET /status`: заказы не должны дублироваться, количество записей и контрольные суммы должны остаться прежними. `Inbox` распознаёт уже обработанные `event_id`.

### 9. Сравнить manifests, количество, ID, версии и контрольные суммы

На каждом узле выполните `GET /manifest` и сравните:

- `count` — одинаковое количество заказов;
- `records[].record_id` — одинаковые UUID одних и тех же заказов;
- `records[].version` — одинаковые версии после завершения репликации;
- `checksum` — одинаковая контрольная сумма всего набора записей.

Сравнивайте именно `record_id`, а не числовой `order_id`: `order_id` — локальный автоинкремент базы и может различаться между узлами. После tombstone удалённая запись остаётся в manifest с `is_deleted: true`.

### 10. Перезапустить узел и показать сохранность данных

Для проверки сохранности перезапустите базу C, не удаляя Docker volumes:

```sh
docker compose restart mysql_c
docker compose ps mysql_c
```

Дождитесь, пока `mysql_c` снова станет `healthy`, затем перезапустите процессы узла C:

```sh
docker compose restart api_c worker_c receiver_c
docker compose ps
```

После восстановления `api_c` откройте Swagger C и выполните `GET /manifest`. Покажите, что заказы, их `record_id`, версии и checksum сохранились. Не выполняйте `docker compose down -v`: эта команда удалит базы и данные RabbitMQ.

## 7. Остановка после демонстрации

Остановите контейнеры, сохранив данные:

```sh
docker compose down
```

Не используйте `docker compose down -v`, если нужно сохранить базы и состояние RabbitMQ: флаг `-v` удаляет volumes.

## Что показать преподавателю

- Три Swagger-документации и успешный `GET /health` на каждом узле.
- Создание заказа на A и его появление на B и C с тем же `record_id`.
- Совпадающие `count` и `checksum` в `/manifest` после репликации.
- При желании — рост `pending_outbox` на изолированном B и его обнуление после восстановления связи.
- Повторный `POST /sync?replay_all=true` без изменения итоговых записей.