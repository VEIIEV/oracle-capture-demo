# Демо: один Oracle, три подхода захвата

## Подготовка до выступления

Пройдите README: общая БД → CDC (режим журналов/права) → JDBC → custom. Должны работать все три task, consumer должен показать seed ID 1–3. Maven dependencies и Docker images загрузите заранее. Docker builds/initial Oracle init оставьте до выступления.

Ожидаемый набор перед началом: четыре контейнера (oracle/kafka/connect-jdbc/connect-cdc), одна JVM из IDEA. Custom не контейнер.

Показывать файлы в IDEA:

- compose.yaml: общая инфраструктура.
- sql/common/01-schema.sql: одна таблица для трёх вариантов.
- sql/jdbc/01-grants.sql и sql/custom/01-grants.sql: одинаковый минимум прав у разных readers.
- sql/cdc/02-logging.sql и sql/cdc/03-grants.sql: дополнительные требования CDC.
- config/jdbc.json, config/cdc.json, custom/config/oracle.properties: настройки.
- OraclePollingSourceConnector / OraclePollingSourceTask / OracleReader: что делает наш код.

## 1. Проверить состояние

В WSL из корня проекта:

```bash
docker compose ps
curl -fsS http://localhost:8083/connectors/oracle-jdbc/status
curl -fsS http://localhost:8084/connectors/oracle-cdc/status
```

В Windows PowerShell:

```powershell
Invoke-RestMethod http://localhost:8085/connectors/oracle-custom/status | ConvertTo-Json -Depth 10
```

Во всех ответах connector и task RUNNING. Если task FAILED, сначала исправьте причину: status.trace, docker compose logs или консоль IDEA. Нельзя демонстрировать отсутствие события у неработающего task как свойство polling.

## 2. Открыть три consumer

Три WSL-терминала, каждый в `/mnt/c/Users/VEIIEV/IdeaProjects/oracle-capture-demo`:

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic jdbc.ORDERS --from-beginning --property print.key=true --property print.offset=true
```

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic custom.connect.orders --from-beginning --property print.key=true --property print.offset=true
```

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc.APP.ORDERS --from-beginning --property print.key=true --property print.offset=true
```

--from-beginning печатает имеющуюся историю, а затем новые сообщения. Повторный запуск consumer перечитывает историю — это не новое событие connector. Отмечайте Kafka offsets и IDs. Ctrl+C завершает только consumer. Если нужен экран без истории, уберите --from-beginning до запуска consumer, но seed так не увидите.

На первом прогоне: JDBC/custom — строки ID 1–3; CDC — op=r. В четвёртом WSL-терминале выполняйте дальнейшие DML.

**Фраза для пояснения:** «Polling читает состояние таблицы по timestamp и ID. Debezium сначала получает snapshot, затем читает журнал изменений».

## 3. Проверить свободные IDs

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/00-check-ids.sql
```

SELECT не должен вернуть 100/101/102. Если вернул — перед выступлением выберите новые IDs и замените их **согласованно во всех sql/demo/*.sql**: например 100→1000, 101→1001, 102→1002. Историю Kafka не очищайте. Значения должны быть новыми и возрастающими для polling.

На каждом этапе дождитесь ожидаемой доставки перед следующим действием. Polling намеренно имеет delay=5 s + poll interval=1 s; быстрый набор DML может скрыть промежуточную версию строки.

## 4. INSERT + COMMIT

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/01-insert.sql
```

Покажите ID 100 / STATUS=INSERTED / AMOUNT=10 в трёх consumers. У CDC op=c, данные в after. У JDBC/custom просто текущее состояние строки, без отдельного INSERT op.

**Не переходите дальше, пока INSERT не появился во всех трёх**. Дополнительный consumer lag/запуск LogMiner может увеличить ожидание.

## 5. UPDATE + новый UPDATED_AT

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/02-update.sql
```

Покажите ID 100 / UPDATED / AMOUNT=20 во всех трёх. CDC op=u. Polling видит новый cursor UPDATED_AT и читает строку повторно.

## 6. UPDATE без изменения UPDATED_AT

Предыдущий UPDATE уже должен быть доставлен обоими pollers.

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/03-update-no-timestamp.sql
```

CDC выдаст op=u / NO_TIMESTAMP / AMOUNT=30. JDBC/custom не выдают новой строки, потому что timestamp/ID не сдвинулись. Наблюдайте минимум 15 секунд, затем при необходимости повторите status-проверку.

**Фраза:** «Это ограничение выбранного polling cursor. Наш custom имеет ту же границу, поскольку использует такой же способ SELECT».

## 7. ROLLBACK

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/04-rollback.sql
```

SELECT в SQL не возвращает ID 101. Ни один вариант не выдаёт событие ID 101: изменение откатилось, а committed polling его не увидел.

## 8. DELETE + COMMIT

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/05-delete.sql
```

CDC: op=d, after=null, затем tombstone с тем же key и null value. JDBC/custom: нового сообщения об удалении нет, исчезнувшая строка не возвращается SELECT. Наблюдайте consumers и подтверждайте RUNNING statuses.

## 9. Перезапуск custom и его offsets

1. Проверьте, что ранее созданные сообщения дошли; подождите несколько секунд на offset flush (настройка 1000 ms).
2. В IDEA покажите `custom/state/connect-offsets.bin`: файл framework, бинарный, не редактируем. Зафиксируйте последний Kafka offset custom consumer.
3. Stop Application в IDEA; дождитесь завершения JVM. Docker остаётся работать.
4. В WSL вставьте ID 102:

```bash
docker compose exec -T oracle sqlplus -s app/app@//localhost:1521/XEPDB1 < sql/demo/06-restart-insert.sql
```

5. JDBC/CDC получают WHILE_STOPPED; custom consumer пока не получает, JVM не работает.
6. Run того же Application, с той же working directory. Custom должен доставить ID 102 после delay. При Debug в start покажите cursor, прочитанный context.offsetStorageReader().

**Фраза:** «Connect отвечает за offsets. Наш код описывает чтение и SourceRecord, а не реализует KafkaProducer/собственный checkpoint».

После жёсткого Stop возможны повторы последних сообщений: это at-least-once. --from-beginning и аварийный повтор различаются по новым Kafka offsets и прежнему ID. Не обещайте exactly-once.

## Таблица ожидаемых результатов

| Действие | JDBC | Custom | CDC |
|---|---|---|---|
| Первый старт с пустыми offsets | Строки | Строки | Snapshot r |
| INSERT + COMMIT | Строка | Строка | c |
| UPDATE + новый timestamp | Строка | Строка | u |
| UPDATE без timestamp, после доставки предыдущего | Нет | Нет | u |
| ROLLBACK | Нет | Нет | Нет |
| DELETE | Нет | Нет | d + tombstone |
| INSERT при выключенном custom | Уже получает | После Run | Уже получает |

## Если опыт не совпал

- Нет INSERT у polling: дождитесь delay, проверьте COMMIT, task RUNNING, UTC UPDATED_AT и что cursor не был сохранён от другого стенда.
- UPDATE без timestamp появился у polling: предыдущая версия ещё не была доставлена; действия выполнены слишком быстро. Повторите с новым ID и дождитесь всех этапов.
- CDC молчит: проверьте streaming/snapshot в логах, logging, common-user privileges, доступность archive logs. DML должен быть под APP.
- Таблица ORA-00942 / неверный count: проверьте XEPDB1, APP и reader grants.
- Повторный прогон: новые IDs; не переустанавливайте БД/коннекторы во время выступления.

## Завершение

Stop custom в IDEA. Остановите контейнеры командами из общей инструкции. Volumes и offsets оставьте для следующего запуска.
