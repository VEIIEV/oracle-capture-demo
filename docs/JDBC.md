# Confluent JDBC Source: настройка и права

Предусловие: выполнена [общая подготовка](ORACLE.md). Для полного демо сначала выполните [CDC](CDC.md), пока polling-workers не запущены. Создавать Oracle или таблицу повторно не нужно.

## 1. Права в Oracle

Пользователь этого варианта — KAFKA_JDBC/jdbc_reader. Его права:

```sql
GRANT CREATE SESSION TO KAFKA_JDBC;
GRANT SELECT ON APP.ORDERS TO KAFKA_JDBC;
```

CREATE SESSION нужен для подключения, SELECT — для конкретной таблицы. Коннектор не получает CREATE TABLE, SYSDBA/DBA, SELECT ANY TABLE или права LogMiner. ARCHIVELOG и supplemental logging **для JDBC не требуются**.

Создание пользователя и эти grants находятся отдельно в `sql/jdbc/01-grants.sql`. Выполнить от SYS:

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/jdbc/01-grants.sql
```

Проверьте, что reader действительно читает таблицу:

```bash
docker compose exec -T oracle sqlplus -s kafka_jdbc/jdbc_reader@//localhost:1521/XEPDB1 <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
SELECT COUNT(*) FROM APP.ORDERS;
EXIT;
SQL
```

В новой таблице с seed ожидайте 3. После других опытов count может отличаться; главное — нет ORA-01017/ORA-00942.

## 2. Проверить конфигурацию

- `docker/Dockerfile.jdbc`: устанавливает Confluent JDBC Source 10.8.0 и Oracle JDBC driver.
- `config/jdbc.json`: читает APP.ORDERS под KAFKA_JDBC, пишет `jdbc.ORDERS`.
- `../docker-compose.yaml`: отдельный distributed worker, REST 8083, служебные топики jdbc-configs/jdbc-offsets/jdbc-status.

| Параметр | Значение в демо | Причина |
|---|---|---|
| mode | timestamp+incrementing | Cursor UPDATED_AT + ID |
| timestamp.column.name | UPDATED_AT | Обнаружение изменившейся строки |
| incrementing.column.name | ID | Порядок строк с одинаковым timestamp |
| poll.interval.ms | 1000 | Опрос раз в секунду |
| timestamp.delay.interval.ms | 5000 | Не читать слишком свежие timestamps |
| batch.max.rows | 2 | Наблюдаемая пагинация seed из 3 строк |
| db.timezone | UTC | Интерпретация TIMESTAMP без timezone |

UPDATED_AT должен меняться при каждом UPDATE, ID новых строк задавайте возрастающим. В демо timestamps задаются UTC в DML. Задержка 5 секунд не гарантирует захват любых поздних commits. JDBC не хранит журнал промежуточных состояний и не обнаруживает DELETE.

## 3. Собрать и запустить worker

```bash
docker compose build connect-jdbc
docker compose up -d connect-jdbc
docker compose logs --tail=100 connect-jdbc
curl -fsS http://localhost:8083/connector-plugins
```

Если REST ещё не готов, дождитесь загрузки worker и повторите curl. В списке должен быть io.confluent.connect.jdbc.JdbcSourceConnector.

## 4. Зарегистрировать только JDBC

```bash
curl -fsS -X PUT http://localhost:8083/connectors/oracle-jdbc/config \
  -H 'Content-Type: application/json' --data-binary @config/jdbc.json
curl -fsS http://localhost:8083/connectors/oracle-jdbc/status
```

Ожидайте connector RUNNING и task RUNNING. RUNNING только connector при FAILED task не означает рабочий захват. Ошибка: status.trace и `docker compose logs --tail=150 connect-jdbc`.

PUT обновляет конфигурацию без удаления offsets. При первом запуске читаются существующие строки; после рестарта продолжается сохранённая позиция в jdbc-offsets.

## 5. Посмотреть сообщения

В отдельном WSL-терминале, корень проекта:

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic jdbc.ORDERS --from-beginning --property print.key=true --property print.offset=true
```

На первом запуске ожидайте ID 1–3. Key — объект с ID (SMT ValueToKey в конфиге); value — строка таблицы, без op INSERT/UPDATE. Повторный --from-beginning показывает историю заново — сравнивайте Kafka offset.

Действия INSERT/UPDATE/DELETE и контрольные результаты общие в [DEMO.md](DEMO.md), здесь не выполняйте лишних изменений перед выступлением.

Источник параметров: [Confluent JDBC Source configuration](https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html).
