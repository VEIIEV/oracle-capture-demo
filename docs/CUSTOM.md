# Custom Kafka Connect — только IDEA

Предусловия: общая БД/Kafka готовы; для полного демо CDC/JDBC уже запущены. Этот вариант читает ту же APP.ORDERS тем же принципом polling, что JDBC, но реализацию SourceConnector/SourceTask можно отлаживать.

## 1. Права: тот же набор, другой пользователь

В WSL, корень проекта:

```bash
docker compose exec -T oracle sqlplus -s / as sysdba < sql/custom/01-grants.sql
```

Создаётся KAFKA_CUSTOM/custom_reader. **Отличие от JDBC — имя пользователя; набор прав одинаковый**: CREATE SESSION + SELECT ON APP.ORDERS. LogMiner, ARCHIVELOG и специальные grants custom не нужны.

## 2. Создать топик

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists --topic custom.connect.orders --partitions 1 --replication-factor 1
```

**Примечание:** topic.creation.enable=false у нашего worker; поэтому топик создаём явно. JDBC в этой лаборатории создаёт свой топик автоматически.

## 3. Run / Debug в IDEA

Откройте корневой pom.xml проекта, дождитесь Maven import, Project SDK / Maven JDK = 17. Выберите готовую конфигурацию **Oracle Custom Connect (IDEA only)** → Run или Debug.

Параметры конфигурации уже заданы в `.run/Oracle_Custom_Connect.run.xml`:

| Поле | Значение |
|---|---|
| Main class | lab.MainApplication |
| Module classpath | oracle-connect-source |
| VM options | -Duser.timezone=UTC |
| Working directory | $PROJECT_DIR$/custom |
| Program arguments | Пустые |

Если IDEA не подхватила shared configuration или назвала модуль иначе: создайте Application вручную с этими полями, выберите модуль, содержащий IdeaConnect.java. Working directory на Windows — `C:\Users\VEIIEV\IdeaProjects\oracle-capture-demo\custom`.

`MainApplication` передаёт два properties-файла стандартному ConnectStandalone:

- `custom/config/worker.properties`: localhost:9092, REST 8085, JSON converters, offset flush 1000 ms.
- `custom/config/oracle.properties`: localhost:1521/XEPDB1, KAFKA_CUSTOM, batch=2, poll=1000 ms, delay=5000 ms.

**Примечание:** из JVM на Windows используем localhost. Docker worker JDBC использует oracle/kafka — эти имена контейнерной сети не заменяют localhost в IDEA.

В **Windows PowerShell**:

```powershell
Invoke-RestMethod http://localhost:8085/connectors/oracle-custom/status | ConvertTo-Json -Depth 10
Test-NetConnection localhost -Port 1521
Test-NetConnection localhost -Port 9092
```

Ожидайте connector/task RUNNING. REST 8085 проверяем с Windows: localhost Windows и WSL могут отличаться в зависимости от сетевого режима. Не регистрируйте custom curl POST/PUT: standalone получает connector config при запуске Main.

## 4. Коротко: отличия от JDBC

| Что | Confluent JDBC | Наш вариант |
|---|---|---|
| Реализация | Готовый JDBC plugin | OraclePollingSourceConnector + OraclePollingSourceTask |
| Запуск | Docker distributed worker | ConnectStandalone из IDEA |
| Регистрация | REST PUT с jdbc.json | oracle.properties при запуске Main |
| Oracle reader | KAFKA_JDBC | KAFKA_CUSTOM, те же права |
| Offsets | Kafka jdbc-offsets | custom/state/connect-offsets.bin, управляет Connect |
| Key/value | Key Struct ID, стандартный JDBC mapping | Key числовой ID; AMOUNT decimal string; UPDATED_AT ISO string |
| Cursor и ограничения | Timestamp + ID polling | Те же принципы: UPDATE требует новый timestamp, DELETE не виден |
| Отладка | Логи worker | Breakpoints в нашем Java-коде |

**Примечание:** custom — это настоящий Connect-плагин, не самостоятельный KafkaProducer. Task возвращает SourceRecord с sourcePartition/sourceOffset; публикацию и подтверждённую позицию обслуживает Kafka Connect. Нет собственного checkpoint-протокола.

## 5. Что показать в Debug

Breakpoints: OraclePollingSourceTask.start → восстановленный cursor; OracleReader.read → SQL и cutoff; OraclePollingSourceTask.poll → rows и SourceRecord.sourceOffset().

Следите за timestamp/id, не редактируйте offsets вручную. JDBC/custom могут пропустить промежуточные быстрые UPDATE и поздний commit позади cursor. Custom — учебный poller одной фиксированной таблицы, не замена журналу изменений.

Consumer в WSL:

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic custom.connect.orders --from-beginning --property print.key=true --property print.offset=true
```

На первом запуске — строки таблицы, затем ждём [общего демо](DEMO.md). Ctrl+C завершает consumer, а не JVM.

## 6. Перезапуск

Stop/Run того же Application с той же working directory. Сохраняйте custom/state/connect-offsets.bin. После жёсткого Stop возможны повторы неподтверждённых offsets; гарантия at-least-once. Не удаляйте offsets для обычного restart. Этот файл должен относиться к тому же Kafka/Oracle стенду: после сброса volumes старый cursor использовать нельзя.
