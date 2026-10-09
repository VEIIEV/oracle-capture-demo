package lab;

import lab.record.Cursor;
import lab.record.Row;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class OraclePollingSourceTask extends SourceTask {
    private static final Map<String, String> PARTITION = Map.of("table", "APP.ORDERS");
    private static final Schema VALUE = SchemaBuilder.struct().name("lab.Order")
            .field("ID", Schema.INT64_SCHEMA).field("STATUS", Schema.OPTIONAL_STRING_SCHEMA)
            .field("AMOUNT", Schema.OPTIONAL_STRING_SCHEMA).field("UPDATED_AT", Schema.STRING_SCHEMA).build();

    private final Object signal = new Object();
    private final Reader injected;

    private Reader reader;
    private Connection connection;
    private Cursor cursor;
    private volatile boolean running;
    private String topic;
    private long interval;
    private long nextPoll;

    public OraclePollingSourceTask() {
        this(null);
    }

    OraclePollingSourceTask(Reader reader) {
        this.injected = reader;
    }

    public String version() {
        return "2.0.0";
    }

    public void start(Map<String, String> props) {
        var cfg = new AbstractConfig(new OraclePollingSourceConnector().config(), props);
        topic = cfg.getString("topic");
        interval = cfg.getLong("poll.interval.ms");
        cursor = Cursor.INITIAL;
        var offset = context.offsetStorageReader().offset(PARTITION);
        if (offset != null) {
            try {
                cursor = new Cursor(LocalDateTime.parse((String) offset.get("timestamp")), ((Number) offset.get("id")).longValue());
            } catch (RuntimeException e) {
                throw new ConnectException("Invalid stored Oracle cursor; do not silently reset offsets", e);
            }
        }
        if (injected != null) reader = injected;
        else try {
            var jdbc = new Properties();
            jdbc.setProperty("user", cfg.getString("connection.user"));
            jdbc.setProperty("password", cfg.getPassword("connection.password").value());
            jdbc.setProperty("oracle.net.CONNECT_TIMEOUT", "10000");
            jdbc.setProperty("oracle.jdbc.ReadTimeout", "30000");
            connection = DriverManager.getConnection(cfg.getString("connection.url"), jdbc);
            var oracle = new OracleReader(cfg.getInt("batch.max.rows"), cfg.getLong("timestamp.delay.interval.ms"));
            reader = c -> oracle.read(connection, c);
        } catch (SQLException e) {
            throw new ConnectException("Cannot connect to Oracle", e);
        }
        nextPoll = 0;
        running = true;
    }

    public List<SourceRecord> poll() throws InterruptedException {
        synchronized (signal) {
            while (running && System.currentTimeMillis() < nextPoll)
                signal.wait(Math.max(1, nextPoll - System.currentTimeMillis()));
        }
        if (!running) return null;
        try {
            var rows = reader.read(cursor);
            var records = new ArrayList<SourceRecord>();
            for (var row : rows) {
                if (row.cursor().compareTo(cursor) <= 0) throw new ConnectException("Oracle cursor did not advance");
                var value = new Struct(VALUE).put("ID", row.id()).put("STATUS", row.status()).put("AMOUNT", row.amount() == null ? null : row.amount().toPlainString()).put("UPDATED_AT", row.updatedAt().toString());
                records.add(new SourceRecord(PARTITION, Map.of("timestamp", row.updatedAt().toString(), "id", row.id()), topic, Schema.INT64_SCHEMA, row.id(), VALUE, value));
                cursor = row.cursor();
            }
            nextPoll = System.currentTimeMillis() + interval;
            return records;
        } catch (SQLException e) {
            throw new ConnectException("Oracle polling failed", e);
        }
    }

    public void stop() {
        running = false;
        synchronized (signal) {
            signal.notifyAll();
        }
        if (connection != null) try {
            connection.close();
        } catch (SQLException ignored) {
        }
    }

    @FunctionalInterface
    interface Reader {
        List<Row> read(Cursor cursor) throws SQLException;
    }
}
