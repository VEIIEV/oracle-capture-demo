package lab;

import lab.record.Cursor;
import lab.record.Row;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleSourceTest {
    static Map<String, String> props() {
        return Map.of("connection.url", "jdbc:oracle:thin:@//localhost:1521/XEPDB1", "connection.user", "reader", "connection.password", "reader", "topic", "test", "poll.interval.ms", "1");
    }

    static SourceTaskContext context(Map<String, Object> offset) {
        return new SourceTaskContext() {
            public Map<String, String> configs() {
                return props();
            }

            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    public <T> Map<String, Object> offset(Map<String, T> p) {
                        assertEquals(Map.of("table", "APP.ORDERS"), p);
                        return offset;
                    }

                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> p) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Test
    void connectorCreatesOneTaskAndCopiesConfig() {
        var c = new OraclePollingSourceConnector();
        c.start(props());
        assertEquals(1, c.taskConfigs(10).size());
        assertEquals(props(), c.taskConfigs(10).get(0));
        assertEquals(OraclePollingSourceTask.class, c.taskClass());
        assertTrue(c.taskConfigs(0).isEmpty());
    }

    @Test
    void resumesExactCursorAndProducesConnectOffsets() throws Exception {
        var saved = Map.<String, Object>of("timestamp", "2020-01-01T00:00:00.123456", "id", 2L);
        var seen = new ArrayList<Cursor>();
        var date = LocalDateTime.parse("2020-01-01T00:00:00.123456");
        var t = new OraclePollingSourceTask(c -> {
            seen.add(c);
            return seen.size() == 1 ? List.of(new Row(3, "NEW", new BigDecimal("10.25"), date)) : List.of();
        });
        t.initialize(context(saved));
        t.start(props());
        var records = t.poll();
        assertEquals(new Cursor(date, 2), seen.get(0));
        assertEquals(1, records.size());
        var record = records.get(0);
        assertEquals(Map.of("timestamp", date.toString(), "id", 3L), record.sourceOffset());
        assertEquals("test", record.topic());
        assertEquals(3L, record.key());
        assertEquals("10.25", ((Struct) record.value()).getString("AMOUNT"));
        t.poll();
        assertEquals(new Cursor(date, 3), seen.get(1));
        t.stop();
        assertNull(t.poll());
    }

    @Test
    void firstStartAndNullFields() throws Exception {
        var t = new OraclePollingSourceTask(c -> {
            assertEquals(Cursor.INITIAL, c);
            return List.of(new Row(1, null, null, LocalDateTime.parse("2020-01-01T00:00:00.123456")));
        });
        t.initialize(context(null));
        t.start(props());
        var value = (Struct) t.poll().get(0).value();
        assertNull(value.get("STATUS"));
        assertNull(value.get("AMOUNT"));
        t.stop();
    }

    @Test
    void corruptOffsetFailsInsteadOfResnapshotting() {
        var t = new OraclePollingSourceTask(c -> List.of());
        t.initialize(context(Map.of("id", 1L)));
        assertThrows(org.apache.kafka.connect.errors.ConnectException.class, () -> t.start(props()));
    }

    @Test
    void pluginCanBeLoadedByKafkaRuntime() {
        {
            var plugins = new org.apache.kafka.connect.runtime.isolation.Plugins(Map.of("plugin.discovery", "only_scan"));
            assertInstanceOf(OraclePollingSourceConnector.class, plugins.newConnector("lab.OraclePollingSourceConnector"));
        }
    }

    @Test
    void kafkaFileOffsetStoreSurvivesWorkerRestart(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        var path = dir.resolve("offsets.bin");
        var props = Map.<String, String>of("bootstrap.servers", "localhost:9092", "key.converter", "org.apache.kafka.connect.json.JsonConverter", "value.converter", "org.apache.kafka.connect.json.JsonConverter", "offset.storage.file.filename", path.toString());
        var cfg = new org.apache.kafka.connect.runtime.standalone.StandaloneConfig(props);
        var converter = new org.apache.kafka.connect.json.JsonConverter();
        converter.configure(Map.of("schemas.enable", false), false);
        var partition = Map.of("table", "APP.ORDERS");
        var offset = Map.<String, Object>of("timestamp", "2020-01-01T00:00:00.123456", "id", 3L);
        var first = new org.apache.kafka.connect.storage.FileOffsetBackingStore(converter);
        first.configure(cfg);
        first.start();
        try {
            var writer = new org.apache.kafka.connect.storage.OffsetStorageWriter(first, "oracle-custom", converter, converter);
            writer.offset(partition, offset);
            assertTrue(writer.beginFlush());
            writer.doFlush((error, result) -> {
            }).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            first.stop();
        }
        assertTrue(java.nio.file.Files.size(path) > 0);
        var second = new org.apache.kafka.connect.storage.FileOffsetBackingStore(converter);
        second.configure(cfg);
        second.start();
        try (var reader = new org.apache.kafka.connect.storage.OffsetStorageReaderImpl(second, "oracle-custom", converter, converter)) {
            assertEquals(offset, reader.offset(partition));
        } finally {
            second.stop();
        }
    }
}
