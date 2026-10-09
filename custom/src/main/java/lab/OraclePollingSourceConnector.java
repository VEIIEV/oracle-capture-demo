package lab;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class OraclePollingSourceConnector extends SourceConnector {
    private Map<String, String> props;

    public String version() {
        return "2.0.0";
    }

    public void start(Map<String, String> props) {
        this.props = new HashMap<>(props);
    }

    public Class<? extends Task> taskClass() {
        return OraclePollingSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return maxTasks < 1 ? List.of() : List.of(new HashMap<>(props));
    }

    public void stop() {
    }

    public ConfigDef config() {
        return new ConfigDef()
                .define("connection.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Oracle JDBC URL")
                .define("connection.user", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Oracle user")
                .define("connection.password", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Oracle password")
                .define("topic", ConfigDef.Type.STRING, "custom.connect.orders", ConfigDef.Importance.HIGH, "Destination topic")
                .define("batch.max.rows", ConfigDef.Type.INT, 2, ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, "Batch size")
                .define("poll.interval.ms", ConfigDef.Type.LONG, 1000L, ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, "Poll interval")
                .define("timestamp.delay.interval.ms", ConfigDef.Type.LONG, 5000L, ConfigDef.Range.atLeast(0), ConfigDef.Importance.MEDIUM, "Delay window");
    }
}
