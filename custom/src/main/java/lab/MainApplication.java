package lab;

import org.apache.kafka.connect.cli.ConnectStandalone;

/**
 * Run this main from IDEA, with the custom directory as working directory.
 */
public final class MainApplication {
    public static void main(String[] args) {
        ConnectStandalone.main(args.length == 0 ? new String[]{"config/worker.properties", "config/oracle.properties"} : args);
    }
}
