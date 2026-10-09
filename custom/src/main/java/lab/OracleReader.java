package lab;

import lab.record.Cursor;
import lab.record.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public final class OracleReader {
    private static final String QUERY = """
            SELECT ID, STATUS, AMOUNT, UPDATED_AT FROM APP.ORDERS
            WHERE (UPDATED_AT > ? OR (UPDATED_AT = ? AND ID > ?))
              AND UPDATED_AT < ?
            ORDER BY UPDATED_AT, ID FETCH FIRST ? ROWS ONLY
            """;
    private final int batchSize;
    private final long delayMs;

    public OracleReader(int batchSize, long delayMs) {
        if (batchSize < 1 || delayMs < 0) throw new IllegalArgumentException("Invalid batch/delay");
        this.batchSize = batchSize;
        this.delayMs = delayMs;
    }

    public List<Row> read(Connection connection, Cursor cursor) throws SQLException {
        Timestamp cutoff;
        // Use Oracle's UTC clock, not the application's local clock.
        try (var statement = connection.prepareStatement(
                "SELECT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) - NUMTODSINTERVAL(? / 1000, 'SECOND') FROM DUAL")) {
            statement.setQueryTimeout(30);
            statement.setLong(1, delayMs);
            try (var rs = statement.executeQuery()) {
                rs.next();
                cutoff = Timestamp.valueOf(rs.getObject(1, java.time.LocalDateTime.class));
            }
        }
        var rows = new ArrayList<Row>();
        try (var statement = connection.prepareStatement(QUERY)) {
            Timestamp previous = Timestamp.valueOf(cursor.timestamp());
            statement.setTimestamp(1, previous);
            statement.setTimestamp(2, previous);
            statement.setLong(3, cursor.id());
            statement.setTimestamp(4, cutoff);
            statement.setInt(5, batchSize);
            statement.setQueryTimeout(30);
            statement.setFetchSize(batchSize);
            try (var rs = statement.executeQuery()) {
                while (rs.next()) rows.add(new Row(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3),
                        rs.getObject(4, java.time.LocalDateTime.class)));
            }
        }
        return rows;
    }
}
