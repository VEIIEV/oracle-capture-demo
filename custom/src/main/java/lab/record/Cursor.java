package lab.record;

import java.time.LocalDateTime;
import java.util.Objects;

public record Cursor(LocalDateTime timestamp, long id) implements Comparable<Cursor> {
    public static final Cursor INITIAL = new Cursor(LocalDateTime.of(1970, 1, 1, 0, 0), -1);

    public Cursor {
        Objects.requireNonNull(timestamp);
    }

    @Override
    public int compareTo(Cursor other) {
        int result = timestamp.compareTo(other.timestamp);
        return result != 0 ? result : Long.compare(id, other.id);
    }
}
