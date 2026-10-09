package lab.record;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record Row(long id, String status, BigDecimal amount, LocalDateTime updatedAt) {
    public Cursor cursor() {
        return new Cursor(updatedAt, id);
    }
}
