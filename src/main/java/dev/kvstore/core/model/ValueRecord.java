package dev.kvstore.core.model;

import java.util.Optional;

public record ValueRecord(byte[] value, Optional<Long> expireAtMillis) {
    public static ValueRecord of(byte[] value) {
        // фабрика без TTL
        return new ValueRecord(value, Optional.empty());
    }
}
