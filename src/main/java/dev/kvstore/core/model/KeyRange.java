package dev.kvstore.core.model;

public record KeyRange(byte[] fromInclusive, byte[] toExclusive) {
    // для снэпшотов или экспорта
    public static KeyRange all() {
        return new KeyRange(null, null);
    }
}
