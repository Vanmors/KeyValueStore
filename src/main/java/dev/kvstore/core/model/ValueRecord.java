package dev.kvstore.core.model;

public record ValueRecord(byte[] value, long version, Long expireAtMillis) {
    public static ValueRecord of(byte[] value, long version) {
        // фабрика без TTL, удобно там, где истечения нет
        return new ValueRecord(value, version, null);
    }
}
