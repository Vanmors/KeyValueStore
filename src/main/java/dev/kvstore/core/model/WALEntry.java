package dev.kvstore.core.model;

public record WALEntry(byte[] key, byte[] value) {
}
