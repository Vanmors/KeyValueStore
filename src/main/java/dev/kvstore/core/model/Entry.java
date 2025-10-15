package dev.kvstore.core.model;


public record Entry(byte[] key, byte[] value, boolean tombstone) {

    public Entry(byte[] key, byte[] value, boolean tombstone) {
        this.key = key;
        this.value = value;
        this.tombstone = tombstone;
    }
}
