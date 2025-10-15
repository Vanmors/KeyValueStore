package dev.kvstore.core.model;

// TODO need to add nodeId when we do sharding
public record WALEntry(Long id, byte[] key, byte[] value, boolean tombstone, WALOperationType operationType, long timestamp) {
}
