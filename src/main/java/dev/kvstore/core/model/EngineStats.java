package dev.kvstore.core.model;

public record EngineStats(
        long keys,
        long sizeBytes,
        long aliveTtlKeys
) {
}
