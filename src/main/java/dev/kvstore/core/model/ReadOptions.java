package dev.kvstore.core.model;

import java.time.Duration;

public record ReadOptions(
        ReadConsistency consistency,
        Duration timeout
) {
    public static final ReadOptions DEFAULT =
            new ReadOptions(ReadConsistency.ANY_REPLICA, Duration.ofSeconds(3));
}
