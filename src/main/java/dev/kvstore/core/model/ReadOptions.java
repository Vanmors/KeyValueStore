package dev.kvstore.core.model;

import java.time.Duration;

public record ReadOptions(Duration timeout) {
    public static final ReadOptions DEFAULT =
            new ReadOptions(Duration.ofSeconds(3));
}