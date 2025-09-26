package dev.kvstore.core.model;

import java.time.Duration;

public record PutOptions(
        Duration ttl,
        boolean sync
) {
    public static final PutOptions DEFAULT =
            new PutOptions(null, false);

    public static PutOptions withTtl(final Duration ttl) {
        return new PutOptions(ttl, false);
    }
}