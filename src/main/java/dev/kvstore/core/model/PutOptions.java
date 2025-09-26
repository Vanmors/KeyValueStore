package dev.kvstore.core.model;

import java.time.Duration;
import java.util.Optional;

public record PutOptions(
        Optional<Duration> ttl,
        boolean sync
) {
    public static final PutOptions DEFAULT =
            new PutOptions(Optional.empty(), false);

    public static PutOptions withTtl(Duration ttl) {
        return new PutOptions(Optional.ofNullable(ttl), false);
    }
}