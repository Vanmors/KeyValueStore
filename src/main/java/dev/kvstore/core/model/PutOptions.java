package dev.kvstore.core.model;

import java.time.Duration;
import java.util.Optional;

public record PutOptions(
        Optional<Duration> ttl,
        Optional<Long> expectedVersion,
        boolean sync
) {
    public static final PutOptions DEFAULT =
            new PutOptions(Optional.empty(), Optional.empty(), false);

    public static PutOptions withTtl(Duration ttl) {
        return new PutOptions(Optional.ofNullable(ttl), Optional.empty(), false);
    }
}
