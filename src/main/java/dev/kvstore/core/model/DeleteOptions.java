package dev.kvstore.core.model;

import java.util.Optional;

public record DeleteOptions(
        Optional<Long> expectedVersion,
        boolean sync
) {
    public static final DeleteOptions DEFAULT =
            new DeleteOptions(Optional.empty(), false);
}
