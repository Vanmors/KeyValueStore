package dev.kvstore.core.model;

import java.util.Optional;

public record DeleteOptions(
        boolean sync
) {
    public static final DeleteOptions DEFAULT =
            new DeleteOptions(false);
}
