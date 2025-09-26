package dev.kvstore.core.model;

public record DeleteOptions(boolean sync) {
    public static final DeleteOptions DEFAULT =
            new DeleteOptions(false);
}
