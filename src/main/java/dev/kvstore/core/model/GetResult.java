package dev.kvstore.core.model;

public record GetResult(boolean found, ValueRecord value) {
    public static GetResult miss() { return new GetResult(false, null); }
    public static GetResult hit(ValueRecord v) { return new GetResult(true, v); }
}
