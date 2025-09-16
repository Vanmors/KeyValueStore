package dev.kvstore.core;

public class KVException extends Exception {
    public KVException(String message) {
        super(message);
    }

    public KVException(String message, Throwable cause) {
        super(message, cause);
    }
}
