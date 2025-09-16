package dev.kvstore.core.model;

public enum ReadConsistency {
    ANY_REPLICA,
    /**
     * tbd: читать только с мастера.
     */
    MASTER_ONLY
}
