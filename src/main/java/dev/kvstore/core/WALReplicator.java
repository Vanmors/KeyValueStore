package dev.kvstore.core;

import dev.kvstore.core.model.WALEntry;


public interface WALReplicator {
    void replicate(WALEntry walEntry);
}
