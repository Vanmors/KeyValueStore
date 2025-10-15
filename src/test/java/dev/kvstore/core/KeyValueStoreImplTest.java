package dev.kvstore.core;

import dev.kvstore.core.model.Entry;
import dev.kvstore.core.model.WALOperationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class KeyValueStoreImplTest {

    private KeyValueStore keyValueStore;

    @BeforeEach
    void setUp() throws IOException {
        this.keyValueStore = new KeyValueStoreImpl(".", 10000);
    }

    @Test
    void putAndGetValue() throws KVException, IOException {
        keyValueStore.put("hello".getBytes(), "world".getBytes());

        final var value = new String(keyValueStore.get("hello".getBytes()).value().value());

        assertEquals("world", value);
    }

    @Test
    void putAndGetAfterFlushValue() throws KVException, IOException {
        keyValueStore.put("hello".getBytes(), "world".getBytes());

        keyValueStore.flush();

        final var value = new String(keyValueStore.get("hello".getBytes()).value().value());

        assertEquals("world", value);
    }

    @Test
    void putDeleteAndGetEntry() throws KVException, IOException {
        keyValueStore.put("hello".getBytes(), "world".getBytes());

        keyValueStore.delete("hello".getBytes());

        final var result = keyValueStore.get("hello".getBytes()).value().value();

        assertNull(result);
    }

    @Test
    void recoverEntriesFromWAL() throws KVException, IOException {
        final WAL wal = new WALImpl("wal.log");

        final Entry entry = new Entry("hello".getBytes(), "world".getBytes(), false);
        final Entry entry1 = new Entry("foo".getBytes(), "bar".getBytes(), false);

        wal.write(entry, WALOperationType.PUT);
        wal.write(entry1, WALOperationType.DELETE);

        this.keyValueStore = new KeyValueStoreImpl(".", 10000);
        final var result = keyValueStore.get("hello".getBytes()).value().value();
        final var result1 = keyValueStore.get("foo".getBytes()).value().value();

        assertEquals(new String(result), "world");
        assertEquals(new String(result1), "bar");
    }
}
