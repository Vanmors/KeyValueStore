package dev.kvstore.core.LSM;

import dev.kvstore.core.KVException;
import dev.kvstore.core.model.*;

import java.io.IOException;


public interface LSMEngine {
    Entry get(byte[] key, ReadOptions options) throws KVException, IOException;

    default Entry get(byte[] key) throws KVException, IOException {
        return get(key, ReadOptions.DEFAULT);
    }

    boolean put(byte[] key, byte[] value, PutOptions options) throws KVException, IOException;

    default boolean put(byte[] key, byte[] value) throws KVException, IOException {
        return put(key, value, PutOptions.DEFAULT);
    }

    boolean delete(byte[] key, DeleteOptions options) throws KVException, IOException;

    default boolean delete(byte[] key) throws KVException, IOException {
        return delete(key, DeleteOptions.DEFAULT);
    }

    ScanCursor scan(KeyRange range, ReadOptions options) throws KVException;

    default ScanCursor scan(KeyRange range) throws KVException {
        return scan(range, ReadOptions.DEFAULT);
    }

    void flush() throws KVException, IOException;

    // убирать могильники для лабы 3?
    void compact(int level) throws KVException;
}
