package dev.kvstore.core;

import dev.kvstore.core.model.*;

import java.io.Closeable;
import java.util.Optional;

public interface KeyValueStore extends Closeable {

    Optional<ValueRecord> get(byte[] key, ReadOptions options) throws KVException;

    default Optional<ValueRecord> get(byte[] key) throws KVException {
        return get(key, ReadOptions.DEFAULT);
    }

    PutResult put(byte[] key, byte[] value, PutOptions options) throws KVException;

    default PutResult put(byte[] key, byte[] value) throws KVException {
        return put(key, value, PutOptions.DEFAULT);
    }

    DeleteResult delete(byte[] key, DeleteOptions options) throws KVException;

    default DeleteResult delete(byte[] key) throws KVException {
        return delete(key, DeleteOptions.DEFAULT);
    }

    ScanCursor scan(KeyRange range, ReadOptions options) throws KVException;

    default ScanCursor scan(KeyRange range) throws KVException {
        return scan(range, ReadOptions.DEFAULT);
    }

    void flush() throws KVException;

    // убирать могильники для лабы 3?
    void compact() throws KVException;

    @Override
    void close();
}