package dev.kvstore.core;

import dev.kvstore.core.model.*;

import java.io.Closeable;
import java.io.IOException;

public interface KeyValueStore {

    GetResult get(byte[] key, ReadOptions options) throws KVException, IOException;

    default GetResult get(byte[] key) throws KVException, IOException {
        return get(key, ReadOptions.DEFAULT);
    }

    PutResult put(byte[] key, byte[] value, PutOptions options) throws KVException, IOException;

    default PutResult put(byte[] key, byte[] value) throws KVException, IOException {
        return put(key, value, PutOptions.DEFAULT);
    }

    DeleteResult delete(byte[] key, DeleteOptions options) throws KVException, IOException;

    default DeleteResult delete(byte[] key) throws KVException, IOException {
        return delete(key, DeleteOptions.DEFAULT);
    }

    ScanCursor scan(KeyRange range, ReadOptions options) throws KVException;

    default ScanCursor scan(KeyRange range) throws KVException {
        return scan(range, ReadOptions.DEFAULT);
    }

    void flush() throws KVException, IOException;

}