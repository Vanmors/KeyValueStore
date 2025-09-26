package dev.kvstore.core;

import dev.kvstore.core.model.DeleteOptions;
import dev.kvstore.core.model.PutOptions;

/**
 * Пакетная запись в рамках одного узла для репликации, склейки операций и транзакций.
 */
public interface WriteBatch extends AutoCloseable {

    WriteBatch put(byte[] key, byte[] value, PutOptions options);

    default WriteBatch put(byte[] key, byte[] value) {
        return put(key, value, PutOptions.DEFAULT);
    }

    WriteBatch delete(byte[] key, DeleteOptions options);

    default WriteBatch delete(byte[] key) {
        return delete(key, DeleteOptions.DEFAULT);
    }

    void commit();

    @Override
    void close();
}
