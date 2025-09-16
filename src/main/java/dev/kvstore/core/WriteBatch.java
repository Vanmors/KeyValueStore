package dev.kvstore.core;

import dev.kvstore.core.model.DeleteOptions;
import dev.kvstore.core.model.PutOptions;

/**
 * Пакетная запись в рамках одного узла для репликации, склейки операций и транзакций.
 */
public interface WriteBatch extends AutoCloseable {

    @SuppressWarnings("UnusedReturnValue")
    WriteBatch put(byte[] key, byte[] value, PutOptions options);

    @SuppressWarnings("UnusedReturnValue")
    WriteBatch delete(byte[] key, DeleteOptions options);

    void commit();

    @Override
    void close();
}
