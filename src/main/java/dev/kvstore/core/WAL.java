package dev.kvstore.core;

import java.io.Closeable;
import java.io.IOException;


public interface WAL extends Closeable {

    void put(byte[] key, Long value) throws IOException;

    void delete(byte[] key) throws IOException;

    /**
     * Восстанавливает данные из WAL в память.
     */
    void recover() throws IOException;

    /**
     * Очищает WAL (удаляет лог после успешного flush).
     */
    void clear() throws IOException;

    /**
     * Закрывает WAL.
     */
    @Override
    void close() throws IOException;
}
