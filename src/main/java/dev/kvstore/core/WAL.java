package dev.kvstore.core;

import dev.kvstore.core.model.WALEntry;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;


public interface WAL extends Closeable {

    void put(byte[] key, byte[] value) throws IOException;

    void delete(byte[] key) throws IOException;

    /**
     * Восстанавливает данные из WAL в память.
     */
    void recover(Consumer<WALEntry> consumer) throws IOException;

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
