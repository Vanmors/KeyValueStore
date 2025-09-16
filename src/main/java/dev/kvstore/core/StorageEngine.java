package dev.kvstore.core;

import dev.kvstore.core.model.EngineStats;

/**
 * Сюда выносим то, что не нужно клиенту, но нужно серверу/админке.
 */

public interface StorageEngine extends KeyValueStore {

    /**
     * Инициализация каких-нибудь тяжелых компонентов
     */
    void start() throws KVException;

    /**
     * Остановка и освобождение ресурсов.
     */
    void stop() throws KVException;

    /**
     * Текущее состоянияе движка.
     */
    EngineStats stats();
}
