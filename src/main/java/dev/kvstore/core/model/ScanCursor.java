package dev.kvstore.core.model;

import java.io.Closeable;
import java.util.Map;

public interface ScanCursor extends Closeable {
    boolean hasNext();

    Map.Entry<byte[], ValueRecord> next();

    @Override
    void close();
}
