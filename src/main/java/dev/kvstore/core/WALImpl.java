package dev.kvstore.core;

import dev.kvstore.core.model.Entry;
import dev.kvstore.core.model.WALEntry;
import dev.kvstore.core.model.WALOperationType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class WALImpl implements WAL {
    private final File file;

    private FileOutputStream fos;

    private final Object lock = new Object();

    private final AtomicLong id = new AtomicLong(0);

    public WALImpl(final String path) throws IOException {
        this.file = new File(path);
        this.fos = new FileOutputStream(file, true);
    }

    @Override
    public void write(final Entry entry, final WALOperationType walOperationType) throws IOException {
        final WALEntry walEntry = new WALEntry(
                id.addAndGet(1),
                entry.key(),
                entry.value(),
                entry.tombstone(),
                walOperationType,
                LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
        );
        synchronized (lock) {
            final ByteBuffer buffer = serializeEntry(walEntry);
            fos.write(buffer.array());
            fos.flush();
        }
    }

    @Override
    public void recover(final Consumer<WALEntry> consumer) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                try {
                    final WALEntry entry = deserializeEntry(raf);
                    consumer.accept(entry);
                } catch (IOException e) {
                    System.err.println("Error reading WAL entry at offset " + raf.getFilePointer() + ": " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void clear() throws IOException {
        synchronized (lock) {
            fos.close();
            if (file.exists()) {
                if (!file.delete()) {
                    throw new IOException("Failed to delete WAL file: " + file.getPath());
                }
            }
            this.fos = new FileOutputStream(file, true);
        }
    }

    @Override
    public void close() throws IOException {
        fos.close();
    }

    private ByteBuffer serializeEntry(final WALEntry entry) {
        final int keyLen = entry.key() != null ? entry.key().length : 0;
        final int valueLen = entry.value() != null ? entry.value().length : 0;
        final int bufferSize = 8 + keyLen + (entry.key() != null ? entry.key().length : 0) + valueLen +
                (entry.tombstone() ? 0 : entry.value() != null ? entry.value().length : 0) + 1 + 1 + 8; // id + keyLen + key + valueLen + value + tombstone + opType + timestamp
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        buffer.putLong(entry.id());

        buffer.putInt(keyLen);
        if (keyLen > 0) {
            buffer.put(entry.key());
        }

        buffer.putInt(valueLen);
        if (valueLen > 0) {
            buffer.put(entry.value());
        }

        buffer.put((byte) (!entry.tombstone() ? 0 : 1));

        buffer.put((byte) (entry.operationType() == WALOperationType.PUT ? 0 : 1));

        buffer.putLong(entry.timestamp());

        buffer.flip();
        return buffer;
    }

    private WALEntry deserializeEntry(RandomAccessFile raf) throws IOException {
        long id = raf.readLong();

        int keyLen = raf.readInt();
        byte[] key = new byte[keyLen];
        if (keyLen > 0) {
            raf.readFully(key);
        }

        int valueLen = raf.readInt();
        byte[] value = valueLen > 0 ? new byte[valueLen] : null;
        if (valueLen > 0) {
            raf.readFully(value);
        }

        byte tombstoneByte = raf.readByte();
        boolean tombstone = tombstoneByte != 0;

        byte opTypeByte = raf.readByte();
        WALOperationType opType = opTypeByte == 0 ? WALOperationType.PUT : WALOperationType.DELETE;

        long timestamp = raf.readLong();

        return new WALEntry(id, key, value, tombstone, opType, timestamp);
    }
}
