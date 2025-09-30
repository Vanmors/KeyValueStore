package dev.kvstore.core.model;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;


public class MemTable {
    private final ConcurrentSkipListMap<byte[], Entry> skipList =
            new ConcurrentSkipListMap<>(Arrays::compare);

    private final AtomicLong sizeInBytes = new AtomicLong(0);

    private final long maxSize;

    public MemTable(final long maxSize) {
        this.maxSize = maxSize;
    }

    public void set(final Entry entry) {
        final long oldSize = getValueSize(entry.key());
        skipList.put(entry.key(), entry);
        sizeInBytes.addAndGet(calculateEntrySize(entry) - oldSize);
    }

    public Set<Map.Entry<byte[], Entry>> getAllEntries() {
        return skipList.entrySet();
    }

    public Entry get(final byte[] key) {
        return skipList.get(key);
    }

    public boolean isFull() {
        return sizeInBytes.get() > maxSize;
    }

    private long calculateEntrySize(final Entry entry) {
        if (entry == null) {
            return 0;
        }
        return (entry.key() != null ? entry.key().length : 0) + (entry.tombstone() ? 0 : entry.value() != null ? entry.value().length : 0) + 1; // tombstone
    }

    private long getValueSize(final byte[] key) {
        final Entry existing = skipList.get(key);
        return existing != null ? calculateEntrySize(existing) : 0;
    }

}
