package dev.kvstore.core.LSM;

import dev.kvstore.core.KVException;
import dev.kvstore.core.WAL;
import dev.kvstore.core.WALImpl;
import dev.kvstore.core.model.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class LSMEngineImpl implements LSMEngine{

    private MemTable memTable;

    private WAL wal;

    private Map<Integer, List<SSTable>> levels = new TreeMap<>();

    private final String dir;

    private final long memSize;

    private final ExecutorService compactor = Executors.newSingleThreadExecutor();

    public LSMEngineImpl(final String dir, final long memSize) throws IOException {
        this.dir = dir;
        this.memSize = memSize;
        this.memTable = new MemTable(memSize);
        this.wal = new WALImpl(dir + "/wal.log");
        replayWAL();
    }


    @Override
    public Entry get(final byte[] key, final ReadOptions options) throws KVException, IOException {
        Entry entry = memTable.get(key);
        if (entry != null && !entry.tombstone()) {
            return entry;
        }
        for (int level = 0; level < levels.size(); level++) {
            for (final SSTable sst : levels.getOrDefault(level, Collections.emptyList())) {
                entry = sst.search(key);
                if (entry != null) {
                    if (entry.tombstone()) {
                        return null;
                    }
                    return entry;
                }
            }
        }
        return null;
    }

    @Override
    public boolean put(final byte[] key, final byte[] value, final PutOptions options) throws KVException, IOException {
        final Entry e = new Entry(key, value, false);
        memTable.set(e);
        wal.write(e, WALOperationType.PUT);
        if (memTable.isFull()) {
            flush();
        }
        return true;
    }

    @Override
    public boolean delete(final byte[] key, final DeleteOptions options) throws KVException, IOException {
        final Entry e = new Entry(key, null, true);
        memTable.set(e);
        wal.write(e, WALOperationType.DELETE);
        if (memTable.isFull()) {
            flush();
        }
        return true;
    }

    @Override
    public ScanCursor scan(KeyRange range, ReadOptions options) throws KVException {
        return null;
    }

    @Override
    public void flush() throws KVException, IOException {
        final Set<Map.Entry<byte[], Entry>> entries = memTable.getAllEntries();
        final List<Entry> entryList = entries.stream().map(Map.Entry::getValue).toList();
        levels.compute(0, (level, oldList) -> {
            final List<SSTable> updated = oldList == null ? new ArrayList<>() : new ArrayList<>(oldList);
            try {
                updated.add(new SSTable(dir + "/level0-" + System.currentTimeMillis(), entryList));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return updated;
        });
        compact(0);
        memTable = new MemTable(memSize);
        wal.clear();
        wal = new WALImpl(dir + "/wal.log");
    }

    @Override
    public void compact(final int level) {
        if (level >= 4) return;

        final List<SSTable> levelSSTs = levels.getOrDefault(level, Collections.emptyList());
        if (levelSSTs.size() > 3) {
            compactor.submit(() -> {
                try {
                    compactLevel(level);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void compactLevel(final int level) throws IOException {
        final List<SSTable> SSTables = new ArrayList<>(levels.getOrDefault(level, Collections.emptyList()));
        if (SSTables.isEmpty()) return;

        final SSTable newSST = mergeSSTables(SSTables, level + 1);

        levels.remove(level);

        levels.computeIfAbsent(level + 1, k -> new ArrayList<>()).add(newSST);

        compact(level + 1);
    }

    private SSTable mergeSSTables(final List<SSTable> SSTables, final int level) throws IOException {
        final List<Entry> entries = new ArrayList<>();

        for (final SSTable SSTable: SSTables) {
            entries.addAll(SSTable.getAllEntries());
        }

        return new SSTable(dir + "/level" + level + "-" + System.currentTimeMillis(), entries);
    }


    private void replayWAL() throws IOException {
        wal.recover(walEntry -> {
            final Entry entry = new Entry(
                    walEntry.key(),
                    walEntry.value(),
                    walEntry.tombstone()
            );
            memTable.set(entry);
        });
    }
}
