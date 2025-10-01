package dev.kvstore.core;

import dev.kvstore.core.LSM.MemTable;
import dev.kvstore.core.LSM.SSTable;
import dev.kvstore.core.model.*;

import java.io.IOException;
import java.util.*;


public class KeyValueStoreImpl implements KeyValueStore {
    private MemTable memTable;

    private WAL wal;

    private Map<Integer, List<SSTable>> levels = new TreeMap<>();

    private final String dir;

    private final long memSize;

    public KeyValueStoreImpl(String dir, long memSize) throws IOException {
        this.dir = dir;
        this.memSize = memSize;
        this.memTable = new MemTable(memSize);
        this.wal = new WALImpl(dir + "/wal.log");
        replayWAL();
        // TODO implement loadSSTables();
    }


    @Override
    public GetResult get(byte[] key, ReadOptions options) throws KVException, IOException {
        Entry e = memTable.get(key);
        if (e != null && !e.tombstone()) {
            return new GetResult(true, new ValueRecord(e.value(), 0, 0L));
        }
        for (int level = 0; level < levels.size(); level++) {
            for (final SSTable sst : levels.getOrDefault(level, Collections.emptyList())) {
                e = sst.search(key);
                if (e != null) {
                    if (e.tombstone()) {
                        return null;
                    }
                    return new GetResult(true, new ValueRecord(e.value(), 0, 0L));
                }
            }
        }
        return new GetResult(false, new ValueRecord(null, 0, 0L));
    }

    @Override
    public PutResult put(byte[] key, byte[] value, PutOptions options) throws KVException, IOException {
        final Entry e = new Entry(key, value, false);
        memTable.set(e);
        wal.write(e, WALOperationType.PUT);
        if (memTable.isFull()) {
            flush();
        }
        return new PutResult(true);
    }

    @Override
    public DeleteResult delete(byte[] key, DeleteOptions options) throws KVException, IOException {
        final Entry e = new Entry(key, null, true);
        memTable.set(e);
        wal.write(e, WALOperationType.DELETE);
        if (memTable.isFull()) {
            flush();
        }
        return new DeleteResult(true);
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

    @Override
    public void flush() throws KVException, IOException {
        final Set<Map.Entry<byte[], Entry>> entries = memTable.getAllEntries();
        final List<Entry> entryList = entries.stream().map(Map.Entry::getValue).toList();
        levels.put(0, List.of(new SSTable(dir + "/level0-" + System.currentTimeMillis(), entryList, 0)));
        compact();
        memTable = new MemTable(memSize);
        wal.clear();
        wal = new WALImpl(dir + "/wal.log");
    }

    @Override
    public ScanCursor scan(KeyRange range, ReadOptions options) throws KVException {
        return null;
    }

    @Override
    public void compact() throws KVException {
//        for (int level: levels.keySet()) {
//            if (levels.get(level).size() < 4) {
//                continue;
//            }
//            levels
//        }
    }

    public void loadSSTables() {

    }
}
