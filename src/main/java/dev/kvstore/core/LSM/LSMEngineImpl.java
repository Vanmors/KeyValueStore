package dev.kvstore.core.LSM;

import dev.kvstore.core.KVException;
import dev.kvstore.core.WAL;
import dev.kvstore.core.WALImpl;
import dev.kvstore.core.model.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class LSMEngineImpl implements LSMEngine {

    private MemTable memTable;

    private WAL wal;

    private Map<Integer, List<SSTable>> levels = new TreeMap<>();

    private final String dir;

    private final long memSize;

    private final ExecutorService compactor = Executors.newSingleThreadExecutor();

    private final java.util.concurrent.locks.ReadWriteLock levelsLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    public LSMEngineImpl(final String dir, final long memSize) throws IOException {
        this.dir = dir;

        final var d = new File(dir);
        if (!d.exists() && !d.mkdirs()) {
            throw new IOException("Cannot create data dir: " + dir);
        }
        this.memSize = memSize;
        this.memTable = new MemTable(memSize);
        this.wal = new WALImpl(dir + File.separator + "wal.log");
        replayWAL();
    }


    @Override
    public Entry get(final byte[] key, final ReadOptions options) throws KVException, IOException {
        Entry entry = memTable.get(key);
        if (entry != null) return entry.tombstone() ? null : entry;

        for (int attempt = 0; attempt < 2; attempt++) {
            final NavigableMap<Integer, List<SSTable>> snapshot = new TreeMap<>();
            levelsLock.readLock().lock();
            try {
                for (var e : levels.entrySet()) {
                    snapshot.put(e.getKey(), new ArrayList<>(e.getValue()));
                }
            } finally {
                levelsLock.readLock().unlock();
            }

            boolean ioRace = false;

            for (var lvl : snapshot.navigableKeySet()) {
                // от нового к старому
                final var tables = new ArrayList<>(snapshot.get(lvl));
                tables.sort(Comparator.comparingLong(SSTable::createdAtMillis).reversed());

                for (final SSTable sst : tables) {
                    try {
                        entry = sst.search(key);
                    } catch (IOException ioe) {
                        ioRace = true; // тк файл могли удалить, просто переснимем уровни и повторим
                        continue;
                    }
                    if (entry != null) return entry.tombstone() ? null : entry;
                }
            }
            if (!ioRace) break;
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
        final var snapshot = memTable.snapshotAndClear();
        if (snapshot.isEmpty()) return;

        final var entries = new ArrayList<Entry>(snapshot.size());
        for (var e : snapshot.entrySet()) entries.add(e.getValue());

        final var ts = System.currentTimeMillis();
        final var base = dir + File.separator + "level0-" + ts;
        final var sstable = new SSTable(base, entries);

        levelsLock.writeLock().lock();
        try {
            levels.computeIfAbsent(0, k -> new ArrayList<>()).add(sstable);
        } finally {
            levelsLock.writeLock().unlock();
        }

        wal.clear();
        compact(0);
    }

    @Override
    public void compact(final int level) {
        if (level >= 4) return;

        levelsLock.readLock().lock();
        boolean need = levels.getOrDefault(level, Collections.emptyList()).size() > 3;
        levelsLock.readLock().unlock();

        if (need) {
            compactor.submit(() -> {
                try {
                    compactLevel(level);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void removeFiles(List<SSTable> tables) {
        for (SSTable s : tables) {
            try {
                final var f = s.file();
                if (f != null && f.exists() && !f.delete()) {
                    System.err.println("Failed to delete: " + f.getAbsolutePath());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private ArrayList<Entry> mergeNewestWins(final List<SSTable> inputs) throws IOException {
        // соберём все записи с пометкой, из какого источника они пришли
        final class WithSrc {
            final Entry e;
            final int src; // 0 — самый новый файл, чем больше — тем старше

            WithSrc(Entry e, int src) {
                this.e = e;
                this.src = src;
            }
        }
        final var all = new ArrayList<WithSrc>();
        for (int i = 0; i < inputs.size(); i++) {
            final var sst = inputs.get(i);
            for (Entry e : sst.getAllEntries()) {
                all.add(new WithSrc(e, i));
            }
        }

        // сортируем по ключу
        all.sort((a, b) -> java.util.Arrays.compare(a.e.key(), b.e.key()));

        // пройдём группами одинаковых ключей и выберем запись с минимальным src
        final var out = new ArrayList<Entry>();
        int i = 0;
        while (i < all.size()) {
            final var key = all.get(i).e.key();
            int j = i + 1;
            int bestIdx = i;
            int bestSrc = all.get(i).src;

            while (j < all.size() && java.util.Arrays.compare(all.get(j).e.key(), key) == 0) {
                if (all.get(j).src < bestSrc) {
                    bestSrc = all.get(j).src;
                    bestIdx = j;
                }
                j++;
            }
            out.add(all.get(bestIdx).e);
            i = j;
        }
        return out;
    }


    // merge
    private void compactLevel(final int level) throws IOException {
        final List<SSTable> inputs;

        levelsLock.readLock().lock();
        try {
            inputs = new ArrayList<>(levels.getOrDefault(level, Collections.emptyList()));
        } finally {
            levelsLock.readLock().unlock();
        }
        if (inputs.isEmpty()) return;

        // от самых новых к старым
        inputs.sort(Comparator.comparingLong(SSTable::createdAtMillis).reversed());

        final var merged = mergeNewestWins(inputs);

        merged.removeIf(Entry::tombstone);
        if (merged.isEmpty()) {
            levelsLock.writeLock().lock();
            try {
                levels.put(level, new ArrayList<>());
            } finally {
                levelsLock.writeLock().unlock();
            }
            removeFiles(inputs);
            return;
        }

        // пишем новый файл на уровень level+1
        final var outBase = dir + File.separator + "level" + (level + 1) + "-" + System.currentTimeMillis();
        final var out = new SSTable(outBase, merged);

        levelsLock.writeLock().lock();
        try {
            final var cur = new ArrayList<>(levels.getOrDefault(level, Collections.emptyList()));
            cur.removeAll(inputs);
            levels.put(level, cur);

            levels.computeIfAbsent(level + 1, k -> new ArrayList<>()).add(out);
        } finally {
            levelsLock.writeLock().unlock();
        }

        // удаляем старые файлы с диска
        removeFiles(inputs);
        // рекурсивно вверх
        compact(level + 1);
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
