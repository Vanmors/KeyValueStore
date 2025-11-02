package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.ObjectProvider;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

@Service
public class KeyValueStoreImpl implements KeyValueStore {

    private final LSMEngine lsmEngine;
    private final WAL wal;
    private final ReplicationMode replicationMode;
    private final org.springframework.beans.factory.ObjectProvider<dev.kvstore.raft.RaftService> raftProvider;

    public KeyValueStoreImpl(@Value("${kvstore.dir}") final String dir,
                             @Value("${kvstore.memSize}") final long memSize,
                             @Value("${kvstore.replicationMode}") final String replicationModeStr,
                             @Value("${kvstore.slaveAddresses}") final List<String> slaveAddresses,
                             org.springframework.beans.factory.ObjectProvider<dev.kvstore.raft.RaftService> raftProvider) throws IOException {
        this.replicationMode = ReplicationMode.valueOf(replicationModeStr.toUpperCase());
        this.raftProvider = raftProvider;

        final var d = new File(dir);
        if (!d.exists() && !d.mkdirs()) {
            throw new IOException("Cannot create data dir: " + dir);
        }

        WAL walTmp = null;
        if (this.replicationMode == ReplicationMode.MASTER) {
            walTmp = new WALImpl(dir + File.separator + "wal.log", replicationMode, slaveAddresses);
        } else if (this.replicationMode == ReplicationMode.RAFT) {
            walTmp = new WALImpl(dir + File.separator + "wal.log", ReplicationMode.SLAVE, List.of());
        }
        this.wal = walTmp;

        this.lsmEngine = new LSMEngineImpl(dir, memSize, wal);
    }

    /**
     * Версия - это Raft commitIndex.
     */
    private long currentVersion(byte[] key) {
        if (replicationMode == ReplicationMode.RAFT) {
            var raft = raftProvider.getIfAvailable();
            if (raft != null && raft.isEnabled()) {
                return raft.commitIndex();
            }
        }
        return 0L;
    }

    @Override
    public GetResult get(byte[] key, ReadOptions options) throws KVException, IOException {
        final var e = lsmEngine.get(key, options);
        if (e == null || e.tombstone()) {
            return new GetResult(false, new ValueRecord(null, currentVersion(key), null));
        }
        return new GetResult(true, new ValueRecord(e.value(), currentVersion(key), null));
    }

    @Override
    public PutResult put(byte[] key, byte[] value, PutOptions options) throws KVException, IOException {
        if (replicationMode == ReplicationMode.SLAVE) {
            throw new KVException("Can't put in slave node");
        }
        final Entry created = lsmEngine.put(key, value, options);
        if (wal != null) wal.write(created, WALOperationType.PUT);
        return new PutResult(true);
    }

    @Override
    public DeleteResult delete(byte[] key, DeleteOptions options) throws KVException, IOException {
        if (replicationMode == ReplicationMode.SLAVE) {
            throw new KVException("Can't delete in slave node");
        }
        final Entry deleted = lsmEngine.delete(key, options);
        if (wal != null) wal.write(deleted, WALOperationType.DELETE);
        return new DeleteResult(true);
    }

    @Override
    public void flush() throws KVException, IOException {
        lsmEngine.flush();
        if (wal != null) {
            wal.clear();
        }
    }

    @Override
    public void applyReplication(final WALEntry walEntry) throws KVException, IOException {
        switch (walEntry.operationType()) {
            case PUT -> lsmEngine.put(walEntry.key(), walEntry.value());
            case DELETE -> lsmEngine.delete(walEntry.key());
        }
    }

    @Override
    public ScanCursor scan(KeyRange range, ReadOptions options) throws KVException {
        final ScanCursor delegate = lsmEngine.scan(range, options);
        return new ScanCursor() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Map.Entry<byte[], ValueRecord> next() {
                final var e = delegate.next();
                final var key = e.getKey();
                final var vr = e.getValue();
                final var valueBytes = (vr == null) ? null : vr.value();
                final var expire = (vr == null) ? null : vr.expireAtMillis();
                return new AbstractMap.SimpleEntry<>(
                        key,
                        new ValueRecord(valueBytes, currentVersion(key), expire)
                );
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}