package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.model.*;
import dev.kvstore.raft.RaftService;
import dev.kvstore.sharding.ShardInfo;
import dev.kvstore.sharding.ShardingClient;
import dev.kvstore.sharding.ShardingService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Service
public class KeyValueStoreImpl implements KeyValueStore {

    private final LSMEngine lsmEngine;

    private final WAL wal;

    private final ReplicationMode replicationMode;

    private final ObjectProvider<RaftService> raftProvider;

    private final String myAddress;

    @Autowired
    private ShardingClient shardingClient;

    @Autowired
    private ShardingService shardingService;

    public KeyValueStoreImpl(@Value("${kvstore.dir}") final String dir,
                             @Value("${kvstore.memSize}") final long memSize,
                             @Value("${kvstore.replicationMode}") final String replicationModeStr,
                             @Value("${kvstore.slaveAddresses}") final List<String> slaveAddresses,
                             @Value("${cluster.shard-id}") final String nodeId,
                             final ObjectProvider<RaftService> raftProvider) throws IOException {
        this.replicationMode = ReplicationMode.valueOf(replicationModeStr.toUpperCase());
        this.raftProvider = raftProvider;
        this.myAddress = nodeId;

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
    private long currentVersion(final byte[] key) {
        if (replicationMode == ReplicationMode.RAFT) {
            final var raft = raftProvider.getIfAvailable();
            if (raft != null && raft.isEnabled()) {
                return raft.commitIndex();
            }
        }
        return 0L;
    }

    @Override
    public GetResult get(final byte[] key, final ReadOptions options) throws KVException, IOException {
        final String shardId = shardingService.getNode(key);

        if (shardId.equals(myAddress)) {
            final var e = lsmEngine.get(key, options);
            if (e == null || e.tombstone()) {
                return new GetResult(false, new ValueRecord(null, currentVersion(key), null));
            }
            return new GetResult(true, new ValueRecord(e.value(), currentVersion(key), null));
        }

        final String targetNode = getAnyLiveNode(shardId);

        // Читаем с другого узла
        final byte[] valueBytes = shardingClient.get(targetNode, key);
        return new GetResult(valueBytes != null, new ValueRecord(valueBytes, 0L, null));
    }

    @Override
    public PutResult put(final byte[] key, final byte[] value, final PutOptions options) throws KVException, IOException {
        if (replicationMode == ReplicationMode.SLAVE) {
            throw new KVException("Can't put in slave node");
        }
        final String shardId = shardingService.getNode(key);

        // Если это наш узел — пишем локально
        if (shardId.equals(myAddress)) {
            final Entry created = lsmEngine.put(key, value, options);
            if (wal != null) {
                wal.write(created, WALOperationType.PUT);
            }
            return new PutResult(true);
        }

        final String targetNode = getAnyLiveNode(shardId);

        // Иначе — отправляем на нужный узел
        final boolean success = shardingClient.put(targetNode, key, value);
        return new PutResult(success);
    }

    private String getAnyLiveNode(final String shardId) throws KVException {
        final ShardInfo shard = shardingService.getAliveMembers(shardId);

//        for (final String addr : members) {
//            if (isAlive(addr)) {  // простой healthcheck: GET /health
//                return addr;
//            }
//        }
        if (shard != null) {
            final List<String> members = new ArrayList<>(shard.members());
            return members.get(0);
        }
        throw new KVException("No live nodes in shard " + shardId);
    }

    @Override
    public DeleteResult delete(final byte[] key, final DeleteOptions options) throws KVException, IOException {
        if (replicationMode == ReplicationMode.SLAVE) {
            throw new KVException("Can't delete in slave node");
        }
        final Entry deleted = lsmEngine.delete(key, options);
        if (wal != null) {
            wal.write(deleted, WALOperationType.DELETE);
        }
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
    public ScanCursor scan(final KeyRange range, final ReadOptions options) throws KVException {
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