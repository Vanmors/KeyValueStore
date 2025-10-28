package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.List;


@Service
public class KeyValueStoreImpl implements KeyValueStore {
    private LSMEngine lsmEngine;

    private WAL wal;

    private ReplicationMode replicationMode;

    public KeyValueStoreImpl(@Value("${kvstore.dir}") final String dir,
                             @Value("${kvstore.memSize}") final long memSize,
                             @Value("${kvstore.replicationMode}") final String replicationModeStr,
                             @Value("${kvstore.slaveAddresses}") final List<String> slaveAddresses) throws IOException {
        this.replicationMode = ReplicationMode.valueOf(replicationModeStr.toUpperCase());
        final var d = new File(dir);
        if (!d.exists() && !d.mkdirs()) {
            throw new IOException("Cannot create data dir: " + dir);
        }
        if (this.replicationMode == ReplicationMode.MASTER) {
            this.wal = new WALImpl(dir + File.separator + "wal.log", replicationMode, slaveAddresses);
        }
        this.lsmEngine = new LSMEngineImpl(dir, memSize, wal);

    }

    @Override
    public GetResult get(byte[] key, ReadOptions options) throws KVException, IOException {
        final Entry entry = lsmEngine.get(key, options);
        if (entry != null) {
            return new GetResult(true, new ValueRecord(entry.value(), 0, 0L));
        }
        return new GetResult(true, new ValueRecord(null, 0, 0L));
    }

    @Override
    public PutResult put(byte[] key, byte[] value, PutOptions options) throws KVException, IOException {
        if (replicationMode == ReplicationMode.SLAVE) {
            throw new KVException("Can't put in slave node");
        }
        final Entry created = lsmEngine.put(key, value, options);
        wal.write(created, WALOperationType.PUT);
        return new PutResult(true);
    }

    @Override
    public DeleteResult delete(byte[] key, DeleteOptions options) throws KVException, IOException {
        if (replicationMode == ReplicationMode.SLAVE) {
            throw new KVException("Can't delete in slave node");
        }
        final Entry deleted = lsmEngine.delete(key, options);
        wal.write(deleted, WALOperationType.DELETE);
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
        return null;
    }

}
