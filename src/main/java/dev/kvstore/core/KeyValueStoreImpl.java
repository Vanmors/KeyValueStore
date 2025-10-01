package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.LSM.MemTable;
import dev.kvstore.core.LSM.SSTable;
import dev.kvstore.core.model.*;

import java.io.IOException;
import java.util.*;


public class KeyValueStoreImpl implements KeyValueStore {
    private LSMEngine lsmEngine;

    public KeyValueStoreImpl(final String dir, final long memSize) throws IOException {
        this.lsmEngine = new LSMEngineImpl(dir, memSize);
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
        final boolean created = lsmEngine.put(key, value, options);
        return new PutResult(created);
    }

    @Override
    public DeleteResult delete(byte[] key, DeleteOptions options) throws KVException, IOException {
        final boolean deleted = lsmEngine.delete(key, options);
        return new DeleteResult(deleted);
    }

    @Override
    public void flush() throws KVException, IOException {
        lsmEngine.flush();
    }

    @Override
    public ScanCursor scan(KeyRange range, ReadOptions options) throws KVException {
        return null;
    }

}
