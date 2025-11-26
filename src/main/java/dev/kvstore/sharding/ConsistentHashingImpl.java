package dev.kvstore.sharding;

import dev.kvstore.core.KVException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
public class ConsistentHashingImpl implements ConsistentHashing {
    private static final Logger log = LoggerFactory.getLogger(ConsistentHashingImpl.class);
    private final SortedMap<Long, String> ring = new TreeMap<>();
    private final int vnodesPerShard;

    public ConsistentHashingImpl(@Value("${cluster.vnodesPerShard:128}") int vnodesPerShard) {
        this.vnodesPerShard = Math.max(1, vnodesPerShard);
    }

    @Override
    public synchronized String getNode(final byte[] key) throws KVException {
        if (ring.isEmpty()) {
            throw new KVException("Hash ring is empty");
        }
        final long h = Murmur3.hash(key);
        final SortedMap<Long, String> tail = ring.tailMap(h);
        final Long chosen = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(chosen);
    }

    @Override
    public synchronized void addNode(final String shardId) {
        for (int i = 0; i < vnodesPerShard; i++) {
            final long hash = Murmur3.hash((shardId + "#" + i).getBytes());
            ring.put(hash, shardId);
        }
        log.info("[HASH] added shard {} (vnodes={}), ringSize={}", shardId, vnodesPerShard, ring.size());
    }

    @Override
    public synchronized void removeNode(final String shardId) {
        for (Iterator<Map.Entry<Long, String>> it = ring.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<Long, String> e = it.next();
            if (shardId.equals(e.getValue())) it.remove();
        }
        log.info("[HASH] removed shard {}, ringSize={}", shardId, ring.size());
    }

    @Override
    public synchronized void rebuildWithShards(final Map<String, ShardInfo> shards) {
        ring.clear();
        if (shards == null || shards.isEmpty()) {
            log.warn("[HASH] rebuild requested with empty shards set");
            return;
        }
        for (final String shardId : shards.keySet()) {
            for (int i = 0; i < vnodesPerShard; i++) {
                final long hash = Murmur3.hash((shardId + "#" + i).getBytes());
                ring.put(hash, shardId);
            }
        }
        log.info("[HASH] rebuilt: shards={}, vnodesPerShard={}, ringKeys={}",
                shards.size(), vnodesPerShard, ring.size());
    }

}
