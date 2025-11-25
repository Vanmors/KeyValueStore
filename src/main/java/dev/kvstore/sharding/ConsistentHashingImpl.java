package dev.kvstore.sharding;

import dev.kvstore.core.KVException;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


@Component
public class ConsistentHashingImpl implements ConsistentHashing {
    private final SortedMap<Long, String> ring = new TreeMap<>();

    private final int virtualNodesPerShard = 10;

    @Override
    public String getNode(final byte[] key) throws KVException {
        if (ring.isEmpty()) {
            throw new KVException("Hash ring is empty");
        }
        final long hash = Murmur3.hash(key);
        final var tail = ring.tailMap(hash);
        final String shardId = tail.isEmpty() ? ring.get(ring.firstKey()) : tail.get(tail.firstKey());
        return shardId;  // возвращаем shardId, например "shard-2"
    }

    @Override
    public void addNode(final String node) {
        for (int i = 0; i < virtualNodesPerShard; i++) {
            final long hash = Murmur3.hash(node + "-" + i);
            ring.put(hash, node);
        }
    }

    @Override
    public void removeNode(String node) {

    }

    public void rebuildWithShards(final Map<String, ShardInfo> shardIds) {
        ring.clear();
        for (final String shardId : shardIds.keySet()) {
            for (int i = 0; i < virtualNodesPerShard; i++) {
                final long hash = Murmur3.hash((shardId + "-" + i).getBytes());
                ring.put(hash, shardId);
            }
        }
        System.out.println("Hash ring rebuilt: " + shardIds.size() + " shards, " +
                (shardIds.size() * virtualNodesPerShard) + " virtual nodes");
    }

}
