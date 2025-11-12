package dev.kvstore.hashing;

import dev.kvstore.core.KVException;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {
    private final SortedMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodesPerNode;

    public ConsistentHashing(final List<String> nodes, final int virtualNodesPerNode) {
        this.virtualNodesPerNode = virtualNodesPerNode;
        for (final String node : nodes) {
            for (int i = 0; i < virtualNodesPerNode; i++) {
                final long hash = Murmur3.hash(node + "-" + i);
                ring.put(hash, node);
            }
        }
    }

    public String getNode(final byte[] key) throws KVException {
        final long hash = Murmur3.hash(key);
        if (ring.isEmpty()) {
            throw new KVException("No nodes in ring");
        }
        final SortedMap<Long, String> tailMap = ring.tailMap(hash);
        if (tailMap.isEmpty()) {
            return ring.get(ring.firstKey());
        }
        return tailMap.get(tailMap.firstKey());
    }

    public void addNode(final String node) {
        for (int i = 0; i < virtualNodesPerNode; i++) {
            final long hash = Murmur3.hash(node + "-" + i);
            ring.put(hash, node);
        }
    }

    public void removeNode(final String node) {
        for (int i = 0; i < virtualNodesPerNode; i++) {
            final long hash = Murmur3.hash(node + "-" + i);
            ring.remove(hash);
        }
    }

}
