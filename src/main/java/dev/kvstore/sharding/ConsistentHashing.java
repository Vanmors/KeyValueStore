package dev.kvstore.sharding;

import dev.kvstore.core.KVException;

import java.util.Map;


public interface ConsistentHashing {
    String getNode(byte[] key) throws KVException;

    void addNode(String node);

    void removeNode(String node);

    void rebuildWithShards(Map<String, ShardInfo> shardIds);
}
