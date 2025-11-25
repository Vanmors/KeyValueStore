package dev.kvstore.config;

import dev.kvstore.sharding.ShardInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public record GlobalClusterConfig(Map<String, ShardInfo> shards, long version) {
    public GlobalClusterConfig addShard(final String shardId, final Set<String> members) {
        final Map<String, ShardInfo> newShards = new HashMap<>(shards);
        newShards.put(shardId, new ShardInfo(shardId, members));
        return new GlobalClusterConfig(newShards, version + 1);
    }
    public static GlobalClusterConfig fromInitial(final List<ShardInfo> initial) {
        final HashMap<String, ShardInfo> map = new HashMap<>();
        for (final ShardInfo s : initial) {
            map.put(s.shardId(), new ShardInfo(s.shardId(), Set.copyOf(s.members())));
        }
        return new GlobalClusterConfig(Map.copyOf(map), 1);
    }
}