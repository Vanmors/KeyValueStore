package dev.kvstore.sharding;

import java.util.Set;


public record ShardInfo(
        String shardId,
        Set<String> members    // "node1:8080", "node2:8080"
) {}
