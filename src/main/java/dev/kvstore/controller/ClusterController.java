package dev.kvstore.controller;

import dev.kvstore.config.GlobalClusterConfig;
import dev.kvstore.sharding.ShardInfo;
import dev.kvstore.sharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * REST для просмотра/изменения конфигурации шардирования.
 */
@RestController
@RequestMapping("/cluster")
public class ClusterController {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    @Autowired
    private ShardingService sharding;

    @GetMapping("/config")
    public GlobalClusterConfig config(
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        var cfg = sharding.currentConfig();
        log.info("[CLUSTER] GET /config shards={} rid={}",
                cfg == null || cfg.shards() == null ? 0 : cfg.shards().size(), rid);
        return cfg;
    }

    @PostMapping("/add-shard")
    public ResponseEntity<GlobalClusterConfig> addShard(
            @RequestBody Map<String, Object> req,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final Object shardIdRaw = req.get("shardId");
        final Object membersRaw = req.get("members");

        if (!(shardIdRaw instanceof String shardId) || shardId.isBlank()) {
            log.warn("[CLUSTER] add-shard invalid shardId={} rid={}", shardIdRaw, rid);
            return ResponseEntity.badRequest().build();
        }
        if (!(membersRaw instanceof Collection<?> membersCollection) || membersCollection.isEmpty()) {
            log.warn("[CLUSTER] add-shard invalid members={} rid={}", membersRaw, rid);
            return ResponseEntity.badRequest().build();
        }
        final Set<String> members = new HashSet<>();
        for (Object o : membersCollection) {
            if (o != null) members.add(o.toString());
        }

        log.info("[CLUSTER] add-shard shardId='{}' members={} rid={}", shardId, members, rid);
        final GlobalClusterConfig updated = sharding.addShard(shardId, members);
        log.info("[CLUSTER] add-shard done shards={} rid={}",
                updated == null || updated.shards() == null ? 0 : updated.shards().size(), rid);
        return ResponseEntity.ok(updated);
    }

    @GetMapping("/shards/{shardId}")
    public ResponseEntity<ShardInfo> shard(
            @PathVariable("shardId") String shardId,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final ShardInfo info = sharding.getAliveMembers(shardId);
        if (info == null) {
            log.warn("[CLUSTER] shard '{}' not found rid={}", shardId, rid);
            return ResponseEntity.notFound().build();
        } else {
            log.info("[CLUSTER] shard '{}' aliveMembers={} rid={}", shardId, info.members().size(), rid);
            return ResponseEntity.ok(info);
        }
    }
}
