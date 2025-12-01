package dev.kvstore.sharding;

import dev.kvstore.config.BootstrapProperties;
import dev.kvstore.config.GlobalClusterConfig;
import dev.kvstore.core.KVException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


@Service
public class ShardingService {

    private static final Logger log = LoggerFactory.getLogger(ShardingService.class);

    private final ConsistentHashing hashRing;

    private final BootstrapProperties properties;

    private final String myShardId;

    @Autowired
    private ShardingClient shardingClient;

    private final AtomicReference<GlobalClusterConfig> configRef = new AtomicReference<>();

    public ShardingService(ConsistentHashing hashRing,
                           BootstrapProperties properties,
                           @Value("${cluster.shard-id}") String myShardId) {
        this.hashRing = Objects.requireNonNull(hashRing);
        this.properties = Objects.requireNonNull(properties);
        this.myShardId = Objects.requireNonNull(myShardId);
    }

    @PostConstruct
    public void init() {
        final var initial = properties.getBootstrap().getInitialShards();
        final GlobalClusterConfig cfg = GlobalClusterConfig.fromInitial(initial);
        configRef.set(cfg);
        hashRing.rebuildWithShards(cfg.shards());
        log.info("[SHARDING] init: myShard={}, shards={}, version={}",
                myShardId, cfg.shards().size(), cfg.version());
    }

    public GlobalClusterConfig currentConfig() {
        return configRef.get();
    }

    public ShardInfo getAliveMembers(final String shardId) {
        final GlobalClusterConfig cfg = configRef.get();
        return cfg.shards().get(shardId);
    }

    public String getNode(final byte[] key) throws KVException {
        return hashRing.getNode(key);
    }

    public boolean isLocalShardFor(final byte[] key) throws KVException {
        return myShardId.equals(hashRing.getNode(key));
    }

    public String getMyShardId() {
        return myShardId;
    }

    public Map<String, ShardInfo> allShards() {
        return configRef.get().shards();
    }

    public synchronized void applyConfig(final GlobalClusterConfig newCfg) {
        if (newCfg == null || newCfg.shards() == null || newCfg.shards().isEmpty()) {
            log.warn("[SHARDING] ignore empty config");
            return;
        }
        configRef.set(newCfg);
        hashRing.rebuildWithShards(newCfg.shards());
        log.info("[SHARDING] applied: shards={}, version={}", newCfg.shards().size(), newCfg.version());
    }

    public synchronized GlobalClusterConfig addShard(final String shardId, final Set<String> members) {
        final GlobalClusterConfig updated = configRef.get().addShard(shardId, members);
        log.info("config {}", updated);
        replicateConfig(updated);
        return updated;
    }

    private void replicateConfig(final GlobalClusterConfig config) {
        final List<String> allNodes = config.shards().values().stream().flatMap(shard -> shard.members().stream()).toList();
        shardingClient.replicateConfig(config, allNodes);
    }
}