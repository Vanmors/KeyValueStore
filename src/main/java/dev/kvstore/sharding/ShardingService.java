package dev.kvstore.sharding;

import dev.kvstore.config.BootstrapProperties;
import dev.kvstore.config.GlobalClusterConfig;
import dev.kvstore.core.KVException;
import dev.kvstore.core.model.ReplicationMode;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ShardingService {

    private static final Logger log = LoggerFactory.getLogger(ShardingService.class);

    private final ReplicationMode replicationMode;

    private final BootstrapProperties properties;

    private final ConsistentHashing hashRing;

    @Autowired
    public ShardingService(
            @Value("${kvstore.replicationMode}") final String replicationModeStr,
            @Qualifier("bootstrapProperties") final BootstrapProperties properties,
            final ConsistentHashing hashRing) {
        this.replicationMode = ReplicationMode.valueOf(replicationModeStr.toUpperCase());
        this.properties = properties;
        this.hashRing = hashRing;
    }

    @PostConstruct
    public void bootstrapIfNeeded() {
        log.warn("BootstrapService started");

//        if (replicationMode == ReplicationMode.MASTER) {
        log.warn("THIS IS THE FIRST MASTER NODE — CREATING GENESIS CONFIG");

        final GlobalClusterConfig genesis = GlobalClusterConfig.fromInitial(
                properties.getBootstrap().getInitialShards()
        );

        // 1. Обновляем хэш-кольцо
        log.info("GENESIS CONFIG APPLIED — {} shards", genesis.shards().size());
        genesis.shards().forEach((id, info) ->
                log.info("  {} → {}", id, String.join(", ", info.members()))
        );
        hashRing.rebuildWithShards(genesis.shards());
//        } else {
//            log.info("This is SLAVE node — waiting for config from master");
//        }
    }

    public ShardInfo getAliveMembers(final String nodeId) {
        return properties.getBootstrap().getInitialShards().stream()
                .filter(shard -> shard.shardId().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    public String getNode(final byte[] key) throws KVException {
        return hashRing.getNode(key);
    }
}