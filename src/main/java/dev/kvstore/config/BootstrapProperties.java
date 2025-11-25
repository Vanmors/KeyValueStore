package dev.kvstore.config;

import dev.kvstore.sharding.ShardInfo;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;


@Configuration
@ConfigurationProperties(prefix = "cluster")
public class BootstrapProperties {

    private String nodeId;
    private String httpAddress = "0.0.0.0:8080";
    private String raftDataDir = "./data/raft";
    private Bootstrap bootstrap = new Bootstrap();
    private String joinAddress;

    // getters and setters
    public String getNodeId() { return nodeId; }
    public void setNodeId(final String nodeId) { this.nodeId = nodeId; }

    public String getHttpAddress() { return httpAddress; }
    public void setHttpAddress(final String httpAddress) { this.httpAddress = httpAddress; }

    public String getRaftDataDir() { return raftDataDir; }
    public void setRaftDataDir(final String raftDataDir) { this.raftDataDir = raftDataDir; }

    public Bootstrap getBootstrap() { return bootstrap; }
    public void setBootstrap(final Bootstrap bootstrap) { this.bootstrap = bootstrap; }

    public String getJoinAddress() { return joinAddress; }
    public void setJoinAddress(final String joinAddress) { this.joinAddress = joinAddress; }

    public static class Bootstrap {
        private boolean enabled = false;
        private List<ShardInfo> initialShards = new ArrayList<>();

        public boolean isEnabled() { return enabled; }
        public void setEnabled(final boolean enabled) { this.enabled = enabled; }

        public List<ShardInfo> getInitialShards() { return initialShards; }
        public void setInitialShards(final List<ShardInfo> initialShards) { this.initialShards = initialShards; }
    }
}
