package dev.kvstore.core;

import dev.kvstore.core.model.ReplicationMode;
import dev.kvstore.core.model.WALEntry;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;


public class WALReplicatorImpl implements WALReplicator {

    private final List<String> slaveAddresses;

    private final WebClient webClient;

    private final ReplicationMode replicationMode;


    public WALReplicatorImpl(final List<String> slaveAddresses, final ReplicationMode replicationMode) {
        this.slaveAddresses = slaveAddresses;
        this.webClient = WebClient.builder().build();
        this.replicationMode = replicationMode;
    }

    @Override
    public void replicate(final WALEntry walEntry) {
        if (replicationMode == ReplicationMode.SLAVE) {
            return;
        }

        for (final String slaveAddress : slaveAddresses) {
            final String url = slaveAddress + "/kvstore/replicate";
            webClient.post()
                    .uri(url)
                    .bodyValue(walEntry)
                    .retrieve()
                    .toBodilessEntity()
                    .doOnError(e -> System.err.println("Replication failed for " + slaveAddress + ": " + e.getMessage()))
                    .subscribe();
        }
    }
}
