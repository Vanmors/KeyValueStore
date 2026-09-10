package dev.kvstore.raft;

import dev.kvstore.core.KeyValueStore;
import dev.kvstore.core.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import jakarta.annotation.PostConstruct;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static dev.kvstore.raft.RaftRole.*;

@Service
public class RaftService {
    private static final Logger log = LoggerFactory.getLogger(RaftService.class);

    private final boolean enabled;
    private final String selfId;
    private final List<String> peers;
    private final long hbIntervalMs;
    private final long electionTimeoutMinMs;
    private final long electionTimeoutMaxMs;

    private final KeyValueStore kv;
    private final WebClient http = WebClient.builder().build();

    private final File metaFile;
    private long currentTerm = 0;
    private String votedFor = null;

    private volatile RaftRole role = FOLLOWER;
    private volatile String leaderId = null;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private final Map<Long, Long> indexTerm = new ConcurrentHashMap<>();
    private final Map<Long, WALEntry> logCache = new ConcurrentHashMap<>();
    private final AtomicLong lastLogIndex = new AtomicLong(0);
    private volatile long lastHeartbeatAt = System.currentTimeMillis();
    private volatile long electionDeadlineMs = nextElectionDeadline();

    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    public RaftService(@Lazy
                       KeyValueStore kv,
                       @Value("${kvstore.replicationMode}") String mode,
                       @Value("${kvstore.nodeId}") String selfId,
                       @Value("${kvstore.peerAddresses:}") List<String> peers,
                       @Value("${kvstore.raft.heartbeatMs:150}") long hbMs,
                       @Value("${kvstore.raft.electionMinMs:900}") long eMin,
                       @Value("${kvstore.raft.electionMaxMs:1800}") long eMax,
                       @Value("${kvstore.dir}") String dir
    ) {
        this.enabled = "RAFT".equalsIgnoreCase(mode);
        this.kv = kv;
        this.selfId = selfId;
        this.peers = new ArrayList<>(Optional.ofNullable(peers).orElse(List.of()));
        this.peers.removeIf(p -> p == null || p.isBlank() || p.equals(selfId));
        this.hbIntervalMs = hbMs;
        this.electionTimeoutMinMs = eMin;
        this.electionTimeoutMaxMs = eMax;
        this.metaFile = new File(dir, "raft.meta");
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isLeader() {
        return enabled && role == LEADER;
    }

    public String leaderId() {
        return leaderId;
    }

    public long term() {
        return currentTerm;
    }

    public long commitIndex() {
        return commitIndex;
    }

    @PostConstruct
    void init() throws IOException {
        if (!enabled) return;
        loadMeta();
        log.info("[RAFT] start node={} term={} peers={}", selfId, currentTerm, peers);
        becomeFollower(currentTerm, null);
    }

    public Optional<String> leaderRedirectUrl(String path) {
        if (leaderId == null) return Optional.empty();
        return Optional.of(leaderId + path);
    }

    public boolean clientPut(byte[] key, byte[] value) throws IOException {
        return submit(WALOperationType.PUT, key, value);
    }

    public boolean clientDelete(byte[] key) throws IOException {
        return submit(WALOperationType.DELETE, key, null);
    }

    public synchronized RaftRpc.RequestVoteResponse onRequestVote(RaftRpc.RequestVoteRequest r) {
        if (!enabled) return new RaftRpc.RequestVoteResponse(0, false);
        if (r.term() > currentTerm) becomeFollower(r.term(), null);

        boolean upToDate = (r.lastLogIndex() >= lastLogIndex.get());
        boolean canVote = (r.term() == currentTerm) && (votedFor == null || votedFor.equals(r.candidateId())) && upToDate;

        if (canVote) {
            votedFor = r.candidateId();
            persistMeta();
            resetElectionTimer();
        }

        return new RaftRpc.RequestVoteResponse(currentTerm, canVote);
    }

    public synchronized RaftRpc.AppendEntriesResponse onAppendEntries(RaftRpc.AppendEntriesRequest r) {
        if (!enabled) return new RaftRpc.AppendEntriesResponse(0, false, 0);

        if (r.term() < currentTerm) return new RaftRpc.AppendEntriesResponse(currentTerm, false, lastLogIndex.get());
        if (r.term() > currentTerm) becomeFollower(r.term(), r.leaderId());
        leaderId = r.leaderId();
        resetElectionTimer();

        if (r.prevLogIndex() > lastLogIndex.get()) {
            return new RaftRpc.AppendEntriesResponse(currentTerm, false, lastLogIndex.get());
        }

        long highest = lastLogIndex.get();
        if (r.entries() != null) {
            for (var le : r.entries()) {
                indexTerm.put(le.index(), le.term());
                logCache.put(le.index(), le.entry());
                highest = Math.max(highest, le.index());
            }
            lastLogIndex.set(highest);
        }

        if (r.leaderCommit() > commitIndex) {
            commitIndex = Math.min(r.leaderCommit(), lastLogIndex.get());
            tryApply();
        }

        return new RaftRpc.AppendEntriesResponse(currentTerm, true, lastLogIndex.get());
    }

    @Scheduled(fixedDelayString = "${kvstore.raft.heartbeatMs:150}")
    void tick() {
        if (!enabled) return;
        long now = System.currentTimeMillis();

        if (role == LEADER) {
            sendHeartbeats();
            return;
        }

        if (now >= electionDeadlineMs) {
            startElection();
        }
    }

    private synchronized boolean submit(WALOperationType op, byte[] key, byte[] value) throws IOException {
        if (role != LEADER) return false;

        long newIndex = lastLogIndex.incrementAndGet();
        var wal = new WALEntry(newIndex, key, value, op == WALOperationType.DELETE, op, Instant.now().toEpochMilli());
        logCache.put(newIndex, wal);
        indexTerm.put(newIndex, currentTerm);

        int acks = 1;
        for (String p : peers) {
            try {
                var req = new RaftRpc.AppendEntriesRequest(
                        currentTerm, selfId,
                        newIndex - 1, indexTerm.getOrDefault(newIndex - 1, 0L),
                        List.of(new RaftRpc.LogEntry(newIndex, currentTerm, wal)),
                        commitIndex
                );
                var resp = http.post().uri(p + "/kvstore/raft/append-entries")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(req)
                        .retrieve()
                        .bodyToMono(RaftRpc.AppendEntriesResponse.class)
                        .block();
                if (resp != null && resp.success()) {
                    acks++;
                    matchIndex.put(p, newIndex);
                    nextIndex.put(p, newIndex + 1);
                }
            } catch (Exception ex) {
                log.warn("[RAFT] AE to {} failed: {}", p, ex.toString());
            }
        }

        if (acks >= majority()) {
            commitIndex = newIndex;
            tryApply();
            sendHeartbeats();
            return true;
        }
        return false;
    }

    private int majority() {
        return (peers.size() + 1) / 2 + 1;
    }

    private void tryApply() {
        while (lastApplied < commitIndex) {
            long idx = lastApplied + 1;
            var e = logCache.get(idx);
            if (e == null) break;
            try {
                switch (e.operationType()) {
                    case PUT ->
                            kv.applyReplication(new WALEntry(e.id(), e.key(), e.value(), false, WALOperationType.PUT, e.timestamp()));
                    case DELETE ->
                            kv.applyReplication(new WALEntry(e.id(), e.key(), null, true, WALOperationType.DELETE, e.timestamp()));
                }
                lastApplied = idx;
            } catch (Exception ex) {
                log.error("[RAFT] apply idx={} failed: {}", idx, ex.toString(), ex);
                break;
            }
        }
    }

    private void sendHeartbeats() {
        for (String p : peers) {
            try {
                var hb = new RaftRpc.AppendEntriesRequest(
                        currentTerm, selfId,
                        lastLogIndex.get(), indexTerm.getOrDefault(lastLogIndex.get(), 0L),
                        List.of(), commitIndex
                );
                http.post().uri(p + "/kvstore/raft/append-entries")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(hb)
                        .retrieve()
                        .bodyToMono(RaftRpc.AppendEntriesResponse.class)
                        .onErrorResume(err -> {
                            log.debug("[RAFT] HB {} -> {}", p, err.toString());
                            return null;
                        })
                        .block();
            } catch (Exception ignore) {
            }
        }
    }

    private void startElection() {
        role = CANDIDATE;
        currentTerm++;
        votedFor = selfId;
        persistMeta();
        resetElectionTimer();

        int votes = 1;
        long lastIdx = lastLogIndex.get();
        long lastTerm = indexTerm.getOrDefault(lastIdx, 0L);

        for (String p : peers) {
            try {
                var req = new RaftRpc.RequestVoteRequest(currentTerm, selfId, lastIdx, lastTerm);
                var resp = http.post().uri(p + "/kvstore/raft/request-vote")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(req)
                        .retrieve()
                        .bodyToMono(RaftRpc.RequestVoteResponse.class)
                        .block();
                if (resp != null) {
                    if (resp.term() > currentTerm) {
                        becomeFollower(resp.term(), null);
                        return;
                    }
                    if (resp.voteGranted()) votes++;
                }
            } catch (Exception ex) {
                log.debug("[RAFT] RV {} -> {}", p, ex.toString());
            }
        }

        if (votes >= majority()) {
            becomeLeader();
        } else {
            becomeFollower(currentTerm, null);
        }
    }

    private void becomeLeader() {
        role = LEADER;
        leaderId = selfId;
        for (String p : peers) {
            nextIndex.put(p, lastLogIndex.get() + 1);
            matchIndex.put(p, 0L);
        }
        log.info("[RAFT] became LEADER term={} id={}", currentTerm, selfId);
        sendHeartbeats();
    }

    private void becomeFollower(long newTerm, String newLeader) {
        role = FOLLOWER;
        currentTerm = newTerm;
        leaderId = newLeader;
        votedFor = null;
        persistMeta();
        resetElectionTimer();
        log.info("[RAFT] became FOLLOWER term={} leader={}", currentTerm, leaderId);
    }

    private void resetElectionTimer() {
        lastHeartbeatAt = System.currentTimeMillis();
        electionDeadlineMs = nextElectionDeadline();
    }

    private long nextElectionDeadline() {
        long base = ThreadLocalRandom.current().nextLong(electionTimeoutMinMs, electionTimeoutMaxMs + 1);
        return System.currentTimeMillis() + base;
    }

    private void persistMeta() {
        try {
            String s = currentTerm + "\n" + (votedFor == null ? "-" : votedFor) + "\n";
            Files.writeString(metaFile.toPath(), s);
        } catch (Exception e) {
            log.warn("[RAFT] persist meta failed: {}", e.toString());
        }
    }

    private void loadMeta() throws IOException {
        if (!metaFile.exists()) return;
        var lines = Files.readAllLines(metaFile.toPath());
        if (!lines.isEmpty()) currentTerm = Long.parseLong(lines.get(0).trim());
        if (lines.size() >= 2) {
            String v = lines.get(1).trim();
            votedFor = "-".equals(v) ? null : v;
        }
    }
}
