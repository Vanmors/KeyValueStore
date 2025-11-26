package dev.kvstore.controller;

import dev.kvstore.controller.request.*;
import dev.kvstore.core.KVException;
import dev.kvstore.core.KeyValueStore;
import dev.kvstore.core.model.*;
import dev.kvstore.raft.RaftRpc;
import dev.kvstore.raft.RaftService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/kvstore")
public class KVStoreController {

    private static final Logger log = LoggerFactory.getLogger(KVStoreController.class);

    @Autowired
    private KeyValueStore keyValueStore;

    private final String replicationMode;
    private final RaftService raft;

    public KVStoreController(@Value("${kvstore.replicationMode}") final String replicationMode,
                             RaftService raft) {
        this.replicationMode = replicationMode;
        this.raft = raft;
    }

    @PostConstruct
    public void onStart() {
        log.info("[BOOT] KVStoreController init: replicationMode={}, keyValueStore.present={}, keyValueStore.impl={}",
                replicationMode,
                keyValueStore != null,
                keyValueStore == null ? "null" : keyValueStore.getClass().getName());
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }

    @GetMapping("mode")
    public ResponseEntity<Map<String, Object>> getReplicationMode() {
        log.debug("[MODE] GET /mode -> {}", replicationMode);
        var out = new java.util.LinkedHashMap<String, Object>();
        out.put("replicationMode", replicationMode);
        if (raft != null && raft.isEnabled()) {
            out.put("raftRole", raft.isLeader() ? "LEADER" : "FOLLOWER/CANDIDATE");
            out.put("raftTerm", raft.term());
            out.put("raftLeader", raft.leaderId());
            out.put("commitIndex", raft.commitIndex());
        }
        return ResponseEntity.ok(out);
    }

    @GetMapping("/get")
    public ResponseEntity<Map<String, Object>> get(@RequestParam("key") String key) {
        final Instant t0 = Instant.now();
        try {
            log.debug("[GET] key='{}'", key);
            if (key == null || key.isBlank()) {
                log.warn("[GET] invalid key: '{}'", key);
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }
            final byte[] kbytes = key.getBytes(StandardCharsets.UTF_8);
            log.trace("[GET] key.bytes.len={}", kbytes.length);

            final GetResult result = keyValueStore.get(kbytes);
            log.trace("[GET] result=null?{} found?{}", (result == null), result != null && result.found());

            return getMapResponseEntity(result);
        } catch (Exception e) {
            log.error("[GET] FAILED key='{}' mode={} err={}", key, replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[GET] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @PostMapping("/get")
    public ResponseEntity<Map<String, Object>> getPost(@RequestBody final GetRequest request) {
        final Instant t0 = Instant.now();
        try {
            log.debug("[GET-POST] body={}", safeObj(request));
            if (request == null || request.key() == null || request.key().isBlank()) {
                log.warn("[GET-POST] invalid request: {}", safeObj(request));
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }
            final byte[] kbytes = request.key().getBytes(StandardCharsets.UTF_8);
            final var gr = keyValueStore.get(kbytes);
            log.trace("[GET-POST] found?{} null?{}", gr != null && gr.found(), gr == null);
            return getMapResponseEntity(gr);
        } catch (Exception e) {
            log.error("[GET-POST] FAILED body={} mode={} err={}", safeObj(request), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[GET-POST] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    private ResponseEntity<Map<String, Object>> getMapResponseEntity(GetResult gr) {
        if (gr == null) {
            log.warn("[GET] GetResult is null -> 404");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "value not found"));
        }
        var vr = gr.value();
        if (vr == null || vr.value() == null) {
            log.warn("[GET] ValueRecord is null or value bytes null -> 404 (found={})", gr.found());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "value not found"));
        }
        log.debug("[GET] OK version={} expire={} value.len={}", vr.version(), vr.expireAtMillis(),
                vr.value().length);

        var out = new java.util.LinkedHashMap<String, Object>();
        out.put("value", new String(vr.value(), StandardCharsets.UTF_8));
        if (vr.expireAtMillis() != null) {
            out.put("expire", vr.expireAtMillis());
        } else {
            out.put("expire", null);
        }
        return ResponseEntity.ok(out);
    }

    @PostMapping("/put")
    public ResponseEntity<Map<String, Object>> put(@RequestBody final PutRequest request) throws KVException, IOException {
        final Instant t0 = Instant.now();
        try {
            log.debug("[PUT] body.key='{}' value.len={}", safeKey(request == null ? null : request.key()),
                    request == null || request.value() == null ? 0 : request.value().getBytes(StandardCharsets.UTF_8).length);

            if (request == null || request.key() == null || request.key().isBlank() || request.value() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "key and value are required"));
            }

            if ("RAFT".equalsIgnoreCase(replicationMode)) {
                if (raft.isLeader()) {
                    boolean ok = raft.clientPut(request.key().getBytes(StandardCharsets.UTF_8),
                            request.value().getBytes(StandardCharsets.UTF_8));
                    if (!ok) return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "replication_failed"));
                    return ResponseEntity.ok(Map.of("success", true, "created", true));
                } else {
                    var loc = raft.leaderRedirectUrl("/kvstore/put");
                    if (loc.isPresent()) {
                        return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                                .header("Location", loc.get())
                                .body(Map.of("error", "not_leader", "leader", raft.leaderId()));
                    }
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "no_leader"));
                }
            }
            // MASTER/SLAVE - старое поведение:
            final PutResult result = keyValueStore.put(
                    request.key().getBytes(StandardCharsets.UTF_8),
                    request.value().getBytes(StandardCharsets.UTF_8)
            );
            log.info("[PUT] OK key='{}' created={}", safeKey(request.key()), result.created());
            return ResponseEntity.ok(Map.of("success", true, "created", result.created()));
        } catch (Exception e) {
            log.error("[PUT] FAILED body={} mode={} err={}", safeObj(request), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[PUT] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @PostMapping("/delete")
    public ResponseEntity<Map<String, Object>> delete(@RequestBody final DeleteRequest request) throws KVException, IOException {
        final Instant t0 = Instant.now();
        try {
            log.debug("[DELETE] key='{}'", safeKey(request == null ? null : request.key()));
            if (request == null || request.key() == null || request.key().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }
            if ("RAFT".equalsIgnoreCase(replicationMode)) {
                if (raft.isLeader()) {
                    boolean ok = raft.clientDelete(request.key().getBytes(StandardCharsets.UTF_8));
                    if (!ok) return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "replication_failed"));
                    return ResponseEntity.ok(Map.of("success", true));
                } else {
                    var loc = raft.leaderRedirectUrl("/kvstore/delete");
                    if (loc.isPresent()) {
                        return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                                .header("Location", loc.get())
                                .body(Map.of("error", "not_leader", "leader", raft.leaderId()));
                    }
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("error", "no_leader"));
                }
            }
            final DeleteResult result = keyValueStore.delete(request.key().getBytes(StandardCharsets.UTF_8));
            log.info("[DELETE] key='{}' deleted={}", safeKey(request.key()), result.deleted());
            return ResponseEntity.ok(Map.of("success", result.deleted()));
        } catch (Exception e) {
            log.error("[DELETE] FAILED body={} mode={} err={}", safeObj(request), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[DELETE] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @PostMapping("/mput")
    public ResponseEntity<Map<String, Object>> mput(@RequestBody final MultiPutRequest request) {
        final Instant t0 = Instant.now();
        try {
            log.debug("[MPUT] items.count={}", request == null || request.items() == null ? 0 : request.items().size());
            if (request == null || request.items() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "items is required"));
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.items().size());
            int success = 0, failed = 0;

            for (var it : request.items()) {
                try {
                    var pr = keyValueStore.put(
                            it.key().getBytes(StandardCharsets.UTF_8),
                            it.value() == null ? null : it.value().getBytes(StandardCharsets.UTF_8)
                    );
                    results.add(Map.of(
                            "key", it.key(),
                            "success", true,
                            "created", pr.created()
                    ));
                    success++;
                } catch (Exception e) {
                    log.warn("[MPUT] item failed key='{}' err={}", safeKey(it.key()), e.toString(), e);
                    results.add(Map.of(
                            "key", it.key(),
                            "success", false,
                            "error", e.getMessage()
                    ));
                    failed++;
                }
            }

            log.info("[MPUT] done success={} failed={}", success, failed);
            return ResponseEntity.ok(Map.of(
                    "successCount", success,
                    "failureCount", failed,
                    "results", results
            ));
        } catch (Exception e) {
            log.error("[MPUT] FAILED body={} mode={} err={}", safeObj(request), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[MPUT] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @PostMapping("/raft/request-vote")
    public RaftRpc.RequestVoteResponse requestVote(@RequestBody RaftRpc.RequestVoteRequest body) {
        return raft.onRequestVote(body);
    }

    @PostMapping("/raft/append-entries")
    public RaftRpc.AppendEntriesResponse appendEntries(@RequestBody RaftRpc.AppendEntriesRequest body) {
        return raft.onAppendEntries(body);
    }

    @PostMapping("/mget")
    public ResponseEntity<Map<String, Object>> mget(@RequestBody final MultiGetRequest request) {
        final Instant t0 = Instant.now();
        try {
            log.debug("[MGET] keys.count={}", request == null || request.keys() == null ? 0 : request.keys().size());
            if (request == null || request.keys() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "keys is required"));
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.keys().size());

            for (var key : request.keys()) {
                try {
                    final GetResult gr = keyValueStore.get(key.getBytes(StandardCharsets.UTF_8));
                    if (gr == null || !gr.found() || gr.value() == null || gr.value().value() == null) {
                        results.add(Map.of("key", key, "found", false));
                    } else {
                        var vr = gr.value();
                        var row = new java.util.LinkedHashMap<String, Object>();
                        row.put("key", key);
                        row.put("found", true);
                        row.put("value", new String(vr.value(), StandardCharsets.UTF_8));
                        row.put("expire", vr.expireAtMillis());
                        results.add(row);
                    }
                } catch (Exception e) {
                    log.warn("[MGET] item failed key='{}' err={}", safeKey(key), e.toString(), e);
                    results.add(Map.of("key", key, "found", false, "error", e.getMessage()));
                }
            }

            return ResponseEntity.ok(Map.of("results", results));
        } catch (Exception e) {
            log.error("[MGET] FAILED body={} mode={} err={}", safeObj(request), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[MGET] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @PostMapping("/mdelete")
    public ResponseEntity<Map<String, Object>> mdelete(@RequestBody final MultiDeleteRequest request) {
        final Instant t0 = Instant.now();
        try {
            log.debug("[MDELETE] keys.count={}", request == null || request.keys() == null ? 0 : request.keys().size());
            if (request == null || request.keys() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "keys is required"));
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.keys().size());
            int success = 0, failed = 0;

            for (var key : request.keys()) {
                try {
                    final DeleteResult dr = keyValueStore.delete(key.getBytes(StandardCharsets.UTF_8));
                    boolean ok = dr.deleted();
                    results.add(Map.of("key", key, "success", ok));
                    if (ok) success++;
                    else failed++;
                } catch (Exception e) {
                    log.warn("[MDELETE] item failed key='{}' err={}", safeKey(key), e.toString(), e);
                    results.add(Map.of("key", key, "success", false, "error", e.getMessage()));
                    failed++;
                }
            }

            log.info("[MDELETE] done success={} failed={}", success, failed);
            return ResponseEntity.ok(Map.of(
                    "successCount", success,
                    "failureCount", failed,
                    "results", results
            ));
        } catch (Exception e) {
            log.error("[MDELETE] FAILED body={} mode={} err={}", safeObj(request), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[MDELETE] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @PostMapping("/replicate")
    public ResponseEntity<Map<String, Object>> replicate(@RequestBody final WALEntry walEntry) throws KVException, IOException {
        final Instant t0 = Instant.now();
        try {
            log.info("[REPL] incoming op={} key='{}' value.len={}",
                    safeEnum(walEntry == null ? null : walEntry.operationType()),
                    walEntry == null ? null : safeKey(bytesToString(walEntry.key())),
                    walEntry == null || walEntry.value() == null ? 0 : walEntry.value().length);

            keyValueStore.applyReplication(walEntry);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (final Exception e) {
            log.error("[REPL] FAILED entry={} mode={} err={}", safeObj(walEntry), replicationMode, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.debug("[REPL] done in {}", Duration.between(t0, Instant.now()));
        }
    }

    @GetMapping("/dump")
    public ResponseEntity<?> dump() {
        final Instant t0 = Instant.now();
        try (var cursor = keyValueStore.scan(null, ReadOptions.DEFAULT)) {
            var out = new java.util.ArrayList<java.util.Map<String, Object>>();
            int n = 0;
            while (cursor.hasNext()) {
                var e = cursor.next();
                var key = new String(e.getKey(), StandardCharsets.UTF_8);
                var vr = e.getValue();

                var row = new java.util.LinkedHashMap<String, Object>();
                row.put("key", key);
                row.put("value", (vr == null || vr.value() == null) ? null : new String(vr.value(), StandardCharsets.UTF_8));
                row.put("expire", vr == null ? null : vr.expireAtMillis());
                out.add(row);

                n++;
                if (n % 1000 == 0) {
                    log.debug("[DUMP] scanned {} items...", n);
                }
            }
            log.info("[DUMP] total={} in {}", n, Duration.between(t0, Instant.now()));
            return ResponseEntity.ok(out);
        } catch (Exception ex) {
            log.error("[DUMP] FAILED err={}", ex.toString(), ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", ex.getMessage()));
        }
    }

    private static String safeKey(String k) {
        if (k == null) return "null";
        return k.length() <= 64 ? k : (k.substring(0, 61) + "...");
    }

    private static String bytesToString(byte[] k) {
        if (k == null) return "null";
        try {
            final String s = new String(k, StandardCharsets.UTF_8);
            return safeKey(s);
        } catch (Exception ignore) {
            return "<bin:" + k.length + ">";
        }
    }

    private static String safeObj(Object o) {
        if (o == null) return "null";
        try {
            return String.valueOf(o);
        } catch (Exception e) {
            return o.getClass().getName();
        }
    }

    private static String safeEnum(Enum<?> e) {
        return e == null ? "null" : e.name();
    }

    private static String errorMessage(Throwable e) {
        if (e == null) return "internal-error";
        String msg = e.getMessage();
        return (msg == null || msg.isBlank()) ? e.getClass().getSimpleName() : msg;
    }
}
