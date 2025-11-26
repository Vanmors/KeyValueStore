package dev.kvstore.controller;

import dev.kvstore.controller.request.*;
import dev.kvstore.core.KVException;
import dev.kvstore.core.KeyValueStore;
import dev.kvstore.core.model.DeleteResult;
import dev.kvstore.core.model.GetResult;
import dev.kvstore.core.model.PutResult;
import dev.kvstore.core.model.WALEntry;
import dev.kvstore.raft.RaftRpc;
import dev.kvstore.raft.RaftService;
import dev.kvstore.sharding.ShardInfo;
import dev.kvstore.sharding.ShardingService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

@RestController
@RequestMapping("/kvstore")
public class KVStoreController {

    private static final Logger log = LoggerFactory.getLogger(KVStoreController.class);

    private final KeyValueStore keyValueStore;
    private final ShardingService sharding;
    private final String replicationMode;
    private final RaftService raft;

    private final RestTemplate http = new RestTemplate();

    public KVStoreController(
            KeyValueStore keyValueStore,
            ShardingService sharding,
            @Value("${kvstore.replicationMode}") final String replicationMode,
            RaftService raft
    ) {
        this.keyValueStore = keyValueStore;
        this.sharding = sharding;
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
    public ResponseEntity<Map<String, Object>> getReplicationMode(
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        var out = new LinkedHashMap<String, Object>();
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
    public ResponseEntity<Map<String, Object>> get(
            @RequestParam("key") String key,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final Instant t0 = Instant.now();
        try {
            if (key == null || key.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }
            log.info("[GET] key='{}' mode={} leaderId={} term={} commitIdx={} rid={}",
                    safeKey(key), replicationMode,
                    raft == null ? "null" : raft.leaderId(),
                    raft == null ? null : raft.term(),
                    raft == null ? null : raft.commitIndex(),
                    rid);

            GetResult result = keyValueStore.get(key.getBytes(UTF_8));
            return getMapResponseEntity(result);
        } catch (Exception e) {
            log.error("[GET] FAILED key='{}' mode={} rid={} err={}", safeKey(key), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[GET] key='{}' done in {} rid={}", safeKey(key), Duration.between(t0, Instant.now()), rid);
        }
    }

    @PostMapping("/get")
    public ResponseEntity<Map<String, Object>> getPost(
            @RequestBody final GetRequest request,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final Instant t0 = Instant.now();
        try {
            if (request == null || request.key() == null || request.key().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }
            log.info("[GET-POST] key='{}' mode={} leaderId={} term={} commitIdx={} rid={}",
                    safeKey(request.key()), replicationMode,
                    raft == null ? "null" : raft.leaderId(),
                    raft == null ? null : raft.term(),
                    raft == null ? null : raft.commitIndex(),
                    rid);

            GetResult gr = keyValueStore.get(request.key().getBytes(UTF_8));
            return getMapResponseEntity(gr);
        } catch (Exception e) {
            log.error("[GET-POST] FAILED body={} mode={} rid={} err={}", safeObj(request), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[GET-POST] key='{}' done in {} rid={}", safeKey(request == null ? null : request.key()), Duration.between(t0, Instant.now()), rid);
        }
    }

    private ResponseEntity<Map<String, Object>> getMapResponseEntity(GetResult gr) {
        if (gr == null || gr.value() == null || gr.value().value() == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "value not found"));
        }
        var vr = gr.value();
        var valBytes = vr.value();
        var out = new LinkedHashMap<String, Object>();
        out.put("value", new String(valBytes, UTF_8));
        out.put("valueB64", Base64.getEncoder().encodeToString(valBytes));
        out.put("expire", vr.expireAtMillis() != null ? vr.expireAtMillis() : null);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/put")
    public ResponseEntity<Map<String, Object>> put(
            @RequestBody final PutRequest request,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) throws KVException, IOException {
        final Instant t0 = Instant.now();
        try {
            if (request == null || request.key() == null || request.key().isBlank() || request.value() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "key and value are required"));
            }

            ResponseEntity<Map<String, Object>> rerouted = routeWriteIfNeeded(
                    request.key().getBytes(UTF_8),
                    "/kvstore/put",
                    request,
                    rid
            );
            if (rerouted != null) return rerouted;

            log.info("[PUT] key='{}' mode={} raftLeader?{} leaderId={} term={} commitIdx={} rid={}",
                    safeKey(request.key()), replicationMode,
                    raft != null && raft.isLeader(), raft == null ? "null" : raft.leaderId(),
                    raft == null ? null : raft.term(), raft == null ? null : raft.commitIndex(), rid);

            if ("RAFT".equalsIgnoreCase(replicationMode)) {
                if (raft.isLeader()) {
                    boolean ok = raft.clientPut(request.key().getBytes(UTF_8), request.value().getBytes(UTF_8));
                    if (!ok) {
                        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body(Map.of("error", "replication_failed"));
                    }
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

            final PutResult result = keyValueStore.put(
                    request.key().getBytes(UTF_8),
                    request.value().getBytes(UTF_8)
            );
            return ResponseEntity.ok(Map.of("success", true, "created", result.created()));
        } catch (Exception e) {
            log.error("[PUT] FAILED body={} mode={} rid={} err={}", safeObj(request), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[PUT] key='{}' done in {} rid={}", safeKey(request == null ? null : request.key()), Duration.between(t0, Instant.now()), rid);
        }
    }

    @PostMapping("/delete")
    public ResponseEntity<Map<String, Object>> delete(
            @RequestBody final DeleteRequest request,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) throws KVException, IOException {
        final Instant t0 = Instant.now();
        try {
            if (request == null || request.key() == null || request.key().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }

            ResponseEntity<Map<String, Object>> rerouted = routeWriteIfNeeded(
                    request.key().getBytes(UTF_8),
                    "/kvstore/delete",
                    request,
                    rid
            );
            if (rerouted != null) return rerouted;

            log.info("[DELETE] key='{}' mode={} leaderId={} term={} commitIdx={} rid={}",
                    safeKey(request.key()), replicationMode,
                    raft == null ? "null" : raft.leaderId(),
                    raft == null ? null : raft.term(),
                    raft == null ? null : raft.commitIndex(),
                    rid);

            if ("RAFT".equalsIgnoreCase(replicationMode)) {
                if (raft.isLeader()) {
                    boolean ok = raft.clientDelete(request.key().getBytes(UTF_8));
                    if (!ok) {
                        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                .body(Map.of("error", "replication_failed"));
                    }
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

            final DeleteResult result = keyValueStore.delete(request.key().getBytes(UTF_8));
            return ResponseEntity.ok(Map.of("success", result.deleted()));
        } catch (Exception e) {
            log.error("[DELETE] FAILED body={} mode={} rid={} err={}", safeObj(request), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[DELETE] key='{}' done in {} rid={}", safeKey(request == null ? null : request.key()), Duration.between(t0, Instant.now()), rid);
        }
    }

    @PostMapping("/mput")
    public ResponseEntity<Map<String, Object>> mput(
            @RequestBody final MultiPutRequest request,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final Instant t0 = Instant.now();
        try {
            if (request == null || request.items() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "items is required"));
            }

            if ("RAFT".equalsIgnoreCase(replicationMode)) {
                log.warn("[MPUT] replicationMode=RAFT: multi-put выполняется локально, без RAFT/шардирования");
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.items().size());
            int success = 0, failed = 0;

            for (var it : request.items()) {
                try {
                    var pr = keyValueStore.put(
                            it.key().getBytes(UTF_8),
                            it.value() == null ? null : it.value().getBytes(UTF_8)
                    );
                    results.add(Map.of("key", it.key(), "success", true, "created", pr.created()));
                    success++;
                } catch (Exception e) {
                    results.add(Map.of("key", it.key(), "success", false, "error", e.getMessage()));
                    failed++;
                }
            }
            return ResponseEntity.ok(Map.of("successCount", success, "failureCount", failed, "results", results));
        } catch (Exception e) {
            log.error("[MPUT] FAILED body={} mode={} rid={} err={}", safeObj(request), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[MPUT] done in {} rid={}", Duration.between(t0, Instant.now()), rid);
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
    public ResponseEntity<Map<String, Object>> mget(
            @RequestBody final MultiGetRequest request,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final Instant t0 = Instant.now();
        try {
            if (request == null || request.keys() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "keys is required"));
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.keys().size());
            for (var key : request.keys()) {
                try {
                    final GetResult gr = keyValueStore.get(key.getBytes(UTF_8));
                    if (gr == null || !gr.found() || gr.value() == null || gr.value().value() == null) {
                        results.add(Map.of("key", key, "found", false));
                    } else {
                        var vr = gr.value();
                        var row = new LinkedHashMap<String, Object>();
                        row.put("key", key);
                        row.put("found", true);
                        row.put("value", new String(vr.value(), UTF_8));
                        row.put("expire", vr.expireAtMillis());
                        results.add(row);
                    }
                } catch (Exception e) {
                    results.add(Map.of("key", key, "found", false, "error", e.getMessage()));
                }
            }
            return ResponseEntity.ok(Map.of("results", results));
        } catch (Exception e) {
            log.error("[MGET] FAILED body={} mode={} rid={} err={}", safeObj(request), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[MGET] done in {} rid={}", Duration.between(t0, Instant.now()), rid);
        }
    }

    @PostMapping("/mdelete")
    public ResponseEntity<Map<String, Object>> mdelete(
            @RequestBody final MultiDeleteRequest request,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) {
        final Instant t0 = Instant.now();
        try {
            if (request == null || request.keys() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "keys is required"));
            }

            if ("RAFT".equalsIgnoreCase(replicationMode)) {
                log.warn("[MDELETE] replicationMode=RAFT: multi-delete выполняется локально, без RAFT/шардирования");
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.keys().size());
            int success = 0, failed = 0;

            for (var key : request.keys()) {
                try {
                    final DeleteResult dr = keyValueStore.delete(key.getBytes(UTF_8));
                    boolean ok = dr.deleted();
                    results.add(Map.of("key", key, "success", ok));
                    if (ok) success++;
                    else failed++;
                } catch (Exception e) {
                    results.add(Map.of("key", key, "success", false, "error", e.getMessage()));
                    failed++;
                }
            }
            return ResponseEntity.ok(Map.of("successCount", success, "failureCount", failed, "results", results));
        } catch (Exception e) {
            log.error("[MDELETE] FAILED body={} mode={} rid={} err={}", safeObj(request), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[MDELETE] done in {} rid={}", Duration.between(t0, Instant.now()), rid);
        }
    }

    @PostMapping("/replicate")
    public ResponseEntity<Map<String, Object>> replicate(
            @RequestBody final WALEntry walEntry,
            @RequestHeader(value = "X-Request-Id", required = false) String rid
    ) throws KVException, IOException {
        final Instant t0 = Instant.now();
        try {
            keyValueStore.applyReplication(walEntry);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (final Exception e) {
            log.error("[REPL] FAILED entry={} mode={} rid={} err={}", safeObj(walEntry), replicationMode, rid, e.toString(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", errorMessage(e)));
        } finally {
            log.info("[REPL] done in {} rid={}", Duration.between(t0, Instant.now()), rid);
        }
    }

    private ResponseEntity<Map<String, Object>> routeWriteIfNeeded(byte[] key,
                                                                   String path,
                                                                   Object body,
                                                                   String rid) {
        if (sharding == null || key == null) {
            return null;
        }

        try {
            String destShard;
            try {
                destShard = sharding.getNode(key);
            } catch (KVException e) {
                log.warn("[ROUTE] can't resolve shard for key -> continue local rid={}", rid);
                return null;
            }

            String myShard = sharding.getMyShardId();
            if (Objects.equals(destShard, myShard)) {
                if (log.isDebugEnabled()) {
                    log.debug("[ROUTE] same shard (destShard={}), skip forwarding rid={}", destShard, rid);
                }
                return null;
            }

            ShardInfo shardInfo = sharding.getAliveMembers(destShard);
            if (shardInfo == null || shardInfo.members() == null || shardInfo.members().isEmpty()) {
                log.warn("[ROUTE] no alive members for shard='{}' -> continue local rid={}", destShard, rid);
                return null;
            }

            String targetNode = shardInfo.members().iterator().next();
            String url = buildUrl(targetNode, path);

            log.info("[ROUTE] forward {} -> {} (destShard={} myShard={}) rid={}",
                    path, url, destShard, myShard, rid);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            if (rid != null) {
                headers.set("X-Request-Id", rid);
            }

            HttpEntity<Object> entity = new HttpEntity<>(body, headers);
            ResponseEntity<Map> resp = http.postForEntity(url, entity, Map.class);

            if (resp.getStatusCode().is3xxRedirection() && resp.getHeaders().getLocation() != null) {
                URI loc = resp.getHeaders().getLocation();
                resp = http.postForEntity(loc, entity, Map.class);
            }

            @SuppressWarnings("unchecked")
            ResponseEntity<Map<String, Object>> cast =
                    (ResponseEntity<Map<String, Object>>) (ResponseEntity<?>) resp;
            return cast;

        } catch (Exception ex) {
            log.error("[ROUTE] forward failed err={} rid={}", ex.toString(), rid, ex);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "route_failed", "message", errorMessage(ex)));
        }
    }

    private static String buildUrl(String node, String path) {
        String base = (node.startsWith("http://") || node.startsWith("https://")) ? node : ("http://" + node);
        return UriComponentsBuilder.fromHttpUrl(base).path(path).toUriString();
    }

    private static String safeKey(String k) {
        if (k == null) return "null";
        return k.length() <= 64 ? k : (k.substring(0, 61) + "...");
    }

    private static String safeObj(Object o) {
        if (o == null) return "null";
        try {
            return String.valueOf(o);
        } catch (Exception e) {
            return o.getClass().getName();
        }
    }

    private static String errorMessage(Throwable e) {
        if (e == null) return "internal-error";
        String msg = e.getMessage();
        return (msg == null || msg.isBlank()) ? e.getClass().getSimpleName() : msg;
    }
}
