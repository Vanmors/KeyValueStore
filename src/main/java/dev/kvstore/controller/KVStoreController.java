package dev.kvstore.controller;

import dev.kvstore.controller.request.DeleteRequest;
import dev.kvstore.controller.request.MultiPutRequest;
import dev.kvstore.controller.request.MultiGetRequest;
import dev.kvstore.controller.request.PutRequest;
import dev.kvstore.core.KVException;
import dev.kvstore.core.KeyValueStore;
import dev.kvstore.core.model.DeleteResult;
import dev.kvstore.core.model.GetResult;
import dev.kvstore.core.model.PutResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;


@RestController
@RequestMapping("/kvstore")
public class KVStoreController {

    @Autowired
    private KeyValueStore keyValueStore;

    @GetMapping("/get")
    public ResponseEntity<Map<String, Object>> get(@RequestParam("key") String key) throws KVException, IOException {
        try {
            final GetResult result = keyValueStore.get(key.getBytes(StandardCharsets.UTF_8));
            return getMapResponseEntity(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/get")
    public ResponseEntity<Map<String, Object>> getPost(@RequestBody final dev.kvstore.controller.request.GetRequest request) {
        try {
            if (request == null || request.key() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "key is required"));
            }
            final var gr = keyValueStore.get(request.key().getBytes(StandardCharsets.UTF_8));
            return getMapResponseEntity(gr);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    private ResponseEntity<Map<String, Object>> getMapResponseEntity(GetResult gr) {
        if (!gr.found()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "value not found"));
        }
        final var vr = gr.value();
        return ResponseEntity.ok(Map.of(
                "value", vr.value() == null ? null : new String(vr.value(), StandardCharsets.UTF_8),
                "version", vr.version(),
                "expire", vr.expireAtMillis()
        ));
    }


    @PostMapping("/put")
    public ResponseEntity<Map<String, Object>> put(@RequestBody final PutRequest request) throws KVException, IOException {
        try {
            final PutResult result = keyValueStore.put(
                    request.key().getBytes(StandardCharsets.UTF_8),
                    request.value().getBytes(StandardCharsets.UTF_8)
            );
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "created", result.created()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/delete")
    public ResponseEntity<Map<String, Object>> delete(@RequestBody final DeleteRequest request) throws KVException, IOException {
        try {
            final DeleteResult result = keyValueStore.delete(request.key().getBytes(StandardCharsets.UTF_8));
            return ResponseEntity.ok(Map.of("success", result.deleted()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/mput")
    public ResponseEntity<Map<String, Object>> mput(@RequestBody final MultiPutRequest request) {
        try {
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
                    results.add(Map.of(
                            "key", it.key(),
                            "success", false,
                            "error", e.getMessage()
                    ));
                    failed++;
                }
            }

            return ResponseEntity.ok(Map.of(
                    "successCount", success,
                    "failureCount", failed,
                    "results", results
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/mget")
    public ResponseEntity<Map<String, Object>> mget(@RequestBody final MultiGetRequest request) {
        try {
            if (request == null || request.keys() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "keys is required"));
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.keys().size());

            for (var key : request.keys()) {
                try {
                    final GetResult gr = keyValueStore.get(key.getBytes(StandardCharsets.UTF_8));
                    if (!gr.found() || gr.value() == null || gr.value().value() == null) {
                        results.add(Map.of(
                                "key", key,
                                "found", false
                        ));
                    } else {
                        var vr = gr.value();
                        results.add(Map.of(
                                "key", key,
                                "found", true,
                                "value", new String(vr.value(), StandardCharsets.UTF_8),
                                "version", vr.version(),
                                "expire", vr.expireAtMillis()
                        ));
                    }
                } catch (Exception e) {
                    results.add(Map.of(
                            "key", key,
                            "found", false,
                            "error", e.getMessage()
                    ));
                }
            }

            return ResponseEntity.ok(Map.of("results", results));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/mdelete")
    public ResponseEntity<Map<String, Object>> mdelete(@RequestBody final dev.kvstore.controller.request.MultiDeleteRequest request) {
        try {
            if (request == null || request.keys() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "keys is required"));
            }

            var results = new java.util.ArrayList<Map<String, Object>>(request.keys().size());
            int success = 0, failed = 0;

            for (var key : request.keys()) {
                try {
                    final DeleteResult dr = keyValueStore.delete(key.getBytes(StandardCharsets.UTF_8));
                    boolean ok = dr.deleted();
                    results.add(Map.of(
                            "key", key,
                            "success", ok
                    ));
                    if (ok) success++;
                    else failed++;
                } catch (Exception e) {
                    results.add(Map.of(
                            "key", key,
                            "success", false,
                            "error", e.getMessage()
                    ));
                    failed++;
                }
            }

            return ResponseEntity.ok(Map.of(
                    "successCount", success,
                    "failureCount", failed,
                    "results", results
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

}
