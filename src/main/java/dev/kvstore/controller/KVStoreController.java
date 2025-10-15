package dev.kvstore.controller;

import dev.kvstore.controller.request.DeleteRequest;
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
            if (!result.found()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "value not found"));
            }
            final var vr = result.value();
            return ResponseEntity.ok(Map.of(
                    "value", vr.value() == null ? null : new String(vr.value(), StandardCharsets.UTF_8),
                    "version", vr.version(),
                    "expire", vr.expireAtMillis()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
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

}
