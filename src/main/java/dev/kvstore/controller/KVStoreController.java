package dev.kvstore.controller;

import dev.kvstore.controller.request.GetRequest;
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
import java.util.HashMap;
import java.util.Map;


@RestController
@RequestMapping("/kvstore")
public class KVStoreController {

    @Autowired
    private KeyValueStore keyValueStore;

    @GetMapping("/get")
    public ResponseEntity<Map<String, Object>> get(@RequestBody final GetRequest request) throws KVException, IOException {
        try {
            final GetResult result = keyValueStore.get(request.key().getBytes(StandardCharsets.UTF_8));
            final Map<String, Object> response = new HashMap<>();
            if (result.value().value() == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "value not found"));
            }
            response.put("value", result.found() ? new String(result.value().value(), StandardCharsets.UTF_8) : null);
            response.put("version", result.value().version());
            response.put("expire", result.value().expireAtMillis());
            return ResponseEntity.ok(response);
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
            return ResponseEntity.ok(Map.of("success", result.created()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/delete")
    public ResponseEntity<Map<String, Object>> delete(@RequestBody final GetRequest request) throws KVException, IOException {
        try {
            final DeleteResult result = keyValueStore.delete(request.key().getBytes(StandardCharsets.UTF_8));
            return ResponseEntity.ok(Map.of("success", result.deleted()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

}
