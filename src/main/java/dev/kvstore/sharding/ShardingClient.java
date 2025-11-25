package dev.kvstore.sharding;

import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Component
public class ShardingClient {

    private final RestTemplate restTemplate = new RestTemplate();

    public byte[] get(final String node, final byte[] key) {
        final String url = node + "/kvstore/get?key=" + java.util.Base64.getEncoder().encodeToString(key);
        final ResponseEntity<byte[]> response = restTemplate.getForEntity(url, byte[].class);
        return response.getStatusCode() == HttpStatus.OK ? response.getBody() : null;
    }

    public boolean put(final String node, final byte[] key, final byte[] value) {
        final String url = node + "/kvstore/put";
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        final Map<String, String> body = Map.of(
                "key", java.util.Base64.getEncoder().encodeToString(key),
                "value", java.util.Base64.getEncoder().encodeToString(value)
        );
        final HttpEntity<Map<String, String>> entity = new HttpEntity<>(body, headers);
        final ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
        return response.getStatusCode() == HttpStatus.OK;
    }
}
