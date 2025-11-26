package dev.kvstore.sharding;

import dev.kvstore.controller.KVStoreController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

@Component
public class ShardingClient {

    private final RestTemplate restTemplate = new RestTemplate();

    private static final Logger log = LoggerFactory.getLogger(ShardingClient.class);

    private static final String BASE_PATH = "/kvstore";
    public byte[] get(final String nodeAddress, final byte[] key) {  // nodeAddress = "node4:9093"
        final String base64Key = Base64.getEncoder().encodeToString(key);
        final String url = "http://" + nodeAddress + BASE_PATH + "/get?key=" + base64Key;

        try {
            final ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<Map<String, Object>>() {}
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                final Map<String, Object> body = response.getBody();
                if (body != null && body.containsKey("value")) {
                    final String base64Value = (String) body.get("value");
                    return Base64.getDecoder().decode(base64Value);
                }
            }
        } catch (final Exception e) {
            System.err.println("Sharding GET failed to http://" + nodeAddress + ": " + e.getMessage());
        }
        return null;
    }

    public boolean put(final String nodeAddress, final byte[] key, final byte[] value) {
        final String url = "http://" + nodeAddress + BASE_PATH + "/put";

        final Map<String, String> requestBody = Map.of(
                "key", Base64.getEncoder().encodeToString(key),
                "value", Base64.getEncoder().encodeToString(value)
        );

        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        final HttpEntity<Map<String, String>> entity = new HttpEntity<>(requestBody, headers);

        try {
            final ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                    url, HttpMethod.POST, entity,
                    new ParameterizedTypeReference<Map<String, Object>>() {}
            );

            return response.getStatusCode() == HttpStatus.OK &&
                    (response.getBody() != null ? (Boolean) response.getBody().get("success") : false);
        } catch (final Exception e) {
            System.err.println("Sharding PUT failed to http://" + nodeAddress + ": " + e.getMessage());
            return false;
        }
    }
}
