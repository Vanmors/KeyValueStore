package dev.kvstore.sharding;

import dev.kvstore.config.GlobalClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Component
public class ShardingClient {

    private static final Logger log = LoggerFactory.getLogger(ShardingClient.class);
    private static final String BASE_PATH = "/kvstore";

    private static final ParameterizedTypeReference<Map<String, Object>> MAP_TYPE =
            new ParameterizedTypeReference<>() {
            };

    private final RestTemplate http;
    private final RestTemplate health;

    public ShardingClient() {
        this.http = build(2500, 5000);
        this.health = build(800, 1200);
    }

    private RestTemplate build(int connectTimeoutMs, int readTimeoutMs) {
        var f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(connectTimeoutMs);
        f.setReadTimeout(readTimeoutMs);
        return new RestTemplate(f);
    }

    private static String baseUrl(String nodeAddress) {
        if (nodeAddress.startsWith("http://") || nodeAddress.startsWith("https://")) {
            return nodeAddress;
        }
        return "http://" + nodeAddress;
    }

    public boolean isAlive(final String nodeAddress) {
        final String node = baseUrl(nodeAddress);
        final String[] probes = new String[]{
                node + "/health",
                node + BASE_PATH + "/health"
        };
        for (String url : probes) {
            try {
                ResponseEntity<String> r = health.exchange(url, HttpMethod.GET, null, String.class);
                if (r.getStatusCode().is2xxSuccessful()) {
                    return true;
                }
            } catch (Exception ignored) {
            }
        }
        log.debug("Node {} is DOWN", nodeAddress);
        return false;
    }

    public byte[] get(final String nodeAddress, final byte[] key) {
        final String node = baseUrl(nodeAddress);

        final String url = UriComponentsBuilder
                .fromHttpUrl(node)
                .path(BASE_PATH + "/get")
                .queryParam("key", new String(key, StandardCharsets.UTF_8))
                .build()
                .encode()
                .toUriString();

        try {
            ResponseEntity<Map<String, Object>> resp =
                    http.exchange(url, HttpMethod.GET, null, MAP_TYPE);

            if (resp.getStatusCode().is3xxRedirection() && resp.getHeaders().getLocation() != null) {
                URI loc = resp.getHeaders().getLocation();
                resp = http.exchange(loc, HttpMethod.GET, null, MAP_TYPE);
            }

            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                Object val = resp.getBody().get("value");
                if (val instanceof String s) {
                    return s.getBytes(StandardCharsets.UTF_8);
                }
            }
        } catch (Exception e) {
            log.warn("Remote GET failed on {}: {}", nodeAddress, e.toString());
        }
        return null;
    }

    public boolean put(final String nodeAddress, final byte[] key, final byte[] value) {
        final String url = baseUrl(nodeAddress) + BASE_PATH + "/put";

        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        final Map<String, String> body = Map.of(
                "key", new String(key, StandardCharsets.UTF_8),
                "value", new String(value, StandardCharsets.UTF_8)
        );

        try {
            ResponseEntity<Map<String, Object>> resp = postJsonWithRedirect(url, body, headers);
            return resp != null && resp.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            log.warn("Remote PUT failed on {}: {}", nodeAddress, e.toString());
            return false;
        }
    }

    public boolean delete(final String nodeAddress, final byte[] key) {
        final String url = baseUrl(nodeAddress) + BASE_PATH + "/delete";

        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        final Map<String, String> body = Map.of(
                "key", new String(key, StandardCharsets.UTF_8)
        );

        try {
            ResponseEntity<Map<String, Object>> resp = postJsonWithRedirect(url, body, headers);
            return resp != null && resp.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            log.warn("Remote DELETE failed on {}: {}", nodeAddress, e.toString());
            return false;
        }
    }

    public void replicateConfig(final GlobalClusterConfig config, final List<String> allNodes) {
        for (final String node: allNodes) {
            try {
                log.info("Send new config to node {}", node);
                final String url = baseUrl(node) + "/cluster" + "/applyConfig";

                log.info("url {}", url);

                http.postForObject(url, config, ResponseEntity.class);

                log.info("Config replicated to {}", node);
            } catch (final Exception ignored) {
            }
        }
    }

    /**
     * Выполнить POST JSON-запрос с обработкой одного уровня redirect (307/302 на лидера и т.п.).
     */
    private ResponseEntity<Map<String, Object>> postJsonWithRedirect(
            String url,
            Map<String, String> body,
            HttpHeaders headers
    ) {

        RequestEntity<Map<String, String>> req =
                new RequestEntity<>(body, headers, HttpMethod.POST, URI.create(url));

        ResponseEntity<Map<String, Object>> resp =
                http.exchange(req, MAP_TYPE);

        if (resp.getStatusCode().is3xxRedirection() && resp.getHeaders().getLocation() != null) {
            URI loc = resp.getHeaders().getLocation();
            RequestEntity<Map<String, String>> redirected =
                    new RequestEntity<>(body, headers, HttpMethod.POST, loc);
            resp = http.exchange(redirected, MAP_TYPE);
        }

        return resp;
    }
}
